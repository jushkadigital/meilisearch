import type { ConsumeMessage } from 'amqplib';
import type { EventEnvelope, EventMetadata } from './types.js';
import { EventEnvelopeSchema, MedusaEnvelopeSchema, normalizeMedusaEnvelope, getPayloadSchema } from './schemas.js';
import { logger } from './logger.js';
import { isDuplicate, markProcessed, startIdempotencyCleanup, stopIdempotencyCleanup } from './idempotency.js';
import { connectRabbitMQ, setupQueueTopology, handleProcessingFailure, gracefulShutdown, retryConfig } from './services/rabbitmq.js';
import { configureMeili } from './services/meilisearch.js';
import { dispatchEvent } from './handlers/registry.js';

type ProcessingChannel = Pick<
  import('amqplib').Channel,
  'ack' | 'sendToQueue' | 'publish'
>;

// ── Message Processing Pipeline ────────────────────────────────────────
//
// 1. Parse raw message → JSON
// 2. Validate EventEnvelope (spec, metadata)
// 3. Idempotency check (deduplicate by eventId)
// 4. Validate payload by eventType (Zod schema)
// 5. Dispatch to handler
// 6. On failure → retry with exponential backoff → DLQ
//

class PoisonMessageError extends Error {
  constructor(
    message: string,
    public readonly detail: unknown,
  ) {
    super(message);
    this.name = 'PoisonMessageError';
  }
}

class EnvelopeValidationError extends Error {
  constructor(
    message: string,
    public readonly detail: unknown,
  ) {
    super(message);
    this.name = 'EnvelopeValidationError';
  }
}

function parseMessageContent(raw: Buffer): unknown {
  const text = raw.toString();
  try {
    return JSON.parse(text);
  } catch {
    throw new PoisonMessageError('Invalid JSON — cannot parse message', text);
  }
}

function validateEnvelope(raw: unknown): EventEnvelope {
  // Try our format first
  const result = EventEnvelopeSchema.safeParse(raw);
  if (result.success) {
    return result.data;
  }

  // Try Medusa format and normalize
  const medusaResult = MedusaEnvelopeSchema.safeParse(raw);
  if (medusaResult.success) {
    return normalizeMedusaEnvelope(medusaResult.data);
  }

  throw new EnvelopeValidationError(
    'Event envelope validation failed',
    result.error,
  );
}

function validatePayload(payload: unknown, eventType: string): unknown {
  const schema = getPayloadSchema(eventType);
  if (!schema) {
    // No schema registered for this eventType — pass through as-is
    // (e.g., product.* events don't have strict payload schemas)
    return payload;
  }

  const result = schema.safeParse(payload);
  if (!result.success) {
    throw new EnvelopeValidationError(
      `Payload validation failed for eventType="${eventType}"`,
      result.error,
    );
  }
  return result.data;
}

export async function processMessage(
  msg: ConsumeMessage | null,
  channel: ProcessingChannel,
): Promise<void> {
  if (!msg) return;

  const routingKey = msg.fields.routingKey;
  let metadata: EventMetadata | undefined;

  try {
    // 1. Parse JSON
    const raw = parseMessageContent(msg.content);

    // 2. Validate envelope
    const envelope = validateEnvelope(raw);
    metadata = envelope.metadata;

    logger.info('Event received', { routingKey, eventType: metadata.eventType }, metadata);

    // 3. Idempotency check
    if (isDuplicate(metadata.eventId)) {
      logger.info('Duplicate event skipped', { eventId: metadata.eventId }, metadata);
      channel.ack(msg);
      return;
    }

    // 4. Validate payload
    const validatedPayload = validatePayload(envelope.payload, metadata.eventType);

    // 5. Dispatch to handler
    await dispatchEvent(validatedPayload, metadata);

    // 6. Mark as processed
    markProcessed(metadata.eventId);
    channel.ack(msg);

    logger.info('Event processed successfully', { eventType: metadata.eventType }, metadata);
  } catch (error) {
    if (error instanceof PoisonMessageError) {
      // Malformed JSON → send directly to DLQ (retries won't fix it)
      logger.error('Poison message — sending to DLQ', {
        routingKey,
        error: error.message,
      });

      channel.publish(
        `${process.env.RABBITMQ_QUEUE_NAME || 'indexer'}.dlx`,
        routingKey,
        msg.content,
        {
          persistent: true,
          headers: {
            'x-poison-message': true,
            'x-reject-reason': error.message,
            'x-rejected-at': new Date().toISOString(),
          },
        },
      );
      channel.ack(msg);
      return;
    }

    if (error instanceof EnvelopeValidationError) {
      // Invalid envelope/payload → send directly to DLQ (retries won't fix schema errors)
      logger.error('Envelope validation failed — sending to DLQ', {
        routingKey,
        error: error.message,
      });

      channel.publish(
        `${process.env.RABBITMQ_QUEUE_NAME || 'indexer'}.dlx`,
        routingKey,
        msg.content,
        {
          persistent: true,
          headers: {
            'x-validation-error': true,
            'x-reject-reason': error.message,
            'x-rejected-at': new Date().toISOString(),
          },
        },
      );
      channel.ack(msg);
      return;
    }

    // Transient error → retry with backoff → DLQ after max retries
    logger.error('Event processing failed', {
      routingKey,
      error: error instanceof Error ? error.message : String(error),
    }, metadata);

    await handleProcessingFailure(msg, channel, error, metadata);
  }
}

// ── Application Startup ─────────────────────────────────────────────────
async function start() {
  try {
    const { conn, channel } = await connectRabbitMQ();

    await configureMeili();
    const queueName = await setupQueueTopology(channel);

    startIdempotencyCleanup();

    logger.info('Indexer worker started', {
      mainQueue: queueName,
      dlq: `${process.env.RABBITMQ_QUEUE_NAME || 'indexer'}.dlq`,
      maxRetries: retryConfig.maxRetries,
      retryDelays: retryConfig.delays,
    });

    channel.prefetch(10);
    channel.consume(queueName, (msg) => {
      void processMessage(msg, channel);
    });

    // Graceful shutdown on SIGTERM / SIGINT
    const shutdown = async (signal: string) => {
      logger.info(`Received ${signal}, shutting down...`);
      stopIdempotencyCleanup();
      await gracefulShutdown(conn, channel);
      process.exit(0);
    };

    process.on('SIGTERM', () => void shutdown('SIGTERM'));
    process.on('SIGINT', () => void shutdown('SIGINT'));
  } catch (error) {
    logger.error('Fatal error during startup', {
      error: error instanceof Error ? error.message : String(error),
    });
    process.exit(1);
  }
}

if (import.meta.main) {
  void start();
}
