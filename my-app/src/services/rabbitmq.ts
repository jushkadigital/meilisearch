import amqp from 'amqplib';
import type {
  Channel,
  ChannelModel,
  ConsumeMessage,
  Options,
} from 'amqplib';
import { logger } from '../logger.js';
import type { EventMetadata, RetryConfig } from '../types.js';

// ── Configuration ──────────────────────────────────────────────────────
const RABBIT_URL = process.env.RABBITMQ_URL || 'amqp://rabbitmq:5672';
const EXCHANGE_NAME = process.env.RABBITMQ_EXCHANGE || 'tourism.integracion';
const INDEXER_QUEUE_NAME = process.env.RABBITMQ_QUEUE_NAME || 'indexer';
const DLQ_QUEUE_NAME = `${INDEXER_QUEUE_NAME}.dlq`;
const DEAD_LETTER_EXCHANGE_NAME = `${INDEXER_QUEUE_NAME}.dlx`;
const RETRY_EXCHANGE_NAME = `${INDEXER_QUEUE_NAME}.retry`;

const ROUTING_PATTERNS = ['integration.tour.*', 'integration.package.*', 'integration.product.*'] as const;

const RETRY_DELAYS = [5_000, 30_000, 120_000]; // 5s, 30s, 2min

function parseMaxRetries(value: string | undefined): number {
  const parsed = Number.parseInt(value ?? '3', 10);
  if (Number.isNaN(parsed) || parsed < 1) return 3;
  return Math.min(parsed, RETRY_DELAYS.length);
}

export const MAX_RETRIES = parseMaxRetries(process.env.RABBITMQ_MAX_RETRIES);

export const retryConfig: RetryConfig = {
  maxRetries: MAX_RETRIES,
  delays: RETRY_DELAYS.slice(0, MAX_RETRIES),
};

// ── Helpers ───────────────────────────────────────────────────────────
const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function normalizeError(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

function getRetryCount(msg: ConsumeMessage): number {
  const retryHeader = msg.properties.headers?.['x-retry-count'];
  if (typeof retryHeader === 'number') return retryHeader;
  if (typeof retryHeader === 'string') {
    const parsed = Number.parseInt(retryHeader, 10);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return 0;
}

function buildRetryHeaders(
  msg: ConsumeMessage,
  retryCount: number,
  error: unknown,
): Record<string, unknown> {
  return {
    ...(msg.properties.headers ?? {}),
    'x-retry-count': retryCount,
    'x-original-routing-key': (msg.properties.headers?.['x-original-routing-key'] as string) ?? msg.fields.routingKey,
    'x-last-error': normalizeError(error),
    'x-first-failed-at':
      msg.properties.headers?.['x-first-failed-at'] ?? new Date().toISOString(),
  };
}

// ── Retry with Exponential Backoff via Delay Queues ─────────────────────
//
// Pattern:
//   Main Queue → Consumer → Failure → Retry Queue N (TTL = delay[N])
//   Retry Queue N → TTL expires → default exchange → Main Queue (via x-dead-letter-routing-key)
//   After MAX_RETRIES → DLQ Exchange → DLQ Queue
//
function getRetryQueueName(retryLevel: number): string {
  return `${INDEXER_QUEUE_NAME}.retry.${retryLevel}`;
}

function getRetryDelay(retryLevel: number): number {
  // retryLevel is 1-indexed (1 = first retry, 2 = second, etc.)
  const idx = retryLevel - 1;
  if (idx < RETRY_DELAYS.length) return RETRY_DELAYS[idx]!;
  return RETRY_DELAYS[RETRY_DELAYS.length - 1]!;
}

export async function handleProcessingFailure(
  msg: ConsumeMessage,
  channel: Pick<Channel, 'ack' | 'publish' | 'sendToQueue'>,
  error: unknown,
  metadata?: EventMetadata,
): Promise<void> {
  const currentRetryCount = getRetryCount(msg);

  if (currentRetryCount < MAX_RETRIES) {
    const nextRetryCount = currentRetryCount + 1;
    const retryLevel = nextRetryCount;
    const retryDelay = getRetryDelay(retryLevel);
    const retryQueueName = getRetryQueueName(retryLevel);
    const headers = buildRetryHeaders(msg, nextRetryCount, error);

    // The retry queue's DLX (default exchange) + x-dead-letter-routing-key
    // will route it back to the main queue when TTL expires
    channel.sendToQueue(retryQueueName, msg.content, {
      persistent: true,
      contentType: msg.properties.contentType,
      contentEncoding: msg.properties.contentEncoding,
      correlationId: msg.properties.correlationId,
      messageId: msg.properties.messageId,
      type: msg.properties.type,
      appId: msg.properties.appId,
      headers,
    });

    channel.ack(msg);

    logger.warn('Retry scheduled with backoff', {
      retryCount: nextRetryCount,
      maxRetries: MAX_RETRIES,
      delayMs: retryDelay,
      retryQueue: retryQueueName,
      routingKey: msg.fields.routingKey,
    }, metadata);

    return;
  }

  // Max retries reached → send to DLQ
  const headers = buildRetryHeaders(msg, currentRetryCount, error);

  channel.publish(
    DEAD_LETTER_EXCHANGE_NAME,
    msg.fields.routingKey,
    msg.content,
    {
      persistent: true,
      contentType: msg.properties.contentType,
      contentEncoding: msg.properties.contentEncoding,
      correlationId: msg.properties.correlationId,
      messageId: msg.properties.messageId,
      type: msg.properties.type,
      appId: msg.properties.appId,
      timestamp: Date.now(),
      headers: {
        ...headers,
        'x-final-failure-at': new Date().toISOString(),
      },
    },
  );

  channel.ack(msg);

  logger.error('Max retries reached, message sent to DLQ', {
    maxRetries: MAX_RETRIES,
    dlqQueue: DLQ_QUEUE_NAME,
    routingKey: msg.fields.routingKey,
    lastError: normalizeError(error),
  }, metadata);
}

// ── Connection ─────────────────────────────────────────────────────────
export interface RabbitConnection {
  conn: ChannelModel;
  channel: Channel;
}

const CONNECTION_RETRIES = 10;
const CONNECTION_RETRY_DELAY_MS = 5_000;

export async function connectRabbitMQ(): Promise<RabbitConnection> {
  for (let attempt = 1; attempt <= CONNECTION_RETRIES; attempt++) {
    try {
      logger.info('Connecting to RabbitMQ', { attempt, url: RABBIT_URL });
      const conn = await amqp.connect(RABBIT_URL);
      const channel = await conn.createChannel();

      conn.on('error', (err) => {
        logger.error('RabbitMQ connection error', { error: normalizeError(err) });
      });

      conn.on('close', () => {
        logger.error('RabbitMQ connection closed, exiting');
        process.exit(1);
      });

      channel.on('error', (err) => {
        logger.error('RabbitMQ channel error', { error: normalizeError(err) });
      });

      logger.info('Connected to RabbitMQ');
      return { conn, channel };
    } catch (err) {
      logger.error('RabbitMQ connection failed, retrying', {
        attempt,
        remaining: CONNECTION_RETRIES - attempt,
        error: normalizeError(err),
      });
      if (attempt < CONNECTION_RETRIES) {
        await delay(CONNECTION_RETRY_DELAY_MS);
      }
    }
  }

  throw new Error(`Failed to connect to RabbitMQ after ${CONNECTION_RETRIES} attempts`);
}

// ── Topology Setup ─────────────────────────────────────────────────────
export async function setupQueueTopology(channel: Channel): Promise<string> {
  // Main exchange (topic)
  await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });

  // DLQ exchange (topic) — receives messages after max retries
  await channel.assertExchange(DEAD_LETTER_EXCHANGE_NAME, 'topic', { durable: true });

  // DLQ queue — binds to DLQ exchange with wildcard
  await channel.assertQueue(DLQ_QUEUE_NAME, { durable: true });
  await channel.bindQueue(DLQ_QUEUE_NAME, DEAD_LETTER_EXCHANGE_NAME, '#');

  // Retry queues — one per retry level, DLX routes back to main queue via default exchange
  for (let level = 1; level <= MAX_RETRIES; level++) {
    const retryQueueName = getRetryQueueName(level);
    const ttl = getRetryDelay(level);

    await channel.assertQueue(retryQueueName, {
      durable: true,
      deadLetterExchange: '',  // default exchange routes directly by queue name
      arguments: {
        'x-message-ttl': ttl,
        'x-dead-letter-routing-key': INDEXER_QUEUE_NAME,  // expired retry → main queue
      },
    });

    logger.info('Retry queue configured', {
      queue: retryQueueName,
      ttlMs: ttl,
      dlx: '(default exchange)',
      dlqRoutingKey: INDEXER_QUEUE_NAME,
    });
  }

  // Main queue — with DLX for broker-level rejections
  const queue = await channel.assertQueue(INDEXER_QUEUE_NAME, {
    durable: true,
    deadLetterExchange: DEAD_LETTER_EXCHANGE_NAME,
  });

  // Bind main queue to main exchange with routing patterns
  for (const pattern of ROUTING_PATTERNS) {
    await channel.bindQueue(queue.queue, EXCHANGE_NAME, pattern);
  }

  logger.info('Queue topology configured', {
    mainQueue: queue.queue,
    exchange: EXCHANGE_NAME,
    dlq: DLQ_QUEUE_NAME,
    patterns: [...ROUTING_PATTERNS],
    maxRetries: MAX_RETRIES,
    retryDelays: RETRY_DELAYS.slice(0, MAX_RETRIES),
  });

  return queue.queue;
}

// ── Graceful Shutdown ──────────────────────────────────────────────────
export async function gracefulShutdown(conn: ChannelModel, channel: Channel): Promise<void> {
  logger.info('Shutting down gracefully...');

  try {
    await channel.close();
    await conn.close();
    logger.info('RabbitMQ connection closed cleanly');
  } catch (err) {
    logger.error('Error during graceful shutdown', { error: normalizeError(err) });
  }
}

// ── Exports ────────────────────────────────────────────────────────────
export {
  DEAD_LETTER_EXCHANGE_NAME,
  DLQ_QUEUE_NAME,
  EXCHANGE_NAME,
  INDEXER_QUEUE_NAME,
  ROUTING_PATTERNS,
};
