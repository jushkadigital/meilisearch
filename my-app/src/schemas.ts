import { z } from 'zod';
import { ENVELOPE_SPEC } from './types.js';
import type { EventMetadata } from './types.js';

// ── Shared Sub-schemas ─────────────────────────────────────────────────
export const MeiliThumbnailSchema = z.object({
  large: z.url().nullable().optional(),
  medium: z.url().nullable().optional(),
  small: z.url().nullable().optional(),
  og: z.url().nullable().optional(),
  square: z.url().nullable().optional(),
});

export const TourCategoryRefSchema = z.object({
  name: z.string(),
});

export const DestinationRefSchema = z.object({
  name: z.string(),
});

export const MoneySchema = z.object({
  amount: z.number(),
  currency: z.enum(['USD', 'EUR', 'PEN']),
});

// ── Tour Data Schema ───────────────────────────────────────────────────
export const TourDataSchema = z.object({
  destination: z.string(),
  description: z.record(z.string(), z.unknown()),
  duration_days: z.number().int().min(1),
  thumbnail: z.url(),
  completeThumbnail: MeiliThumbnailSchema.nullable().optional(),
  price: z.number(),
  categories: z.array(TourCategoryRefSchema),
  destinos: DestinationRefSchema,
  difficulty: z.string(),
});

export const TourPublishedV1PayloadSchema = z.object({
  id: z.number().int(),
  slug: z.string(),
  data: TourDataSchema,
});

export const TourUpdatedV1PayloadSchema = TourPublishedV1PayloadSchema;

export const TourDeletedV1PayloadSchema = z.object({
  id: z.string(),
  slug: z.string(),
  deletedAt: z.string().datetime(),
});

// ── Package / Paquete Data Schema ──────────────────────────────────────
export const PackageDataSchema = z.object({
  destination: z.string(),
  description: z.record(z.string(), z.unknown()),
  duration_days: z.number().int().min(1),
  thumbnail: z.url(),
  completeThumbnail: MeiliThumbnailSchema.nullable().optional(),
  price: z.number(),
  destinos: z.array(DestinationRefSchema),
  difficulty: z.string(),
});

export const PackagePublishedV1PayloadSchema = z.object({
  id: z.number().int(),
  slug: z.string(),
  data: PackageDataSchema,
});

export const PackageUpdatedV1PayloadSchema = PackagePublishedV1PayloadSchema;

export const PackageDeletedV1PayloadSchema = z.object({
  id: z.string(),
  slug: z.string(),
  deletedAt: z.string().datetime(),
});

// ── Product (Medusa) Schema ───────────────────────────────────────────
export const ProductEventPayloadSchema = z.object({
  data: z.object({
    id: z.string(),
    external_id: z.string().optional(),
    variants: z
      .array(
        z.object({
          prices: z
            .array(z.object({ currency_code: z.string(), amount: z.number() }))
            .optional(),
          price_set: z
            .object({
              prices: z.array(
                z.object({ currency_code: z.string(), amount: z.number() }),
              ),
            })
            .optional(),
        }),
      )
      .optional(),
  }),
});

// ── Event Metadata Schema ──────────────────────────────────────────────
export const EventMetadataSchema = z.object({
  eventId: z.string().uuid(),
  eventType: z.string(),
  eventVersion: z.number().int().min(1),
  aggregateId: z.string(),
  aggregateType: z.enum(['tour', 'package', 'user', 'media', 'page', 'product']),
  correlationId: z.string().uuid(),
  causationId: z.string().nullable(),
  traceId: z.string(),
  spanId: z.string(),
  producer: z.enum(['catalog', 'identity', 'user-service', 'capacity-service']),
  occurredAt: z.string().datetime(),
  actorId: z.string().nullable().optional(),
  tenantId: z.string().nullable().optional(),
});

// ── Event Envelope Schema ──────────────────────────────────────────────
function parseTraceParent(traceparent: string): { traceId: string; spanId: string } {
  const parts = traceparent.split('-');
  return {
    traceId: parts[1] ?? '',
    spanId: parts[2] ?? '',
  };
}

export const EventEnvelopeSchema = z.preprocess(
  (raw) => {
    const obj = raw as Record<string, unknown>;
    // Medusa format: has 'id' and 'action' at root level
    if (typeof obj.id === 'string' && typeof obj.action === 'string') {
      const metadata = (obj.metadata as Record<string, unknown>) ?? {};
      const traceContext = (metadata.traceContext as Record<string, unknown>) ?? {};
      const traceparent = typeof traceContext.traceparent === 'string' ? traceContext.traceparent : '';
      const { traceId, spanId } = traceparent ? parseTraceParent(traceparent) : { traceId: '', spanId: '' };

      return {
        spec: obj.spec,
        metadata: {
          eventId: obj.id,
          eventType: obj.action,
          eventVersion: obj.version ?? 1,
          aggregateId: obj.id,
          aggregateType: obj.aggregateType,
          correlationId: metadata.correlationId ?? obj.id,
          causationId: metadata.causationId ?? null,
          traceId,
          spanId,
          producer: obj.source ?? 'catalog',
          occurredAt: obj.timestamp ?? new Date().toISOString(),
        },
        payload: obj.payload,
      };
    }
    // Our format: has 'metadata' with 'eventId'
    return raw;
  },
  z.object({
    spec: z.literal(ENVELOPE_SPEC),
    metadata: EventMetadataSchema,
    payload: z.unknown(),
  }),
);

// ── Payload resolver by eventType ───────────────────────────────────────
const PAYLOAD_SCHEMAS: Record<string, z.ZodType> = {
  'tour.published': TourPublishedV1PayloadSchema,
  'tour.updated': TourUpdatedV1PayloadSchema,
  'tour.deleted': TourDeletedV1PayloadSchema,
  'package.published': PackagePublishedV1PayloadSchema,
  'package.updated': PackageUpdatedV1PayloadSchema,
  'package.deleted': PackageDeletedV1PayloadSchema,
};

export function getPayloadSchema(eventType: string): z.ZodType | undefined {
  return PAYLOAD_SCHEMAS[eventType];
}
