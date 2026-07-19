// ── Event Envelope ─────────────────────────────────────────────────────
export const ENVELOPE_SPEC = 'tourism-events/v1' as const;

export type AggregateType = 'tour' | 'package' | 'user' | 'media' | 'page' | 'product';
export type Producer = 'catalog' | 'identity' | 'user-service' | 'capacity-service';

export interface EventMetadata {
  eventId: string;
  eventType: string;
  eventVersion: number;
  aggregateId: string;
  aggregateType: AggregateType;
  correlationId: string;
  causationId: string | null;
  traceId: string;
  spanId: string;
  producer: Producer;
  occurredAt: string;
  actorId?: string | null;
  tenantId?: string | null;
}

export interface EventEnvelope<T = unknown> {
  spec: typeof ENVELOPE_SPEC;
  metadata: EventMetadata;
  payload: T;
}

// ── Shared Sub-schemas ─────────────────────────────────────────────────
export interface Money {
  amount: number;
  currency: 'USD' | 'EUR' | 'PEN';
}

export interface MeiliThumbnail {
  large?: string | null;
  medium?: string | null;
  small?: string | null;
  og?: string | null;
  square?: string | null;
}

export interface TourCategoryRef {
  name: string;
}

export interface DestinationRef {
  name: string;
}

// ── Tour Payloads ──────────────────────────────────────────────────────
export interface TourData {
  destination: string;
  description: Record<string, unknown>;
  duration_days: number;
  thumbnail: string;
  completeThumbnail?: MeiliThumbnail | null;
  price: number;
  categories: TourCategoryRef[];
  destinos: DestinationRef;
  difficulty: string;
}

export interface TourPublishedV1Payload {
  id: number;
  slug: string;
  data: TourData;
}

export type TourUpdatedV1Payload = TourPublishedV1Payload;

export interface TourDeletedV1Payload {
  id: string;
  slug: string;
  deletedAt: string;
}

// ── Package / Paquete Payloads ─────────────────────────────────────────
export interface PackageData {
  destination: string;
  description: Record<string, unknown>;
  duration_days: number;
  thumbnail: string;
  completeThumbnail?: MeiliThumbnail | null;
  price: number;
  destinos: DestinationRef[];
  difficulty: string;
}

export interface PackagePublishedV1Payload {
  id: number;
  slug: string;
  data: PackageData;
}

export type PackageUpdatedV1Payload = PackagePublishedV1Payload;

export interface PackageDeletedV1Payload {
  id: string;
  slug: string;
  deletedAt: string;
}

// ── Product (Medusa) Payload ───────────────────────────────────────────
export interface ProductData {
  id: string;
  external_id?: string;
  variants?: Array<{
    title?: string;
    prices?: Array<{ currency_code: string; amount: number }>;
    price_set?: { prices: Array<{ currency_code: string; amount: number }> };
  }>;
}

export interface ProductEventPayload {
  data: ProductData;
}

// ── MeiliSearch Document Types ────────────────────────────────────────
export interface TourMeiliDocument {
  id: string;
  title: string;
  slug: string;
  image: string;
  completeImage?: MeiliThumbnail | null;
  description: Record<string, unknown>;
  categories: string[];
  destination: string;
  difficulty: string;
  type: 'tour';
  price?: number;
  duration_days?: number;
  _updated_at_payload: string;
}

export interface PackageMeiliDocument {
  id: string;
  title: string;
  slug: string;
  image: string;
  completeImage?: MeiliThumbnail | null;
  description: Record<string, unknown>;
  destination: string[];
  difficulty: string;
  type: 'package';
  price?: number;
  duration_days?: number;
  _updated_at_payload: string;
}

export interface ProductMeiliDocument {
  id: string;
  price: number;
  currency: string;
  medusa_id: string;
  _updated_at_medusa: string;
}

// ── Handler Types ──────────────────────────────────────────────────────
export type EventHandler<T = unknown> = (
  payload: T,
  metadata: EventMetadata,
) => Promise<void>;

export interface HandlerEntry<T = unknown> {
  eventType: string;
  aggregateType: AggregateType | '*';
  handler: EventHandler<T>;
}

// ── Retry Configuration ────────────────────────────────────────────────
export interface RetryConfig {
  maxRetries: number;
  delays: number[];
}
