import type { EventMetadata } from './types.js';

type LogMeta = Record<string, unknown>;

function formatTimestamp(): string {
  return new Date().toISOString();
}

function buildBaseMeta(metadata?: EventMetadata): LogMeta {
  if (!metadata) return {};
  return {
    eventId: metadata.eventId,
    eventType: metadata.eventType,
    correlationId: metadata.correlationId,
    traceId: metadata.traceId,
    spanId: metadata.spanId,
    aggregateId: metadata.aggregateId,
    aggregateType: metadata.aggregateType,
  };
}

function serialize(meta: LogMeta): string {
  return Object.entries(meta)
    .filter(([, v]) => v !== undefined && v !== null && v !== '')
    .map(([k, v]) => `${k}=${typeof v === 'object' ? JSON.stringify(v) : v}`)
    .join(' ');
}

export const logger = {
  info(message: string, extra: LogMeta = {}, metadata?: EventMetadata) {
    const meta = { ...buildBaseMeta(metadata), ...extra };
    console.log(`${formatTimestamp()} INFO  ${message} ${serialize(meta)}`.trimEnd());
  },

  warn(message: string, extra: LogMeta = {}, metadata?: EventMetadata) {
    const meta = { ...buildBaseMeta(metadata), ...extra };
    console.warn(`${formatTimestamp()} WARN  ${message} ${serialize(meta)}`.trimEnd());
  },

  error(message: string, extra: LogMeta = {}, metadata?: EventMetadata) {
    const meta = { ...buildBaseMeta(metadata), ...extra };
    console.error(`${formatTimestamp()} ERROR ${message} ${serialize(meta)}`.trimEnd());
  },
};
