import { logger } from './logger.js';

interface TrackedEvent {
  eventId: string;
  processedAt: number;
}

const IDEMPOTENCY_TTL_MS = 5 * 60 * 1000; // 5 minutes
const CLEANUP_INTERVAL_MS = 60 * 1000; // 1 minute

const processedEvents = new Map<string, TrackedEvent>();
let cleanupTimer: ReturnType<typeof setInterval> | null = null;

export function startIdempotencyCleanup() {
  if (cleanupTimer) return;
  cleanupTimer = setInterval(cleanup, CLEANUP_INTERVAL_MS);
}

export function stopIdempotencyCleanup() {
  if (cleanupTimer) {
    clearInterval(cleanupTimer);
    cleanupTimer = null;
  }
}

function cleanup() {
  const now = Date.now();
  let removed = 0;
  for (const [key, entry] of processedEvents) {
    if (now - entry.processedAt > IDEMPOTENCY_TTL_MS) {
      processedEvents.delete(key);
      removed++;
    }
  }
  if (removed > 0) {
    logger.info('Idempotency cleanup', { removed, remaining: processedEvents.size });
  }
}

export function isDuplicate(eventId: string): boolean {
  return processedEvents.has(eventId);
}

export function markProcessed(eventId: string): void {
  processedEvents.set(eventId, { eventId, processedAt: Date.now() });
}
