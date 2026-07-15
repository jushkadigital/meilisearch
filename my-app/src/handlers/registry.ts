import type { EventMetadata, EventHandler } from '../types.js';
import { logger } from '../logger.js';

import { handleTourPublished, handleTourUpdated, handleTourDeleted } from './tour.js';
import { handlePackagePublished, handlePackageUpdated, handlePackageDeleted } from './package.js';
import { handleProductSynced, handleProductUpdated } from './product.js';

// ── Handler Registry ───────────────────────────────────────────────────
//
// Dispatches events by eventType. Supports exact match and prefix match.
// Priority: exact match > prefix match > no handler
//

interface HandlerRoute {
  eventType: string;
  handler: EventHandler;
}

const routes: HandlerRoute[] = [
  // Tour events
  { eventType: 'integration.tour.published.v1', handler: handleTourPublished as EventHandler },
  { eventType: 'integration.tour.updated.v1', handler: handleTourUpdated as EventHandler },
  { eventType: 'integration.tour.deleted.v1', handler: handleTourDeleted as EventHandler },

  // Package events
  { eventType: 'integration.package.published.v1', handler: handlePackagePublished as EventHandler },
  { eventType: 'integration.package.updated.v1', handler: handlePackageUpdated as EventHandler },
  { eventType: 'integration.package.deleted.v1', handler: handlePackageDeleted as EventHandler },

  // Product events
  { eventType: 'integration.product.synced.v1', handler: handleProductSynced as EventHandler },
  { eventType: 'integration.product.updated.v1', handler: handleProductUpdated as EventHandler },
];

function findHandler(eventType: string): EventHandler | undefined {
  // 1. Exact match
  const exact = routes.find((r) => r.eventType === eventType);
  if (exact) return exact.handler;

  // 2. Prefix match (e.g., "product." matches "product.updated", "product.created")
  const prefix = routes.find(
    (r) => r.eventType.endsWith('.') && eventType.startsWith(r.eventType),
  );
  if (prefix) return prefix.handler;

  return undefined;
}

export async function dispatchEvent(
  payload: unknown,
  metadata: EventMetadata,
): Promise<void> {
  const handler = findHandler(metadata.eventType);

  if (!handler) {
    logger.warn('No handler found for event', {
      eventType: metadata.eventType,
      aggregateType: metadata.aggregateType,
    }, metadata);
    return;
  }

  await handler(payload, metadata);
}
