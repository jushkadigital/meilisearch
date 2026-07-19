import type { EventMetadata, ProductEventPayload } from '../types.js';
import { getIndex } from '../services/meilisearch.js';
import { logger } from '../logger.js';

export async function handleProductSynced(
  payload: ProductEventPayload,
  metadata: EventMetadata,
): Promise<void> {
  const sharedId = payload.data.external_id;

  if (!sharedId) {
    logger.warn('Product ignored — missing external_id', { medusaId: payload.data.id }, metadata);
    return;
  }

  let price = 0;

  if (payload.data.variants && Array.isArray(payload.data.variants)) {
    const adultVariant = payload.data.variants.find(
      (v) => v.title?.toLowerCase() === 'adult',
    );

    if (adultVariant) {
      const pricesArray = adultVariant.prices ?? adultVariant.price_set?.prices ?? [];
      const amounts = pricesArray.map((p) => p.amount).filter((a) => a > 0);

      if (amounts.length > 0) {
        price = Math.max(...amounts);
      }
    }
  }

  const index = getIndex();
  await index.updateDocuments([{ id: sharedId, price }]);
  logger.info('Product price updated', { docId: sharedId, price }, metadata);
}

export async function handleProductUpdated(
  payload: ProductEventPayload,
  metadata: EventMetadata,
): Promise<void> {
  await handleProductSynced(payload, metadata);
}
