import type { EventMetadata, ProductEventPayload, ProductMeiliDocument } from '../types.js';
import { upsertProductDocument } from '../services/meilisearch.js';
import { logger } from '../logger.js';

export async function handleProductUpdated(
  payload: ProductEventPayload,
  metadata: EventMetadata,
): Promise<void> {
  const sharedId = payload.data.external_id;

  if (!sharedId) {
    logger.warn('Product ignored — missing external_id', { medusaId: payload.data.id }, metadata);
    return;
  }

  let maxPrice = 0;
  const currency = 'PEN';

  if (payload.data.variants && Array.isArray(payload.data.variants)) {
    const allPrices: number[] = [];

    for (const variant of payload.data.variants) {
      const pricesArray = variant.prices ?? variant.price_set?.prices ?? [];
      const priceObj = pricesArray.find(
        (p) => p.currency_code === 'pen' || p.currency_code === 'PEN',
      );

      if (priceObj) {
        allPrices.push(priceObj.amount);
      }
    }

    if (allPrices.length > 0) {
      maxPrice = Math.max(...allPrices);
    }
  }

  const doc: ProductMeiliDocument = {
    id: sharedId,
    price: maxPrice,
    currency,
    medusa_id: payload.data.id,
    _updated_at_medusa: new Date().toISOString(),
  };

  await upsertProductDocument(doc);
}
