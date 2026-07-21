import type { EventMetadata, ProductEventPayload } from '../types.js';
import { getIndex } from '../services/meilisearch.js';
import { logger } from '../logger.js';

function parseExternalId(externalId: string): { aggregateId: string; type: 'tour' | 'package' } | null {
  const match = externalId.match(/^(\d+)(tour|package)$/);
  if (!match || !match[1] || !match[2]) return null;
  return { aggregateId: match[1], type: match[2] as 'tour' | 'package' };
}

function buildDocumentId(aggregateId: string, type: 'tour' | 'package'): string {
  return `${aggregateId}-${type}`;
}

export async function handleProductSynced(
  payload: ProductEventPayload,
  metadata: EventMetadata,
): Promise<void> {
  const externalId = payload.data.external_id;

  if (!externalId) {
    logger.warn('Product ignored — missing external_id', { medusaId: payload.data.id }, metadata);
    return;
  }

  const parsed = parseExternalId(externalId);
  if (!parsed) {
    logger.warn('Product ignored — invalid external_id format', { externalId }, metadata);
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

  const docId = buildDocumentId(parsed.aggregateId, parsed.type);
  const index = getIndex();
  await index.updateDocuments([{ id: docId, price }]);
  logger.info('Product price updated', { docId, price }, metadata);
}

export async function handleProductUpdated(
  payload: ProductEventPayload,
  metadata: EventMetadata,
): Promise<void> {
  await handleProductSynced(payload, metadata);
}
