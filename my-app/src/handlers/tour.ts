import type { EventMetadata, TourPublishedV1Payload, TourDeletedV1Payload, TourMeiliDocument } from '../types.js';
import { upsertTourDocument, deleteDocument } from '../services/meilisearch.js';
import { logger } from '../logger.js';

function buildTourDocumentId(aggregateId: string): string {
  return `${aggregateId}-tour`;
}

export async function handleTourPublished(
  payload: TourPublishedV1Payload,
  metadata: EventMetadata,
): Promise<void> {
  const doc: TourMeiliDocument = {
    id: buildTourDocumentId(metadata.aggregateId),
    title: payload.data.destination,
    slug: payload.slug,
    image: payload.data.thumbnail,
    completeImage: payload.data.completeThumbnail ?? null,
    description: payload.data.description,
    categories: payload.data.categories.map((c) => c.name),
    destination: payload.data.destinos.name,
    difficulty: payload.data.difficulty,
    type: 'tour',
    price: payload.data.price,
    duration_days: payload.data.duration_days,
    _updated_at_payload: new Date().toISOString(),
  };

  await upsertTourDocument(doc);
}

export async function handleTourUpdated(
  payload: TourPublishedV1Payload,
  metadata: EventMetadata,
): Promise<void> {
  // Updated payload has the same shape as published — same upsert logic
  await handleTourPublished(payload, metadata);
}

export async function handleTourDeleted(
  payload: TourDeletedV1Payload,
  metadata: EventMetadata,
): Promise<void> {
  const docId = buildTourDocumentId(metadata.aggregateId);
  await deleteDocument(docId);
  logger.info('Tour document deleted', { docId, slug: payload.slug }, metadata);
}
