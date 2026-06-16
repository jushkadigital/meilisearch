import type {
  EventMetadata,
  PackagePublishedV1Payload,
  PackageDeletedV1Payload,
  PackageMeiliDocument,
} from '../types.js';
import { upsertPackageDocument, deleteDocument } from '../services/meilisearch.js';
import { logger } from '../logger.js';

function buildPackageDocumentId(aggregateId: string): string {
  return `${aggregateId}-package`;
}

export async function handlePackagePublished(
  payload: PackagePublishedV1Payload,
  metadata: EventMetadata,
): Promise<void> {
  const doc: PackageMeiliDocument = {
    id: buildPackageDocumentId(metadata.aggregateId),
    title: payload.data.destination,
    slug: payload.slug,
    image: payload.data.thumbnail,
    completeImage: payload.data.completeThumbnail ?? null,
    description: payload.data.description,
    destination: payload.data.destinos.map((d) => d.name),
    difficulty: payload.data.difficulty,
    type: 'package',
    price: payload.data.price,
    duration_days: payload.data.duration_days,
    _updated_at_payload: new Date().toISOString(),
  };

  await upsertPackageDocument(doc);
}

export async function handlePackageUpdated(
  payload: PackagePublishedV1Payload,
  metadata: EventMetadata,
): Promise<void> {
  await handlePackagePublished(payload, metadata);
}

export async function handlePackageDeleted(
  payload: PackageDeletedV1Payload,
  metadata: EventMetadata,
): Promise<void> {
  const docId = buildPackageDocumentId(metadata.aggregateId);
  await deleteDocument(docId);
  logger.info('Package document deleted', { docId, slug: payload.slug }, metadata);
}


