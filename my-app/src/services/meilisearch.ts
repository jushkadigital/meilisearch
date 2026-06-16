import { MeiliSearch } from 'meilisearch';
import { logger } from '../logger.js';
import type {
  TourMeiliDocument,
  PackageMeiliDocument,
  ProductMeiliDocument,
} from '../types.js';

const MEILI_HOST = process.env.MEILI_HOST || 'http://meilisearch:7700';
const MEILI_KEY = process.env.MEILI_MASTER_KEY || 'masterKey';
const INDEX_NAME = process.env.MEILI_INDEX_NAME || 'tours';

export const meili = new MeiliSearch({ host: MEILI_HOST, apiKey: MEILI_KEY });

export function getIndex() {
  return meili.index(INDEX_NAME);
}

export async function configureMeili() {
  logger.info('Configuring Meilisearch index', { index: INDEX_NAME });

  await meili.createIndex(INDEX_NAME, { primaryKey: 'id' }).catch(() => {
    // Index already exists — safe to ignore
  });

  const index = getIndex();

  await index.updateSettings({
    filterableAttributes: [
      'price',
      'categories',
      'destination',
      'tags',
      'type',
      'difficulty',
    ],
    sortableAttributes: ['price', '_updated_at_payload', 'title'],
    searchableAttributes: ['title'],
  });

  logger.info('Meilisearch index configured', { index: INDEX_NAME });
}

// ── Document Operations ────────────────────────────────────────────────
export async function upsertTourDocument(doc: TourMeiliDocument): Promise<void> {
  const index = getIndex();
  await index.updateDocuments([doc]);
  logger.info('Tour document synced', { docId: doc.id, title: doc.title });
}

export async function upsertPackageDocument(doc: PackageMeiliDocument): Promise<void> {
  const index = getIndex();
  await index.updateDocuments([doc]);
  logger.info('Package document synced', { docId: doc.id, title: doc.title });
}

export async function upsertProductDocument(doc: ProductMeiliDocument): Promise<void> {
  const index = getIndex();
  await index.updateDocuments([doc]);
  logger.info('Product document synced', { docId: doc.id, price: doc.price });
}

export async function deleteDocument(docId: string): Promise<void> {
  const index = getIndex();
  await index.deleteDocument(docId);
  logger.info('Document deleted', { docId });
}
