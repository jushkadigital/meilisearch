import amqp from 'amqplib';
import type { Channel, ChannelModel, ConsumeMessage, Options } from 'amqplib';
import { MeiliSearch } from 'meilisearch';

const RABBIT_URL = process.env.RABBITMQ_URL || 'amqp://rabbitmq:5672';
const MEILI_HOST = process.env.MEILI_HOST || 'http://meilisearch:7700';
const MEILI_KEY = process.env.MEILI_MASTER_KEY || 'masterKey';
const EXCHANGE_NAME = 'tourism-exchange';
const INDEXER_QUEUE_NAME = process.env.RABBITMQ_QUEUE_NAME || 'indexer';
const DLQ_QUEUE_NAME = `${INDEXER_QUEUE_NAME}.dlq`;
const DEAD_LETTER_EXCHANGE_NAME = `${INDEXER_QUEUE_NAME}.dlx`;
const ROUTING_PATTERNS = ['tour.*', 'package.*', 'product.*'] as const;

const parseMaxRetries = (value: string | undefined) => {
  const parsed = Number.parseInt(value ?? '3', 10);

  if (Number.isNaN(parsed) || parsed < 0) {
    return 3;
  }

  return parsed;
};

const MAX_RETRIES = parseMaxRetries(process.env.RABBITMQ_MAX_RETRIES);

const meili = new MeiliSearch({ host: MEILI_HOST, apiKey: MEILI_KEY });
const INDEX_NAME = 'tours';

type ProcessingChannel = Pick<Channel, 'ack' | 'sendToQueue' | 'publish'>;
type TopologyChannel = Pick<Channel, 'assertExchange' | 'assertQueue' | 'bindQueue'>;

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

function normalizeErrorMessage(error: unknown) {
  if (error instanceof Error) {
    return error.message;
  }

  return String(error);
}

function getRetryCount(msg: ConsumeMessage) {
  const retryHeader = msg.properties.headers?.['x-retry-count'];

  if (typeof retryHeader === 'number') {
    return retryHeader;
  }

  if (typeof retryHeader === 'string') {
    const parsed = Number.parseInt(retryHeader, 10);

    if (!Number.isNaN(parsed)) {
      return parsed;
    }
  }

  return 0;
}

function buildPublishOptions(
  msg: ConsumeMessage,
  retryCount: number,
  error: unknown,
): Options.Publish {
  return {
    persistent: true,
    contentType: msg.properties.contentType,
    contentEncoding: msg.properties.contentEncoding,
    correlationId: msg.properties.correlationId,
    messageId: msg.properties.messageId,
    type: msg.properties.type,
    appId: msg.properties.appId,
    timestamp: Date.now(),
    headers: {
      ...(msg.properties.headers ?? {}),
      'x-retry-count': retryCount,
      'x-original-routing-key': msg.fields.routingKey,
      'x-last-error': normalizeErrorMessage(error),
    },
  };
}

function handleProcessingFailure(
  msg: ConsumeMessage,
  channel: ProcessingChannel,
  error: unknown,
) {
  const currentRetryCount = getRetryCount(msg);

  if (currentRetryCount < MAX_RETRIES) {
    const nextRetryCount = currentRetryCount + 1;
    const publishOptions = buildPublishOptions(msg, nextRetryCount, error);

    channel.sendToQueue(INDEXER_QUEUE_NAME, msg.content, publishOptions);
    channel.ack(msg);
    console.warn(
      `↩️ Retry ${nextRetryCount}/${MAX_RETRIES} reenviado a ${INDEXER_QUEUE_NAME} para ${msg.fields.routingKey}`,
    );
    return;
  }

  const publishOptions = buildPublishOptions(msg, currentRetryCount, error);

  channel.publish(
    DEAD_LETTER_EXCHANGE_NAME,
    msg.fields.routingKey,
    msg.content,
    {
      ...publishOptions,
      headers: {
        ...(publishOptions.headers ?? {}),
        'x-final-failure-at': new Date().toISOString(),
      },
    },
  );
  channel.ack(msg);
  console.error(
    `☠️ Max retries alcanzados (${MAX_RETRIES}). Mensaje enviado a ${DLQ_QUEUE_NAME}.`,
  );
}

async function connectRabbitMQ(): Promise<{ conn: ChannelModel; channel: Channel }> {
  let retries = 5;

  while (retries) {
    try {
      console.log('🐰 Intentando conectar a RabbitMQ...');
      const conn = await amqp.connect(RABBIT_URL);
      const channel = await conn.createChannel();

      conn.on('error', (err) => console.error('RabbitMQ Connection Error', err));
      conn.on('close', () => {
        console.error('RabbitMQ Connection Closed. Exiting...');
        process.exit(1);
      });

      console.log('✅ Conectado a RabbitMQ');
      return { conn, channel };
    } catch (err) {
      console.error(`❌ Error conectando. Reintentando en 5s... (${retries} left)`);
      retries -= 1;
      await delay(5000);
    }
  }

  throw new Error('No se pudo conectar a RabbitMQ');
}

async function configureMeili() {
  console.log('⚙️ Configurando índice en Meilisearch...');

  await meili.createIndex(INDEX_NAME, { primaryKey: 'id' }).catch(() => {});
  const index = meili.index(INDEX_NAME);

  await index.updateSettings({
    filterableAttributes: [
      'price',
      'categories',
      'destination',
      'tags',
      'type',
      'max_capacity',
      'difficulty',
    ],
    sortableAttributes: ['price', 'created_at', 'title'],
    searchableAttributes: ['title'],
  });
  console.log('✅ Configuración de filtros y ordenamiento aplicada.');
}

async function syncTourDocument(content: any) {
  const index = meili.index(INDEX_NAME);

  if (content.id == null) {
    throw new Error('Payload de tour sin id.');
  }

  if (content.data == null) {
    throw new Error(`Payload de tour ${content.id} sin data.`);
  }

  if (content.data.destinos == null) {
    throw new Error(`Payload de tour ${content.id} sin destinos.`);
  }

  if (content.data.categories == null || !Array.isArray(content.data.categories)) {
    throw new Error(`Payload de tour ${content.id} sin categories válidas.`);
  }

  await index.updateDocuments([
    {
      id: `${content.id.toString()}tour`,
      title: content.data.destination,
      slug: content.slug,
      image: content.data.thumbnail,
      completeImage: content.data.completeThumbnail,
      description: content.data.description,
      categories: content.data.categories.map((ele: any) => ele.name),
      destination: content.data.destinos.name,
      max_capacity: content.data.max_capacity,
      difficulty: content.data.difficulty,
      type: 'tour',
      _updated_at_payload: new Date().toISOString(),
    },
  ]);

  console.log(`📝 Marketing Data actualizado: ${content.id}`);
}

async function syncPackageDocument(content: any) {
  const index = meili.index(INDEX_NAME);

  if (content.id == null) {
    throw new Error('Payload de package sin id.');
  }

  if (content.data == null) {
    throw new Error(`Payload de package ${content.id} sin data.`);
  }

  if (content.data.destinos == null || !Array.isArray(content.data.destinos)) {
    throw new Error(`Payload de package ${content.id} sin destinos válidos.`);
  }

  await index.updateDocuments([
    {
      id: `${content.id.toString()}package`,
      title: content.data.destination,
      slug: content.slug,
      image: content.data.thumbnail,
      completeImage: content.data.completeThumbnail,
      description: content.data.description,
      destination: content.data.destinos.map((ele: any) => ele.name),
      max_capacity: content.data.max_capacity,
      difficulty: content.data.difficulty,
      type: 'paquete',
      _updated_at_payload: new Date().toISOString(),
    },
  ]);

  console.log(`📝 Marketing Data actualizado: ${content.id}`);
}

async function syncProductDocument(content: any) {
  const index = meili.index(INDEX_NAME);
  const sharedId = content.data?.external_id;

  if (!sharedId) {
    console.warn(`⚠️ Producto ignorado (Falta payloadId en metadata): ${content.data?.id}`);
    return;
  }

  let maxPrice = 0;
  const currency = 'PEN';

  if (content.data?.variants && Array.isArray(content.data.variants)) {
    const allPrices: number[] = [];

    content.data.variants.forEach((variant: any) => {
      const pricesArray = variant.prices || variant.price_set?.prices || [];
      const priceObj = pricesArray.find(
        (price: any) => price.currency_code === 'pen' || price.currency_code === 'PEN',
      );

      if (priceObj) {
        allPrices.push(priceObj.amount);
      }
    });

    if (allPrices.length > 0) {
      maxPrice = Math.max(...allPrices);
    }
  }

  await index.updateDocuments([
    {
      id: sharedId,
      price: maxPrice,
      currency,
      medusa_id: content.data.id,
      _updated_at_medusa: new Date().toISOString(),
    },
  ]);

  console.log(`💰 Ecommerce Data actualizado: ${sharedId} -> S/ ${maxPrice}`);
}

async function handleBusinessMessage(routingKey: string, content: any) {
  const index = meili.index(INDEX_NAME);

  if (routingKey.startsWith('tour.')) {
    if (routingKey.endsWith('deleted')) {
      await index.deleteDocument(`${content.id.toString()}tour`);
      console.log(`🗑️ Eliminado tour: ${content.id}`);
      return;
    }

    await syncTourDocument(content);
    return;
  }

  if (routingKey.startsWith('package.')) {
    if (routingKey.endsWith('deleted')) {
      await index.deleteDocument(`${content.id.toString()}package`);
      console.log(`🗑️ Eliminado Package: ${content.id}`);
      return;
    }

    await syncPackageDocument(content);
    return;
  }

  if (routingKey.startsWith('product.')) {
    await syncProductDocument(content);
  }
}

export async function processMessage(msg: ConsumeMessage | null, channel: ProcessingChannel) {
  if (!msg) {
    return;
  }

  const routingKey = msg.fields.routingKey;
  const contentStr = msg.content.toString();

  console.log(`📥 Evento recibido: ${routingKey}`);

  try {
    const content = JSON.parse(contentStr);
    await handleBusinessMessage(routingKey, content);
    channel.ack(msg);
  } catch (error) {
    console.error('🔥 Error procesando mensaje:', error);
    handleProcessingFailure(msg, channel, error);
  }
}

export async function setupQueueTopology(channel: TopologyChannel) {
  await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
  await channel.assertExchange(DEAD_LETTER_EXCHANGE_NAME, 'topic', { durable: true });
  await channel.assertQueue(DLQ_QUEUE_NAME, { durable: true });
  await channel.bindQueue(DLQ_QUEUE_NAME, DEAD_LETTER_EXCHANGE_NAME, '#');

  const queue = await channel.assertQueue(INDEXER_QUEUE_NAME, {
    durable: true,
    deadLetterExchange: DEAD_LETTER_EXCHANGE_NAME,
  });

  for (const routingPattern of ROUTING_PATTERNS) {
    await channel.bindQueue(queue.queue, EXCHANGE_NAME, routingPattern);
  }

  return queue.queue;
}

async function start() {
  try {
    const { channel } = await connectRabbitMQ();

    await configureMeili();
    const queueName = await setupQueueTopology(channel);

    console.log(
      `🚀 Worker sincronizador iniciado. Cola principal: ${queueName}. DLQ: ${DLQ_QUEUE_NAME}. Retries máximos: ${MAX_RETRIES}`,
    );

    channel.prefetch(1);
    channel.consume(queueName, (msg) => {
      void processMessage(msg, channel);
    });
  } catch (error) {
    console.error('FATAL ERROR AL INICIAR:', error);
    process.exit(1);
  }
}

export {
  DEAD_LETTER_EXCHANGE_NAME,
  DLQ_QUEUE_NAME,
  EXCHANGE_NAME,
  INDEXER_QUEUE_NAME,
  MAX_RETRIES,
};

if (import.meta.main) {
  void start();
}
