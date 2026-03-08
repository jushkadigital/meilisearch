import amqp, { Connection, Channel } from 'amqplib';
import { MeiliSearch } from 'meilisearch';

// Configuración desde Variables de Entorno
const RABBIT_URL = process.env.RABBIT_URL || 'amqp://rabbitmq:5672';
const MEILI_HOST = process.env.MEILI_HOST || 'http://meilisearch:7700';
const MEILI_KEY = process.env.MEILI_MASTER_KEY || 'masterKey';
const QUEUE_NAME = 'cms_sync_queue';
const EXCHANGE_NAME = 'tourism-exchange'; // Asegúrate de que coincida con tu exchange

const meili = new MeiliSearch({ host: MEILI_HOST, apiKey: MEILI_KEY });

// Función para esperar (sleep)
const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

async function connectRabbitMQ(): Promise<{ conn: Connection, channel: Channel }> {
  let retries = 5;
  while (retries) {
    try {
      console.log(`🐰 Intentando conectar a RabbitMQ en ${RABBIT_URL}...`);
      const conn = await amqp.connect(RABBIT_URL);
      const channel = await conn.createChannel();
      console.log("✅ Conectado a RabbitMQ");
      return { conn, channel };
    } catch (err) {
      console.error(`❌ Error conectando a RabbitMQ. Reintentando en 5s... (${retries} intentos restantes)`);
      retries -= 1;
      await delay(5000);
    }
  }
  throw new Error("No se pudo conectar a RabbitMQ después de varios intentos");
}

async function processMessage(msg: amqp.ConsumeMessage | null, channel: Channel) {
  if (!msg) return;

  const routingKey = msg.fields.routingKey;
  const content = JSON.parse(msg.content.toString());
  const index = meili.index('tours');


  console.log(`📥 Recibido evento: ${routingKey}`);

  try {
    // CASO 1: Evento desde PayloadCMS (Marketing)
    // Routing keys: tour.created, tour.updated
    if (routingKey == 'tour.created' && routingKey == 'tour.updated') {
      const type = content.action

      let sourceData
      if (type == 'updated') {
        sourceData = content.changes
      } else {
        sourceData = content.data
      }
      await index.updateDocuments([{
        id: sourceData.id, // ID compartido
        title: sourceData.title,
        slug: sourceData.slug,
        image: sourceData.meta.image.sizes.og.url ?? '',
        description: "GAAA",
        _type: 'tour_marketing'
      }]);
      console.log(`📝 Indexado datos de Marketing: ${content.payload_tour_id}`);
    }

    // CASO 2: Evento desde MedusaJS (Ecommerce)
    // Routing keys: product.created, product.updated
    if (routingKey.startsWith('productHH.')) {
      // Extraer el ID compartido desde metadata
      const sharedId = content.metadata?.tour_id;

      if (sharedId) {
        // Calcular precio min (ejemplo simplificado)
        const prices = content.variants?.flatMap((v: any) => v.prices.map((p: any) => p.amount)) || [];
        const minPrice = prices.length > 0 ? Math.min(...prices) : 0;

        await index.updateDocuments([{
          id: sharedId, // ID compartido
          price_min: minPrice,
          currency: 'USD', // O dinámico según tu lógica
          medusa_id: content.id,
          is_purchasable: content.status === 'published',
          _type: 'tour_ecommerce'
        }]);
        console.log(`💰 Indexado datos de Ecommerce: ${sharedId} - Precio: ${minPrice}`);
      } else {
        console.log(`⚠️ Producto ${content.id} ignorado (sin tour_id en metadata)`);
      }
    }

    channel.ack(msg); // Confirmar procesado exitoso

  } catch (error) {
    console.error("🔥 Error procesando mensaje:", error);
    // channel.nack(msg); // Descomentar si quieres que se reencole tras error
  }
}

async function start() {
  try {
    // 1. Asegurar índice en Meilisearch
    await meili.createIndex('tours', { primaryKey: 'id' }); // Opcional si ya existe

    // 2. Conectar a Rabbit
    const { channel } = await connectRabbitMQ();

    // 3. Configurar Topología (Exchange y Cola)
    await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
    const q = await channel.assertQueue(QUEUE_NAME, { durable: true });

    // 4. Bindings (Unir cola con eventos específicos)
    await channel.bindQueue(q.queue, EXCHANGE_NAME, 'tour.*');
    await channel.bindQueue(q.queue, EXCHANGE_NAME, 'product.*');

    console.log("🚀 Worker escuchando eventos...");

    channel.consume(q.queue, (msg) => processMessage(msg, channel));

  } catch (error) {
    console.error("FATAL:", error);
    process.exit(1);
  }
}

start();
