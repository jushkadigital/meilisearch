import amqp, { Connection, Channel, ConsumeMessage } from 'amqplib';
import { MeiliSearch } from 'meilisearch';

// --- CONFIGURACIÓN ---
const RABBIT_URL = process.env.RABBITMQ_URL || 'amqp://rabbitmq:5672';
const MEILI_HOST = process.env.MEILI_HOST || 'http://meilisearch:7700';
const MEILI_KEY = process.env.MEILI_MASTER_KEY || 'masterKey';
const QUEUE_NAME = 'consumer_sync_queue';
const EXCHANGE_NAME = 'tourism-exchange';

const meili = new MeiliSearch({ host: MEILI_HOST, apiKey: MEILI_KEY });
const INDEX_NAME = 'tours';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

// --- 1. CONEXIÓN RESILIENTE ---
async function connectRabbitMQ(): Promise<{ conn: Connection, channel: Channel }> {
  let retries = 5;
  while (retries) {
    try {
      console.log(`🐰 Intentando conectar a RabbitMQ...`);
      const conn = await amqp.connect(RABBIT_URL);
      const channel = await conn.createChannel();

      // Manejo de errores de conexión cerrada inesperadamente
      conn.on("error", (err) => console.error("RabbitMQ Connection Error", err));
      conn.on("close", () => {
        console.error("RabbitMQ Connection Closed. Exiting...");
        process.exit(1); // Docker reiniciará el proceso
      });

      console.log("✅ Conectado a RabbitMQ");
      return { conn, channel };
    } catch (err) {
      console.error(`❌ Error conectando. Reintentando en 5s... (${retries} left)`);
      retries -= 1;
      await delay(5000);
    }
  }
  throw new Error("No se pudo conectar a RabbitMQ");
}

// --- 2. CONFIGURACIÓN DE MEILISEARCH (CRÍTICO PARA FILTROS) ---
async function configureMeili() {
  console.log("⚙️ Configurando índice en Meilisearch...");

  // Crear índice si no existe
  await meili.createIndex(INDEX_NAME, { primaryKey: 'id' }).catch(() => { });
  const index = meili.index(INDEX_NAME);

  // AQUÍ ES DONDE HABILITAS LOS FILTROS Y ORDENAMIENTO
  await index.updateSettings({
    // Campos que usarás en el WHERE (ej: filter="price > 50 AND currency = 'PEN'")
    filterableAttributes: [
      'price',
      'categories', // Asegúrate de enviarlo desde Payload
      'destination',
      'tags',
      'type',
      'max_capacity',
      'difficulty'
    ],
    // Campos que usarás en el SORT (ej: sort=['price:asc'])
    sortableAttributes: [
      'price',
      'created_at',
      'title'
    ],
    // Campos donde buscará texto (la barra de búsqueda)
    searchableAttributes: [
      'title'
    ]
  });
  console.log("✅ Configuración de filtros y ordenamiento aplicada.");
}

// --- 3. PROCESAMIENTO DE MENSAJES ---
async function processMessage(msg: ConsumeMessage | null, channel: Channel) {
  if (!msg) return;

  const routingKey = msg.fields.routingKey;
  const contentStr = msg.content.toString();
  const index = meili.index(INDEX_NAME);

  console.log(`📥 Evento recibido: ${routingKey}`);

  try {
    const content = JSON.parse(contentStr);

    // --- LOGICA PAYLOAD CMS (Contenido) ---
    if (routingKey.startsWith('tour.')) {
      // Determinar si es 'create', 'update' o 'delete'
      if (content.action === 'delete') {
        await index.deleteDocument(content.id.toString() + "tour");
        console.log(`🗑️ Eliminado tour: ${content.id}`);
      } else {
        console.log(content)
        // Unificar estructura de payload (hooks afterChange suelen enviar 'doc' o 'data')

        // Extracción segura (Optional Chaining ?.)

        await index.updateDocuments([{
          id: content.id.toString() + "tour",
          title: content.data.destination,
          slug: content.slug,
          image: content.data.thumbnail,
          description: content.data.description,
          categories: content.data.categories.map(ele => ele.name),
          destination: content.data.destinos.name,
          max_capacity: content.data.max_capacity,
          difficulty: content.data.difficulty,
          type: 'tour',
          _updated_at_payload: new Date().toISOString()
        }]);
        console.log(`📝 Marketing Data actualizado: ${content.id}`);
      }
    }
    else if (routingKey.startsWith('package.')) {
      // Determinar si es 'create', 'update' o 'delete'
      if (content.action === 'delete') {
        await index.deleteDocument(content.id.toString() + "package");
        console.log(`🗑️ Eliminado Package: ${content.id}`);
      } else {
        // Unificar estructura de payload (hooks afterChange suelen enviar 'doc' o 'data')
        await index.updateDocuments([{
          id: content.id.toString() + "package",
          title: content.data.destination,
          slug: content.slug,
          image: content.data.thumbnail,
          description: content.data.description,
          destination: content.data.destinos.map(ele => ele.name),
          max_capacity: content.data.max_capacity,
          difficulty: content.data.difficulty,
          type: 'paquete',
          _updated_at_payload: new Date().toISOString()
        }]);
        console.log(`📝 Marketing Data actualizado: ${content.id}`);
      }
    }

    // --- LOGICA MEDUSA JS (Precios e Inventario) ---
    else if (routingKey.startsWith('product.')) {
      // 1. Buscar el ID de enlace (Payload ID)
      // En Medusa V2, a veces metadata está en el producto raiz
      console.log(content)
      const sharedId = (content.data.external_id);

      if (sharedId) {
        // Cálculo de precios más seguro
        let maxPrice = 0;
        let currency = 'PEN';

        if (content.data.variants && Array.isArray(content.data.variants)) {
          const allPrices: number[] = [];

          content.data.variants.forEach((v: any) => {
            // Lógica para Medusa (V1 o V2 puede variar estructura)
            const pricesArray = v.prices || v.price_set?.prices || [];
            const priceObj = pricesArray.find((p: any) => p.currency_code === 'pen' || p.currency_code === 'PEN');

            if (priceObj) {
              allPrices.push(priceObj.amount);
            }
          });
          console.log(allPrices)

          if (allPrices.length > 0) maxPrice = Math.max(...allPrices);
        }

        // Partial Update: Meilisearch mezclará esto con los datos de Payload
        await index.updateDocuments([{
          id: sharedId, // ¡Importante! Debe ser el MISMO ID que usa Payload
          price: maxPrice,
          currency: currency,
          medusa_id: content.data.id,
          _updated_at_medusa: new Date().toISOString()
        }]);
        console.log(`💰 Ecommerce Data actualizado: ${sharedId} -> S/ ${maxPrice}`);
      } else {
        console.warn(`⚠️ Producto ignorado (Falta payloadId en metadata): ${content.data.id}`);
      }
    }

    // Confirmar mensaje procesado
    channel.ack(msg);

  } catch (error) {
    console.error("🔥 Error procesando mensaje (JSON malformado o lógica):", error);
    // IMPORTANTE: Si es un error de código, mejor hacer ACK o mover a Dead Letter Queue
    // para no bloquear la cola con reintentos infinitos.
    channel.ack(msg); // Hacemos ACK para descartar mensaje venenoso
  }
}

// --- 4. INICIO ---
async function start() {
  try {
    const { channel } = await connectRabbitMQ();

    // Configurar Meilisearch antes de consumir
    await configureMeili();

    // Topología RabbitMQ
    await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
    const q = await channel.assertQueue(QUEUE_NAME, { durable: true });

    // Bindings
    await channel.bindQueue(q.queue, EXCHANGE_NAME, 'tour.*');
    await channel.bindQueue(q.queue, EXCHANGE_NAME, 'package.*');
    await channel.bindQueue(q.queue, EXCHANGE_NAME, 'product.*');

    console.log("🚀 Worker sincronizador iniciado y esperando eventos...");

    // Prefetch 1: Procesa 1 mensaje a la vez para no saturar memoria
    channel.prefetch(1);

    channel.consume(q.queue, (msg) => processMessage(msg, channel));

  } catch (error) {
    console.error("FATAL ERROR AL INICIAR:", error);
    process.exit(1);
  }
}

start();
