# 1. Usamos la imagen oficial de Bun en su versión Alpine (ligera y segura para producción)
FROM oven/bun:1-alpine

# 2. Establecemos el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiamos los archivos de dependencias primero para aprovechar la caché de Docker
# (Si aún no tienes bun.lockb, Bun lo generará, pero es buena práctica incluirlo)
COPY package.json bun.lockb* ./

# 4. Instalamos las dependencias de producción
RUN bun install --frozen-lockfile --production

# 5. Copiamos el resto del código de tu aplicación (incluyendo index.ts)
COPY . .

# 6. Por seguridad, cambiamos al usuario 'bun' que viene por defecto en la imagen (no root)
USER bun

# 7. Ejecutamos tu archivo principal
CMD ["bun", "run", "index.ts"]
