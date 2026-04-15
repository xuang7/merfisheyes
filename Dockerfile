# MERFISHeyes — Next.js visualization microservice for ADMAP.
# Build: docker buildx build --platform linux/amd64 -t <registry>/merfisheyes:<tag> --push .
FROM node:20-bookworm AS base
LABEL org.opencontainers.image.title="merfisheyes" \
      org.opencontainers.image.description="ADMAP visualization microservice (Next.js, basePath=/merfisheyes)"

# --- Dependencies stage ---
FROM base AS deps
WORKDIR /app
COPY package.json package-lock.json ./
COPY prisma ./prisma/
RUN npm ci

# --- Build stage ---
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npx prisma generate && \
    NEXT_DISABLE_ESLINT=true NODE_OPTIONS='--max-old-space-size=4096' npm run build

# --- Production stage ---
FROM base AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOSTNAME=0.0.0.0

RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

COPY --from=builder /app/public ./public
COPY --from=builder /app/package.json ./package.json

# Next.js standalone output
COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs
EXPOSE 3000

CMD ["node", "server.js"]
