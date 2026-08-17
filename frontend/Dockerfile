FROM node:22-bookworm-slim AS deps
WORKDIR /app
# `install`, not `ci`: frontend/package-lock.json is gitignored, so CI checks
# out a tree without one. Commit the lockfile and switch to `npm ci` if the
# production image should build from a pinned dependency tree.
COPY package.json ./
RUN npm install

FROM node:22-bookworm-slim AS dev
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN npx svelte-kit sync || true
ENV HOST=0.0.0.0 \
    PORT=5173 \
    API_PROXY_TARGET=http://api:8000
EXPOSE 5173
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "5173"]

FROM deps AS build
COPY . .
RUN npm run build

# Production runtime. adapter-node emits a self-contained bundle — it imports
# nothing but node: builtins — so no node_modules ships here.
#
# There is no Vite dev server in production, which means no `/api` proxy: the
# edge (Traefik) routes /api and /ws straight to the api service instead. See
# deploy/docker-compose.yml.
FROM node:22-bookworm-slim AS prod
# See the note in the root Dockerfile — this is what links the package to the
# repo that builds it.
LABEL org.opencontainers.image.source="https://github.com/lynxlinkage/mftik"
WORKDIR /app
ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000
COPY --from=build /app/build ./build
COPY --from=build /app/package.json ./package.json
EXPOSE 3000
# One image serves any origin: the browser opens /api and /ws on whatever host
# served the document, and the edge routes them (see src/lib/ws.ts for why a
# WebSocket in particular cannot be pointed anywhere else).
CMD ["node", "build"]
