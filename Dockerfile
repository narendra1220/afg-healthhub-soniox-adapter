FROM node:22-alpine

WORKDIR /app

COPY package.json pnpm-lock.yaml ./
RUN corepack enable && pnpm install --frozen-lockfile --prod

COPY adapter.mjs ./

ENV ADAPTER_HOST=0.0.0.0

USER node

CMD ["node", "adapter.mjs"]
