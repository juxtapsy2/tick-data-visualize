# Build stage
FROM node:20-alpine AS builder

WORKDIR /build

# Copy frontend package files
COPY services/market-frontend/package*.json ./services/market-frontend/

# Install dependencies
WORKDIR /build/services/market-frontend
RUN npm ci

# Copy frontend source (including pre-generated proto files)
COPY services/market-frontend ./

# Build the application
RUN npm run build

# Production stage
FROM nginx:alpine

# Copy built files from builder
COPY --from=builder /build/services/market-frontend/dist /usr/share/nginx/html

# Copy nginx configuration
COPY services/market-frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
