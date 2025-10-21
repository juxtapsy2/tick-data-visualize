# ========================================
# Stage 1: Build React Frontend
# ========================================
FROM node:20-alpine AS frontend-builder

WORKDIR /build

# Copy frontend package files
COPY services/frontend/package*.json ./services/frontend/

# Install dependencies
WORKDIR /build/services/frontend
RUN npm ci

# Copy frontend source
COPY services/frontend ./

# Build the application
RUN npm run build

# ========================================
# Stage 2: Build Go Backend
# ========================================
FROM golang:1.25.1-alpine3.22 AS backend-builder

RUN apk add --no-cache git make

WORKDIR /build

# Copy shared library
COPY shared/ ./shared/

# Copy backend service code
COPY services/backend/ ./services/backend/

# Work in backend directory
WORKDIR /build/services/backend

# Download dependencies and build
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o /build/market-service \
    ./cmd/market-service

# ========================================
# Stage 3: Build Data Generator
# ========================================
FROM golang:1.25.1-alpine3.22 AS data-generator-builder

RUN apk add --no-cache git make

WORKDIR /build

# Copy shared library
COPY shared/ ./shared/

# Copy data generator service code
COPY services/data-generator/ ./services/data-generator/

# Work in data generator directory
WORKDIR /build/services/data-generator

# Download dependencies and build
RUN go mod tidy && go mod download
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o /build/data-generator \
    ./cmd/data-generator

# ========================================
# Final Stage: Nginx + Supervisord
# ========================================
FROM alpine:3.19

# Install required packages
RUN apk --no-cache add \
    ca-certificates \
    tzdata \
    wget \
    nginx \
    supervisor

# Create app user
RUN addgroup -g 1000 app && \
    adduser -D -u 1000 -G app app

WORKDIR /app

# Copy built binaries
COPY --from=backend-builder /build/market-service /app/market-service
COPY --from=data-generator-builder /build/data-generator /app/data-generator

# Copy config files
COPY configs /app/configs

# Copy CSV data for data generator
COPY services/data-generator/data /app/data

# Copy frontend build to nginx directory
COPY --from=frontend-builder /build/services/frontend/dist /usr/share/nginx/html

# Copy nginx configuration
COPY docker/nginx/nginx.conf /etc/nginx/http.d/default.conf

# Copy supervisord configuration
COPY docker/supervisord/supervisord.conf /etc/supervisord.conf

# Create necessary directories
RUN mkdir -p /var/log/supervisor /var/run/nginx /var/log/nginx

# Fix permissions
RUN chown -R app:app /app /usr/share/nginx/html /var/log/supervisor /var/run/nginx /var/log/nginx && \
    chmod -R 755 /app

# Switch to app user for running services
USER app

# Expose port 80 (Nginx)
EXPOSE 80

# Start supervisord
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisord.conf"]
