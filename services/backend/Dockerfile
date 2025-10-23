# Build stage
FROM golang:1.25.1-alpine3.22 AS builder

RUN apk add --no-cache git make

WORKDIR /build

# Copy shared library
COPY shared/ ./shared/

# Copy service code
COPY services/market-service/ /build/services/market-service/

# Work in service directory
WORKDIR /build/services/market-service

# Download dependencies and generate go.sum
RUN go mod tidy && go mod download

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o /build/market-service \
    ./cmd/market-service

# Final stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata wget

RUN addgroup -g 1000 app && \
    adduser -D -u 1000 -G app app

WORKDIR /app

COPY --from=builder /build/market-service /app/market-service
COPY configs /app/configs

RUN chown -R app:app /app

USER app

ENTRYPOINT ["/app/market-service"]
