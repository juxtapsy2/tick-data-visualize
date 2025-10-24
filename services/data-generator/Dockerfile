# Build stage
FROM golang:1.25.1-alpine3.22 AS builder

RUN apk add --no-cache git make

WORKDIR /build

# Copy shared library
COPY shared/ ./shared/

# Generate proto files in shared library (even though data-generator doesn't use them directly)
WORKDIR /build/shared
RUN if [ -f proto/market.proto ]; then \
    protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    proto/market.proto || true; \
    fi

# Copy service code
COPY services/data-generator/ /build/services/data-generator/

# Work in service directory
WORKDIR /build/services/data-generator

# Download dependencies and generate go.sum
RUN go mod tidy && go mod download

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -o /build/data-generator \
    ./cmd/data-generator

# Final stage
FROM alpine:3.22

RUN apk --no-cache add ca-certificates tzdata

RUN addgroup -g 1000 app && \
    adduser -D -u 1000 -G app app

WORKDIR /app

COPY --from=builder /build/data-generator /app/data-generator
COPY services/data-generator/data /app/data

RUN chown -R app:app /app

USER app

ENTRYPOINT ["/app/data-generator"]
