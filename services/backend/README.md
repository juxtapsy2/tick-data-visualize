# Market Service

Real-time market data streaming service using gRPC.

## Features

- **Real-time Streaming**: PostgreSQL LISTEN/NOTIFY for sub-20ms latency
- **Redis Caching**: Fast historical data retrieval
- **Forward-Fill**: VN30 (5s) synchronized with HNX (1s) updates
- **gRPC Streaming**: Efficient binary protocol
- **Health Checks**: Kubernetes-ready liveness/readiness probes

## API

### gRPC Methods

- `GetHistoricalData`: Fetch historical market data for a specific date
- `StreamMarketData`: Real-time streaming of market updates

## Configuration

Set environment variables with `MARKET_` prefix:

```bash
MARKET_DATABASE_HOST=localhost
MARKET_DATABASE_PORT=5432
MARKET_REDIS_ADDRESS=localhost:6379
MARKET_LOGGING_LEVEL=info
```

## Development

```bash
# Run locally
go run cmd/market-service/main.go

# Build
go build -o market-service cmd/market-service/main.go

# Docker
docker build -t market-service .
```
