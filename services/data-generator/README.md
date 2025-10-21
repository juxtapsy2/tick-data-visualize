# Data Generator

Sample market data generator for testing and development.

## Features

- **Realistic Price Movement**: Simulates VN30 and HNX index fluctuations
- **Configurable Intervals**: VN30 every 5s, HNX every 1s
- **Graceful Shutdown**: Handles SIGINT/SIGTERM signals

## Usage

```bash
# Run locally
go run cmd/data-generator/main.go

# Build
go build -o data-generator cmd/data-generator/main.go

# Docker
docker build -t data-generator .
```

## Configuration

Uses the same configuration as market-service (database connection only).

Set environment variables:
```bash
MARKET_DATABASE_HOST=localhost
MARKET_DATABASE_PORT=5432
MARKET_LOGGING_LEVEL=info
```
