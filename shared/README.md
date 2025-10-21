# Market Shared Library

Common packages shared across all market microservices.

## Packages

### Logger (`logger/`)
Structured logging wrapper around zerolog with context support.

### Config (`config/`)
Configuration management using Viper with environment variable support.

### Proto (`proto/`)
gRPC protocol buffer definitions for market data streaming.

## Usage

Import in your service:

```go
import (
    "github.com/lnvi/market-shared/config"
    "github.com/lnvi/market-shared/logger"
    pb "github.com/lnvi/market-shared/proto"
)
```

## Development

Generate proto files:
```bash
./generate_proto.sh
```
