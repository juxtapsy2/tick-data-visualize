#!/bin/bash
# Start all services (TimescaleDB + Application Stack)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🚀 Starting Market Data Visualization Stack"
echo "=============================================="

# Check if .env exists
if [ ! -f "$PROJECT_ROOT/.env" ]; then
    echo "⚠️  .env file not found. Creating from .env.example..."
    cp "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"
    echo "✅ .env file created. Please review and update values if needed."
fi

# Start TimescaleDB first
echo ""
echo "📊 Starting TimescaleDB..."
docker-compose -f "$PROJECT_ROOT/docker-compose.timescaledb.yml" up -d

# Wait for TimescaleDB to be healthy
echo ""
echo "⏳ Waiting for TimescaleDB to be ready..."
timeout=60
counter=0
until docker exec timescaledb-host pg_isready -U market_user -d trading_db > /dev/null 2>&1; do
    counter=$((counter + 1))
    if [ $counter -gt $timeout ]; then
        echo "❌ TimescaleDB failed to start within ${timeout} seconds"
        exit 1
    fi
    echo -n "."
    sleep 1
done

echo ""
echo "✅ TimescaleDB is ready!"

# Start application services
echo ""
echo "🔧 Starting Application Services (Redis, Backend, Data Generator, Frontend)..."
docker-compose -f "$PROJECT_ROOT/docker-compose.yml" up -d

echo ""
echo "✅ All services started!"
echo ""
echo "📋 Service Status:"
echo "=============================================="
docker-compose -f "$PROJECT_ROOT/docker-compose.timescaledb.yml" ps
echo ""
docker-compose -f "$PROJECT_ROOT/docker-compose.yml" ps
echo ""
echo "=============================================="
echo "🌐 Access URLs:"
echo "  - Frontend:        http://localhost:3000"
echo "  - Backend API:     http://localhost:8080"
echo "  - WebSocket:       ws://localhost:8080/ws"
echo "  - TimescaleDB:     localhost:5432"
echo "  - Redis:           localhost:6379"
echo ""
echo "📝 Useful Commands:"
echo "  View logs:         ./scripts/logs.sh"
echo "  Stop all:          ./scripts/stop-all.sh"
echo "  Restart:           ./scripts/restart-all.sh"
echo "=============================================="
