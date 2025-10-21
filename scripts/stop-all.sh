#!/bin/bash
# Stop all services

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🛑 Stopping Market Data Visualization Stack"
echo "=============================================="

# Stop application services first
echo ""
echo "🔧 Stopping Application Services..."
docker-compose -f "$PROJECT_ROOT/docker-compose.yml" down

# Stop TimescaleDB
echo ""
echo "📊 Stopping TimescaleDB..."
docker-compose -f "$PROJECT_ROOT/docker-compose.timescaledb.yml" down

echo ""
echo "✅ All services stopped!"
echo ""
echo "💡 Data persists in Docker volumes. To remove data:"
echo "   docker volume rm timescaledb_market_data"
