#!/bin/bash
# Stop local development environment

set -e

echo "Stopping application services..."
docker compose down

echo ""
echo "Stopping local TimescaleDB..."
cd timescaledb_host
docker compose down
cd ..

echo ""
echo "All services stopped successfully!"
