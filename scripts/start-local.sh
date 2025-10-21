#!/bin/bash
# Start local development environment

set -e

echo "Starting local TimescaleDB..."
cd timescaledb_host
docker compose up -d
cd ..

echo ""
echo "Waiting for TimescaleDB to be ready..."
sleep 5

echo ""
echo "Starting application services..."
docker compose up -d

echo ""
echo "Services started successfully!"
echo ""
echo "Access points:"
echo "  - Frontend: http://localhost:3000"
echo "  - Backend API: http://localhost:8080"
echo "  - TimescaleDB: localhost:5433"
echo "  - Redis: localhost:6379"
echo ""
echo "Useful commands:"
echo "  - View logs: docker compose logs -f [service-name]"
echo "  - Stop all: ./scripts/stop-local.sh"
echo "  - Database shell: ./scripts/db-shell.sh"
