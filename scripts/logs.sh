#!/bin/bash
# View logs for all services or specific service

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

SERVICE=$1

if [ -z "$SERVICE" ]; then
    echo "📋 Viewing logs for all services..."
    echo "=============================================="
    echo ""
    echo "Press Ctrl+C to stop"
    echo ""

    # Tail logs from both compose files
    docker-compose -f "$PROJECT_ROOT/docker-compose.timescaledb.yml" logs -f &
    PID1=$!
    docker-compose -f "$PROJECT_ROOT/docker-compose.yml" logs -f &
    PID2=$!

    # Wait for both and cleanup on exit
    trap "kill $PID1 $PID2 2>/dev/null; exit" INT TERM
    wait
else
    # Check which compose file has the service
    if docker-compose -f "$PROJECT_ROOT/docker-compose.timescaledb.yml" ps | grep -q "$SERVICE"; then
        docker-compose -f "$PROJECT_ROOT/docker-compose.timescaledb.yml" logs -f "$SERVICE"
    elif docker-compose -f "$PROJECT_ROOT/docker-compose.yml" ps | grep -q "$SERVICE"; then
        docker-compose -f "$PROJECT_ROOT/docker-compose.yml" logs -f "$SERVICE"
    else
        echo "❌ Service '$SERVICE' not found"
        echo ""
        echo "Available services:"
        echo "  - timescaledb (from docker-compose.timescaledb.yml)"
        docker-compose -f "$PROJECT_ROOT/docker-compose.yml" config --services
    fi
fi
