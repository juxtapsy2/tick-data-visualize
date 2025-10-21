#!/bin/bash
# Connect to TimescaleDB shell

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Load environment variables
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(cat "$PROJECT_ROOT/.env" | grep -v '^#' | xargs)
fi

DB_USER=${POSTGRES_USER:-market_user}
DB_NAME=${POSTGRES_DB:-trading_db}

echo "🔌 Connecting to TimescaleDB..."
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo "=============================================="

docker exec -it timescaledb-host psql -U "$DB_USER" -d "$DB_NAME"
