#!/bin/bash
# Initialize database schema

set -e

SETUP_FILE="timescaledb_host/MANUAL_SETUP_SIMPLE.sql"

# Check if connecting to local or Railway
if [ "$1" == "production" ] || [ "$1" == "railway" ]; then
    echo "Setting up Railway TimescaleDB schema..."
    echo "Password: qubit2025_db$"
    psql "postgres://market_user:qubit2025_db\$@timescaledbhost-production.up.railway.app:5432/trading_db" < "$SETUP_FILE"
else
    echo "Setting up local TimescaleDB schema..."
    docker exec -i timescaledb-host psql -U market_user -d trading_db < "$SETUP_FILE"
fi

echo ""
echo "Database schema setup complete!"
echo ""
echo "Tables created:"
echo "  - index_tick (for VN30 and other indexes)"
echo "  - futures_table (for futures data)"
echo ""
echo "Next steps:"
echo "  1. Start data-generator: docker compose up data-generator"
echo "  2. Check data: ./scripts/db-shell.sh"
