#!/bin/bash
# Restart all services

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "🔄 Restarting Market Data Visualization Stack"
echo "=============================================="

# Stop all
"$SCRIPT_DIR/stop-all.sh"

echo ""
echo "⏳ Waiting 3 seconds..."
sleep 3

# Start all
"$SCRIPT_DIR/start-all.sh"
