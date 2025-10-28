# Database Migrations

This directory contains SQL migration files for the data-visualize TimescaleDB schema.

## Running Migrations

### Option 1: Using psql (Recommended)

```bash
# Connect to your TimescaleDB database
psql -U postgres -d market_data -f migrations/001_add_futures_positions_columns.sql
```

### Option 2: Using Docker

```bash
# If running TimescaleDB in Docker
docker exec -i timescaledb psql -U postgres -d market_data < migrations/001_add_futures_positions_columns.sql
```

## Migration List

### 001_add_futures_positions_columns.sql
- **Date**: 2025-10-28
- **Description**: Adds columns for foreign positions and order book data
  - `total_f_buy_vol` - Foreign buy volume
  - `total_f_sell_vol` - Foreign sell volume
  - `total_bid` - Total bid (long orders)
  - `total_ask` - Total ask (short orders)
- **Impact**: Required for Chart 2 (Foreign Long/Short) and Chart 3 (Total Bid/Ask)
- **Rollback**: Not recommended - data loss will occur

## Notes

- Migrations modify the `futures_table` schema and recreate the `futures_15s_cagg` continuous aggregate
- Always backup your database before running migrations
- The continuous aggregate will be refreshed automatically after migration
