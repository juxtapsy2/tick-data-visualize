package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/lnvi/market-shared/config"
	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/logger"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repository implements repository.MarketRepository for PostgreSQL
type Repository struct {
	pool   *pgxpool.Pool
	config *config.MarketConfig
	log    *logger.Logger
}

// NewRepository creates a new PostgreSQL repository
func NewRepository(ctx context.Context, dbURL string, cfg *config.MarketConfig, log *logger.Logger) (*Repository, error) {
	poolConfig, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	// Create connection pool
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Info("PostgreSQL repository initialized successfully")

	return &Repository{
		pool:   pool,
		config: cfg,
		log:    log,
	}, nil
}

// GetHistoricalData retrieves market data for a time range
// Queries base tables with time_bucket to create 15-second aggregates on-the-fly
func (r *Repository) GetHistoricalData(ctx context.Context, startTime, endTime time.Time) ([]repository.MarketData, error) {
	// Query with time_bucket to aggregate into 15-second intervals
	// Use COALESCE to handle NULL values from AVG() and FULL OUTER JOIN
	query := `
		WITH vn30_buckets AS (
			SELECT
				time_bucket('15 seconds', ts) AS bucket,
				AVG(last) as value
			FROM index_tick
			WHERE ticker = 'VN30' AND ts >= $1 AND ts <= $2 AND last IS NOT NULL AND last > 0
			GROUP BY bucket
		),
		futures_buckets AS (
			SELECT
				time_bucket('15 seconds', ts) AS bucket,
				AVG(last) as value
			FROM futures_table
			WHERE ticker = '41I1FA000' AND ts >= $1 AND ts <= $2 AND last IS NOT NULL AND last > 0
			GROUP BY bucket
		)
		SELECT
			EXTRACT(EPOCH FROM v.bucket)::bigint as timestamp,
			v.value as vn30_value,
			f.value as hnx_value
		FROM vn30_buckets v
		INNER JOIN futures_buckets f ON v.bucket = f.bucket
		ORDER BY timestamp;
	`

	rows, err := r.pool.Query(ctx, query, startTime, endTime)
	if err != nil {
		r.log.WithError(err).Error("failed to query historical data")
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var results []repository.MarketData
	for rows.Next() {
		var data repository.MarketData
		if err := rows.Scan(&data.Timestamp, &data.VN30Value, &data.HNXValue); err != nil {
			r.log.WithError(err).Error("failed to scan row")
			return nil, fmt.Errorf("scan error: %w", err)
		}
		results = append(results, data)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	r.log.WithField("count", len(results)).Debug("retrieved historical data")
	return results, nil
}

// GetLatestData retrieves the most recent market data point
// Uses GetLast15sAverages to get latest averages from base tables
func (r *Repository) GetLatestData(ctx context.Context) (*repository.MarketData, error) {
	// Reuse the GetLast15sAverages method
	chartData, err := r.GetLast15sAverages(ctx, []string{"VN30"}, []string{"41I1FA000"})
	if err != nil {
		return nil, fmt.Errorf("failed to get latest averages: %w", err)
	}

	// Convert ChartData to MarketData
	data := &repository.MarketData{
		Timestamp: time.Now().Unix(),
	}

	for _, chart := range chartData {
		switch chart.Ticker {
		case "VN30":
			data.VN30Value = chart.Value
		case "41I1FA000":
			data.HNXValue = chart.Value
		}
	}

	return data, nil
}

// GetDataAtTimestamp retrieves market data for a specific 15s bucket
func (r *Repository) GetDataAtTimestamp(ctx context.Context, timestamp int64) (*repository.MarketData, error) {
	// Round timestamp to nearest 15s bucket
	bucketTimestamp := (timestamp / 15) * 15

	query := `
		SELECT
			EXTRACT(EPOCH FROM bucket)::bigint as timestamp,
			vn30_value,
			hnx_value
		FROM market_data_15s
		WHERE bucket = to_timestamp($1)
		LIMIT 1;
	`

	var data repository.MarketData
	err := r.pool.QueryRow(ctx, query, bucketTimestamp).Scan(&data.Timestamp, &data.VN30Value, &data.HNXValue)
	if err != nil {
		r.log.WithError(err).WithField("timestamp", bucketTimestamp).Debug("aggregate bucket not ready yet")
		return nil, fmt.Errorf("query bucket error: %w", err)
	}

	return &data, nil
}

// GetDataAfterTimestamp retrieves data after a specific timestamp
func (r *Repository) GetDataAfterTimestamp(ctx context.Context, afterTimestamp int64) ([]repository.MarketData, error) {
	startTime := time.Unix(afterTimestamp, 0)
	endTime := time.Now()

	r.log.WithFields(map[string]interface{}{
		"from": startTime,
		"to":   endTime,
	}).Debug("fetching data after timestamp")

	return r.GetHistoricalData(ctx, startTime, endTime)
}

// Close closes the database connection pool
func (r *Repository) Close() error {
	r.pool.Close()
	r.log.Info("PostgreSQL repository closed")
	return nil
}

// GetLast15sAverages retrieves AVG of last 15 seconds for multiple tickers
// Queries base tables directly with composite indexes for optimal performance
func (r *Repository) GetLast15sAverages(ctx context.Context, indexTickers []string, futuresTickers []string) ([]repository.ChartData, error) {
	var results []repository.ChartData

	// Query index_tick table for index tickers (VN30, HNX, etc.)
	if len(indexTickers) > 0 {
		query := `
			SELECT
				ticker,
				AVG(last) as avg_value
			FROM index_tick
			WHERE ticker = ANY($1)
				AND ts > NOW() - INTERVAL '15 seconds'
				AND last IS NOT NULL
				AND last > 0
			GROUP BY ticker;
		`

		rows, err := r.pool.Query(ctx, query, indexTickers)
		if err != nil {
			r.log.WithError(err).Error("failed to query index averages")
			return nil, fmt.Errorf("query index averages error: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var data repository.ChartData
			if err := rows.Scan(&data.Ticker, &data.Value); err != nil {
				r.log.WithError(err).Error("failed to scan index row")
				return nil, fmt.Errorf("scan error: %w", err)
			}
			results = append(results, data)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("rows error: %w", err)
		}
	}

	// Query futures_table for futures tickers (41I1FA000, etc.)
	if len(futuresTickers) > 0 {
		query := `
			SELECT
				ticker,
				AVG(last) as avg_value
			FROM futures_table
			WHERE ticker = ANY($1)
				AND ts > NOW() - INTERVAL '15 seconds'
				AND last IS NOT NULL
				AND last > 0
			GROUP BY ticker;
		`

		rows, err := r.pool.Query(ctx, query, futuresTickers)
		if err != nil {
			r.log.WithError(err).Error("failed to query futures averages")
			return nil, fmt.Errorf("query futures averages error: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var data repository.ChartData
			if err := rows.Scan(&data.Ticker, &data.Value); err != nil {
				r.log.WithError(err).Error("failed to scan futures row")
				return nil, fmt.Errorf("scan error: %w", err)
			}
			results = append(results, data)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("rows error: %w", err)
		}
	}

	r.log.WithField("count", len(results)).Debug("retrieved last 15s averages")
	return results, nil
}

// Stats returns pool statistics
func (r *Repository) Stats() *pgxpool.Stat {
	return r.pool.Stat()
}
