package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/config"
	"github.com/lnvi/market-shared/logger"
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

// GetHistoricalData retrieves market data for a time range with forward fill
// Queries base tables with time_bucket and uses window functions to forward fill missing data
func (r *Repository) GetHistoricalData(ctx context.Context, startTime, endTime time.Time) ([]repository.MarketData, error) {
	// Query with forward fill logic:
	// 1. Generate complete time series at 15-second intervals
	// 2. Aggregate actual data into 15-second buckets
	// 3. LEFT JOIN to preserve all time buckets
	// 4. Use LAST_VALUE() window function to forward fill missing values
	query := `
		WITH time_series AS (
			-- Generate complete time series at 15-second intervals
			SELECT generate_series(
				date_trunc('second', $1::timestamp),
				date_trunc('second', $2::timestamp),
				'15 seconds'::interval
			) AS bucket
		),
		vn30_buckets AS (
			-- Aggregate VN30 data into 15-second buckets
			SELECT
				time_bucket('15 seconds', ts) AS bucket,
				AVG(last) as value
			FROM index_tick
			WHERE ticker = 'VN30' AND ts >= $1 AND ts <= $2 AND last IS NOT NULL
			GROUP BY bucket
		),
		futures_buckets AS (
			-- Aggregate futures data into 15-second buckets
			SELECT
				time_bucket('15 seconds', ts) AS bucket,
				AVG(last) as value
			FROM futures_table
			WHERE f = 'f1' AND ts >= $1 AND ts <= $2 AND last IS NOT NULL
			GROUP BY bucket
		),
		joined_data AS (
			-- Join time series with actual data
			SELECT
				t.bucket,
				v.value as vn30_raw,
				f.value as hnx_raw
			FROM time_series t
			LEFT JOIN vn30_buckets v ON t.bucket = v.bucket
			LEFT JOIN futures_buckets f ON t.bucket = f.bucket
		),
		forward_filled AS (
			-- Forward fill using lateral subquery to get last available value from all history
			SELECT
				bucket,
				vn30_raw,
				hnx_raw,
				-- Forward fill VN30: use current value or last non-null value from entire history
				COALESCE(
					vn30_raw,
					(
						SELECT AVG(last)
						FROM index_tick
						WHERE ticker = 'VN30'
							AND ts <= joined_data.bucket
							AND last IS NOT NULL
						GROUP BY time_bucket('15 seconds', ts)
						ORDER BY time_bucket('15 seconds', ts) DESC
						LIMIT 1
					)
				) as vn30_filled,
				-- Forward fill HNX: use current value or last non-null value from entire history
				COALESCE(
					hnx_raw,
					(
						SELECT AVG(last)
						FROM futures_table
						WHERE f = 'f1'
							AND ts <= joined_data.bucket
							AND last IS NOT NULL
						GROUP BY time_bucket('15 seconds', ts)
						ORDER BY time_bucket('15 seconds', ts) DESC
						LIMIT 1
					)
				) as hnx_filled
			FROM joined_data
		)
		SELECT
			EXTRACT(EPOCH FROM bucket)::bigint as timestamp,
			vn30_filled as vn30_value,
			hnx_filled as hnx_value
		FROM forward_filled
		WHERE vn30_filled IS NOT NULL OR hnx_filled IS NOT NULL
		ORDER BY bucket;
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
		var vn30Val, hnxVal *float64

		if err := rows.Scan(&data.Timestamp, &vn30Val, &hnxVal); err != nil {
			r.log.WithError(err).Error("failed to scan row")
			return nil, fmt.Errorf("scan error: %w", err)
		}

		// Use 0 if still NULL (before first data point)
		if vn30Val != nil {
			data.VN30Value = *vn30Val
		}
		if hnxVal != nil {
			data.HNXValue = *hnxVal
		}

		results = append(results, data)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	r.log.WithField("count", len(results)).Debug("retrieved historical data with forward fill")
	return results, nil
}

// GetLatestData retrieves the most recent market data point
// Uses GetLast15sAverages to get latest averages from base tables
func (r *Repository) GetLatestData(ctx context.Context) (*repository.MarketData, error) {
	// Reuse the GetLast15sAverages method
	chartData, err := r.GetLast15sAverages(ctx, []string{"VN30"}, []string{"f1"})
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
		case "f1":
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

// GetLast15sAverages retrieves AVG of last 15 seconds for multiple tickers with forward fill
// If no data in last 15 seconds, uses the last known value to avoid gaps
func (r *Repository) GetLast15sAverages(ctx context.Context, indexTickers []string, futuresTickers []string) ([]repository.ChartData, error) {
	var results []repository.ChartData

	// Query index_tick table for index tickers (VN30, HNX, etc.) with forward fill
	if len(indexTickers) > 0 {
		query := `
			WITH recent_data AS (
				SELECT
					ticker,
					AVG(last) as avg_value
				FROM index_tick
				WHERE ticker = ANY($1)
					AND ts > NOW() - INTERVAL '15 seconds'
					AND last IS NOT NULL
				GROUP BY ticker
			),
			requested_tickers AS (
				SELECT unnest($1::text[]) as ticker
			)
			SELECT
				rt.ticker,
				COALESCE(
					rd.avg_value,
					(
						SELECT last
						FROM index_tick
						WHERE ticker = rt.ticker
							AND last IS NOT NULL
						ORDER BY ts DESC
						LIMIT 1
					)
				) as avg_value
			FROM requested_tickers rt
			LEFT JOIN recent_data rd ON rt.ticker = rd.ticker;
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

	// Query futures_table for futures tickers (f1, etc.) with forward fill
	if len(futuresTickers) > 0 {
		query := `
			WITH recent_data AS (
				SELECT
					f,
					AVG(last) as avg_value
				FROM futures_table
				WHERE f = ANY($1)
					AND ts > NOW() - INTERVAL '15 seconds'
					AND last IS NOT NULL
				GROUP BY f
			),
			requested_tickers AS (
				SELECT unnest($1::text[]) as ticker
			)
			SELECT
				rt.ticker,
				COALESCE(
					rd.avg_value,
					(
						SELECT last
						FROM futures_table
						WHERE f = rt.ticker
							AND last IS NOT NULL
						ORDER BY ts DESC
						LIMIT 1
					)
				) as avg_value
			FROM requested_tickers rt
			LEFT JOIN recent_data rd ON rt.ticker = rd.f;
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

	r.log.WithField("count", len(results)).Debug("retrieved last 15s averages with forward fill")
	return results, nil
}

// Stats returns pool statistics
func (r *Repository) Stats() *pgxpool.Stat {
	return r.pool.Stat()
}
