package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lnvi/market-shared/config"
	"github.com/lnvi/market-shared/logger"
	"github.com/redis/go-redis/v9"
)

type IndexTickRow struct {
	Timestamp     int64
	FormattedTime string
	Session       string
	Ticker        string
	Last          *float64
	Change        *float64
	PctChange     *float64
	MatchedVol    *float64
	MatchedVal    *float64
	Category      string
}

type FuturesRow struct {
	FormattedTime string
	Session       string
	Ticker        string
	F             string
	Last          *float64
	Change        *float64
	PctChange     *float64
	TotalVol      *float64
	TotalVal      *float64
	Timestamp     int64
	Category      string
}

func main() {
	// Load config
	cfg, err := config.Load("")
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// Create logger
	log := logger.New(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.TimeFormat)
	log.Info("starting market data generator from CSV files")

	// Connect to database
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, cfg.Database.DatabaseURL())
	if err != nil {
		log.WithError(err).Fatal("failed to connect to database")
	}
	defer pool.Close()

	log.Info("connected to database")

	// Connect to Redis
	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Address,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer redisClient.Close()

	// Test Redis connection
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.WithError(err).Fatal("failed to connect to Redis")
	}
	log.Info("connected to Redis")

	// Create context that listens for shutdown signals
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Info("shutdown signal received, stopping data generator")
		cancel()
	}()

	// Find CSV files
	indexCSV := "/app/data/20251003_index_tick.csv"
	futuresCSV := "/app/data/20251003_futures_second.csv"

	// Check if files exist
	if _, err := os.Stat(indexCSV); os.IsNotExist(err) {
		log.WithError(err).Fatal("index_tick CSV file not found")
	}
	if _, err := os.Stat(futuresCSV); os.IsNotExist(err) {
		log.WithError(err).Fatal("futures_second CSV file not found")
	}

	log.WithFields(map[string]interface{}{
		"index_csv":   indexCSV,
		"futures_csv": futuresCSV,
	}).Info("found CSV files")

	// Read CSV files
	indexRows, err := readIndexTickCSV(indexCSV, log)
	if err != nil {
		log.WithError(err).Fatal("failed to read index_tick CSV")
	}

	futuresRows, err := readFuturesCSV(futuresCSV, log)
	if err != nil {
		log.WithError(err).Fatal("failed to read futures_second CSV")
	}

	log.WithFields(map[string]interface{}{
		"index_rows":   len(indexRows),
		"futures_rows": len(futuresRows),
	}).Info("loaded CSV data")

	// Calculate time offset to shift CSV data to current date
	// CSV data is from Oct 3, 2025 - we shift it to today
	csvBaseDate := time.Date(2025, 10, 3, 0, 0, 0, 0, time.UTC)
	currentDate := time.Now().UTC().Truncate(24 * time.Hour)
	dateOffset := currentDate.Sub(csvBaseDate)

	log.WithFields(map[string]interface{}{
		"csv_base_date": csvBaseDate.Format("2006-01-02"),
		"current_date":  currentDate.Format("2006-01-02"),
		"offset_days":   dateOffset.Hours() / 24,
	}).Info("calculated time offset for CSV data")

	// Check current time in Vietnam timezone
	vietnamLocation := time.FixedZone("ICT", 7*60*60)
	now := time.Now().In(vietnamLocation)
	currentHour := now.Hour()
	currentMinute := now.Minute()
	currentSecond := now.Second()

	// Determine if we're in trading hours and should bulk insert historical data
	indexIdx := 0
	futuresIdx := 0

	if currentHour >= 9 && (currentHour < 14 || (currentHour == 14 && currentMinute <= 45)) {
		// We're in morning session (9:00 AM - 2:45 PM)
		// Bulk insert all data from 9:00 AM to current time
		log.Info("in trading hours - bulk inserting historical data from 9:00 AM to current time")

		// Calculate current time in HH:MM:SS format
		currentTimeInSeconds := currentHour*3600 + currentMinute*60 + currentSecond

		// Bulk insert index_tick data up to current time
		bulkInsertCount := 0
		for i, row := range indexRows {
			csvTime := time.UnixMilli(row.Timestamp).In(vietnamLocation)
			csvTimeInSeconds := csvTime.Hour()*3600 + csvTime.Minute()*60 + csvTime.Second()

			if csvTimeInSeconds <= currentTimeInSeconds {
				if err := insertIndexTick(ctx, pool, row, dateOffset, log); err != nil {
					log.WithError(err).Error("failed to bulk insert index tick")
				} else {
					bulkInsertCount++
				}
				indexIdx = i + 1
			} else {
				break
			}
		}
		log.WithField("count", bulkInsertCount).Info("bulk inserted index_tick historical data")

		// Bulk insert futures data up to current time
		bulkInsertCount = 0
		for i, row := range futuresRows {
			csvTime := time.UnixMilli(row.Timestamp).In(vietnamLocation)
			csvTimeInSeconds := csvTime.Hour()*3600 + csvTime.Minute()*60 + csvTime.Second()

			if csvTimeInSeconds <= currentTimeInSeconds {
				if err := insertFutures(ctx, pool, row, dateOffset, log); err != nil {
					log.WithError(err).Error("failed to bulk insert futures")
				} else {
					bulkInsertCount++
				}
				futuresIdx = i + 1
			} else {
				break
			}
		}
		log.WithField("count", bulkInsertCount).Info("bulk inserted futures historical data")

		log.WithFields(map[string]interface{}{
			"index_position":   indexIdx,
			"futures_position": futuresIdx,
			"current_time":     now.Format("15:04:05"),
		}).Info("bulk insert complete - starting real-time streaming from current position")

		// Backfill Redis stream with 15-second aggregated data
		log.Info("backfilling Redis stream with aggregated historical data")
		if err := backfillRedisStream(ctx, pool, redisClient, currentDate, log); err != nil {
			log.WithError(err).Warn("failed to backfill Redis stream (non-critical)")
		} else {
			log.Info("Redis stream backfill completed")
		}
	} else {
		log.Info("outside trading hours - will wait for session to start")
	}

	// Start real-time streaming
	insertInterval := 1 * time.Second
	ticker := time.NewTicker(insertInterval)
	defer ticker.Stop()

	log.WithField("interval", insertInterval).Info("starting real-time data insertion")

	// Session tracking
	var currentSession string
	var sessionStarted bool

	// If we already bulk inserted, mark session as started
	if indexIdx > 0 || futuresIdx > 0 {
		currentSession = "morning"
		sessionStarted = true
	}

	for {
		select {
		case <-ctx.Done():
			log.Info("data generator stopped")
			return

		case <-ticker.C:
			now := time.Now().In(vietnamLocation)
			currentHour := now.Hour()

			// Determine if we should start a new session
			if currentHour >= 9 && currentHour < 15 && currentSession != "morning" {
				// Start morning session (9:00 AM - before 3:00 PM)
				currentSession = "morning"
				if indexIdx == 0 && futuresIdx == 0 {
					// Only reset if we haven't bulk inserted
					indexIdx = 0
					futuresIdx = 0
				}
				sessionStarted = true
				log.Info("starting morning session (9:00 AM - 2:45 PM)")
			} else if currentHour >= 19 && currentSession != "evening" {
				// Start evening session (7:00 PM onwards)
				currentSession = "evening"
				indexIdx = 0
				futuresIdx = 0
				sessionStarted = true
				log.Info("starting evening session (7:00 PM)")
			} else if currentHour < 9 || (currentHour >= 15 && currentHour < 19) {
				// Before 9:00 AM or between 3:00 PM - 6:59 PM - reset session
				currentSession = ""
				sessionStarted = false
			}

			// Skip if no active session or CSV data exhausted
			if !sessionStarted {
				continue
			}

			// Check if both CSV datasets are exhausted
			if indexIdx >= len(indexRows) && futuresIdx >= len(futuresRows) {
				log.WithField("session", currentSession).Info("CSV data exhausted, waiting for next session")
				sessionStarted = false
				continue
			}

			// Insert all index tick rows with the same timestamp
			if indexIdx < len(indexRows) {
				currentTimestamp := indexRows[indexIdx].Timestamp
				insertedCount := 0

				// Find all rows with the same timestamp and insert them
				for indexIdx < len(indexRows) && indexRows[indexIdx].Timestamp == currentTimestamp {
					row := indexRows[indexIdx]
					if err := insertIndexTick(ctx, pool, row, dateOffset, log); err != nil {
						log.WithError(err).Error("failed to insert index tick")
					} else {
						var valueStr string
						if row.Last != nil {
							valueStr = fmt.Sprintf("%.2f", *row.Last)
						} else {
							valueStr = "NULL"
						}
						log.WithFields(map[string]interface{}{
							"ticker":    row.Ticker,
							"value":     valueStr,
							"index":     indexIdx,
							"total":     len(indexRows),
							"timestamp": currentTimestamp,
						}).Debug("inserted index tick")
					}
					indexIdx++
					insertedCount++
				}

				if insertedCount > 1 {
					log.WithFields(map[string]interface{}{
						"count":     insertedCount,
						"timestamp": currentTimestamp,
					}).Info("inserted multiple index rows with same timestamp")
				}
			}

			// Insert all futures rows with the same timestamp
			if futuresIdx < len(futuresRows) {
				currentTimestamp := futuresRows[futuresIdx].Timestamp
				insertedCount := 0

				// Find all rows with the same timestamp and insert them
				for futuresIdx < len(futuresRows) && futuresRows[futuresIdx].Timestamp == currentTimestamp {
					row := futuresRows[futuresIdx]
					if err := insertFutures(ctx, pool, row, dateOffset, log); err != nil {
						log.WithError(err).Error("failed to insert futures")
					} else {
						var valueStr string
						if row.Last != nil {
							valueStr = fmt.Sprintf("%.2f", *row.Last)
						} else {
							valueStr = "NULL"
						}
						log.WithFields(map[string]interface{}{
							"ticker":    row.Ticker,
							"value":     valueStr,
							"index":     futuresIdx,
							"total":     len(futuresRows),
							"timestamp": currentTimestamp,
						}).Debug("inserted futures")
					}
					futuresIdx++
					insertedCount++
				}

				if insertedCount > 1 {
					log.WithFields(map[string]interface{}{
						"count":     insertedCount,
						"timestamp": currentTimestamp,
					}).Info("inserted multiple futures rows with same timestamp")
				}
			}
		}
	}
}

func readIndexTickCSV(filePath string, log *logger.Logger) ([]IndexTickRow, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// Map column names to indices
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	var rows []IndexTickRow
	lineNum := 1

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.WithError(err).WithField("line", lineNum).Warn("failed to read CSV line")
			lineNum++
			continue
		}

		row := IndexTickRow{}

		// Parse timestamp
		if val, err := strconv.ParseInt(record[colMap["timestamp"]], 10, 64); err == nil {
			row.Timestamp = val
		}

		row.FormattedTime = record[colMap["formatted_time"]]
		row.Session = record[colMap["session"]]
		row.Ticker = record[colMap["ticker"]]

		// Parse numeric fields - use pointers to properly handle NULL values
		if val, err := strconv.ParseFloat(record[colMap["last"]], 64); err == nil {
			row.Last = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["change"]], 64); err == nil {
			row.Change = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["pct_change"]], 64); err == nil {
			row.PctChange = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["matched_vol"]], 64); err == nil {
			row.MatchedVol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["matched_val"]], 64); err == nil {
			row.MatchedVal = &val
		}

		row.Category = record[colMap["category"]]

		rows = append(rows, row)
		lineNum++
	}

	return rows, nil
}

func readFuturesCSV(filePath string, log *logger.Logger) ([]FuturesRow, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	header, err := reader.Read()
	if err != nil {
		return nil, err
	}

	// Map column names to indices
	colMap := make(map[string]int)
	for i, col := range header {
		colMap[col] = i
	}

	var rows []FuturesRow
	lineNum := 1

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.WithError(err).WithField("line", lineNum).Warn("failed to read CSV line")
			lineNum++
			continue
		}

		row := FuturesRow{}

		row.FormattedTime = record[colMap["formatted_time"]]
		row.Session = record[colMap["session"]]
		row.Ticker = record[colMap["ticker"]]
		row.F = record[colMap["f"]]

		// Parse timestamp
		if val, err := strconv.ParseInt(record[colMap["timestamp"]], 10, 64); err == nil {
			row.Timestamp = val
		}

		// Parse numeric fields - use pointers to properly handle NULL values
		if val, err := strconv.ParseFloat(record[colMap["last"]], 64); err == nil {
			row.Last = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["change"]], 64); err == nil {
			row.Change = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["pct_change"]], 64); err == nil {
			row.PctChange = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_vol"]], 64); err == nil {
			row.TotalVol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_val"]], 64); err == nil {
			row.TotalVal = &val
		}

		row.Category = record[colMap["category"]]

		rows = append(rows, row)
		lineNum++
	}

	return rows, nil
}

func insertIndexTick(ctx context.Context, pool *pgxpool.Pool, row IndexTickRow, timeOffset time.Duration, log *logger.Logger) error {
	// Apply dynamic time offset to shift CSV data to current date
	csvTimestamp := time.UnixMilli(row.Timestamp)
	shiftedTimestamp := csvTimestamp.Add(timeOffset)

	query := `
		INSERT INTO index_tick (
			ts, timestamp, formatted_time, session, ticker,
			last, change, pct_change, matched_vol, matched_val, category
		)
		VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10, $11
		)
	`

	_, err := pool.Exec(ctx, query,
		shiftedTimestamp,
		shiftedTimestamp.UnixMilli(),
		row.FormattedTime,
		row.Session,
		row.Ticker,
		row.Last,
		row.Change,
		row.PctChange,
		row.MatchedVol,
		row.MatchedVal,
		row.Category,
	)

	return err
}

func insertFutures(ctx context.Context, pool *pgxpool.Pool, row FuturesRow, timeOffset time.Duration, log *logger.Logger) error {
	// Apply dynamic time offset to shift CSV data to current date
	csvTimestamp := time.UnixMilli(row.Timestamp)
	shiftedTimestamp := csvTimestamp.Add(timeOffset)

	query := `
		INSERT INTO futures_table (
			ts, timestamp, formatted_time, session, ticker, f,
			last, change, pct_change, total_vol, total_val, category
		)
		VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12
		)
	`

	_, err := pool.Exec(ctx, query,
		shiftedTimestamp,
		shiftedTimestamp.UnixMilli(),
		row.FormattedTime,
		row.Session,
		row.Ticker,
		row.F,
		row.Last,
		row.Change,
		row.PctChange,
		row.TotalVol,
		row.TotalVal,
		row.Category,
	)

	return err
}

// backfillRedisStream queries PostgreSQL for 15-second aggregated data and writes to Redis stream
// Uses the same query logic as GetHistoricalData() to ensure consistency
func backfillRedisStream(ctx context.Context, pool *pgxpool.Pool, redisClient *redis.Client, currentDate time.Time, log *logger.Logger) error {
	// Query for aggregated data from 9:00 AM to current time
	startOfDay := time.Date(currentDate.Year(), currentDate.Month(), currentDate.Day(), 2, 0, 0, 0, time.UTC) // 9:00 AM Vietnam = 02:00 UTC
	endTime := time.Now().UTC()

	// Same query as GetHistoricalData() in market-service/internal/repository/postgres/repository.go
	query := `
		WITH time_series AS (
			-- Generate complete time series at 15-second intervals
			-- Exclude break time 11:30 AM - 12:59:55 PM Vietnam time (04:30 - 05:59:55 UTC)
			SELECT ts AS bucket
			FROM generate_series(
				date_trunc('second', $1::timestamp),
				date_trunc('second', $2::timestamp),
				'15 seconds'::interval
			) AS ts
			WHERE NOT (
				EXTRACT(HOUR FROM ts AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Bangkok') = 11
				AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Bangkok') >= 30
			)
			AND NOT (EXTRACT(HOUR FROM ts AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Bangkok') = 12)
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

	rows, err := pool.Query(ctx, query, startOfDay, endTime)
	if err != nil {
		return fmt.Errorf("failed to query aggregated data: %w", err)
	}
	defer rows.Close()

	count := 0
	streamKey := "market:stream"

	for rows.Next() {
		var timestamp int64
		var vn30Value, hnxValue *float64

		if err := rows.Scan(&timestamp, &vn30Value, &hnxValue); err != nil {
			log.WithError(err).Warn("failed to scan row")
			continue
		}

		// Skip if both values are NULL
		if vn30Value == nil && hnxValue == nil {
			continue
		}

		// Use 0 for NULL values
		vn30 := float64(0)
		hnx := float64(0)
		if vn30Value != nil {
			vn30 = *vn30Value
		}
		if hnxValue != nil {
			hnx = *hnxValue
		}

		// Write to Redis stream
		err := redisClient.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: 10000, // Keep enough for full trading day (6 hours * 240 points/hour = 1440, so 10000 is plenty)
			Approx: true,
			Values: map[string]interface{}{
				"timestamp": timestamp,
				"vn30":      vn30,
				"hnx":       hnx,
			},
		}).Err()

		if err != nil {
			log.WithError(err).Warn("failed to write to Redis stream")
		} else {
			count++
		}
	}

	log.WithField("count", count).Info("wrote aggregated data points to Redis stream")
	return nil
}
