package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
	FormattedTime  string
	Session        string
	Ticker         string
	F              string
	Last           *float64
	Change         *float64
	PctChange      *float64
	TotalVol       *float64
	TotalVal       *float64
	TotalFBuyVol   *float64 // total_f_buy_vol
	TotalFSellVol  *float64 // total_f_sell_vol
	TotalBid       *float64 // total_bid
	TotalAsk       *float64 // total_ask
	Timestamp      int64
	Category       string
}

type HOSE500SecondRow struct {
	Timestamp     int64
	FormattedTime string
	Session       string
	Ticker        string
	OrderType     *string  // "Buy" or "Sell" or NULL
	Last          *float64
	Change        *float64
	PctChange     *float64
	TotalVol      *float64
	TotalVal      *float64
	MatchedVol    *float64
	MatchedVal    *float64
	Bid1          *float64
	Bid1Vol       *float64
	Bid2          *float64
	Bid2Vol       *float64
	Bid3          *float64
	Bid3Vol       *float64
	Ask1          *float64
	Ask1Vol       *float64
	Ask2          *float64
	Ask2Vol       *float64
	Ask3          *float64
	Ask3Vol       *float64
	TotalFBuyVol  *float64 // total_f_buy_vol
	TotalFSellVol *float64 // total_f_sell_vol
	TotalFBuyVal  *float64 // total_f_buy_val
	TotalFSellVal *float64 // total_f_sell_val
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
	hose500CSV := "/app/data/20251003_hose500_second.csv"

	// Check if files exist
	if _, err := os.Stat(indexCSV); os.IsNotExist(err) {
		log.WithError(err).Fatal("index_tick CSV file not found")
	}
	if _, err := os.Stat(futuresCSV); os.IsNotExist(err) {
		log.WithError(err).Fatal("futures_second CSV file not found")
	}
	if _, err := os.Stat(hose500CSV); os.IsNotExist(err) {
		log.WithError(err).Warn("hose500_second CSV file not found (non-critical)")
	}

	log.WithFields(map[string]interface{}{
		"index_csv":   indexCSV,
		"futures_csv": futuresCSV,
		"hose500_csv": hose500CSV,
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

	var hose500Rows []HOSE500SecondRow
	if _, err := os.Stat(hose500CSV); err == nil {
		hose500Rows, err = readHOSE500SecondCSV(hose500CSV, log)
		if err != nil {
			log.WithError(err).Warn("failed to read hose500_second CSV (non-critical)")
			hose500Rows = []HOSE500SecondRow{}
		}
	}

	log.WithFields(map[string]interface{}{
		"index_rows":   len(indexRows),
		"futures_rows": len(futuresRows),
		"hose500_rows": len(hose500Rows),
	}).Info("loaded CSV data")

	// Calculate time offset to shift CSV data to current date
	// CSV data is from Oct 3, 2025 - we shift it to today
	csvBaseDate := time.Date(2025, 10, 3, 0, 0, 0, 0, time.UTC)
	nowUTC := time.Now().UTC()
	currentDate := time.Date(nowUTC.Year(), nowUTC.Month(), nowUTC.Day(), 0, 0, 0, 0, time.UTC)
	dateOffset := currentDate.Sub(csvBaseDate)

	log.WithFields(map[string]interface{}{
		"csv_base_date":       csvBaseDate.Format("2006-01-02 15:04:05 MST"),
		"csv_base_unix":       csvBaseDate.Unix(),
		"current_date":        currentDate.Format("2006-01-02 15:04:05 MST"),
		"current_date_unix":   currentDate.Unix(),
		"offset_days":         dateOffset.Hours() / 24,
		"offset_seconds":      int64(dateOffset.Seconds()),
		"csv_sample_original": 1759456800,
		"csv_sample_expected": 1759456800 + int64(dateOffset.Seconds()),
	}).Info("calculated time offset for CSV data")

	// Check current time in Vietnam timezone
	vietnamLocation := time.FixedZone("ICT", 7*60*60)
	now := time.Now().In(vietnamLocation)
	currentHour := now.Hour()
	currentMinute := now.Minute()
	currentSecond := now.Second()

	// Check if we have data for today - if not, clear old data and re-import
	var todayDataCount int
	checkQuery := `SELECT COUNT(*) FROM index_tick WHERE ts >= $1::date`
	if err := pool.QueryRow(ctx, checkQuery, currentDate).Scan(&todayDataCount); err != nil {
		log.WithError(err).Warn("failed to check for today's data")
	}

	if todayDataCount == 0 && currentHour >= 9 {
		// No data for today and it's past 9 AM - clear old data and prepare for fresh import
		log.Info("no data found for today - clearing old data and preparing fresh import")

		truncateQuery := `TRUNCATE index_tick, futures_table, hose500_second CASCADE;`
		if _, err := pool.Exec(ctx, truncateQuery); err != nil {
			log.WithError(err).Warn("failed to truncate old data (non-critical)")
		} else {
			log.Info("old data cleared successfully")
		}
	}

	// Determine if we should bulk insert historical data
	// Bulk insert anytime from 9:00 AM onwards
	indexIdx := 0
	futuresIdx := 0
	hose500Idx := 0

	if currentHour >= 9 {
		// Bulk insert data from 9:00 AM up to current time (or 14:45 if past market close)
		log.Info("bulk inserting historical data from 9:00 AM")

		// Calculate target time in HH:MM:SS format
		// Cap at 14:45 (market close time) if current time is past that
		var targetTimeInSeconds int
		if currentHour > 14 || (currentHour == 14 && currentMinute >= 45) {
			// Past 14:45, insert up to end of trading day
			targetTimeInSeconds = 14*3600 + 45*60 // 14:45:00
			log.Info("past market close - bulk inserting up to 14:45")
		} else {
			// During market hours, insert up to current time
			targetTimeInSeconds = currentHour*3600 + currentMinute*60 + currentSecond
			log.WithField("target_time", now.Format("15:04:05")).Info("during market hours - bulk inserting up to current time")
		}

		// Bulk insert index_tick data up to target time (in batches of 1000)
		bulkInsertCount := 0
		batchSize := 1000
		batch := make([]IndexTickRow, 0, batchSize)

		for i, row := range indexRows {
			csvTime := time.UnixMilli(row.Timestamp).In(vietnamLocation)
			csvTimeInSeconds := csvTime.Hour()*3600 + csvTime.Minute()*60 + csvTime.Second()

			if csvTimeInSeconds <= targetTimeInSeconds {
				batch = append(batch, row)
				indexIdx = i + 1

				// Insert batch when it reaches 1000 rows
				if len(batch) >= batchSize {
					if err := batchInsertIndexTick(ctx, pool, batch, dateOffset, log); err != nil {
						log.WithError(err).Error("failed to batch insert index tick")
					} else {
						bulkInsertCount += len(batch)
						log.WithField("count", bulkInsertCount).Debug("batch inserted index_tick rows")
					}
					batch = make([]IndexTickRow, 0, batchSize)
				}
			} else {
				break
			}
		}

		// Insert remaining rows in the batch
		if len(batch) > 0 {
			if err := batchInsertIndexTick(ctx, pool, batch, dateOffset, log); err != nil {
				log.WithError(err).Error("failed to batch insert index tick (final batch)")
			} else {
				bulkInsertCount += len(batch)
			}
		}
		log.WithField("count", bulkInsertCount).Info("bulk inserted index_tick historical data")

		// Bulk insert futures data up to target time (in batches of 1000)
		bulkInsertCount = 0
		futuresBatch := make([]FuturesRow, 0, batchSize)

		for i, row := range futuresRows {
			csvTime := time.UnixMilli(row.Timestamp).In(vietnamLocation)
			csvTimeInSeconds := csvTime.Hour()*3600 + csvTime.Minute()*60 + csvTime.Second()

			if csvTimeInSeconds <= targetTimeInSeconds {
				futuresBatch = append(futuresBatch, row)
				futuresIdx = i + 1

				// Insert batch when it reaches 1000 rows
				if len(futuresBatch) >= batchSize {
					if err := batchInsertFutures(ctx, pool, futuresBatch, dateOffset, log); err != nil {
						log.WithError(err).Error("failed to batch insert futures")
					} else {
						bulkInsertCount += len(futuresBatch)
						log.WithField("count", bulkInsertCount).Debug("batch inserted futures rows")
					}
					futuresBatch = make([]FuturesRow, 0, batchSize)
				}
			} else {
				break
			}
		}

		// Insert remaining rows in the batch
		if len(futuresBatch) > 0 {
			if err := batchInsertFutures(ctx, pool, futuresBatch, dateOffset, log); err != nil {
				log.WithError(err).Error("failed to batch insert futures (final batch)")
			} else {
				bulkInsertCount += len(futuresBatch)
			}
		}
		log.WithField("count", bulkInsertCount).Info("bulk inserted futures historical data")

		// Bulk insert hose500_second data up to target time (in batches of 1000)
		if len(hose500Rows) > 0 {
			bulkInsertCount = 0
			hose500Batch := make([]HOSE500SecondRow, 0, batchSize)

			for i, row := range hose500Rows {
				csvTime := time.UnixMilli(row.Timestamp).In(vietnamLocation)
				csvTimeInSeconds := csvTime.Hour()*3600 + csvTime.Minute()*60 + csvTime.Second()

				if csvTimeInSeconds <= targetTimeInSeconds {
					hose500Batch = append(hose500Batch, row)
					hose500Idx = i + 1

					// Insert batch when it reaches 1000 rows
					if len(hose500Batch) >= batchSize {
						if err := batchInsertHOSE500Second(ctx, pool, hose500Batch, dateOffset, log); err != nil {
							log.WithError(err).Error("failed to batch insert hose500_second")
						} else {
							bulkInsertCount += len(hose500Batch)
							log.WithField("count", bulkInsertCount).Debug("batch inserted hose500_second rows")
						}
						hose500Batch = make([]HOSE500SecondRow, 0, batchSize)
					}
				} else {
					break
				}
			}

			// Insert remaining rows in the batch
			if len(hose500Batch) > 0 {
				if err := batchInsertHOSE500Second(ctx, pool, hose500Batch, dateOffset, log); err != nil {
					log.WithError(err).Error("failed to batch insert hose500_second (final batch)")
				} else {
					bulkInsertCount += len(hose500Batch)
				}
			}
			log.WithField("count", bulkInsertCount).Info("bulk inserted hose500_second historical data")
		}

		log.WithFields(map[string]interface{}{
			"index_position":    indexIdx,
			"futures_position":  futuresIdx,
			"hose500_position":  hose500Idx,
			"current_time":      now.Format("15:04:05"),
		}).Info("bulk insert complete - starting real-time streaming from current position")

		// Refresh continuous aggregates to include data from 9:00 AM today
		log.Info("refreshing continuous aggregates to include today's data from market open")
		marketOpenTime := time.Date(currentDate.Year(), currentDate.Month(), currentDate.Day(), 2, 0, 0, 0, time.UTC) // 9:00 AM Vietnam = 2:00 AM UTC

		refreshQueries := []string{
			fmt.Sprintf("CALL refresh_continuous_aggregate('index_tick_15s_cagg', '%s'::timestamptz, '%s'::timestamptz)",
				marketOpenTime.Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)),
			fmt.Sprintf("CALL refresh_continuous_aggregate('futures_15s_cagg', '%s'::timestamptz, '%s'::timestamptz)",
				marketOpenTime.Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)),
			fmt.Sprintf("CALL refresh_continuous_aggregate('vn30_15s_cagg', '%s'::timestamptz, '%s'::timestamptz)",
				marketOpenTime.Format(time.RFC3339), time.Now().UTC().Format(time.RFC3339)),
		}

		for _, query := range refreshQueries {
			if _, err := pool.Exec(ctx, query); err != nil {
				log.WithError(err).Warn("failed to refresh continuous aggregate (non-critical)")
			}
		}
		log.Info("continuous aggregates refreshed successfully")

		// Backfill Redis stream with 15-second aggregated data
		log.Info("backfilling Redis stream with aggregated historical data")
		if err := backfillRedisStream(ctx, pool, redisClient, currentDate, log); err != nil {
			log.WithError(err).Warn("failed to backfill Redis stream (non-critical)")
		} else {
			log.Info("Redis stream backfill completed")
		}
	} else {
		log.WithField("current_hour", currentHour).Info("before market hours (00:00-08:59) - skipping bulk insert, will wait for 9:00 AM")
	}

	// Start real-time streaming
	log.Info("starting real-time data insertion")

	// Session tracking
	var currentSession string
	var sessionStarted bool

	// If we already bulk inserted, mark session as started
	if indexIdx > 0 || futuresIdx > 0 || hose500Idx > 0 {
		currentSession = "morning"
		sessionStarted = true
	}

	// Track previous timestamp to calculate sleep duration
	var lastInsertTime int64 = 0

	for {
		select {
		case <-ctx.Done():
			log.Info("data generator stopped")
			return

		default:
			now := time.Now().In(vietnamLocation)
			currentHour := now.Hour()

			// Determine if we should start a new session
			if currentHour >= 9 && currentHour < 15 && currentSession != "morning" {
				// Start trading session (9:00 AM - before 3:00 PM)
				currentSession = "morning"
				if indexIdx == 0 && futuresIdx == 0 {
					// Only reset if we haven't bulk inserted
					indexIdx = 0
					futuresIdx = 0
				}
				sessionStarted = true
				lastInsertTime = 0 // Reset timing on session start
				log.Info("starting trading session (9:00 AM - 2:45 PM)")
			} else if currentHour < 9 || currentHour >= 15 {
				// Before 9:00 AM or after 3:00 PM - end trading session
				if sessionStarted {
					log.WithFields(map[string]interface{}{
						"current_hour":      currentHour,
						"index_position":    indexIdx,
						"futures_position":  futuresIdx,
						"hose500_position":  hose500Idx,
					}).Info("market closed - stopping data insertion until next trading day")
				}
				currentSession = ""
				sessionStarted = false
			}

			// Skip if no active session or CSV data exhausted
			if !sessionStarted {
				time.Sleep(1 * time.Second) // Check again in 1 second
				continue
			}

			// Check if all CSV datasets are exhausted
			if indexIdx >= len(indexRows) && futuresIdx >= len(futuresRows) && hose500Idx >= len(hose500Rows) {
				log.WithField("session", currentSession).Info("CSV data exhausted, waiting for next session")
				sessionStarted = false
				time.Sleep(1 * time.Second) // Check again in 1 second
				continue
			}

			// Track the earliest timestamp we're about to insert
			var currentTimestamp int64 = 0

			// Get next timestamps from all datasets
			var nextIndexTimestamp int64 = 0
			var nextFuturesTimestamp int64 = 0
			var nextHOSE500Timestamp int64 = 0

			if indexIdx < len(indexRows) {
				nextIndexTimestamp = indexRows[indexIdx].Timestamp
			}
			if futuresIdx < len(futuresRows) {
				nextFuturesTimestamp = futuresRows[futuresIdx].Timestamp
			}
			if hose500Idx < len(hose500Rows) {
				nextHOSE500Timestamp = hose500Rows[hose500Idx].Timestamp
			}

			// Find the earliest timestamp to process across all datasets
			timestamps := []int64{}
			if nextIndexTimestamp > 0 {
				timestamps = append(timestamps, nextIndexTimestamp)
			}
			if nextFuturesTimestamp > 0 {
				timestamps = append(timestamps, nextFuturesTimestamp)
			}
			if nextHOSE500Timestamp > 0 {
				timestamps = append(timestamps, nextHOSE500Timestamp)
			}

			if len(timestamps) > 0 {
				currentTimestamp = timestamps[0]
				for _, ts := range timestamps {
					if ts < currentTimestamp {
						currentTimestamp = ts
					}
				}
			}

			// Calculate target real time for this CSV data point
			// Apply date offset to get today's timestamp (same logic as bulk insert)
			csvTimestamp := time.UnixMilli(currentTimestamp)
			targetTime := csvTimestamp.Add(dateOffset) // Correctly add Duration

			// Check if this CSV timestamp falls within market hours (9:00 AM - 2:45 PM Vietnam time)
			targetTimeVietnam := targetTime.In(vietnamLocation)
			targetHour := targetTimeVietnam.Hour()
			targetMinute := targetTimeVietnam.Minute()

			// Market hours: 9:00 AM - 2:45 PM (14:45)
			isWithinMarketHours := (targetHour >= 9 && targetHour < 14) || (targetHour == 14 && targetMinute <= 45)

			if !isWithinMarketHours {
				// Skip CSV data outside market hours
				log.WithFields(map[string]interface{}{
					"csv_time_vietnam": targetTimeVietnam.Format("15:04:05"),
					"csv_timestamp":    currentTimestamp,
				}).Debug("skipping CSV data outside market hours (before 9:00 AM or after 2:45 PM)")

				// Advance all indices to skip this timestamp
				if indexIdx < len(indexRows) && indexRows[indexIdx].Timestamp == currentTimestamp {
					for indexIdx < len(indexRows) && indexRows[indexIdx].Timestamp == currentTimestamp {
						indexIdx++
					}
				}
				if futuresIdx < len(futuresRows) && futuresRows[futuresIdx].Timestamp == currentTimestamp {
					for futuresIdx < len(futuresRows) && futuresRows[futuresIdx].Timestamp == currentTimestamp {
						futuresIdx++
					}
				}
				if hose500Idx < len(hose500Rows) && hose500Rows[hose500Idx].Timestamp == currentTimestamp {
					for hose500Idx < len(hose500Rows) && hose500Rows[hose500Idx].Timestamp == currentTimestamp {
						hose500Idx++
					}
				}
				continue
			}

			currentRealTime := time.Now()

			// If target time is in the future, wait until that time
			if targetTime.After(currentRealTime) {
				sleepDuration := targetTime.Sub(currentRealTime)
				log.WithFields(map[string]interface{}{
					"sleep_ms":      sleepDuration.Milliseconds(),
					"target_time":   targetTime.Format("15:04:05.000"),
					"current_time":  currentRealTime.Format("15:04:05.000"),
					"csv_timestamp": currentTimestamp,
				}).Debug("waiting until target real time")
				time.Sleep(sleepDuration)
			} else if lastInsertTime > 0 {
				// If we're behind, log how far behind we are
				behindBy := currentRealTime.Sub(targetTime)
				if behindBy > 5*time.Second {
					log.WithFields(map[string]interface{}{
						"behind_seconds": behindBy.Seconds(),
						"target_time":    targetTime.Format("15:04:05.000"),
						"current_time":   currentRealTime.Format("15:04:05.000"),
					}).Warn("catching up - inserting data from the past")
				}
			}

			// Insert all index tick rows with the same timestamp
			if indexIdx < len(indexRows) && indexRows[indexIdx].Timestamp == currentTimestamp {
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
			if futuresIdx < len(futuresRows) && futuresRows[futuresIdx].Timestamp == currentTimestamp {
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

			// Insert all hose500_second rows with the same timestamp
			if hose500Idx < len(hose500Rows) && hose500Rows[hose500Idx].Timestamp == currentTimestamp {
				insertedCount := 0

				// Find all rows with the same timestamp and insert them
				for hose500Idx < len(hose500Rows) && hose500Rows[hose500Idx].Timestamp == currentTimestamp {
					row := hose500Rows[hose500Idx]
					if err := insertHOSE500Second(ctx, pool, row, dateOffset, log); err != nil {
						log.WithError(err).Error("failed to insert hose500_second")
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
							"index":     hose500Idx,
							"total":     len(hose500Rows),
							"timestamp": currentTimestamp,
						}).Debug("inserted hose500_second")
					}
					hose500Idx++
					insertedCount++
				}

				if insertedCount > 1 {
					log.WithFields(map[string]interface{}{
						"count":     insertedCount,
						"timestamp": currentTimestamp,
					}).Info("inserted multiple hose500_second rows with same timestamp")
				}
			}

			// Update last insert time for next iteration
			lastInsertTime = currentTimestamp
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
		if val, err := strconv.ParseFloat(record[colMap["total_f_buy_vol"]], 64); err == nil {
			row.TotalFBuyVol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_f_sell_vol"]], 64); err == nil {
			row.TotalFSellVol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_bid"]], 64); err == nil {
			row.TotalBid = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_ask"]], 64); err == nil {
			row.TotalAsk = &val
		}

		row.Category = record[colMap["category"]]

		rows = append(rows, row)
		lineNum++
	}

	return rows, nil
}

func insertIndexTick(ctx context.Context, pool *pgxpool.Pool, row IndexTickRow, timeOffset time.Duration, log *logger.Logger) error {
	// Apply dynamic time offset to shift CSV data to current date
	// Ensure timestamp is explicitly in UTC to avoid timezone issues
	csvTimestamp := time.UnixMilli(row.Timestamp).UTC()
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
		ON CONFLICT DO NOTHING
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
	// Ensure timestamp is explicitly in UTC to avoid timezone issues
	csvTimestamp := time.UnixMilli(row.Timestamp).UTC()
	shiftedTimestamp := csvTimestamp.Add(timeOffset)

	query := `
		INSERT INTO futures_table (
			ts, timestamp, formatted_time, session, ticker, f,
			last, change, pct_change, total_vol, total_val,
			total_f_buy_vol, total_f_sell_vol, total_bid, total_ask,
			category
		)
		VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11,
			$12, $13, $14, $15,
			$16
		)
		ON CONFLICT DO NOTHING
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
		row.TotalFBuyVol,
		row.TotalFSellVol,
		row.TotalBid,
		row.TotalAsk,
		row.Category,
	)

	return err
}

// batchInsertIndexTick inserts multiple index_tick rows in a single database call
func batchInsertIndexTick(ctx context.Context, pool *pgxpool.Pool, rows []IndexTickRow, timeOffset time.Duration, log *logger.Logger) error {
	if len(rows) == 0 {
		return nil
	}

	// Build multi-row INSERT statement
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*11)

	for i, row := range rows {
		csvTimestamp := time.UnixMilli(row.Timestamp).UTC()
		shiftedTimestamp := csvTimestamp.Add(timeOffset)

		// Debug log first row
		if i == 0 {
			log.WithFields(map[string]interface{}{
				"csv_ts_ms":      row.Timestamp,
				"csv_ts":         csvTimestamp.Format("2006-01-02 15:04:05 UTC"),
				"csv_unix":       csvTimestamp.Unix(),
				"offset_seconds": int64(timeOffset.Seconds()),
				"shifted_ts":     shiftedTimestamp.Format("2006-01-02 15:04:05 UTC"),
				"shifted_unix":   shiftedTimestamp.Unix(),
			}).Info("first batch insert row")
		}

		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*11+1, i*11+2, i*11+3, i*11+4, i*11+5,
			i*11+6, i*11+7, i*11+8, i*11+9, i*11+10, i*11+11,
		))

		valueArgs = append(valueArgs,
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
	}

	query := fmt.Sprintf(`
		INSERT INTO index_tick (
			ts, timestamp, formatted_time, session, ticker,
			last, change, pct_change, matched_vol, matched_val, category
		) VALUES %s
		ON CONFLICT DO NOTHING
	`, strings.Join(valueStrings, ","))

	_, err := pool.Exec(ctx, query, valueArgs...)
	return err
}

// batchInsertFutures inserts multiple futures rows in a single database call
func batchInsertFutures(ctx context.Context, pool *pgxpool.Pool, rows []FuturesRow, timeOffset time.Duration, log *logger.Logger) error {
	if len(rows) == 0 {
		return nil
	}

	// Build multi-row INSERT statement
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*16)

	for i, row := range rows {
		csvTimestamp := time.UnixMilli(row.Timestamp).UTC()
		shiftedTimestamp := csvTimestamp.Add(timeOffset)

		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*16+1, i*16+2, i*16+3, i*16+4, i*16+5, i*16+6,
			i*16+7, i*16+8, i*16+9, i*16+10, i*16+11, i*16+12,
			i*16+13, i*16+14, i*16+15, i*16+16,
		))

		valueArgs = append(valueArgs,
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
			row.TotalFBuyVol,
			row.TotalFSellVol,
			row.TotalBid,
			row.TotalAsk,
			row.Category,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO futures_table (
			ts, timestamp, formatted_time, session, ticker,
			f, last, change, pct_change, total_vol, total_val,
			total_f_buy_vol, total_f_sell_vol, total_bid, total_ask,
			category
		) VALUES %s
		ON CONFLICT DO NOTHING
	`, strings.Join(valueStrings, ","))

	_, err := pool.Exec(ctx, query, valueArgs...)
	return err
}

func readHOSE500SecondCSV(filePath string, log *logger.Logger) ([]HOSE500SecondRow, error) {
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

	var rows []HOSE500SecondRow
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

		row := HOSE500SecondRow{}

		// Parse timestamp
		if val, err := strconv.ParseInt(record[colMap["timestamp"]], 10, 64); err == nil {
			row.Timestamp = val
		}

		row.FormattedTime = record[colMap["formatted_time"]]
		row.Session = record[colMap["session"]]
		row.Ticker = record[colMap["ticker"]]

		// Parse order_type (nullable)
		if orderType := record[colMap["order_type"]]; orderType != "" {
			row.OrderType = &orderType
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
		if val, err := strconv.ParseFloat(record[colMap["matched_vol"]], 64); err == nil {
			row.MatchedVol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["matched_val"]], 64); err == nil {
			row.MatchedVal = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["bid1"]], 64); err == nil {
			row.Bid1 = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["bid1_vol"]], 64); err == nil {
			row.Bid1Vol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["bid2"]], 64); err == nil {
			row.Bid2 = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["bid2_vol"]], 64); err == nil {
			row.Bid2Vol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["bid3"]], 64); err == nil {
			row.Bid3 = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["bid3_vol"]], 64); err == nil {
			row.Bid3Vol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["ask1"]], 64); err == nil {
			row.Ask1 = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["ask1_vol"]], 64); err == nil {
			row.Ask1Vol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["ask2"]], 64); err == nil {
			row.Ask2 = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["ask2_vol"]], 64); err == nil {
			row.Ask2Vol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["ask3"]], 64); err == nil {
			row.Ask3 = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["ask3_vol"]], 64); err == nil {
			row.Ask3Vol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_f_buy_vol"]], 64); err == nil {
			row.TotalFBuyVol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_f_sell_vol"]], 64); err == nil {
			row.TotalFSellVol = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_f_buy_val"]], 64); err == nil {
			row.TotalFBuyVal = &val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_f_sell_val"]], 64); err == nil {
			row.TotalFSellVal = &val
		}

		row.Category = record[colMap["category"]]

		rows = append(rows, row)
		lineNum++
	}

	return rows, nil
}

func insertHOSE500Second(ctx context.Context, pool *pgxpool.Pool, row HOSE500SecondRow, timeOffset time.Duration, log *logger.Logger) error {
	// Apply dynamic time offset to shift CSV data to current date
	csvTimestamp := time.UnixMilli(row.Timestamp).UTC()
	shiftedTimestamp := csvTimestamp.Add(timeOffset)

	query := `
		INSERT INTO hose500_second (
			ts, timestamp, formatted_time, session, ticker, order_type,
			last, change, pct_change, total_vol, total_val, matched_vol, matched_val,
			bid1, bid1_vol, bid2, bid2_vol, bid3, bid3_vol,
			ask1, ask1_vol, ask2, ask2_vol, ask3, ask3_vol,
			total_f_buy_vol, total_f_sell_vol, total_f_buy_val, total_f_sell_val,
			category
		)
		VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19,
			$20, $21, $22, $23, $24, $25,
			$26, $27, $28, $29,
			$30
		)
		ON CONFLICT DO NOTHING
	`

	_, err := pool.Exec(ctx, query,
		shiftedTimestamp,
		shiftedTimestamp.UnixMilli(),
		row.FormattedTime,
		row.Session,
		row.Ticker,
		row.OrderType,
		row.Last,
		row.Change,
		row.PctChange,
		row.TotalVol,
		row.TotalVal,
		row.MatchedVol,
		row.MatchedVal,
		row.Bid1,
		row.Bid1Vol,
		row.Bid2,
		row.Bid2Vol,
		row.Bid3,
		row.Bid3Vol,
		row.Ask1,
		row.Ask1Vol,
		row.Ask2,
		row.Ask2Vol,
		row.Ask3,
		row.Ask3Vol,
		row.TotalFBuyVol,
		row.TotalFSellVol,
		row.TotalFBuyVal,
		row.TotalFSellVal,
		row.Category,
	)

	return err
}

func batchInsertHOSE500Second(ctx context.Context, pool *pgxpool.Pool, rows []HOSE500SecondRow, timeOffset time.Duration, log *logger.Logger) error {
	if len(rows) == 0 {
		return nil
	}

	// Build multi-row INSERT statement
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*30)

	for i, row := range rows {
		csvTimestamp := time.UnixMilli(row.Timestamp).UTC()
		shiftedTimestamp := csvTimestamp.Add(timeOffset)

		valueStrings = append(valueStrings, fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			i*30+1, i*30+2, i*30+3, i*30+4, i*30+5, i*30+6, i*30+7, i*30+8, i*30+9, i*30+10,
			i*30+11, i*30+12, i*30+13, i*30+14, i*30+15, i*30+16, i*30+17, i*30+18, i*30+19, i*30+20,
			i*30+21, i*30+22, i*30+23, i*30+24, i*30+25, i*30+26, i*30+27, i*30+28, i*30+29, i*30+30,
		))

		valueArgs = append(valueArgs,
			shiftedTimestamp,
			shiftedTimestamp.UnixMilli(),
			row.FormattedTime,
			row.Session,
			row.Ticker,
			row.OrderType,
			row.Last,
			row.Change,
			row.PctChange,
			row.TotalVol,
			row.TotalVal,
			row.MatchedVol,
			row.MatchedVal,
			row.Bid1,
			row.Bid1Vol,
			row.Bid2,
			row.Bid2Vol,
			row.Bid3,
			row.Bid3Vol,
			row.Ask1,
			row.Ask1Vol,
			row.Ask2,
			row.Ask2Vol,
			row.Ask3,
			row.Ask3Vol,
			row.TotalFBuyVol,
			row.TotalFSellVol,
			row.TotalFBuyVal,
			row.TotalFSellVal,
			row.Category,
		)
	}

	query := fmt.Sprintf(`
		INSERT INTO hose500_second (
			ts, timestamp, formatted_time, session, ticker, order_type,
			last, change, pct_change, total_vol, total_val, matched_vol, matched_val,
			bid1, bid1_vol, bid2, bid2_vol, bid3, bid3_vol,
			ask1, ask1_vol, ask2, ask2_vol, ask3, ask3_vol,
			total_f_buy_vol, total_f_sell_vol, total_f_buy_val, total_f_sell_val,
			category
		) VALUES %s
		ON CONFLICT DO NOTHING
	`, strings.Join(valueStrings, ","))

	_, err := pool.Exec(ctx, query, valueArgs...)
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
				date_trunc('second', $1::timestamptz),
				date_trunc('second', $2::timestamptz),
				'15 seconds'::interval
			) AS ts
			WHERE NOT (
				EXTRACT(HOUR FROM ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 11
				AND EXTRACT(MINUTE FROM ts AT TIME ZONE 'Asia/Ho_Chi_Minh') >= 30
			)
			AND NOT (EXTRACT(HOUR FROM ts AT TIME ZONE 'Asia/Ho_Chi_Minh') = 12)
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

	log.WithFields(map[string]interface{}{
		"start_time": startOfDay.Format("2006-01-02 15:04:05 UTC"),
		"end_time":   endTime.Format("2006-01-02 15:04:05 UTC"),
	}).Info("querying aggregated data for Redis backfill")

	rows, err := pool.Query(ctx, query, startOfDay, endTime)
	if err != nil {
		return fmt.Errorf("failed to query aggregated data: %w", err)
	}
	defer rows.Close()

	count := 0
	streamKey := "market:stream"

	for rows.Next() {
		var timestamp int64
		var vn30Value, hnxValue float64

		if err := rows.Scan(&timestamp, &vn30Value, &hnxValue); err != nil {
			log.WithError(err).Warn("failed to scan row")
			continue
		}

		// Log first timestamp for debugging
		if count == 0 {
			log.WithFields(map[string]interface{}{
				"unix_timestamp": timestamp,
				"readable_time":  time.Unix(timestamp, 0).UTC().Format("2006-01-02 15:04:05 UTC"),
			}).Info("first Redis stream entry")
		}

		// Write to Redis stream with explicit ID from historical timestamp
		// Redis stream IDs are in milliseconds, our timestamp is in seconds
		streamID := fmt.Sprintf("%d-0", timestamp*1000)
		err := redisClient.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			ID:     streamID, // Use data timestamp as Stream ID for efficient filtering
			MaxLen: 1200,     // Keep max 1200 entries (~5 hours of 15s intervals)
			Approx: true,
			Values: map[string]interface{}{
				"timestamp": timestamp, // Keep for easy reading
				"vn30":      vn30Value,
				"hnx":       hnxValue,
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
