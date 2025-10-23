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

	// Filter CSV data based on current minute:second
	// Match MM:SS only, so if now is 17:27:35, find CSV row at XX:27:35
	currentTime := time.Now()
	currentMinSec := currentTime.Minute()*60 + currentTime.Second()

	// Find first index row matching current minute:second
	indexIdx := 0
	for i, row := range indexRows {
		csvTime := time.UnixMilli(row.Timestamp)
		csvMinSec := csvTime.Minute()*60 + csvTime.Second()
		if csvMinSec >= currentMinSec {
			indexIdx = i
			break
		}
	}

	// Find first futures row matching current minute:second
	futuresIdx := 0
	for i, row := range futuresRows {
		csvTime := time.UnixMilli(row.Timestamp)
		csvMinSec := csvTime.Minute()*60 + csvTime.Second()
		if csvMinSec >= currentMinSec {
			futuresIdx = i
			break
		}
	}

	// Calculate time offset from the first matching row
	var timeOffset time.Duration
	if indexIdx < len(indexRows) {
		csvFirstTime := time.UnixMilli(indexRows[indexIdx].Timestamp)
		timeOffset = currentTime.Sub(csvFirstTime)
		log.WithFields(map[string]interface{}{
			"current_time":    currentTime.Format("15:04:05"),
			"csv_start_time":  csvFirstTime.Format("15:04:05"),
			"offset":          timeOffset,
			"skipped_index":   indexIdx,
			"skipped_futures": futuresIdx,
		}).Info("filtered CSV data to match current minute:second")
	}

	// Start inserting data with delay to simulate real-time
	insertInterval := 1 * time.Second
	ticker := time.NewTicker(insertInterval)
	defer ticker.Stop()

	log.WithField("interval", insertInterval).Info("starting data insertion")

	// Session tracking: morning (9:00 AM) and evening (7:00 PM)
	vietnamLocation := time.FixedZone("ICT", 7*60*60) // Vietnam is UTC+7
	var currentSession string                         // "morning", "evening", or ""
	var sessionStarted bool

	for {
		select {
		case <-ctx.Done():
			log.Info("data generator stopped")
			return

		case <-ticker.C:
			now := time.Now().In(vietnamLocation)
			currentHour := now.Hour()

			// Determine if we should start a new session
			if currentHour >= 9 && currentHour < 18 && currentSession != "morning" {
				// Start morning session (9:00 AM - before 6:00 PM)
				currentSession = "morning"
				indexIdx = 0
				futuresIdx = 0
				sessionStarted = true
				log.Info("starting morning session (9:00 AM - 5:59 PM)")
			} else if currentHour >= 19 && currentSession != "evening" {
				// Start evening session (7:00 PM onwards)
				currentSession = "evening"
				indexIdx = 0
				futuresIdx = 0
				sessionStarted = true
				log.Info("starting evening session (7:00 PM)")
			} else if currentHour < 9 || (currentHour >= 18 && currentHour < 19) {
				// Before 9:00 AM or between 6:00 PM - 6:59 PM - reset session
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
					if err := insertIndexTick(ctx, pool, row, timeOffset, log); err != nil {
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
					if err := insertFutures(ctx, pool, row, timeOffset, log); err != nil {
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
	// Use current time for real-time data insertion
	newTimestamp := time.Now()

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
		newTimestamp,
		newTimestamp.UnixMilli(),
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
	// Use current time for real-time data insertion
	newTimestamp := time.Now()

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
		newTimestamp,
		newTimestamp.UnixMilli(),
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
