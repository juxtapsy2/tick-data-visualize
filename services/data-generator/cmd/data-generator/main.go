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
	Timestamp      int64
	FormattedTime  string
	Session        string
	Ticker         string
	Last           float64
	Change         float64
	PctChange      float64
	MatchedVol     float64
	MatchedVal     float64
	Category       string
}

type FuturesRow struct {
	FormattedTime  string
	Session        string
	Ticker         string
	Last           float64
	Change         float64
	PctChange      float64
	TotalVol       float64
	TotalVal       float64
	Timestamp      int64
	Category       string
}

// findClosestTimeIndex finds the index of the row closest to the target time
func findClosestTimeIndex[T interface{ GetFormattedTime() string }](rows []T, targetTime string, log *logger.Logger) int {
	if len(rows) == 0 {
		return 0
	}

	// Parse target time
	target, err := time.Parse("2006-01-02 15:04:05", targetTime)
	if err != nil {
		log.WithError(err).Warn("failed to parse target time, starting from beginning")
		return 0
	}

	// Extract just the time part (HH:MM:SS) for comparison
	targetHour := target.Hour()
	targetMinute := target.Minute()
	targetSecond := target.Second()

	// Binary search or linear search for the closest time
	// Since data is sequential by time, we can do a linear search
	for i, row := range rows {
		rowTime, err := time.Parse("2006-01-02 15:04:05", row.GetFormattedTime())
		if err != nil {
			continue
		}

		// Compare only the time component (ignore date)
		rowHour := rowTime.Hour()
		rowMinute := rowTime.Minute()
		rowSecond := rowTime.Second()

		// If row time is >= target time, use this as starting point
		if rowHour > targetHour ||
		   (rowHour == targetHour && rowMinute > targetMinute) ||
		   (rowHour == targetHour && rowMinute == targetMinute && rowSecond >= targetSecond) {
			log.WithFields(map[string]interface{}{
				"index": i,
				"time":  row.GetFormattedTime(),
			}).Info("found starting position")
			return i
		}
	}

	// If target time is after all rows, start from beginning (loop data)
	log.Info("target time is after all data, starting from beginning")
	return 0
}

// GetFormattedTime methods for interface compliance
func (r IndexTickRow) GetFormattedTime() string {
	return r.FormattedTime
}

func (r FuturesRow) GetFormattedTime() string {
	return r.FormattedTime
}

// isWithinTradingHours checks if a timestamp is within Vietnam trading hours
// Morning session: 8:45-11:30, Afternoon session: 13:00-14:45
func isWithinTradingHours(formattedTime string) bool {
	// Parse the formatted time (format: "2006-01-02 15:04:05")
	t, err := time.Parse("2006-01-02 15:04:05", formattedTime)
	if err != nil {
		return false
	}

	hour := t.Hour()
	minute := t.Minute()

	// Morning session: 8:45 to 11:30
	if hour == 8 && minute >= 45 {
		return true
	}
	if hour >= 9 && hour < 11 {
		return true
	}
	if hour == 11 && minute <= 30 {
		return true
	}

	// Afternoon session: 13:00 to 14:45
	if hour == 13 {
		return true
	}
	if hour == 14 && minute <= 45 {
		return true
	}

	return false
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
	}).Info("loaded CSV data after filtering")

	// Find starting index based on current time
	now := time.Now()
	vietnamLocation := time.FixedZone("ICT", 7*60*60)
	currentVietnamTime := now.In(vietnamLocation)
	targetTime := currentVietnamTime.Format("2006-01-02 15:04:05")

	log.WithField("target_time", targetTime).Info("finding starting position for current time")

	// Find the closest index in indexRows to current time
	indexIdx := findClosestTimeIndex(indexRows, targetTime, log)
	futuresIdx := findClosestTimeIndex(futuresRows, targetTime, log)

	if indexIdx >= len(indexRows) {
		indexIdx = 0
		log.Warn("current time not found in index data, starting from beginning")
	}
	if futuresIdx >= len(futuresRows) {
		futuresIdx = 0
		log.Warn("current time not found in futures data, starting from beginning")
	}

	log.WithFields(map[string]interface{}{
		"index_start":   indexIdx,
		"futures_start": futuresIdx,
		"index_time":    indexRows[indexIdx].FormattedTime,
		"futures_time":  futuresRows[futuresIdx].FormattedTime,
	}).Info("starting data insertion from current time position")

	// Start inserting data with delay to simulate real-time
	insertInterval := 1 * time.Second
	ticker := time.NewTicker(insertInterval)
	defer ticker.Stop()

	log.WithField("interval", insertInterval).Info("starting data insertion")

	for {
		select {
		case <-ctx.Done():
			log.Info("data generator stopped")
			return

		case <-ticker.C:
			// Insert index tick data
			if indexIdx < len(indexRows) {
				row := indexRows[indexIdx]
				if err := insertIndexTick(ctx, pool, row, log); err != nil {
					log.WithError(err).Error("failed to insert index tick")
				} else {
					log.WithFields(map[string]interface{}{
						"ticker": row.Ticker,
						"value":  fmt.Sprintf("%.2f", row.Last),
						"index":  indexIdx,
						"total":  len(indexRows),
					}).Debug("inserted index tick")
				}
				indexIdx++
			}

			// Insert futures data
			if futuresIdx < len(futuresRows) {
				row := futuresRows[futuresIdx]
				if err := insertFutures(ctx, pool, row, log); err != nil {
					log.WithError(err).Error("failed to insert futures")
				} else {
					log.WithFields(map[string]interface{}{
						"ticker": row.Ticker,
						"value":  fmt.Sprintf("%.2f", row.Last),
						"index":  futuresIdx,
						"total":  len(futuresRows),
					}).Debug("inserted futures")
				}
				futuresIdx++
			}

			// Loop back to start when we reach the end
			if indexIdx >= len(indexRows) {
				indexIdx = 0
				log.Info("restarting index tick data from beginning")
			}
			if futuresIdx >= len(futuresRows) {
				futuresIdx = 0
				log.Info("restarting futures data from beginning")
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

		// Parse numeric fields
		if val, err := strconv.ParseFloat(record[colMap["last"]], 64); err == nil {
			row.Last = val
		}
		if val, err := strconv.ParseFloat(record[colMap["change"]], 64); err == nil {
			row.Change = val
		}
		if val, err := strconv.ParseFloat(record[colMap["pct_change"]], 64); err == nil {
			row.PctChange = val
		}
		if val, err := strconv.ParseFloat(record[colMap["matched_vol"]], 64); err == nil {
			row.MatchedVol = val
		}
		if val, err := strconv.ParseFloat(record[colMap["matched_val"]], 64); err == nil {
			row.MatchedVal = val
		}

		row.Category = record[colMap["category"]]

		// Skip rows with invalid or zero price data to prevent anomalies
		if row.Last <= 0 {
			log.WithFields(map[string]interface{}{
				"line":   lineNum,
				"ticker": row.Ticker,
				"last":   row.Last,
			}).Debug("skipping row with invalid price")
			lineNum++
			continue
		}

		// Filter to trading hours only: 8:45-11:30 and 13:00-14:45
		if !isWithinTradingHours(row.FormattedTime) {
			lineNum++
			continue
		}

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

		// Parse timestamp
		if val, err := strconv.ParseInt(record[colMap["timestamp"]], 10, 64); err == nil {
			row.Timestamp = val
		}

		// Parse numeric fields
		if val, err := strconv.ParseFloat(record[colMap["last"]], 64); err == nil {
			row.Last = val
		}
		if val, err := strconv.ParseFloat(record[colMap["change"]], 64); err == nil {
			row.Change = val
		}
		if val, err := strconv.ParseFloat(record[colMap["pct_change"]], 64); err == nil {
			row.PctChange = val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_vol"]], 64); err == nil {
			row.TotalVol = val
		}
		if val, err := strconv.ParseFloat(record[colMap["total_val"]], 64); err == nil {
			row.TotalVal = val
		}

		row.Category = record[colMap["category"]]

		// Skip rows with invalid or zero price data to prevent anomalies
		if row.Last <= 0 {
			log.WithFields(map[string]interface{}{
				"line":   lineNum,
				"ticker": row.Ticker,
				"last":   row.Last,
			}).Debug("skipping row with invalid price")
			lineNum++
			continue
		}

		// Filter to trading hours only: 8:45-11:30 and 13:00-14:45
		if !isWithinTradingHours(row.FormattedTime) {
			lineNum++
			continue
		}

		rows = append(rows, row)
		lineNum++
	}

	return rows, nil
}

func insertIndexTick(ctx context.Context, pool *pgxpool.Pool, row IndexTickRow, log *logger.Logger) error {
	// Use current time instead of CSV timestamp for real-time simulation
	now := time.Now()
	currentTimestamp := now.UnixMilli()

	query := `
		INSERT INTO index_tick (
			ts, timestamp, formatted_time, session, ticker,
			last, change, pct_change, matched_vol, matched_val, category
		)
		VALUES (
			NOW(), $1, $2, $3, $4,
			$5, $6, $7, $8, $9, $10
		)
	`

	_, err := pool.Exec(ctx, query,
		currentTimestamp,
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

func insertFutures(ctx context.Context, pool *pgxpool.Pool, row FuturesRow, log *logger.Logger) error {
	// Use current time instead of CSV timestamp for real-time simulation
	now := time.Now()
	currentTimestamp := now.UnixMilli()

	query := `
		INSERT INTO futures_table (
			ts, timestamp, formatted_time, session, ticker,
			last, change, pct_change, total_vol, total_val, category
		)
		VALUES (
			NOW(), $1, $2, $3, $4,
			$5, $6, $7, $8, $9, $10
		)
	`

	_, err := pool.Exec(ctx, query,
		currentTimestamp,
		row.FormattedTime,
		row.Session,
		row.Ticker,
		row.Last,
		row.Change,
		row.PctChange,
		row.TotalVol,
		row.TotalVal,
		row.Category,
	)

	return err
}
