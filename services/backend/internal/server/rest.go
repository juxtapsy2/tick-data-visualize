package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/logger"
)

// RESTHandler handles REST API endpoints for market data
type RESTHandler struct {
	repo  repository.MarketRepository
	cache repository.CacheRepository
	log   *logger.Logger
}

// NewRESTHandler creates a new REST API handler
func NewRESTHandler(repo repository.MarketRepository, cache repository.CacheRepository, log *logger.Logger) *RESTHandler {
	return &RESTHandler{
		repo:  repo,
		cache: cache,
		log:   log,
	}
}

// HistoricalResponse represents the response for historical data queries
type HistoricalResponse struct {
	Success bool                    `json:"success"`
	Data    []repository.MarketData `json:"data"`
	Count   int                     `json:"count"`
	Error   string                  `json:"error,omitempty"`
}

// LatestResponse represents the response for latest data query
type LatestResponse struct {
	Success   bool                   `json:"success"`
	Data      *repository.MarketData `json:"data,omitempty"`
	Timestamp int64                  `json:"timestamp"`
	Error     string                 `json:"error,omitempty"`
}

// ChartResponse represents the response for chart data (last 15s averages)
type ChartResponse struct {
	Success bool                   `json:"success"`
	Data    []repository.ChartData `json:"data"`
	Count   int                    `json:"count"`
	Error   string                 `json:"error,omitempty"`
}

// HandleHistorical handles GET /api/v1/market/historical
// Query params: from (ISO8601 or Unix timestamp), to (ISO8601 or Unix timestamp)
// Defaults: from=9:00 AM Vietnam time today, to=now
// Example: /api/v1/market/historical
// Example: /api/v1/market/historical?from=2025-10-19T00:00:00Z&to=2025-10-19T23:59:59Z
// Example: /api/v1/market/historical?from=1760843100&to=1760843200
func (h *RESTHandler) HandleHistorical(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.sendError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	fromParam := r.URL.Query().Get("from")
	toParam := r.URL.Query().Get("to")

	var fromTime, toTime time.Time
	var err error

	// Default 'from' to 9:00 AM Vietnam time (UTC+7)
	// If current time is before 9 AM, use yesterday's 9 AM
	if fromParam == "" {
		// Vietnam timezone is UTC+7
		vietnamLocation := time.FixedZone("ICT", 7*60*60)
		now := time.Now().In(vietnamLocation)

		// Start from 9:00 AM today
		fromTime = time.Date(now.Year(), now.Month(), now.Day(), 9, 0, 0, 0, vietnamLocation)

		// If we're before 9 AM, use yesterday's 9 AM instead
		if now.Hour() < 9 {
			fromTime = fromTime.Add(-24 * time.Hour)
		}

		fromTime = fromTime.UTC()
	} else {
		fromTime, err = h.parseTimestamp(fromParam)
		if err != nil {
			h.sendError(w, "invalid 'from' timestamp: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Default 'to' to now
	if toParam == "" {
		toTime = time.Now().UTC()
	} else {
		toTime, err = h.parseTimestamp(toParam)
		if err != nil {
			h.sendError(w, "invalid 'to' timestamp: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// Validate time range
	if toTime.Before(fromTime) {
		h.sendError(w, "'to' must be after 'from'", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	var data []repository.MarketData
	fromCache := false

	// Calculate time range duration
	timeRange := toTime.Sub(fromTime)

	// Try Redis first (if cache is available and time range is less than 24 hours)
	// For longer ranges, go straight to PostgreSQL which has full historical data
	if h.cache != nil && timeRange < 24*time.Hour {
		data, err = h.cache.GetStreamDataByTimeRange(ctx, fromTime, toTime)
		if err != nil {
			h.log.WithError(err).Debug("failed to get data from Redis, falling back to PostgreSQL")
		} else if len(data) > 0 {
			fromCache = true
			h.log.WithFields(map[string]interface{}{
				"from":  fromTime,
				"to":    toTime,
				"count": len(data),
			}).Info("historical data served from Redis cache")
		} else {
			h.log.Debug("no data in Redis stream, falling back to PostgreSQL")
		}
	} else if timeRange >= 24*time.Hour {
		h.log.WithField("range", timeRange).Debug("time range > 24h, skipping Redis and querying PostgreSQL directly")
	}

	// Fallback to PostgreSQL if Redis has no data or failed
	if !fromCache {
		data, err = h.repo.GetHistoricalData(ctx, fromTime, toTime)
		if err != nil {
			h.log.WithError(err).Error("failed to get historical data from PostgreSQL")
			h.sendError(w, "database error", http.StatusInternalServerError)
			return
		}

		h.log.WithFields(map[string]interface{}{
			"from":  fromTime,
			"to":    toTime,
			"count": len(data),
		}).Info("historical data served from PostgreSQL")
	}

	// Send response
	response := HistoricalResponse{
		Success: true,
		Data:    data,
		Count:   len(data),
	}

	h.sendJSON(w, response, http.StatusOK)
}

// HandleLatest handles GET /api/v1/market/latest
// Returns the most recent 15s average for all tickers
func (h *RESTHandler) HandleLatest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.sendError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Query database
	ctx := r.Context()
	data, err := h.repo.GetLatestData(ctx)
	if err != nil {
		h.log.WithError(err).Error("failed to get latest data")
		h.sendError(w, "database error", http.StatusInternalServerError)
		return
	}

	// Send response
	response := LatestResponse{
		Success:   true,
		Data:      data,
		Timestamp: time.Now().Unix(),
	}

	h.sendJSON(w, response, http.StatusOK)

	h.log.WithFields(map[string]interface{}{
		"vn30": data.VN30Value,
		"hnx":  data.HNXValue,
	}).Debug("latest data served")
}

// HandleChart handles GET /api/v1/market/chart
// Query params: index (comma-separated), futures (comma-separated)
// Example: /api/v1/market/chart?index=VN30&futures=41I1FA000
// Returns last 15 seconds average for requested tickers
func (h *RESTHandler) HandleChart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.sendError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	indexTickers := h.parseCommaSeparated(r.URL.Query().Get("index"))
	futuresTickers := h.parseCommaSeparated(r.URL.Query().Get("futures"))

	if len(indexTickers) == 0 && len(futuresTickers) == 0 {
		h.sendError(w, "at least one ticker required (index or futures)", http.StatusBadRequest)
		return
	}

	// Query database
	ctx := r.Context()
	data, err := h.repo.GetLast15sAverages(ctx, indexTickers, futuresTickers)
	if err != nil {
		h.log.WithError(err).Error("failed to get chart data")
		h.sendError(w, "database error", http.StatusInternalServerError)
		return
	}

	// Send response
	response := ChartResponse{
		Success: true,
		Data:    data,
		Count:   len(data),
	}

	h.sendJSON(w, response, http.StatusOK)

	h.log.WithFields(map[string]interface{}{
		"index":   indexTickers,
		"futures": futuresTickers,
		"count":   len(data),
	}).Debug("chart data served")
}

// parseTimestamp parses a timestamp from either ISO8601 format or Unix timestamp
func (h *RESTHandler) parseTimestamp(value string) (time.Time, error) {
	// Try parsing as Unix timestamp first
	if unixTime, err := strconv.ParseInt(value, 10, 64); err == nil {
		return time.Unix(unixTime, 0).UTC(), nil
	}

	// Try parsing as ISO8601 (RFC3339)
	if t, err := time.Parse(time.RFC3339, value); err == nil {
		return t.UTC(), nil
	}

	// Try parsing as date only (2006-01-02)
	if t, err := time.Parse("2006-01-02", value); err == nil {
		return t.UTC(), nil
	}

	return time.Time{}, strconv.ErrSyntax
}

// parseCommaSeparated parses a comma-separated string into a slice
func (h *RESTHandler) parseCommaSeparated(value string) []string {
	if value == "" {
		return nil
	}

	var result []string
	for _, item := range splitByComma(value) {
		trimmed := trim(item)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// splitByComma splits a string by comma
func splitByComma(s string) []string {
	if s == "" {
		return nil
	}

	var result []string
	current := ""
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			result = append(result, current)
			current = ""
		} else {
			current += string(s[i])
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

// trim removes leading and trailing whitespace
func trim(s string) string {
	start := 0
	end := len(s)

	// Trim leading whitespace
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\n' || s[start] == '\r') {
		start++
	}

	// Trim trailing whitespace
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\n' || s[end-1] == '\r') {
		end--
	}

	return s[start:end]
}

// sendJSON sends a JSON response
func (h *RESTHandler) sendJSON(w http.ResponseWriter, data interface{}, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*") // CORS for development
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.log.WithError(err).Error("failed to encode JSON response")
	}
}

// sendError sends an error response
func (h *RESTHandler) sendError(w http.ResponseWriter, message string, statusCode int) {
	response := map[string]interface{}{
		"success": false,
		"error":   message,
	}
	h.sendJSON(w, response, statusCode)
}
