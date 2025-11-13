package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/logger"
)

// TickerWebSocketServer handles WebSocket connections for per-ticker bubble chart data
type TickerWebSocketServer struct {
	repo     repository.MarketRepository
	cache    repository.CacheRepository
	upgrader websocket.Upgrader
	log      *logger.Logger
}

// TickerWebSocketMessage represents a message for per-ticker data
type TickerWebSocketMessage struct {
	Type       string                        `json:"type"`  // "subscribe", "fetch_range", "historical", "data", "error"
	Tickers    []string                      `json:"tickers,omitempty"` // For subscribe message
	StartTime  int64                         `json:"startTime,omitempty"` // Unix timestamp for fetch_range
	EndTime    int64                         `json:"endTime,omitempty"`   // Unix timestamp for fetch_range
	Data       []repository.BubbleChartData  `json:"data,omitempty"`    // For data/historical messages
	Error      string                        `json:"error,omitempty"`   // For error messages
}

// NewTickerWebSocketServer creates a new WebSocket server for per-ticker data
func NewTickerWebSocketServer(repo repository.MarketRepository, cache repository.CacheRepository, log *logger.Logger) *TickerWebSocketServer {
	return &TickerWebSocketServer{
		repo:  repo,
		cache: cache,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for development
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		log: log,
	}
}

// HandleWebSocket handles WebSocket upgrade and client communication for ticker data
func (tws *TickerWebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := tws.upgrader.Upgrade(w, r, nil)
	if err != nil {
		tws.log.WithError(err).Error("failed to upgrade connection to websocket")
		return
	}

	clientID := uuid.New().String()
	tws.log.WithField("client_id", clientID).Info("new ticker websocket client connected")

	writer := &connWriter{conn: conn}
	defer conn.Close()

	// Set up pong handler for heartbeat
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Channel for graceful shutdown
	done := make(chan struct{})

	// Start goroutine to send ping messages
	go tws.sendPings(writer, clientID, done)

	// Track subscribed tickers
	var subscribedTickers []string
	var stopStreaming chan struct{}

	// Read messages from client
	for {
		var msg TickerWebSocketMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				tws.log.WithError(err).WithField("client_id", clientID).Warn("unexpected websocket close")
			}
			close(done)
			if stopStreaming != nil {
				close(stopStreaming)
			}
			return
		}

		// Handle different message types
		switch msg.Type {
		case "subscribe":
			if len(msg.Tickers) == 0 {
				tws.sendError(writer, "at least one ticker required")
				continue
			}

			tws.log.WithFields(map[string]interface{}{
				"client_id": clientID,
				"tickers":   msg.Tickers,
			}).Info("client subscribed to ticker stream")

			// Stop previous streaming if any
			if stopStreaming != nil {
				close(stopStreaming)
			}

			// Update subscribed tickers
			subscribedTickers = msg.Tickers
			stopStreaming = make(chan struct{})

			// Send initial historical data
			go tws.sendHistoricalData(writer, clientID, subscribedTickers)

			// Start real-time streaming
			go tws.streamTickerData(writer, clientID, subscribedTickers, stopStreaming)

			// Acknowledge subscription
			ack := TickerWebSocketMessage{Type: "subscribed"}
			writer.WriteJSON(ack)

		case "fetch_range":
			if len(subscribedTickers) == 0 {
				tws.sendError(writer, "not subscribed to any tickers")
				continue
			}
			if msg.StartTime == 0 || msg.EndTime == 0 {
				tws.sendError(writer, "startTime and endTime required for fetch_range")
				continue
			}

			tws.log.WithFields(map[string]interface{}{
				"client_id":  clientID,
				"start_time": time.Unix(msg.StartTime, 0).Format("15:04:05"),
				"end_time":   time.Unix(msg.EndTime, 0).Format("15:04:05"),
			}).Info("fetching specific time range")

			// Fetch the requested time range
			go tws.sendTimeRangeData(writer, clientID, subscribedTickers, msg.StartTime, msg.EndTime)

		case "unsubscribe":
			tws.log.WithField("client_id", clientID).Info("client unsubscribed")
			if stopStreaming != nil {
				close(stopStreaming)
				stopStreaming = nil
			}
			subscribedTickers = nil

		default:
			tws.log.WithField("type", msg.Type).Warn("unknown message type")
		}
	}
}

// sendHistoricalData sends recent 30-second window of ticker data to client
// Uses small time window to avoid lag with 30 tickers querying raw per-second data
func (tws *TickerWebSocketServer) sendHistoricalData(writer *connWriter, clientID string, tickers []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Query last 15 minutes of data for initial load
	// User can request older data by dragging/scrolling
	endTime := time.Now().UTC()
	startTime := endTime.Add(-15 * time.Minute)

	tws.log.WithFields(map[string]interface{}{
		"client_id":  clientID,
		"tickers":    tickers,
		"start_time": startTime.Format("2006-01-02 15:04:05 MST"),
		"end_time":   endTime.Format("2006-01-02 15:04:05 MST"),
		"window":     "15m",
	}).Info("fetching recent ticker data (15 minute window)")

	// Query raw per-second data (no cache for real-time window)
	data, err := tws.repo.GetBubbleChartData(ctx, tickers, startTime, endTime)
	if err != nil {
		tws.log.WithError(err).Error("failed to get recent ticker data from database")
		tws.sendError(writer, fmt.Sprintf("database error: %v", err))
		return
	}

	// If no data in last 15 minutes, search backwards for most recent data
	if len(data) == 0 {
		tws.log.WithField("client_id", clientID).Info("no data in last 15m, searching for most recent available data")

		vietnamLocation := time.FixedZone("ICT", 7*60*60)
		now := time.Now().In(vietnamLocation)

		// Start from market end time (14:45) or current time if during market hours
		searchEndTime := time.Date(now.Year(), now.Month(), now.Day(), 14, 45, 0, 0, vietnamLocation)
		if now.Hour() < 9 {
			// Before market open, use yesterday's data
			searchEndTime = searchEndTime.Add(-24 * time.Hour)
		} else if now.Hour() >= 9 && now.Hour() < 15 {
			// During market hours, use current time
			searchEndTime = now
		}

		// Search backwards in 15-minute windows until we find data (max 2 hours back)
		maxAttempts := 8
		for attempt := 0; attempt < maxAttempts; attempt++ {
			fallbackEndTime := searchEndTime.Add(-time.Duration(attempt) * 15 * time.Minute)
			fallbackStartTime := fallbackEndTime.Add(-15 * time.Minute)

			tws.log.WithFields(map[string]interface{}{
				"client_id":  clientID,
				"attempt":    attempt + 1,
				"start_time": fallbackStartTime.Format("15:04:05"),
				"end_time":   fallbackEndTime.Format("15:04:05"),
			}).Debug("searching for data in time window")

			data, err = tws.repo.GetBubbleChartData(ctx, tickers, fallbackStartTime.UTC(), fallbackEndTime.UTC())
			if err != nil {
				tws.log.WithError(err).Error("failed to query fallback ticker data")
				continue
			}

			if len(data) > 0 {
				tws.log.WithFields(map[string]interface{}{
					"client_id": clientID,
					"attempt":   attempt + 1,
					"count":     len(data),
					"window":    fallbackStartTime.Format("15:04:05") + " to " + fallbackEndTime.Format("15:04:05"),
				}).Info("found data in fallback window")
				break
			}
		}

		if len(data) == 0 {
			tws.log.WithField("client_id", clientID).Warn("no data found in any fallback window (searched 2 hours)")
		}
	}

	// Send data
	if len(data) > 0 {
		msg := TickerWebSocketMessage{
			Type: "historical",
			Data: data,
		}
		if err := writer.WriteJSON(msg); err != nil {
			tws.log.WithError(err).WithField("client_id", clientID).Error("failed to send historical data")
			return
		}
	}

	// Send completion message
	completion := TickerWebSocketMessage{Type: "historical_complete"}
	writer.WriteJSON(completion)

	tws.log.WithFields(map[string]interface{}{
		"client_id": clientID,
		"count":     len(data),
	}).Info("historical ticker data sent")
}

// sendTimeRangeData sends data for a specific time range requested by the client
func (tws *TickerWebSocketServer) sendTimeRangeData(writer *connWriter, clientID string, tickers []string, startTimeUnix, endTimeUnix int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	startTime := time.Unix(startTimeUnix, 0)
	endTime := time.Unix(endTimeUnix, 0)

	tws.log.WithFields(map[string]interface{}{
		"client_id":  clientID,
		"start_time": startTime.Format("2006-01-02 15:04:05"),
		"end_time":   endTime.Format("2006-01-02 15:04:05"),
	}).Info("fetching requested time range")

	data, err := tws.repo.GetBubbleChartData(ctx, tickers, startTime, endTime)
	if err != nil {
		tws.log.WithError(err).Error("failed to get time range data")
		tws.sendError(writer, fmt.Sprintf("database error: %v", err))
		return
	}

	// Send data with "data" type (not "historical")
	if len(data) > 0 {
		msg := TickerWebSocketMessage{
			Type: "data",
			Data: data,
		}
		if err := writer.WriteJSON(msg); err != nil {
			tws.log.WithError(err).WithField("client_id", clientID).Error("failed to send time range data")
			return
		}
	}

	tws.log.WithFields(map[string]interface{}{
		"client_id": clientID,
		"count":     len(data),
	}).Info("time range data sent")
}

// streamTickerData continuously streams real-time ticker updates
func (tws *TickerWebSocketServer) streamTickerData(writer *connWriter, clientID string, tickers []string, stop chan struct{}) {
	ticker := time.NewTicker(1 * time.Second) // Poll every second for new data
	defer ticker.Stop()

	var lastTimestamp time.Time

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

			// Query for data newer than last timestamp
			startTime := lastTimestamp
			if startTime.IsZero() {
				// First query - get data from last 5 seconds
				startTime = time.Now().Add(-5 * time.Second)
			}
			endTime := time.Now()

			data, err := tws.repo.GetBubbleChartData(ctx, tickers, startTime, endTime)
			cancel()

			if err != nil {
				tws.log.WithError(err).Warn("failed to get real-time ticker data")
				continue
			}

			// Send new data if any
			if len(data) > 0 {
				msg := TickerWebSocketMessage{
					Type: "data",
					Data: data,
				}
				if err := writer.WriteJSON(msg); err != nil {
					tws.log.WithError(err).WithField("client_id", clientID).Error("failed to send real-time data")
					return
				}
				lastTimestamp = endTime
			}

		case <-stop:
			tws.log.WithField("client_id", clientID).Info("ticker streaming stopped")
			return
		}
	}
}

// sendPings sends periodic ping messages to keep connection alive
func (tws *TickerWebSocketServer) sendPings(writer *connWriter, clientID string, done chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := writer.WriteMessage(websocket.PingMessage, nil); err != nil {
				tws.log.WithError(err).WithField("client_id", clientID).Debug("ping failed")
				return
			}
		case <-done:
			return
		}
	}
}

// sendError sends an error message to the client
func (tws *TickerWebSocketServer) sendError(writer *connWriter, errorMsg string) {
	msg := TickerWebSocketMessage{
		Type:  "error",
		Error: errorMsg,
	}
	writer.WriteJSON(msg)
}
