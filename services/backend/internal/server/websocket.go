package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/logger"
)

// WebSocketServer handles WebSocket connections for real-time market data
type WebSocketServer struct {
	broadcaster *Broadcaster
	repo        repository.MarketRepository
	upgrader    websocket.Upgrader
	log         *logger.Logger
}

// connWriter wraps a WebSocket connection with a mutex for safe concurrent writes
type connWriter struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (cw *connWriter) WriteJSON(v interface{}) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.conn.WriteJSON(v)
}

func (cw *connWriter) WriteMessage(messageType int, data []byte) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.conn.WriteMessage(messageType, data)
}

// WebSocketMessage represents a message sent/received over WebSocket
type WebSocketMessage struct {
	Type            string  `json:"type"`                       // "subscribe", "historical", "data", "error"
	Timestamp       int64   `json:"timestamp"`                  // For data messages
	VN30Value       float64 `json:"vn30"`                       // VN30 index value
	HNXValue        float64 `json:"hnx"`                        // F1 futures last price (legacy field name)
	F1Last          float64 `json:"f1_last"`                    // F1 futures last price
	F1ForeignLong   float64 `json:"f1_foreign_long"`            // F1 foreign buy volume
	F1ForeignShort  float64 `json:"f1_foreign_short"`           // F1 foreign sell volume
	F1TotalBid      float64 `json:"f1_total_bid"`               // F1 total bid (long orders)
	F1TotalAsk      float64 `json:"f1_total_ask"`               // F1 total ask (short orders)
	Date            string  `json:"date"`                       // For historical requests
	FuturesContract string  `json:"futures,omitempty"`          // Futures contract to query (f1, f2, f3, f4)
	FuturesResponse string  `json:"futures_contract,omitempty"` // Futures contract in response (f1, f2, f3, f4)
	Error           string  `json:"error"`                      // For error messages

	// VN30 Stocks Data (Charts 4-6)
	VN30TotalBuyOrder  float64 `json:"vn30_total_buy_order"`  // Chart 4: Total buy orders
	VN30TotalSellOrder float64 `json:"vn30_total_sell_order"` // Chart 4: Total sell orders
	VN30BuyUp          float64 `json:"vn30_buy_up"`           // Chart 5: Buy-up volume
	VN30SellDown       float64 `json:"vn30_sell_down"`        // Chart 5: Sell-down volume
	VN30ForeignNet     float64 `json:"vn30_foreign_net"`      // Chart 6: Foreign net value
}

// NewWebSocketServer creates a new WebSocket server
func NewWebSocketServer(broadcaster *Broadcaster, repo repository.MarketRepository, log *logger.Logger) *WebSocketServer {
	return &WebSocketServer{
		broadcaster: broadcaster,
		repo:        repo,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				// Allow all origins for development
				// In production, you should restrict this to your frontend domain
				return true
			},
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		log: log,
	}
}

// HandleWebSocket handles WebSocket upgrade and client communication
func (ws *WebSocketServer) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := ws.upgrader.Upgrade(w, r, nil)
	if err != nil {
		ws.log.WithError(err).Error("failed to upgrade connection to websocket")
		return
	}

	clientID := uuid.New().String()
	ws.log.WithField("client_id", clientID).Info("new websocket client connected")

	// Wrap connection with mutex for safe concurrent writes
	writer := &connWriter{conn: conn}

	// Register with broadcaster
	client := ws.broadcaster.Register(clientID)
	defer ws.broadcaster.Unregister(clientID)
	defer conn.Close()

	// Set up pong handler for heartbeat
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Channel for graceful shutdown
	done := make(chan struct{})

	// Start goroutine to read messages from client
	go ws.readMessages(writer, conn, clientID, done)

	// Start goroutine to send ping messages
	go ws.sendPings(writer, clientID, done)

	// Send real-time market data updates
	for {
		select {
		case msg, ok := <-client.Ch:
			if !ok {
				ws.log.WithField("client_id", clientID).Info("client channel closed")
				return
			}

			// Convert to WebSocket message format
			wsMsg := WebSocketMessage{
				Type:               "data",
				Timestamp:          msg.Timestamp,
				VN30Value:          msg.VN30Value,
				HNXValue:           msg.HNXValue, // Legacy field
				F1Last:             msg.HNXValue, // Same as HNXValue for compatibility
				F1ForeignLong:      msg.F1ForeignLong,
				F1ForeignShort:     msg.F1ForeignShort,
				F1TotalBid:         msg.F1TotalBid,
				F1TotalAsk:         msg.F1TotalAsk,
				VN30TotalBuyOrder:  msg.VN30TotalBuyOrder,
				VN30TotalSellOrder: msg.VN30TotalSellOrder,
				VN30BuyUp:          msg.VN30BuyUp,
				VN30SellDown:       msg.VN30SellDown,
				VN30ForeignNet:     msg.VN30ForeignNet,
			}

			// Send message to client using thread-safe writer
			if err := writer.WriteJSON(wsMsg); err != nil {
				ws.log.WithError(err).WithField("client_id", clientID).Error("failed to write message")
				return
			}

		case <-client.Done:
			ws.log.WithField("client_id", clientID).Info("client done signal received")
			return

		case <-done:
			ws.log.WithField("client_id", clientID).Info("client disconnected")
			return
		}
	}
}

// readMessages reads messages from the WebSocket client
func (ws *WebSocketServer) readMessages(writer *connWriter, conn *websocket.Conn, clientID string, done chan struct{}) {
	defer close(done)

	for {
		var msg WebSocketMessage
		err := conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				ws.log.WithError(err).WithField("client_id", clientID).Warn("unexpected websocket close")
			}
			return
		}

		// Handle different message types
		switch msg.Type {
		case "subscribe":
			ws.log.WithField("client_id", clientID).Info("client subscribed to stream")
			// Already registered, just acknowledge
			ack := WebSocketMessage{Type: "subscribed"}
			writer.WriteJSON(ack)

		case "historical":
			futuresContract := msg.FuturesContract
			if futuresContract == "" {
				futuresContract = "f1" // Default to f1
			}
			ws.log.WithFields(map[string]interface{}{
				"client_id": clientID,
				"date":      msg.Date,
				"futures":   futuresContract,
			}).Info("historical data requested")

			go ws.handleHistoricalRequest(writer, clientID, msg.Date, futuresContract)

		case "catchup":
			futuresContract := msg.FuturesContract
			if futuresContract == "" {
				futuresContract = "f1" // Default to f1
			}
			ws.log.WithFields(map[string]interface{}{
				"client_id": clientID,
				"from":      msg.Timestamp,
				"futures":   futuresContract,
			}).Info("catchup data requested")

			go ws.handleCatchupRequest(writer, clientID, msg.Timestamp, futuresContract)

		default:
			ws.log.WithField("type", msg.Type).Warn("unknown message type")
		}
	}
}

// sendPings sends periodic ping messages to keep connection alive
func (ws *WebSocketServer) sendPings(writer *connWriter, clientID string, done chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := writer.WriteMessage(websocket.PingMessage, nil); err != nil {
				ws.log.WithError(err).WithField("client_id", clientID).Debug("ping failed")
				return
			}
		case <-done:
			return
		}
	}
}

// handleHistoricalRequest handles requests for historical data
func (ws *WebSocketServer) handleHistoricalRequest(writer *connWriter, clientID string, date string, futuresContract string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Parse date
	parsedDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		ws.sendError(writer, fmt.Sprintf("invalid date format: %v", err))
		return
	}

	// Get all data for the requested date
	// Market opens at 9:00 AM Vietnam time = 2:00 AM UTC
	startTime := time.Date(parsedDate.Year(), parsedDate.Month(), parsedDate.Day(), 2, 0, 0, 0, time.UTC)
	endTime := time.Now()

	// If requesting historical date (not today), set end time to market close
	if !parsedDate.Equal(time.Now().UTC().Truncate(24 * time.Hour)) {
		// Historical date: end at market close (2:45 PM Vietnam = 7:45 AM UTC)
		endTime = time.Date(parsedDate.Year(), parsedDate.Month(), parsedDate.Day(), 7, 45, 59, 0, time.UTC)
	} else {
		// For today's data, cap at market close (2:45 PM Vietnam = 7:45 AM UTC)
		marketCloseTime := time.Date(parsedDate.Year(), parsedDate.Month(), parsedDate.Day(), 7, 45, 59, 0, time.UTC)
		if endTime.After(marketCloseTime) {
			ws.log.WithFields(map[string]interface{}{
				"current_time":      endTime.Format("15:04:05 MST"),
				"market_close_time": marketCloseTime.Format("15:04:05 MST"),
			}).Info("capping historical data at market close (2:45 PM Vietnam time)")
			endTime = marketCloseTime
		}
	}

	// Query database
	data, err := ws.repo.GetHistoricalData(ctx, startTime, endTime, futuresContract)
	if err != nil {
		ws.log.WithError(err).Error("failed to get historical data")
		ws.sendError(writer, fmt.Sprintf("database error: %v", err))
		return
	}

	// Send historical data as individual messages
	for _, point := range data {
		msg := WebSocketMessage{
			Type:               "historical",
			Timestamp:          point.Timestamp,
			VN30Value:          point.VN30Value,
			HNXValue:           point.HNXValue,
			F1Last:             point.HNXValue,
			F1ForeignLong:      point.F1ForeignLong,
			F1ForeignShort:     point.F1ForeignShort,
			F1TotalBid:         point.F1TotalBid,
			F1TotalAsk:         point.F1TotalAsk,
			VN30TotalBuyOrder:  point.VN30TotalBuyOrder,
			VN30TotalSellOrder: point.VN30TotalSellOrder,
			VN30BuyUp:          point.VN30BuyUp,
			VN30SellDown:       point.VN30SellDown,
			VN30ForeignNet:     point.VN30ForeignNet,
			FuturesResponse:    futuresContract, // Include which futures contract this data is for
		}
		if err := writer.WriteJSON(msg); err != nil {
			ws.log.WithError(err).WithField("client_id", clientID).Error("failed to send historical data")
			return
		}
	}

	// Send completion message with futures contract identifier
	completion := WebSocketMessage{
		Type:            "historical_complete",
		FuturesResponse: futuresContract,
	}
	writer.WriteJSON(completion)

	ws.log.WithFields(map[string]interface{}{
		"client_id": clientID,
		"count":     len(data),
	}).Info("historical data sent")
}

// handleCatchupRequest handles requests for catchup data from a timestamp
func (ws *WebSocketServer) handleCatchupRequest(writer *connWriter, clientID string, fromTimestamp int64, futuresContract string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := ws.repo.GetDataAfterTimestamp(ctx, fromTimestamp, futuresContract)
	if err != nil {
		ws.log.WithError(err).Error("failed to get catchup data")
		ws.sendError(writer, fmt.Sprintf("database error: %v", err))
		return
	}

	// Send catchup data
	for _, point := range data {
		msg := WebSocketMessage{
			Type:               "catchup",
			Timestamp:          point.Timestamp,
			VN30Value:          point.VN30Value,
			HNXValue:           point.HNXValue,
			F1Last:             point.HNXValue,
			F1ForeignLong:      point.F1ForeignLong,
			F1ForeignShort:     point.F1ForeignShort,
			F1TotalBid:         point.F1TotalBid,
			F1TotalAsk:         point.F1TotalAsk,
			VN30TotalBuyOrder:  point.VN30TotalBuyOrder,
			VN30TotalSellOrder: point.VN30TotalSellOrder,
			VN30BuyUp:          point.VN30BuyUp,
			VN30SellDown:       point.VN30SellDown,
			VN30ForeignNet:     point.VN30ForeignNet,
		}
		if err := writer.WriteJSON(msg); err != nil {
			ws.log.WithError(err).WithField("client_id", clientID).Error("failed to send catchup data")
			return
		}
	}

	// Send completion message
	completion := WebSocketMessage{Type: "catchup_complete"}
	writer.WriteJSON(completion)

	ws.log.WithFields(map[string]interface{}{
		"client_id": clientID,
		"count":     len(data),
	}).Info("catchup data sent")
}

// sendError sends an error message to the client
func (ws *WebSocketServer) sendError(writer *connWriter, errorMsg string) {
	msg := WebSocketMessage{
		Type:  "error",
		Error: errorMsg,
	}
	writer.WriteJSON(msg)
}

// ServeHTTP implements http.Handler
func (ws *WebSocketServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ws" {
		ws.HandleWebSocket(w, r)
	} else {
		http.NotFound(w, r)
	}
}
