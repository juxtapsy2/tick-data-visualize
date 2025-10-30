package server

import (
	"context"
	"sync"
	"time"

	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/logger"
)

// MarketDataMessage represents a market data update
type MarketDataMessage struct {
	Timestamp      int64
	VN30Value      float64
	HNXValue       float64 // F1 futures last price
	F1ForeignLong  float64 // F1 foreign buy volume
	F1ForeignShort float64 // F1 foreign sell volume
	F1TotalBid     float64 // F1 total bid (long orders)
	F1TotalAsk     float64 // F1 total ask (short orders)

	// VN30 Stocks Data (Charts 4-6)
	VN30TotalBuyOrder  float64 // Chart 4: Total buy orders
	VN30TotalSellOrder float64 // Chart 4: Total sell orders
	VN30BuyUp          float64 // Chart 5: Buy-up volume
	VN30SellDown       float64 // Chart 5: Sell-down volume
	VN30ForeignNet     float64 // Chart 6: Foreign net value
}

// StreamClient represents a connected client
type StreamClient struct {
	ID   string
	Ch   chan MarketDataMessage
	Done chan struct{}
}

// Broadcaster manages broadcasting to multiple clients
type Broadcaster struct {
	mu      sync.RWMutex
	clients map[string]*StreamClient

	// Broadcasting throttle (15s intervals)
	lastBroadcastTime int64

	// Repository for querying aggregated data
	repo repository.MarketRepository

	// Redis cache for async writes (optional)
	cache repository.CacheRepository

	log *logger.Logger
}

// NewBroadcaster creates a new broadcaster
func NewBroadcaster(repo repository.MarketRepository, cache repository.CacheRepository, log *logger.Logger) *Broadcaster {
	return &Broadcaster{
		clients: make(map[string]*StreamClient),
		repo:    repo,
		cache:   cache,
		log:     log,
	}
}

// Register registers a new client
func (b *Broadcaster) Register(clientID string) *StreamClient {
	b.mu.Lock()
	defer b.mu.Unlock()

	client := &StreamClient{
		ID:   clientID,
		Ch:   make(chan MarketDataMessage, 100),
		Done: make(chan struct{}),
	}

	b.clients[clientID] = client
	b.log.WithField("client_id", clientID).WithField("total", len(b.clients)).Info("client registered")

	return client
}

// Unregister removes a client
func (b *Broadcaster) Unregister(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if client, exists := b.clients[clientID]; exists {
		close(client.Ch)
		close(client.Done)
		delete(b.clients, clientID)
		b.log.WithField("client_id", clientID).WithField("remaining", len(b.clients)).Info("client unregistered")
	}
}

// BroadcastMarketData broadcasts market data from the aggregated view (every 15s)
// This function queries the continuous aggregate which already has VN30+HNX joined
func (b *Broadcaster) BroadcastMarketData(timestamp int64) {
	// Only broadcast every 15 seconds (align with continuous aggregate buckets)
	if timestamp%15 != 0 {
		return // Skip non-15s timestamps
	}

	// Check if within market hours (9:00 AM - 2:45 PM Vietnam time)
	vietnamLocation := time.FixedZone("ICT", 7*60*60)
	now := time.Now().In(vietnamLocation)
	currentHour := now.Hour()
	currentMinute := now.Minute()

	// Market hours: 9:00 AM - 2:45 PM (14:45)
	isWithinMarketHours := (currentHour >= 9 && currentHour < 14) || (currentHour == 14 && currentMinute <= 45)
	if !isWithinMarketHours {
		b.log.Debug("outside market hours - skipping WebSocket broadcast")
		return
	}

	// Query the latest available data from continuous aggregate (VN30+HNX already joined with forward-fill!)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Broadcaster always uses f1 for now (can be enhanced to broadcast multiple contracts later)
	data, err := b.repo.GetLatestData(ctx, "f1")
	if err != nil {
		b.log.WithError(err).Debug("failed to get latest data from aggregate")
		return
	}

	// Skip if this is old data we've already broadcast
	b.mu.Lock()
	if data.Timestamp <= b.lastBroadcastTime {
		b.mu.Unlock()
		return
	}
	b.lastBroadcastTime = data.Timestamp
	b.mu.Unlock()

	msg := MarketDataMessage{
		Timestamp:          data.Timestamp,
		VN30Value:          data.VN30Value,
		HNXValue:           data.HNXValue,
		F1ForeignLong:      data.F1ForeignLong,
		F1ForeignShort:     data.F1ForeignShort,
		F1TotalBid:         data.F1TotalBid,
		F1TotalAsk:         data.F1TotalAsk,
		VN30TotalBuyOrder:  data.VN30TotalBuyOrder,
		VN30TotalSellOrder: data.VN30TotalSellOrder,
		VN30BuyUp:          data.VN30BuyUp,
		VN30SellDown:       data.VN30SellDown,
		VN30ForeignNet:     data.VN30ForeignNet,
	}

	// Broadcast to connected clients (synchronous, in-memory)
	b.broadcast(msg)

	// Write to Redis asynchronously (non-blocking)
	go b.asyncWriteToRedis(msg)

	b.log.WithFields(map[string]interface{}{
		"vn30":      data.VN30Value,
		"hnx":       data.HNXValue,
		"timestamp": data.Timestamp,
		"clients":   len(b.clients),
	}).Info("broadcasting market data from aggregate (15s interval)")
}

// asyncWriteToRedis writes data to Redis Stream without blocking the broadcast
func (b *Broadcaster) asyncWriteToRedis(msg MarketDataMessage) {
	if b.cache == nil {
		return // Redis not configured
	}

	// Create context with timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Convert to repository.MarketData
	point := repository.MarketData{
		Timestamp:          msg.Timestamp,
		VN30Value:          msg.VN30Value,
		HNXValue:           msg.HNXValue,
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

	// Write to Redis Stream (non-blocking)
	// Type assertion to access Redis-specific method
	if redisCache, ok := b.cache.(interface {
		AppendToStream(ctx context.Context, point repository.MarketData) error
	}); ok {
		if err := redisCache.AppendToStream(ctx, point); err != nil {
			// Log but don't fail - Redis writes are best-effort
			b.log.WithError(err).Debug("redis stream write failed (non-critical)")
		}
	}
}

// BroadcastComplete broadcasts a complete market data point
func (b *Broadcaster) BroadcastComplete(msg MarketDataMessage) {
	b.broadcast(msg)
}

// broadcast sends message to all clients
func (b *Broadcaster) broadcast(msg MarketDataMessage) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for clientID, client := range b.clients {
		select {
		case client.Ch <- msg:
			// Success
		case <-client.Done:
			// Client disconnected
		default:
			// Channel full
			b.log.WithField("client_id", clientID).Warn("client channel full, dropping message")
		}
	}
}

// BroadcastChartData broadcasts chart data from query results
func (b *Broadcaster) BroadcastChartData(chartData []repository.ChartData) {
	if len(chartData) == 0 {
		return
	}

	// Convert ChartData to MarketDataMessage
	// For now, we map VN30 and 41I1FA000 to the existing message structure
	msg := MarketDataMessage{
		Timestamp: time.Now().Unix(),
	}

	// Map chart data to message fields
	for _, chart := range chartData {
		switch chart.Ticker {
		case "VN30":
			msg.VN30Value = chart.Value
		case "f1":
			msg.HNXValue = chart.Value // Temporarily use HNXValue for futures
		}
	}

	// Broadcast to connected clients (synchronous, in-memory)
	b.broadcast(msg)

	// Write to Redis asynchronously (non-blocking)
	go b.asyncWriteToRedis(msg)

	b.log.WithFields(map[string]interface{}{
		"charts":  len(chartData),
		"clients": len(b.clients),
	}).Info("broadcasting chart data")
}

// GetActiveClientsCount returns the number of active clients
func (b *Broadcaster) GetActiveClientsCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}
