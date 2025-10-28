package repository

import (
	"context"
	"time"
)

// MarketData represents joined VN30 and F1 futures data with positions
type MarketData struct {
	Timestamp      int64
	VN30Value      float64
	HNXValue       float64 // F1 futures last price
	F1ForeignLong  float64 // F1 foreign buy volume (total_f_buy_vol)
	F1ForeignShort float64 // F1 foreign sell volume (total_f_sell_vol)
	F1TotalBid     float64 // F1 total bid (long orders)
	F1TotalAsk     float64 // F1 total ask (short orders)
}

// ChartData represents aggregated data for a specific ticker
type ChartData struct {
	Ticker string
	Value  float64
}

// MarketRepository defines the interface for market data operations
type MarketRepository interface {
	// GetHistoricalData retrieves market data for a time range with specified futures contract (f1, f2, f3, f4)
	GetHistoricalData(ctx context.Context, startTime, endTime time.Time, futuresContract string) ([]MarketData, error)

	// GetLatestData retrieves the most recent market data point for specified futures contract
	GetLatestData(ctx context.Context, futuresContract string) (*MarketData, error)

	// GetDataAtTimestamp retrieves market data for a specific timestamp (15s bucket) with specified futures contract
	GetDataAtTimestamp(ctx context.Context, timestamp int64, futuresContract string) (*MarketData, error)

	// GetDataAfterTimestamp retrieves data after a specific timestamp for specified futures contract
	GetDataAfterTimestamp(ctx context.Context, afterTimestamp int64, futuresContract string) ([]MarketData, error)

	// GetLast15sAverages retrieves AVG of last 15 seconds for multiple tickers
	GetLast15sAverages(ctx context.Context, indexTickers []string, futuresTickers []string) ([]ChartData, error)

	// Close closes the repository connection
	Close() error
}

// CacheRepository defines the interface for cache operations
type CacheRepository interface {
	// GetHistoricalData retrieves cached market data
	GetHistoricalData(ctx context.Context, date string) ([]MarketData, error)

	// SetHistoricalData caches market data
	SetHistoricalData(ctx context.Context, date string, data []MarketData, ttl time.Duration) error

	// AppendDataPoint appends a data point to cached data
	AppendDataPoint(ctx context.Context, date string, point MarketData) error

	// GetLatestTimestamp gets the timestamp of the last cached point
	GetLatestTimestamp(ctx context.Context, date string) (int64, error)

	// GetStreamDataByTimeRange retrieves data from Redis Stream within a time range
	GetStreamDataByTimeRange(ctx context.Context, fromTime, toTime time.Time) ([]MarketData, error)

	// Close closes the cache connection
	Close() error
}

// ListenerRepository defines the interface for database notification listening
type ListenerRepository interface {
	// Start begins listening for notifications
	Start(ctx context.Context) error

	// Close closes the listener connection
	Close(ctx context.Context) error
}
