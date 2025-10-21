package repository

import (
	"context"
	"time"
)

// MarketData represents joined VN30 and HNX data
type MarketData struct {
	Timestamp int64
	VN30Value float64
	HNXValue  float64
}

// ChartData represents aggregated data for a specific ticker
type ChartData struct {
	Ticker string
	Value  float64
}

// MarketRepository defines the interface for market data operations
type MarketRepository interface {
	// GetHistoricalData retrieves market data for a time range
	GetHistoricalData(ctx context.Context, startTime, endTime time.Time) ([]MarketData, error)

	// GetLatestData retrieves the most recent market data point
	GetLatestData(ctx context.Context) (*MarketData, error)

	// GetDataAtTimestamp retrieves market data for a specific timestamp (15s bucket)
	GetDataAtTimestamp(ctx context.Context, timestamp int64) (*MarketData, error)

	// GetDataAfterTimestamp retrieves data after a specific timestamp
	GetDataAfterTimestamp(ctx context.Context, afterTimestamp int64) ([]MarketData, error)

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
