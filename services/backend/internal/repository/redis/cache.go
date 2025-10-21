package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lnvi/market-shared/config"
	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/logger"
	"github.com/redis/go-redis/v9"
)

// Cache implements repository.CacheRepository for Redis
type Cache struct {
	client *redis.Client
	config *config.RedisConfig
	log    *logger.Logger
}

// NewCache creates a new Redis cache
func NewCache(cfg *config.RedisConfig, log *logger.Logger) (*Cache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Address,
		Password:     cfg.Password,
		DB:           cfg.DB,
		MaxRetries:   cfg.MaxRetries,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.ReadTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.Info("Redis cache initialized successfully")

	return &Cache{
		client: client,
		config: cfg,
		log:    log,
	}, nil
}

// GetHistoricalData retrieves cached market data
func (c *Cache) GetHistoricalData(ctx context.Context, date string) ([]repository.MarketData, error) {
	key := fmt.Sprintf("market:historical:%s", date)

	data, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		c.log.WithField("date", date).Debug("cache miss")
		return nil, nil
	} else if err != nil {
		c.log.WithError(err).WithField("date", date).Error("redis get error")
		return nil, fmt.Errorf("redis get error: %w", err)
	}

	var points []repository.MarketData
	if err := json.Unmarshal([]byte(data), &points); err != nil {
		c.log.WithError(err).Error("failed to unmarshal cached data")
		return nil, fmt.Errorf("failed to unmarshal cached data: %w", err)
	}

	c.log.WithFields(map[string]interface{}{
		"date":  date,
		"count": len(points),
	}).Debug("cache hit")

	return points, nil
}

// SetHistoricalData caches market data
func (c *Cache) SetHistoricalData(ctx context.Context, date string, data []repository.MarketData, ttl time.Duration) error {
	key := fmt.Sprintf("market:historical:%s", date)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	if err := c.client.Set(ctx, key, jsonData, ttl).Err(); err != nil {
		c.log.WithError(err).WithField("date", date).Error("redis set error")
		return fmt.Errorf("redis set error: %w", err)
	}

	c.log.WithFields(map[string]interface{}{
		"date":  date,
		"count": len(data),
		"ttl":   ttl,
	}).Debug("data cached successfully")

	return nil
}

// AppendDataPoint appends a data point to cached data
func (c *Cache) AppendDataPoint(ctx context.Context, date string, point repository.MarketData) error {
	// Get existing data
	existing, err := c.GetHistoricalData(ctx, date)
	if err != nil {
		return err
	}

	if existing == nil {
		existing = []repository.MarketData{}
	}

	// Append new point
	existing = append(existing, point)

	// Calculate TTL until end of day
	now := time.Now()
	endOfDay := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, now.Location())
	ttl := time.Until(endOfDay) + time.Hour

	if ttl < time.Hour {
		ttl = time.Hour
	}

	return c.SetHistoricalData(ctx, date, existing, ttl)
}

// GetLatestTimestamp gets the timestamp of the last cached point
func (c *Cache) GetLatestTimestamp(ctx context.Context, date string) (int64, error) {
	data, err := c.GetHistoricalData(ctx, date)
	if err != nil {
		return 0, err
	}

	if len(data) == 0 {
		return 0, nil
	}

	return data[len(data)-1].Timestamp, nil
}

// Close closes the Redis connection
func (c *Cache) Close() error {
	if err := c.client.Close(); err != nil {
		c.log.WithError(err).Error("failed to close Redis connection")
		return err
	}
	c.log.Info("Redis cache closed")
	return nil
}

// Ping tests the Redis connection
func (c *Cache) Ping(ctx context.Context) error {
	return c.client.Ping(ctx).Err()
}

// FlushAll flushes all cached data (use with caution)
func (c *Cache) FlushAll(ctx context.Context) error {
	return c.client.FlushAll(ctx).Err()
}

// AppendToStream appends a market data point to Redis Stream (for real-time data)
// This is more efficient than AppendDataPoint for streaming use cases
func (c *Cache) AppendToStream(ctx context.Context, point repository.MarketData) error {
	streamKey := "market:stream"

	// Add to stream with auto-generated ID
	err := c.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 3600, // Keep last 1 hour (3600 seconds at 1Hz)
		Approx: true,  // Allow approximate trimming for performance
		Values: map[string]interface{}{
			"timestamp": point.Timestamp,
			"vn30":      point.VN30Value,
			"hnx":       point.HNXValue,
		},
	}).Err()

	if err != nil {
		c.log.WithError(err).Warn("failed to append to Redis stream")
		return fmt.Errorf("redis stream append error: %w", err)
	}

	return nil
}

// GetRecentDataFromStream retrieves recent data from Redis Stream (last N seconds)
func (c *Cache) GetRecentDataFromStream(ctx context.Context, seconds int) ([]repository.MarketData, error) {
	streamKey := "market:stream"

	// Get last N entries
	result, err := c.client.XRevRangeN(ctx, streamKey, "+", "-", int64(seconds)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis stream read error: %w", err)
	}

	var data []repository.MarketData
	for i := len(result) - 1; i >= 0; i-- { // Reverse to get chronological order
		msg := result[i]

		timestamp, _ := msg.Values["timestamp"].(string)
		vn30, _ := msg.Values["vn30"].(string)
		hnx, _ := msg.Values["hnx"].(string)

		var ts int64
		var vn30Val, hnxVal float64
		fmt.Sscanf(timestamp, "%d", &ts)
		fmt.Sscanf(vn30, "%f", &vn30Val)
		fmt.Sscanf(hnx, "%f", &hnxVal)

		data = append(data, repository.MarketData{
			Timestamp: ts,
			VN30Value: vn30Val,
			HNXValue:  hnxVal,
		})
	}

	return data, nil
}

// GetStreamDataByTimeRange retrieves data from Redis Stream within a time range
// Returns data points where fromTime <= timestamp <= toTime
func (c *Cache) GetStreamDataByTimeRange(ctx context.Context, fromTime, toTime time.Time) ([]repository.MarketData, error) {
	streamKey := "market:stream"

	// Read all entries from stream (limited by MaxLen in XAdd)
	result, err := c.client.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("redis stream read error: %w", err)
	}

	if len(result) == 0 {
		c.log.Debug("no data in Redis stream")
		return nil, nil
	}

	fromUnix := fromTime.Unix()
	toUnix := toTime.Unix()

	var data []repository.MarketData
	for _, msg := range result {
		timestamp, _ := msg.Values["timestamp"].(string)
		vn30, _ := msg.Values["vn30"].(string)
		hnx, _ := msg.Values["hnx"].(string)

		var ts int64
		var vn30Val, hnxVal float64
		fmt.Sscanf(timestamp, "%d", &ts)
		fmt.Sscanf(vn30, "%f", &vn30Val)
		fmt.Sscanf(hnx, "%f", &hnxVal)

		// Filter by time range
		if ts >= fromUnix && ts <= toUnix {
			data = append(data, repository.MarketData{
				Timestamp: ts,
				VN30Value: vn30Val,
				HNXValue:  hnxVal,
			})
		}
	}

	c.log.WithFields(map[string]interface{}{
		"from":  fromTime,
		"to":    toTime,
		"count": len(data),
		"total": len(result),
	}).Debug("retrieved data from Redis stream by time range")

	return data, nil
}
