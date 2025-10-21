package health

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-shared/logger"
)

// Status represents the health status of a component
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
	StatusDegraded  Status = "degraded"
)

// ComponentHealth represents the health of a single component
type ComponentHealth struct {
	Status    Status        `json:"status"`
	Message   string        `json:"message,omitempty"`
	Latency   time.Duration `json:"latency_ms,omitempty"`
	Timestamp time.Time     `json:"timestamp"`
}

// HealthResponse represents the overall health response
type HealthResponse struct {
	Status     Status                      `json:"status"`
	Version    string                      `json:"version"`
	Uptime     time.Duration               `json:"uptime_seconds"`
	Components map[string]ComponentHealth `json:"components"`
	Timestamp  time.Time                   `json:"timestamp"`
}

// Checker defines the interface for health checks
type Checker interface {
	Check(ctx context.Context) ComponentHealth
	Name() string
}

// Service manages health checks
type Service struct {
	checkers  []Checker
	startTime time.Time
	version   string
	mu        sync.RWMutex
	log       *logger.Logger
}

// NewService creates a new health check service
func NewService(version string, log *logger.Logger) *Service {
	return &Service{
		checkers:  []Checker{},
		startTime: time.Now(),
		version:   version,
		log:       log,
	}
}

// RegisterChecker registers a health checker
func (s *Service) RegisterChecker(checker Checker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkers = append(s.checkers, checker)
	s.log.WithField("checker", checker.Name()).Debug("health checker registered")
}

// Check performs all health checks
func (s *Service) Check(ctx context.Context) HealthResponse {
	s.mu.RLock()
	checkers := make([]Checker, len(s.checkers))
	copy(checkers, s.checkers)
	s.mu.RUnlock()

	components := make(map[string]ComponentHealth)
	overallStatus := StatusHealthy

	// Run checks concurrently
	var wg sync.WaitGroup
	resultsChan := make(chan struct {
		name   string
		health ComponentHealth
	}, len(checkers))

	for _, checker := range checkers {
		wg.Add(1)
		go func(c Checker) {
			defer wg.Done()
			health := c.Check(ctx)
			resultsChan <- struct {
				name   string
				health ComponentHealth
			}{name: c.Name(), health: health}
		}(checker)
	}

	// Wait for all checks to complete
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	// Collect results
	for result := range resultsChan {
		components[result.name] = result.health

		// Determine overall status
		if result.health.Status == StatusUnhealthy {
			overallStatus = StatusUnhealthy
		} else if result.health.Status == StatusDegraded && overallStatus != StatusUnhealthy {
			overallStatus = StatusDegraded
		}
	}

	return HealthResponse{
		Status:     overallStatus,
		Version:    s.version,
		Uptime:     time.Since(s.startTime),
		Components: components,
		Timestamp:  time.Now(),
	}
}

// HTTPHandler returns an HTTP handler for health checks
func (s *Service) HTTPHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		health := s.Check(ctx)

		w.Header().Set("Content-Type", "application/json")

		// Set HTTP status code based on health
		switch health.Status {
		case StatusHealthy:
			w.WriteHeader(http.StatusOK)
		case StatusDegraded:
			w.WriteHeader(http.StatusOK) // Still accepting traffic
		case StatusUnhealthy:
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		if err := json.NewEncoder(w).Encode(health); err != nil {
			s.log.WithError(err).Error("failed to encode health response")
		}
	}
}

// ReadinessHandler returns an HTTP handler for readiness checks
func (s *Service) ReadinessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		health := s.Check(ctx)

		w.Header().Set("Content-Type", "application/json")

		// Readiness is stricter - only healthy services are ready
		if health.Status == StatusHealthy {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": string(health.Status),
		}); err != nil {
			s.log.WithError(err).Error("failed to encode readiness response")
		}
	}
}

// LivenessHandler returns an HTTP handler for liveness checks
func (s *Service) LivenessHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Liveness is simple - if we can respond, we're alive
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "alive",
		})
	}
}

// PostgresChecker checks PostgreSQL health
type PostgresChecker struct {
	repo repository.MarketRepository
}

// NewPostgresChecker creates a PostgreSQL health checker
func NewPostgresChecker(repo repository.MarketRepository) *PostgresChecker {
	return &PostgresChecker{repo: repo}
}

// Name returns the checker name
func (c *PostgresChecker) Name() string {
	return "postgres"
}

// Check performs the health check
func (c *PostgresChecker) Check(ctx context.Context) ComponentHealth {
	start := time.Now()

	// Try to get latest data
	_, err := c.repo.GetLatestData(ctx)
	latency := time.Since(start)

	if err != nil {
		return ComponentHealth{
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("failed to query: %v", err),
			Latency:   latency,
			Timestamp: time.Now(),
		}
	}

	return ComponentHealth{
		Status:    StatusHealthy,
		Latency:   latency,
		Timestamp: time.Now(),
	}
}

// RedisChecker checks Redis health
type RedisChecker struct {
	cache repository.CacheRepository
}

// NewRedisChecker creates a Redis health checker
func NewRedisChecker(cache repository.CacheRepository) *RedisChecker {
	return &RedisChecker{cache: cache}
}

// Name returns the checker name
func (c *RedisChecker) Name() string {
	return "redis"
}

// Check performs the health check
func (c *RedisChecker) Check(ctx context.Context) ComponentHealth {
	start := time.Now()

	// Try to get timestamp (lightweight operation)
	_, err := c.cache.GetLatestTimestamp(ctx, time.Now().Format("2006-01-02"))
	latency := time.Since(start)

	if err != nil {
		return ComponentHealth{
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("failed to query: %v", err),
			Latency:   latency,
			Timestamp: time.Now(),
		}
	}

	return ComponentHealth{
		Status:    StatusHealthy,
		Latency:   latency,
		Timestamp: time.Now(),
	}
}
