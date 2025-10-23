package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/lnvi/market-service/internal/health"
	"github.com/lnvi/market-service/internal/repository"
	"github.com/lnvi/market-service/internal/repository/postgres"
	"github.com/lnvi/market-service/internal/repository/redis"
	"github.com/lnvi/market-service/internal/server"
	"github.com/lnvi/market-shared/config"
	"github.com/lnvi/market-shared/logger"
	"go.uber.org/fx"
)

const version = "1.0.0"

func main() {
	app := fx.New(
		// Provide dependencies
		fx.Provide(
			provideConfig,
			provideLogger,
			providePostgresRepo,
			provideRedisCache,
			provideBroadcaster,
			provideWebSocketServer,
			provideRESTHandler,
			provideHealthService,
			provideHTTPServer,
		),

		// Invoke lifecycle hooks - this triggers initialization
		fx.Invoke(
			func(*http.Server) {
				// This forces fx to initialize HTTP server
				// which in turn initializes all dependencies (postgres, redis)
			},
			registerLifecycleHooks,
		),
	)

	app.Run()
}

// provideConfig loads configuration
func provideConfig() (*config.Config, error) {
	cfg, err := config.Load("")
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	return cfg, nil
}

// provideLogger creates a logger
func provideLogger(cfg *config.Config) *logger.Logger {
	return logger.New(cfg.Logging.Level, cfg.Logging.Format, cfg.Logging.TimeFormat)
}

// providePostgresRepo creates PostgreSQL repository
func providePostgresRepo(lc fx.Lifecycle, cfg *config.Config, log *logger.Logger) (repository.MarketRepository, error) {
	log.Info("initializing PostgreSQL repository")

	// Initialize immediately, not in lifecycle hook
	repo, err := postgres.NewRepository(context.Background(), cfg.Database.DatabaseURL(), &cfg.Market, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres repository: %w", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("PostgreSQL repository ready")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("closing PostgreSQL repository")
			return repo.Close()
		},
	})

	return repo, nil
}

// provideRedisCache creates Redis cache
func provideRedisCache(lc fx.Lifecycle, cfg *config.Config, log *logger.Logger) (repository.CacheRepository, error) {
	log.Info("initializing Redis cache")

	// Initialize immediately, not in lifecycle hook
	cache, err := redis.NewCache(&cfg.Redis, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create redis cache: %w", err)
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("Redis cache ready")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("closing Redis cache")
			return cache.Close()
		},
	})

	return cache, nil
}

// provideBroadcaster creates broadcaster
func provideBroadcaster(repo repository.MarketRepository, cache repository.CacheRepository, log *logger.Logger) *server.Broadcaster {
	return server.NewBroadcaster(repo, cache, log)
}

// isMarketOpen checks if Vietnam stock market is currently open
// Vietnam market hours: 9:00 AM - 3:00 PM, Monday-Friday
func isMarketOpen() bool {
	// TESTING: Always return true to test broadcast functionality
	return true

	// TODO: Re-enable in production
	// now := time.Now()
	// if now.Weekday() == time.Saturday || now.Weekday() == time.Sunday {
	// 	return false
	// }
	// hour := now.Hour()
	// return hour >= 9 && hour < 15
}

// provideWebSocketServer creates WebSocket server
func provideWebSocketServer(
	broadcaster *server.Broadcaster,
	repo repository.MarketRepository,
	log *logger.Logger,
) *server.WebSocketServer {
	return server.NewWebSocketServer(broadcaster, repo, log)
}

// provideRESTHandler creates REST API handler
func provideRESTHandler(repo repository.MarketRepository, cache repository.CacheRepository, log *logger.Logger) *server.RESTHandler {
	return server.NewRESTHandler(repo, cache, log)
}

// provideHealthService creates health service
func provideHealthService(
	repo repository.MarketRepository,
	cache repository.CacheRepository,
	log *logger.Logger,
) *health.Service {
	healthSvc := health.NewService(version, log)
	healthSvc.RegisterChecker(health.NewPostgresChecker(repo))
	healthSvc.RegisterChecker(health.NewRedisChecker(cache))
	return healthSvc
}

// provideHTTPServer creates HTTP server for health checks, REST API, and WebSocket
func provideHTTPServer(
	lc fx.Lifecycle,
	cfg *config.Config,
	healthSvc *health.Service,
	broadcaster *server.Broadcaster,
	wsSvc *server.WebSocketServer,
	restHandler *server.RESTHandler,
	log *logger.Logger,
) *http.Server {
	mux := http.NewServeMux()

	// Health endpoints
	mux.HandleFunc("/health", healthSvc.HTTPHandler())
	mux.HandleFunc("/health/ready", healthSvc.ReadinessHandler())
	mux.HandleFunc("/health/live", healthSvc.LivenessHandler())

	// Metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"active_clients": %d}`, broadcaster.GetActiveClientsCount())
	})

	// REST API endpoints for market data
	mux.HandleFunc("/api/v1/market/historical", restHandler.HandleHistorical)
	mux.HandleFunc("/api/v1/market/latest", restHandler.HandleLatest)
	mux.HandleFunc("/api/v1/market/chart", restHandler.HandleChart)

	// WebSocket endpoint for real-time market data
	mux.HandleFunc("/ws", wsSvc.HandleWebSocket)

	server := &http.Server{
		Addr:         cfg.Server.HTTPPort,
		Handler:      mux,
		ReadTimeout:  30 * time.Second, // Increased for WebSocket
		WriteTimeout: 30 * time.Second,
	}

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.WithField("port", cfg.Server.HTTPPort).Info("starting HTTP server with WebSocket support")
			go func() {
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					log.WithError(err).Error("HTTP server error")
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("stopping HTTP server")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return server.Shutdown(shutdownCtx)
		},
	})

	return server
}

// registerLifecycleHooks registers application lifecycle hooks
func registerLifecycleHooks(lc fx.Lifecycle, log *logger.Logger, broadcaster *server.Broadcaster, repo repository.MarketRepository) {
	var broadcastTicker *time.Ticker
	var broadcastDone chan bool

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			log.Info("market data service started successfully")

			// Start timer-based polling scheduler (every 15 seconds)
			broadcastTicker = time.NewTicker(15 * time.Second)
			broadcastDone = make(chan bool)

			go func() {
				log.Info("starting 15-second broadcast scheduler")

				// Define tickers to query
				indexTickers := []string{"VN30"}
				futuresTickers := []string{"f1"}

				for {
					select {
					case <-broadcastTicker.C:
						// Skip if market is closed
						if !isMarketOpen() {
							log.Debug("market closed, skipping broadcast")
							continue
						}

						// Create query context with timeout
						queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

						// Query last 15 seconds averages
						chartData, err := repo.GetLast15sAverages(queryCtx, indexTickers, futuresTickers)
						cancel()

						if err != nil {
							log.WithError(err).Error("failed to get 15s averages")
							continue
						}

						// Broadcast to all WebSocket clients
						broadcaster.BroadcastChartData(chartData)

						log.WithFields(map[string]interface{}{
							"charts":  len(chartData),
							"clients": broadcaster.GetActiveClientsCount(),
						}).Info("broadcast completed")

					case <-broadcastDone:
						log.Info("stopping broadcast scheduler")
						return
					}
				}
			}()

			// Monitor active connections
			go func() {
				statusTicker := time.NewTicker(30 * time.Second)
				defer statusTicker.Stop()
				for range statusTicker.C {
					log.WithField("active_clients", broadcaster.GetActiveClientsCount()).Info("status update")
				}
			}()

			return nil
		},
		OnStop: func(ctx context.Context) error {
			log.Info("market data service shutting down")

			// Stop broadcast scheduler
			if broadcastTicker != nil {
				broadcastTicker.Stop()
			}
			if broadcastDone != nil {
				close(broadcastDone)
			}

			return nil
		},
	})
}
