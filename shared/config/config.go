package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all configuration for the market data service
type Config struct {
	Server   ServerConfig
	Database DatabaseConfig
	Redis    RedisConfig
	Logging  LoggingConfig
	Market   MarketConfig
}

// ServerConfig holds server-specific configuration
type ServerConfig struct {
	GRPCPort       string        `mapstructure:"grpc_port"`
	HTTPPort       string        `mapstructure:"http_port"` // For health checks
	Environment    string        `mapstructure:"environment"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// DatabaseConfig holds database connection configuration
type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            int           `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	Database        string        `mapstructure:"database"`
	SSLMode         string        `mapstructure:"ssl_mode"`
	MaxConnections  int           `mapstructure:"max_connections"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	ListenChannel   string        `mapstructure:"listen_channel"`
}

// RedisConfig holds Redis connection configuration
type RedisConfig struct {
	Address     string        `mapstructure:"address"`
	Password    string        `mapstructure:"password"`
	DB          int           `mapstructure:"db"`
	MaxRetries  int           `mapstructure:"max_retries"`
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout time.Duration `mapstructure:"read_timeout"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string `mapstructure:"level"`      // debug, info, warn, error
	Format     string `mapstructure:"format"`     // json, console
	TimeFormat string `mapstructure:"time_format"` // unix, rfc3339
}

// MarketConfig holds market-specific configuration
type MarketConfig struct {
	VN30TableName      string        `mapstructure:"vn30_table_name"`
	HNXTableName       string        `mapstructure:"hnx_table_name"`
	CacheTTL           time.Duration `mapstructure:"cache_ttl"`
	StreamBufferSize   int           `mapstructure:"stream_buffer_size"`
	MaxConcurrentConns int           `mapstructure:"max_concurrent_connections"`
}

// Load loads configuration from file and environment variables
func Load(configPath string) (*Config, error) {
	v := viper.New()

	// Set default values
	setDefaults(v)

	// Set config file path and name
	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		v.AddConfigPath("./configs")
		v.AddConfigPath("/etc/market-service")
	}

	// Read config file
	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
		// Config file not found, use defaults and env vars
	}

	// Environment variables override config file
	v.SetEnvPrefix("MARKET")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Unmarshal config
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Validate config
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

// setDefaults sets default configuration values
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("server.grpc_port", ":50051")
	v.SetDefault("server.http_port", ":8080")
	v.SetDefault("server.environment", "development")
	v.SetDefault("server.shutdown_timeout", 30*time.Second)

	// Database defaults
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.user", "postgres")
	v.SetDefault("database.password", "")
	v.SetDefault("database.database", "trading_db")
	v.SetDefault("database.ssl_mode", "disable")
	v.SetDefault("database.max_connections", 25)
	v.SetDefault("database.conn_max_lifetime", 5*time.Minute)
	v.SetDefault("database.listen_channel", "market_data_updates")

	// Redis defaults
	v.SetDefault("redis.address", "localhost:6379")
	v.SetDefault("redis.password", "")
	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.max_retries", 3)
	v.SetDefault("redis.dial_timeout", 5*time.Second)
	v.SetDefault("redis.read_timeout", 3*time.Second)

	// Logging defaults
	v.SetDefault("logging.level", "info")
	v.SetDefault("logging.format", "json")
	v.SetDefault("logging.time_format", "unix")

	// Market defaults
	v.SetDefault("market.vn30_table_name", "vn30_table")
	v.SetDefault("market.hnx_table_name", "hnx_table")
	v.SetDefault("market.cache_ttl", 24*time.Hour)
	v.SetDefault("market.stream_buffer_size", 100)
	v.SetDefault("market.max_concurrent_connections", 1000)
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.GRPCPort == "" {
		return fmt.Errorf("server.grpc_port is required")
	}

	if c.Database.Host == "" {
		return fmt.Errorf("database.host is required")
	}

	if c.Database.Database == "" {
		return fmt.Errorf("database.database is required")
	}

	if c.Redis.Address == "" {
		return fmt.Errorf("redis.address is required")
	}

	validLogLevels := map[string]bool{"debug": true, "info": true, "warn": true, "error": true}
	if !validLogLevels[c.Logging.Level] {
		return fmt.Errorf("invalid logging.level: %s", c.Logging.Level)
	}

	return nil
}

// DatabaseURL returns the PostgreSQL connection string
func (c *DatabaseConfig) DatabaseURL() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Database, c.SSLMode)
}

// IsProduction returns true if running in production environment
func (c *Config) IsProduction() bool {
	return c.Server.Environment == "production"
}

// IsDevelopment returns true if running in development environment
func (c *Config) IsDevelopment() bool {
	return c.Server.Environment == "development"
}
