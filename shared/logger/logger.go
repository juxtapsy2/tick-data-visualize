package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

type contextKey string

const (
	loggerKey contextKey = "logger"
)

// Logger wraps zerolog.Logger with additional functionality
type Logger struct {
	logger zerolog.Logger
}

// New creates a new logger instance
func New(level, format, timeFormat string) *Logger {
	var output io.Writer = os.Stdout

	// Configure time format
	switch timeFormat {
	case "unix":
		zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	case "rfc3339":
		zerolog.TimeFieldFormat = time.RFC3339
	default:
		zerolog.TimeFieldFormat = time.RFC3339
	}

	// Configure output format
	if format == "console" {
		output = zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "15:04:05",
			NoColor:    false, // Enable colors
			FormatLevel: func(i interface{}) string {
				var level string
				if ll, ok := i.(string); ok {
					switch ll {
					case "debug":
						level = "\033[35mDBG\033[0m" // Magenta
					case "info":
						level = "\033[36mINF\033[0m" // Cyan
					case "warn":
						level = "\033[33mWRN\033[0m" // Yellow
					case "error":
						level = "\033[31mERR\033[0m" // Red
					case "fatal":
						level = "\033[1;31mFTL\033[0m" // Bold Red
					default:
						level = "\033[37m???\033[0m" // White
					}
				}
				return level
			},
			FormatMessage: func(i interface{}) string {
				return fmt.Sprintf("\033[1m%s\033[0m", i) // Bold message
			},
			FormatFieldName: func(i interface{}) string {
				return fmt.Sprintf("\033[2m%s=\033[0m", i) // Dim field names
			},
			FormatFieldValue: func(i interface{}) string {
				return fmt.Sprintf("\033[36m%s\033[0m", i) // Cyan field values
			},
		}
	}

	// Parse log level
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	// Create logger
	zlog := zerolog.New(output).
		Level(logLevel).
		With().
		Timestamp().
		Caller().
		Logger()

	return &Logger{logger: zlog}
}

// WithContext returns a logger with context values
func (l *Logger) WithContext(ctx context.Context) *Logger {
	return &Logger{logger: l.logger.With().Logger()}
}

// WithField adds a field to the logger
func (l *Logger) WithField(key string, value interface{}) *Logger {
	return &Logger{logger: l.logger.With().Interface(key, value).Logger()}
}

// WithFields adds multiple fields to the logger
func (l *Logger) WithFields(fields map[string]interface{}) *Logger {
	zlog := l.logger.With()
	for k, v := range fields {
		zlog = zlog.Interface(k, v)
	}
	return &Logger{logger: zlog.Logger()}
}

// WithError adds an error to the logger
func (l *Logger) WithError(err error) *Logger {
	return &Logger{logger: l.logger.With().Err(err).Logger()}
}

// Debug logs a debug message
func (l *Logger) Debug(msg string) {
	l.logger.Debug().Msg(msg)
}

// Debugf logs a formatted debug message
func (l *Logger) Debugf(format string, args ...interface{}) {
	l.logger.Debug().Msgf(format, args...)
}

// Info logs an info message
func (l *Logger) Info(msg string) {
	l.logger.Info().Msg(msg)
}

// Infof logs a formatted info message
func (l *Logger) Infof(format string, args ...interface{}) {
	l.logger.Info().Msgf(format, args...)
}

// Warn logs a warning message
func (l *Logger) Warn(msg string) {
	l.logger.Warn().Msg(msg)
}

// Warnf logs a formatted warning message
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.logger.Warn().Msgf(format, args...)
}

// Error logs an error message
func (l *Logger) Error(msg string) {
	l.logger.Error().Msg(msg)
}

// Errorf logs a formatted error message
func (l *Logger) Errorf(format string, args ...interface{}) {
	l.logger.Error().Msgf(format, args...)
}

// Fatal logs a fatal message and exits
func (l *Logger) Fatal(msg string) {
	l.logger.Fatal().Msg(msg)
}

// Fatalf logs a formatted fatal message and exits
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.logger.Fatal().Msgf(format, args...)
}

// ToContext adds the logger to a context
func (l *Logger) ToContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, loggerKey, l)
}

// FromContext retrieves a logger from context
func FromContext(ctx context.Context) *Logger {
	if l, ok := ctx.Value(loggerKey).(*Logger); ok {
		return l
	}
	return New("info", "json", "unix") // Return default logger if not in context
}

// GetZerolog returns the underlying zerolog.Logger
func (l *Logger) GetZerolog() *zerolog.Logger {
	return &l.logger
}
