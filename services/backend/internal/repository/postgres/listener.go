package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lnvi/market-shared/logger"
	"github.com/jackc/pgx/v5"
)

// NotificationPayload represents the data sent via PostgreSQL NOTIFY
type NotificationPayload struct {
	Table     string  `json:"table"`
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// NotificationHandler is a callback function for handling notifications
type NotificationHandler func(NotificationPayload)

// Listener implements repository.ListenerRepository for PostgreSQL
type Listener struct {
	conn      *pgx.Conn
	channel   string
	handler   NotificationHandler
	log       *logger.Logger
}

// NewListener creates a new PostgreSQL listener
func NewListener(ctx context.Context, connString, channel string, handler NotificationHandler, log *logger.Logger) (*Listener, error) {
	// Connect to PostgreSQL
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	listener := &Listener{
		conn:    conn,
		channel: channel,
		handler: handler,
		log:     log,
	}

	// Start listening to the channel
	_, err = conn.Exec(ctx, fmt.Sprintf("LISTEN %s", channel))
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to listen on channel %s: %w", channel, err)
	}

	log.WithField("channel", channel).Info("PostgreSQL listener initialized")
	return listener, nil
}

// Start begins listening for notifications
func (l *Listener) Start(ctx context.Context) error {
	l.log.Info("started listening for PostgreSQL notifications")

	for {
		select {
		case <-ctx.Done():
			l.log.Info("listener context cancelled, shutting down")
			return ctx.Err()
		default:
			// Wait for notification
			notification, err := l.conn.WaitForNotification(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				l.log.WithError(err).Error("error waiting for notification")
				time.Sleep(1 * time.Second)
				continue
			}

			// Parse notification payload
			var payload NotificationPayload
			if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
				l.log.WithError(err).WithField("payload", notification.Payload).Error("failed to parse notification payload")
				continue
			}

			l.log.WithFields(map[string]interface{}{
				"table":     payload.Table,
				"timestamp": payload.Timestamp,
				"value":     payload.Value,
			}).Debug("notification received, calling handler")

			// Call the handler
			l.handler(payload)
		}
	}
}

// Close closes the PostgreSQL connection
func (l *Listener) Close(ctx context.Context) error {
	if err := l.conn.Close(ctx); err != nil {
		l.log.WithError(err).Error("failed to close listener connection")
		return err
	}
	l.log.Info("PostgreSQL listener closed")
	return nil
}
