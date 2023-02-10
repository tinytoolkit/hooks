package hook

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TableOp represents an operation performed on a table.
type TableOp struct {
	Table string `json:"table"`
	Op    string `json:"op"`
}

// Payload represents the payload of a notification from the database.
type Payload struct {
	Channel string         `json:"channel"`
	Table   string         `json:"table"`
	Op      string         `json:"op"`
	Data    map[string]any `json:"data"`
}

// Handler is an interface that handles a payload.
type Handler interface {
	Handle(ctx context.Context, payload Payload)
}

// HandlerFunc is a function that handles a payload. It implements the Handler interface.
type HandlerFunc func(ctx context.Context, payload Payload)

// Handle is a function that handles a payload.
func (hf HandlerFunc) Handle(ctx context.Context, payload Payload) {
	hf(ctx, payload)
}

// Mux is a multiplexer that routes notifications to the appropriate handler.
type Mux struct {
	mu       sync.Mutex
	handlers map[string]map[TableOp][]Handler
}

// NewMux creates a new Mux.
func NewMux() *Mux {
	return &Mux{
		handlers: make(map[string]map[TableOp][]Handler),
	}
}

// Handle adds a handler for the specified table operation on the specified channel.
func (m *Mux) Handle(channel string, tableOp TableOp, h Handler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.handlers[channel]; !ok {
		m.handlers[channel] = make(map[TableOp][]Handler)
	}
	m.handlers[channel][tableOp] = append(m.handlers[channel][tableOp], h)
}

// HandleFunc adds a handler function for the specified table operation on the specified channel.
func (m *Mux) HandleFunc(channel string, tableOp TableOp, handler func(ctx context.Context, payload Payload)) {
	m.Handle(channel, tableOp, HandlerFunc(handler))
}

// HandleInsert adds a handler for INSERT operations on the specified table on the specified channel.
func (m *Mux) HandleInsert(channel string, table string, h HandlerFunc) {
	m.Handle(channel, TableOp{Table: table, Op: "INSERT"}, h)
}

// HandleUpdate adds a handler for UPDATE operations on the specified table on the specified channel.
func (m *Mux) HandleUpdate(channel string, table string, h HandlerFunc) {
	m.Handle(channel, TableOp{Table: table, Op: "UPDATE"}, h)
}

// HandleDelete adds a handler for DELETE operations on the specified table on the specified channel.
func (m *Mux) HandleDelete(channel string, table string, h HandlerFunc) {
	m.Handle(channel, TableOp{Table: table, Op: "DELETE"}, h)
}

// Listener listens for notifications from a PostgreSQL database and invokes the appropriate handler functions.
type Listener struct {
	pool     *pgxpool.Pool
	mu       sync.Mutex
	channels []string
}

// NewListener creates a new Listener.
func NewListener(pool *pgxpool.Pool) *Listener {
	return &Listener{
		pool:     pool,
		channels: make([]string, 0),
	}
}

// SetChannel sets the channel to listen on.
func (l *Listener) SetChannels(channels []string) *Listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.channels = channels
	return l
}

// Listen starts listening for notifications from the database.
func (l *Listener) Listen(ctx context.Context, mux *Mux) error {
	if len(mux.handlers) == 0 {
		return errors.New("no handlers registered in mux")
	}

	pool, err := l.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer pool.Release()

	for _, channel := range l.channels {
		query := "LISTEN " + channel
		if _, err := pool.Exec(ctx, query); err != nil {
			return err
		}
	}

	for {
		notification, err := pool.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}

		go func() error {
			var payload struct {
				Table string          `json:"table"`
				Op    string          `json:"op"`
				Data  json.RawMessage `json:"data"`
			}

			if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
				return err
			}

			var data map[string]any
			if err := json.Unmarshal(payload.Data, &data); err != nil {
				return err
			}

			l.mu.Lock()
			handlers, ok := mux.handlers[notification.Channel]
			l.mu.Unlock()
			if !ok {
				return errors.New("no handlers registered for channel " + notification.Channel)
			}

			op := TableOp{Table: payload.Table, Op: payload.Op}
			for _, h := range handlers[op] {
				h.Handle(ctx, Payload{
					Channel: notification.Channel,
					Table:   payload.Table,
					Op:      payload.Op,
					Data:    data,
				})
			}
			return nil
		}()
	}
}
