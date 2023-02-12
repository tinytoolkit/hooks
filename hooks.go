package hooks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	InsertOp Op = "INSERT"
	UpdateOp Op = "UPDATE"
	DeleteOp Op = "DELETE"
)

type (
	// Hook is a struct that holds information about a connection pool and the handlers that should be triggered for each table operation.
	Hook struct {
		pool     *pgxpool.Pool
		handlers map[TableOp][]Handler
		mu       sync.Mutex
	}

	// Op is a type alias for string that represents an table operation.
	Op string

	// TableOp is a struct that holds information about a table and the operation that should be performed on it.
	TableOp struct {
		Table string `json:"table"`
		Op    Op     `json:"op"`
	}

	// Payload is a struct that holds information about a payload sent from the database.
	Payload struct {
		Op     Op             `json:"op"`
		Schema string         `json:"schema"`
		Table  string         `json:"table"`
		Row    map[string]any `json:"row"`
		OldRow map[string]any `json:"old_row"`
	}
)

// New creates a new Hook with a connection to the database using the given DSN.
func New(ctx context.Context, dsn string) (*Hook, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}

	return NewWithPool(pool), nil
}

// NewWithPool creates a new Hook with the given connection pool.
func NewWithPool(pool *pgxpool.Pool) *Hook {
	return &Hook{
		pool:     pool,
		handlers: make(map[TableOp][]Handler),
	}
}

// Hook adds a new handler for the given table and operation.
func (h *Hook) Hook(table string, op Op, handler Handler) {
	tableOp := TableOp{Table: table, Op: op}

	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.handlers[tableOp]; !ok {
		h.handlers[tableOp] = make([]Handler, 0)
	}
	h.handlers[tableOp] = append(h.handlers[tableOp], handler)
}

// InsertHook adds a new handler for the INSERT operation on the given table.
func (h *Hook) InsertHook(table string, handler HandlerFunc) {
	h.Hook(table, InsertOp, handler)
}

// UpdateHook adds a new handler for the UPDATE operation on the given table.
func (h *Hook) UpdateHook(table string, handler HandlerFunc) {
	h.Hook(table, UpdateOp, handler)
}

// DeleteHook adds a new handler for the DELETE operation on the given table.
func (h *Hook) DeleteHook(table string, handler HandlerFunc) {
	h.Hook(table, DeleteOp, handler)
}

// Listen starts listening for hook notifications and triggers the registered handlers for each operation.
func (h *Hook) Listen(ctx context.Context) error {
	if len(h.handlers) == 0 {
		return errors.New("no handlers registered")
	}

	pool, err := h.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer pool.Release()

	if err := h.createFunction(ctx); err != nil {
		return fmt.Errorf("failed to create function: %w", err)
	}

	for tableOp := range h.handlers {
		if err := h.createTrigger(ctx, tableOp.Table, tableOp.Op); err != nil {
			return fmt.Errorf("failed to create trigger: %w", err)
		}
	}

	if _, err := pool.Exec(ctx, "LISTEN hooks"); err != nil {
		return fmt.Errorf("failed to listen to hooks: %w", err)
	}

	log.Println("listening for notifications...")
	for {
		notification, err := pool.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}

		go func() {
			var payload Payload

			if err := json.Unmarshal([]byte(notification.Payload), &payload); err != nil {
				log.Printf("failed to unmarshal payload: %v", err)
				return
			}

			op := TableOp{Table: payload.Table, Op: payload.Op}

			h.mu.Lock()
			handlers, ok := h.handlers[op]
			h.mu.Unlock()

			if !ok {
				log.Printf("no handlers registered for table operation %v", op)
				return
			}

			for _, h := range handlers {
				h.Handle(ctx, payload)
			}
		}()
	}
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

func (h *Hook) createFunction(ctx context.Context) error {
	query := `
		CREATE OR REPLACE FUNCTION notify_hooks() RETURNS TRIGGER AS $$
		BEGIN
			PERFORM pg_notify(
				'hooks', 
				json_build_object(
					'op', TG_OP, 
					'schema', TG_TABLE_SCHEMA, 
					'table', TG_TABLE_NAME, 
					'row', row_to_json(NEW),
					'old_row', row_to_json(OLD)
				)::text
			);
			RETURN NULL;
		END;
		$$ LANGUAGE plpgsql;
	`
	if _, err := h.pool.Exec(ctx, query); err != nil {
		return err
	}

	log.Println("created function notify_hooks")
	return nil
}

func (h *Hook) createTrigger(ctx context.Context, table string, op Op) error {
	query := `
		CREATE OR REPLACE TRIGGER ` + table + `_` + string(op) + `_hooks
		AFTER ` + string(op) + ` ON ` + table + `
		FOR EACH ROW
		EXECUTE PROCEDURE notify_hooks();
	`
	if _, err := h.pool.Exec(ctx, query); err != nil {
		return err
	}

	log.Printf("created trigger %s_%s_hooks", table, op)
	return nil
}
