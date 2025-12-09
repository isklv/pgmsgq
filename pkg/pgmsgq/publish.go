package pgmsgq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/isklv/pgmsgq/internal/pg"
)

// Publish sends a message to the queue.
func (q *Queue) Publish(ctx context.Context, payload any, opts ...PublishOption) error {
	if q.closed.Load() {
		return ErrClosed
	}

	options := defaultPublishOptions()
	for _, opt := range opts {
		opt(options)
	}

	data, err := encodePayload(payload)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}

	tx, err := q.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	table := q.tableName()
	status := "pending"
	args := []any{q.name, data, options.Priority, options.MaxRetries, options.DedupID}

	var query string
	if options.Delay > 0 {
		status = "delayed"
		query = `INSERT INTO ` + table + `
			(queue_name, payload, priority, max_retries, dedup_id, status, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (dedup_id) WHERE dedup_id IS NOT NULL DO NOTHING`
		args = append(args, status, time.Now().Add(options.Delay))
	} else {
		query = `INSERT INTO ` + table + `
			(queue_name, payload, priority, max_retries, dedup_id)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (dedup_id) WHERE dedup_id IS NOT NULL DO NOTHING`
	}

	res, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return err
	}

	rows, _ := res.RowsAffected()
	if rows == 0 && options.DedupID != "" {
		q.cfg.Metrics.OnDedup(q.name)
		return nil
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if q.cfg.PushEnabled {
		_, _ = q.db.Exec(ctx, "NOTIFY "+q.pushChannel())
	}

	q.cfg.Metrics.OnPublished(q.name)
	return nil
}

// BatchPublish sends multiple messages in one transaction.
func (q *Queue) BatchPublish(ctx context.Context, payloads []any, opts ...PublishOption) error {
	if q.closed.Load() {
		return ErrClosed
	}
	if len(payloads) == 0 {
		return nil
	}

	options := defaultPublishOptions()
	for _, opt := range opts {
		opt(options)
	}

	tx, err := q.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	table := q.tableName()
	stmt, err := tx.Prepare(ctx, "batch_insert",
		`INSERT INTO `+table+`
			(queue_name, payload, priority, max_retries, dedup_id)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (dedup_id) WHERE dedup_id IS NOT NULL DO NOTHING`,
	)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, p := range payloads {
		data, err := encodePayload(p)
		if err != nil {
			return fmt.Errorf("encode payload: %w", err)
		}
		_, err = tx.Exec(ctx, "batch_insert", q.name, data, options.Priority, options.MaxRetries, options.DedupID)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	if q.cfg.PushEnabled {
		_, _ = q.db.Exec(ctx, "NOTIFY "+q.pushChannel())
	}

	q.cfg.Metrics.OnPublishedBatch(q.name, len(payloads))
	return nil
}

func encodePayload(p any) ([]byte, error) {
	switch p := p.(type) {
	case []byte:
		return p, nil
	case string:
		return []byte(p), nil
	case json.Marshaler:
		return p.MarshalJSON()
	default:
		return json.Marshal(p)
	}
}