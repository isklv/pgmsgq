package pgmsgq

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	consumeQuery = `
		UPDATE %s
		SET status = 'processing', updated_at = NOW()
		WHERE id = (
			SELECT id
			FROM %s
			WHERE queue_name = $1 AND status = 'pending'
			ORDER BY priority DESC, created_at
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, payload, retry_count, max_retries;
	`

	batchConsumeQuery = `
		UPDATE %s
		SET status = 'processing', updated_at = NOW()
		WHERE id IN (
			SELECT id
			FROM %s
			WHERE queue_name = $1 AND status = 'pending'
			ORDER BY priority DESC, created_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, payload, retry_count, max_retries;
	`
)

// Consume retrieves one message. Returns nil if no message available.
func (q *Queue) Consume(ctx context.Context) (*Message, error) {
	if q.closed.Load() {
		return nil, ErrClosed
	}

	table := q.tableName()
	query := fmt.Sprintf(consumeQuery, table, table)

	msgCtx, cancel := context.WithTimeout(ctx, q.cfg.DefaultTimeout)
	rows, err := q.db.Query(msgCtx, query, q.name)
	if err != nil {
		cancel()
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		cancel()
		return nil, nil
	}

	var id int64
	var payload []byte
	var retryCount, maxRetries int
	if err := rows.Scan(&id, &payload, &retryCount, &maxRetries); err != nil {
		cancel()
		return nil, err
	}

	return NewMessage(id, payload, retryCount, q, msgCtx, cancel), nil
}

// BatchConsume retrieves up to `limit` messages.
func (q *Queue) BatchConsume(ctx context.Context, limit int) ([]*Message, error) {
	if q.closed.Load() {
		return nil, ErrClosed
	}
	if limit <= 0 {
		return nil, errors.New("limit must be > 0")
	}

	table := q.tableName()
	query := fmt.Sprintf(batchConsumeQuery, table, table)

	msgCtx, cancel := context.WithTimeout(ctx, q.cfg.DefaultTimeout)
	rows, err := q.db.Query(msgCtx, query, q.name, limit)
	if err != nil {
		cancel()
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var id int64
		var payload []byte
		var retryCount, maxRetries int
		if err := rows.Scan(&id, &payload, &retryCount, &maxRetries); err != nil {
			cancel()
			return nil, err
		}
		msgs = append(msgs, NewMessage(id, payload, retryCount, q, msgCtx, func() {}))
	}

	if len(msgs) == 0 {
		cancel()
		return nil, nil
	}

	// Share one cancel func
	for _, m := range msgs {
		m.cancel = cancel
	}

	return msgs, nil
}

// nackMessage handles retry/DLQ logic.
func (q *Queue) nackMessage(ctx context.Context, id int64, retryCount int, reason string) error {
	retryCount++
	delay := q.cfg.BackoffStrategy(retryCount)
	updatedAt := time.Now().Add(delay)

	table := q.tableName()
	updateQuery := `
		UPDATE ` + table + `
		SET retry_count = $1, updated_at = $2, status = 'delayed'
		WHERE id = $3 AND queue_name = $4 AND retry_count < max_retries
		RETURNING retry_count, max_retries;
	`

	var newRetryCount, maxRetries int
	err := q.db.QueryRow(ctx, updateQuery, retryCount, updatedAt, id, q.name).
		Scan(&newRetryCount, &maxRetries)
	if err == nil {
		q.cfg.Metrics.OnNacked(q.name)
		return nil
	}

	if errors.Is(err, pgx.ErrNoRows) {
		// Max retries reached → DLQ
		if err := q.dlq.inner.Insert(ctx, q.name, nil, reason, id); err != nil {
			return fmt.Errorf("dlq insert: %w", err)
		}

		_, err = q.db.Exec(ctx,
			`DELETE FROM `+table+` WHERE id = $1 AND queue_name = $2`,
			id, q.name,
		)
		if err == nil {
			q.cfg.Metrics.OnFailed(q.name)
		}
		return err
	}

	return err
}
