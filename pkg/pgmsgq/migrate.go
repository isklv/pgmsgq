package pgmsgq

import (
	"context"
	"fmt"
)

const createMessageTable = `CREATE TABLE IF NOT EXISTS %v (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'delayed', 'failed')) DEFAULT 'pending',
    priority SMALLINT NOT NULL DEFAULT 128 CHECK (priority BETWEEN 0 AND 255),
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dedup_id TEXT
);
`

const createDlqTable = `CREATE TABLE IF NOT EXISTS %v (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload BYTEA NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error TEXT,
    original_id BIGINT
);
`

const createMessageIndex = `
CREATE INDEX IF NOT EXISTS idx_pgmsgq_pending ON %v (queue_name, priority DESC, created_at)
    WHERE status = 'pending';
`

const createMessageDelayedIndex = `
CREATE INDEX IF NOT EXISTS idx_pgmsgq_delayed ON %v (queue_name, updated_at)
    WHERE status = 'delayed';
`
const createMessageDedupIndex = `
CREATE UNIQUE INDEX IF NOT EXISTS idx_pgmsgq_dedup ON %v (dedup_id)
    WHERE dedup_id IS NOT NULL;
`

func (q *Queue) InitDB(ctx context.Context) error {
	_, err := q.db.Query(ctx, fmt.Sprintf(`SELECT 1 FROM %v LIMIT 0`, q.tableName()))
	if err != nil {
		_, err := q.db.Exec(ctx, fmt.Sprintf(createMessageTable, q.tableName()))
		if err != nil {
			return err
		}
		_, err = q.db.Exec(ctx, fmt.Sprintf(createDlqTable, q.cfg.TablePrefix+"dlq"))
		if err != nil {
			return err
		}
		_, err = q.db.Exec(ctx, fmt.Sprintf(createMessageIndex, q.tableName()))
		if err != nil {
			return err
		}
		_, err = q.db.Exec(ctx, fmt.Sprintf(createMessageDelayedIndex, q.tableName()))
		if err != nil {
			return err
		}
		_, err = q.db.Exec(ctx, fmt.Sprintf(createMessageDedupIndex, q.tableName()))
		if err != nil {
			return err
		}
	}
	return nil
}
