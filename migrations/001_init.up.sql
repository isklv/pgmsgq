-- pgmsgq v1.0 schema
-- idempotent

CREATE TABLE IF NOT EXISTS pgmsgq_messages (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload BYTEA NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'delayed', 'failed')),
    priority SMALLINT NOT NULL DEFAULT 128 CHECK (priority BETWEEN 0 AND 255),
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dedup_id TEXT
);

CREATE TABLE IF NOT EXISTS pgmsgq_dlq (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload BYTEA NOT NULL,
    failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error TEXT,
    original_id BIGINT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pgmsgq_pending ON pgmsgq_messages (queue_name, priority DESC, created_at)
    WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_pgmsgq_delayed ON pgmsgq_messages (queue_name, updated_at)
    WHERE status = 'delayed';

CREATE UNIQUE INDEX IF NOT EXISTS idx_pgmsgq_dedup ON pgmsgq_messages (dedup_id)
    WHERE dedup_id IS NOT NULL;

-- Optional: notify function (for trigger-based NOTIFY, not used by default)
-- We use app-side NOTIFY for better control.