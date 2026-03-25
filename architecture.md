# Architecture of pgmsgq

## Overview

**pgmsgq** is a reliable message queue library built on top of PostgreSQL. It provides a simple API for publishing and consuming messages with features like retries, dead-letter queue, priorities, and idempotency.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Application Layer                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │   Producer  │  │  Consumer   │  │   Subscription Handler  │ │
│  └──────┬──────┘  └──────┬──────┘  └─────────────┬───────────┘ │
│         │                │                       │             │
└─────────┼────────────────┼───────────────────────┼─────────────┘
          │                │                       │
          ▼                ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                        pgmsgq Library                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Queue Core                            │   │
│  │  ┌───────────┐  ┌───────────┐  ┌─────────────────────┐  │   │
│  │  │ Publish   │  │  Consume  │  │    Subscribe        │  │   │
│  │  │ (Insert)  │  │ (Update)  │  │  (LISTEN/NOTIFY)    │  │   │
│  │  └───────────┘  └───────────┘  └─────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                   Message Flow                          │   │
│  │  pending → processing → acked (deleted)                 │   │
│  │                    │                                    │   │
│  │                    ▼                                    │   │
│  │              delayed (retry)                           │   │
│  │                    │                                    │   │
│  │                    ▼                                    │   │
│  │              failed → DLQ                              │   │
│  └─────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      PostgreSQL Database                        │
│  ┌──────────────────────┐    ┌─────────────────────────────┐   │
│  │  pgmsgq_messages     │    │     pgmsgq_dlq              │   │
│  │  - id (PK)           │    │     - id (PK)               │   │
│  │  - queue_name        │    │     - queue_name            │   │
│  │  - payload (BYTEA)   │    │     - payload (BYTEA)       │   │
│  │  - status            │    │     - failed_at             │   │
│  │  - priority          │    │     - error                 │   │
│  │  - retry_count       │    │     - original_id           │   │
│  │  - max_retries       │    │                             │   │
│  │  - created_at        │    │                             │   │
│  │  - updated_at        │    │                             │   │
│  │  - dedup_id (UNIQUE) │    │                             │   │
│  └──────────────────────┘    └─────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Queue (`pkg/pgmsgq/queue.go`)

The central component managing all queue operations:

```go
type Queue struct {
    db     *pgxpool.Pool    // PostgreSQL connection pool
    cfg    Config           // Queue configuration
    name   string           // Queue name
    closed atomicBool       // Closure flag
    dlq    *dlqManager      // Dead-letter queue manager
}
```

**Responsibilities:**
- Message publishing and batch publishing
- Single and batch message consumption
- Push-based subscription via LISTEN/NOTIFY
- Delayed message handling via `StartDelayedReleaser`
- Database initialization and migrations

### 2. Message (`pkg/pgmsgq/message.go`)

Represents a consumed message ready for processing:

```go
type Message struct {
    id         int64
    payload    []byte
    retryCount int
    queue      *Queue
    ackedMu    sync.RWMutex
    acked      bool
    ctx        context.Context
    cancel     context.CancelFunc
}
```

**Methods:**
- `Ack()` — Confirm successful processing, delete from queue
- `Nack(reason)` — Mark as failed, retry or move to DLQ
- `ID()`, `Payload()`, `RetryCount()`, `DecodeJSON()`

### 3. Configuration (`pkg/pgmsgq/config.go`)

```go
type Config struct {
    TablePrefix     string          // Default: "pgmsgq_"
    PushEnabled     bool            // Enable LISTEN/NOTIFY
    PushChannelName string          // Default: "pgmq"
    MaxRetries      int             // Default: 3
    DelayAfterRetry time.Duration   // Default: 5s
    BackoffStrategy BackoffStrategy // Default: Exponential(2s)
    DefaultTimeout  time.Duration   // Default: 30s
    Metrics         *metrics.Metrics
}
```

## Message Lifecycle

```
                    ┌─────────────────────────────────────┐
                    │                                     │
                    ▼                                     │
              ┌─────────┐                               │
              │pending  │◄──────────────────────────────┘
              └────┬────┘     delayed releaser (periodic)
                   │
                   │ Consume() / BatchConsume()
                   ▼
              ┌────────────┐
              │processing  │
              └─────┬──────┘
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
   ┌───────┐              ┌────────┐
   │  Ack  │              │  Nack  │
   └───┬───┘              └───┬────┘
       │                     │
       │                     ├──► retry_count < max_retries?
       │                     │
       ▼                     ▼
  (deleted)           ┌───────────┐
                      │  delayed  │
                      └─────┬─────┘
                            │
                            │ retry_count >= max_retries?
                            │
                            ▼
                      ┌───────────┐
                      │  failed   │
                      └─────┬─────┘
                            │
                            ▼
                      ┌───────────┐
                      │    DLQ    │
                      └───────────┘
```

## Key Features

### 1. Pull-Based Consumption

```go
// Single message
msg, _ := queue.Consume(ctx)
msg.Ack()

// Batch consumption
msgs, _ := queue.BatchConsume(ctx, 10)
for _, m := range msgs {
    process(m)
    m.Ack()
}
```

### 2. Push-Based Subscription

```go
sub, _ := queue.Subscribe(ctx, func(msg *Message) {
    // Handle message
    msg.Ack()
})
defer sub.Close()
```

### 3. Publishing with Options

```go
queue.Publish(ctx, payload,
    pgmsgq.WithPriority(200),
    pgmsgq.WithDedupID("unique-id"),
    pgmsgq.WithDelay(5*time.Minute),
)
```

### 4. Backoff Strategies

```go
import "github.com/isklv/pgmsgq/internal/backoff"

// Fixed delay
backoff.Fixed(5 * time.Second)

// Linear: 5s, 10s, 15s...
backoff.Linear(5 * time.Second)

// Exponential: 2s, 4s, 8s, 16s...
backoff.Exponential(2 * time.Second)
```

## Database Schema

### Messages Table

```sql
CREATE TABLE pgmsgq_messages (
    id            BIGSERIAL PRIMARY KEY,
    queue_name    TEXT NOT NULL,
    payload       BYTEA NOT NULL,
    status        TEXT NOT NULL 
                  CHECK (status IN ('pending', 'processing', 'delayed', 'failed')) 
                  DEFAULT 'pending',
    priority      SMALLINT NOT NULL DEFAULT 128 
                  CHECK (priority BETWEEN 0 AND 255),
    retry_count   INT NOT NULL DEFAULT 0,
    max_retries   INT NOT NULL DEFAULT 3,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dedup_id      TEXT
);

-- Indexes
CREATE INDEX idx_pgmsgq_pending ON pgmsgq_messages 
    (queue_name, priority DESC, created_at) WHERE status = 'pending';

CREATE INDEX idx_pgmsgq_delayed ON pgmsgq_messages 
    (queue_name, updated_at) WHERE status = 'delayed';

CREATE UNIQUE INDEX idx_pgmsgq_dedup ON pgmsgq_messages 
    (dedup_id) WHERE dedup_id IS NOT NULL;
```

### Dead-Letter Queue Table

```sql
CREATE TABLE pgmsgq_dlq (
    id         BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL,
    payload    BYTEA NOT NULL,
    failed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error      TEXT,
    original_id BIGINT
);
```

## Internal Packages

```
internal/
├── backoff/     # Retry delay strategies (Fixed, Linear, Exponential)
├── dlq/         # Dead-letter queue operations manager
├── metrics/     # Prometheus metrics implementation
├── pg/          # PostgreSQL utilities (QuoteIdentifier)
└── testutil/    # Test utilities
```

## Metrics

The library provides comprehensive Prometheus metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `pgmsgq_queue_length` | Gauge | Number of pending messages per queue |
| `pgmsgq_processed_total` | Counter | Total processed messages by status |
| `pgmsgq_consume_duration_seconds` | Histogram | Message processing duration |
| `pgmsgq_dedup_total` | Counter | Deduplicated messages |
| `pgmsgq_subscribe_errors_total` | Counter | Subscription errors |
| `pgmsgq_consume_errors_total` | Counter | Consume errors |

## Project Structure

```
pgmsgq/
├── cmd/
│   ├── basic/          # Simple example
│   └── push/           # Push-based example
├── internal/
│   ├── backoff/        # Retry strategies
│   ├── dlq/            # DLQ manager
│   ├── metrics/        # Prometheus metrics
│   ├── pg/             # PG utilities
│   └── testutil/       # Test helpers
├── pkg/
│   └── pgmsgq/         # Main library
│       ├── config.go   # Configuration
│       ├── consume.go  # Pull-based consumption
│       ├── dlq.go      # DLQ operations
│       ├── message.go  # Message struct
│       ├── migrate.go  # Database migrations
│       ├── options.go  # Publish options
│       ├── publish.go  # Publishing
│       ├── queue.go    # Queue core
│       └── subscribe.go # Push-based subscription
├── tests/              # Integration tests
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

## Design Decisions

1. **Single Table Per Queue**: Messages are stored in one table with `queue_name` partitioning for simplicity and flexibility.

2. **Optimistic Locking via UPDATE**: The consume operation uses `UPDATE ... RETURNING` with `FOR UPDATE SKIP LOCKED` for atomic message claiming.

3. **Delayed Messages as "delayed" Status**: Instead of separate delayed queue, messages are marked as "delayed" with `updated_at` timestamp, moved back by a background releaser.

4. **Transaction-Based Publishing**: Ensures atomicity and consistency, especially for batch operations.

5. **Configurable Backoff**: Allows different retry strategies per use case.

6. **Prometheus Integration**: Built-in metrics for monitoring and alerting.