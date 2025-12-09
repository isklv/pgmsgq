package pgmsgq

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/isklv/pgmsgq/internal/backoff"
	internalDLQ "github.com/isklv/pgmsgq/internal/dlq"
	"github.com/isklv/pgmsgq/internal/metrics"
	"github.com/isklv/pgmsgq/internal/pg"
)

type atomicBool struct {
	mu sync.RWMutex
	v  bool
}

func (a *atomicBool) Load() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.v
}

func (a *atomicBool) Store(v bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.v = v
}

type Queue struct {
	db     *pgxpool.Pool
	cfg    Config
	name   string
	closed atomicBool
	dlq    *dlqManager
}

type dlqManager struct {
	inner     *internalDLQ.Manager
	queueName string
	metrics   Metrics
}

func New(db *pgxpool.Pool, name string, cfg *Config) *Queue {
	if cfg == nil {
		cfg = &DefaultConfig
	}
	cfg = cfg.withDefaults()

	tablePrefix := cfg.TablePrefix
	if tablePrefix == "" {
		tablePrefix = "pgmsgq_"
	}

	return &Queue{
		db:   db,
		cfg:  *cfg,
		name: name,
		dlq: &dlqManager{
			inner:     internalDLQ.New(db, tablePrefix+"dlq"),
			queueName: name,
			metrics:   cfg.Metrics,
		},
	}
}

func (q *Queue) tableName() string {
	return q.cfg.TablePrefix + "messages"
}

func (q *Queue) pushChannel() string {
	return pg.QuoteIdentifier(q.cfg.PushChannelName + "_q_" + q.name)
}

// StartDelayedReleaser moves 'delayed' messages back to 'pending' periodically.
func (q *Queue) StartDelayedReleaser(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = q.db.Exec(ctx,
				`UPDATE `+q.tableName()+`
				 SET status = 'pending', updated_at = NOW()
				 WHERE queue_name = $1 AND status = 'delayed' AND updated_at <= NOW()`,
				q.name,
			)
		}
	}
}

// Close marks queue as closed. Ongoing operations may still complete.
func (q *Queue) Close() error {
	q.closed.Store(true)
	return nil
}