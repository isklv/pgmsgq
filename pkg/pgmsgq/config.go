package pgmsgq

import (
	"time"

	"github.com/isklv/pgmsgq/internal/backoff"
	"github.com/isklv/pgmsgq/internal/metrics"
)

// BackoffStrategy alias for internal strategy.
type BackoffStrategy = backoff.Strategy

// Config controls queue behavior.
type Config struct {
	// TablePrefix for message and DLQ tables. Default: "pgmsgq_".
	// Full names: <TablePrefix>messages, <TablePrefix>dlq.
	TablePrefix string

	// PushEnabled enables LISTEN/NOTIFY after Publish().
	PushEnabled bool

	// PushChannelName is the base for notification channels.
	// Final name: <PushChannelName>_q_<queue_name>. Default: "pgmq".
	PushChannelName string

	// MaxRetries before sending to DLQ. Default: 3.
	MaxRetries int

	// DelayAfterRetry is fallback delay if BackoffStrategy is nil.
	// Default: 5s.
	DelayAfterRetry time.Duration

	// BackoffStrategy controls retry delays. Default: Exponential(2s).
	BackoffStrategy BackoffStrategy

	// DefaultTimeout for message processing context. Default: 30s.
	DefaultTimeout time.Duration

	// Metrics implementation. Default: Prometheus via metrics.Default.
	Metrics Metrics
}

// DefaultConfig is the default configuration.
var DefaultConfig = Config{
	TablePrefix:     "pgmsgq_",
	PushChannelName: "pgmq",
	MaxRetries:      3,
	DelayAfterRetry: 5 * time.Second,
	BackoffStrategy: backoff.Exponential(2 * time.Second),
	DefaultTimeout:  30 * time.Second,
	Metrics:         DefaultMetrics,
}

// withDefaults ensures all fields are set.
func (c *Config) withDefaults() *Config {
	nc := *c
	if nc.TablePrefix == "" {
		nc.TablePrefix = "pgmsgq_"
	}
	if nc.PushChannelName == "" {
		nc.PushChannelName = "pgmq"
	}
	if nc.BackoffStrategy == nil {
		nc.BackoffStrategy = backoff.Exponential(nc.DelayAfterRetry)
	}
	if nc.Metrics == nil {
		nc.Metrics = DefaultMetrics
	}
	if nc.MaxRetries == 0 {
		nc.MaxRetries = 3
	}
	if nc.DefaultTimeout == 0 {
		nc.DefaultTimeout = 30 * time.Second
	}
	return &nc
}