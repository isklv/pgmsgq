package pgmsgq

import "time"

// PublishOptions holds publish-time settings.
type PublishOptions struct {
	Priority   int
	MaxRetries int
	DedupID    string
	Delay      time.Duration
}

func defaultPublishOptions() *PublishOptions {
	return &PublishOptions{
		Priority:   128,
		MaxRetries: 3,
	}
}

// PublishOption configures message publishing.
type PublishOption func(*PublishOptions)

// WithPriority sets message priority (0–255, 255 = highest). Default: 128.
func WithPriority(priority int) PublishOption {
	return func(o *PublishOptions) {
		if priority < 0 {
			priority = 0
		} else if priority > 255 {
			priority = 255
		}
		o.Priority = priority
	}
}

// WithMaxRetries sets per-message max retries. Overrides queue default.
func WithMaxRetries(n int) PublishOption {
	return func(o *PublishOptions) {
		if n < 0 {
			n = 0
		}
		o.MaxRetries = n
	}
}

// WithDedupID enables idempotent publishing.
// Messages with same DedupID are ignored on duplicate Publish.
func WithDedupID(id string) PublishOption {
	return func(o *PublishOptions) {
		o.DedupID = id
	}
}

// WithDelay schedules message for future delivery.
func WithDelay(d time.Duration) PublishOption {
	return func(o *PublishOptions) {
		o.Delay = d
	}
}