package metrics

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	QueueLength     *prometheus.GaugeVec
	ProcessedTotal  *prometheus.CounterVec
	ConsumeDuration prometheus.Histogram
	DedupCounter    prometheus.Counter
	SubscribeErrors prometheus.Counter
	ConsumeErrors   prometheus.Counter
}

func New() *Metrics {
	m := &Metrics{
		QueueLength: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pgmsgq_queue_length",
			Help: "Number of pending messages per queue",
		}, []string{"queue"}),

		ProcessedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pgmsgq_processed_total",
			Help: "Total processed messages",
		}, []string{"queue", "status"}),

		ConsumeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pgmsgq_consume_duration_seconds",
			Help:    "Processing duration",
			Buckets: prometheus.DefBuckets,
		}),

		DedupCounter:    prometheus.NewCounter(prometheus.CounterOpts{Name: "pgmsgq_dedup_total"}),
		SubscribeErrors: prometheus.NewCounter(prometheus.CounterOpts{Name: "pgmsgq_subscribe_errors_total"}),
		ConsumeErrors:   prometheus.NewCounter(prometheus.CounterOpts{Name: "pgmsgq_consume_errors_total"}),
	}

	prometheus.MustRegister(
		m.QueueLength, m.ProcessedTotal, m.ConsumeDuration,
		m.DedupCounter, m.SubscribeErrors, m.ConsumeErrors,
	)

	return m
}

// Default is globally registered metrics.
var Default = New()