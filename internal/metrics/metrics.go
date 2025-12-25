package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	QueueLength     *prometheus.GaugeVec
	ProcessedTotal  *prometheus.CounterVec
	ConsumeDuration prometheus.Histogram
	DedupCounter    prometheus.Counter
	SubscribeErrors prometheus.Counter
	ConsumeErrors   prometheus.Counter

	OperationsTotal    *prometheus.CounterVec
	OperationsDuration *prometheus.HistogramVec
	ActiveOperations   *prometheus.GaugeVec

	FailedOperations  *prometheus.CounterVec
	NackedOperations  *prometheus.CounterVec
	RetriedOperations *prometheus.CounterVec
	TimeoutOperations *prometheus.CounterVec

	AckedCounter          *prometheus.CounterVec
	DLQAckedCounter       *prometheus.CounterVec
	PublishedCounter      *prometheus.CounterVec
	PublishedBatchCounter *prometheus.CounterVec
}

func (m *Metrics) OnFailed(operationName string) {
	m.FailedOperations.WithLabelValues(operationName, "general").Inc()
	m.OperationsTotal.WithLabelValues(operationName, "failed").Inc()
}

func (m *Metrics) OnFailedWithError(operationName string, errorType string) {
	m.FailedOperations.WithLabelValues(operationName, errorType).Inc()
	m.OperationsTotal.WithLabelValues(operationName, "failed").Inc()
}

func (m *Metrics) OnNacked(operationName string) {
	m.NackedOperations.WithLabelValues(operationName, "general").Inc()
	m.OperationsTotal.WithLabelValues(operationName, "nacked").Inc()
}

func (m *Metrics) OnNackedWithReason(operationName string, reason string) {
	m.NackedOperations.WithLabelValues(operationName, reason).Inc()
	m.OperationsTotal.WithLabelValues(operationName, "nacked").Inc()
}

func (m *Metrics) OnRetried(operationName string, retryCount int) {
	m.RetriedOperations.WithLabelValues(operationName, fmt.Sprintf("%d", retryCount)).Inc()
}

func (m *Metrics) OnTimeout(operationName string, timeoutType string) {
	m.TimeoutOperations.WithLabelValues(operationName, timeoutType).Inc()
	m.OperationsTotal.WithLabelValues(operationName, "timeout").Inc()
}

// Методы для отслеживания успешных операций
func (m *Metrics) OnSuccess(operationName string) {
	m.OperationsTotal.WithLabelValues(operationName, "success").Inc()
}

// Методы для отслеживания активности
func (m *Metrics) StartOperation(operationName string) {
	m.ActiveOperations.WithLabelValues(operationName).Inc()
}

func (m *Metrics) EndOperation(operationName string) {
	m.ActiveOperations.WithLabelValues(operationName).Dec()
}

func (m *Metrics) OnAcked(operationName string) {
	m.AckedCounter.WithLabelValues(operationName).Inc()
	m.OperationsTotal.WithLabelValues(operationName, "acked").Inc()
}

func (m *Metrics) OnDedup(operationName string) {
	m.DedupCounter.Inc()
	m.ProcessedTotal.WithLabelValues(operationName, "deduplicated").Inc()
}

func (m *Metrics) OnSubscribeError(operationName string) {
	m.SubscribeErrors.Inc()
	m.OperationsTotal.WithLabelValues(operationName, "subscribe_error").Inc()
}

func (m *Metrics) OnConsumeError(operationName string) {
	m.ConsumeErrors.Inc()
	m.OperationsTotal.WithLabelValues(operationName, "consume_error").Inc()
}

func (m *Metrics) OnDLQAcked(operationName string) {
	m.DLQAckedCounter.WithLabelValues(operationName).Inc()
	m.OperationsTotal.WithLabelValues(operationName, "dlq_acked").Inc()
}

func (m *Metrics) OnPublished(operationName string) {
	m.PublishedCounter.WithLabelValues(operationName).Inc()
	m.ProcessedTotal.WithLabelValues(operationName, "published").Inc()
}

func (m *Metrics) OnPublishedBatch(operationName string, count int) {
	m.PublishedBatchCounter.WithLabelValues(operationName).Add(float64(count))
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

		OperationsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "operations_total",
				Help: "Total number of operations",
			},
			[]string{"operation_name", "operation_type"},
		),
		FailedOperations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "failed_operations_total",
				Help: "Total number of failed operations",
			},
			[]string{"operation_name", "error_type"},
		),
		NackedOperations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nacked_operations_total",
				Help: "Total number of nacked operations",
			},
			[]string{"operation_name", "reason"},
		),
		RetriedOperations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "retried_operations_total",
				Help: "Total number of retried operations",
			},
			[]string{"operation_name", "retry_count"},
		),
		TimeoutOperations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "timeout_operations_total",
				Help: "Total number of timeout operations",
			},
			[]string{"operation_name", "timeout_type"},
		),
		AckedCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "acked_operations_total",
				Help: "Total number of acknowledged operations",
			},
			[]string{"operation_name"},
		),
		DLQAckedCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "dlq_acked_operations_total",
				Help: "Total number of DLQ acknowledged operations",
			},
			[]string{"operation_name"},
		),
		PublishedCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "published_operations_total",
				Help: "Total number of publish operations",
			},
			[]string{"operation_name"},
		),
		PublishedBatchCounter: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "published_batch_operations_total",
				Help: "Total number of publish batch operations",
			},
			[]string{"operation_name"},
		),
	}

	return m
}

// Default is globally registered metrics.
var DefaultMetrics = New()
