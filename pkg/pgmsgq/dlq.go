package pgmsgq

import (
	"context"
	"github.com/isklv/pgmsgq/internal/dlq" as internalDLQ
)

type dlqManager struct {
	inner     *internalDLQ.Manager
	queueName string
	metrics   *Metrics
}

type DLQMessage struct {
	*internalDLQ.Message
	queue *Queue
}

func (m *DLQMessage) Ack() error {
	err := m.queue.dlq.inner.Delete(context.Background(), m.ID)
	if err == nil {
		m.queue.cfg.Metrics.OnDLQAcked(m.queue.name)
	}
	return err
}

func (m *DLQMessage) Requeue() error {
	_, err := m.queue.db.Exec(context.Background(),
		`INSERT INTO `+m.queue.tableName()+`
			(queue_name, payload, status, retry_count, max_retries)
			VALUES ($1, $2, 'pending', 0, $3)`,
		m.queue.name, m.Payload, m.queue.cfg.MaxRetries,
	)
	if err != nil {
		return err
	}
	return m.Ack()
}

func (q *Queue) ListDLQ(ctx context.Context, limit int) ([]*DLQMessage, error) {
	msgs, err := q.dlq.inner.List(ctx, q.name, limit)
	if err != nil {
		return nil, err
	}

	result := make([]*DLQMessage, len(msgs))
	for i, m := range msgs {
		result[i] = &DLQMessage{Message: m, queue: q}
	}
	return result, nil
}