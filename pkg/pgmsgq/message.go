package pgmsgq

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
)

var (
	ErrClosed      = errors.New("queue is closed")
	ErrAcked       = errors.New("message already acked/nacked")
	ErrNotConsumed = errors.New("message not consumed")
)

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

func newMessage(id int64, payload []byte, retryCount int, q *Queue, ctx context.Context, cancel context.CancelFunc) *Message {
	return &Message{
		id:         id,
		payload:    payload,
		retryCount: retryCount,
		queue:      q,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (m *Message) ID() int64              { return m.id }
func (m *Message) Payload() []byte        { return m.payload }
func (m *Message) RetryCount() int        { return m.retryCount }
func (m *Message) DecodeJSON(v any) error { return json.Unmarshal(m.payload, v) }

// Ack confirms successful processing.
func (m *Message) Ack() error {
	if m.isAcked() {
		return ErrAcked
	}
	defer m.cancel()
	m.setAcked()

	_, err := m.queue.db.Exec(m.ctx,
		`DELETE FROM `+m.queue.tableName()+` WHERE id = $1 AND queue_name = $2`,
		m.id, m.queue.name,
	)
	if err == nil {
		m.queue.cfg.Metrics.OnAcked(m.queue.name)
	}
	return err
}

// Nack marks processing as failed.
func (m *Message) Nack(reason string) error {
	if m.isAcked() {
		return ErrAcked
	}
	defer m.cancel()
	m.setAcked()

	return m.queue.nackMessage(m.ctx, m.id, m.retryCount, reason)
}

func (m *Message) isAcked() bool {
	m.ackedMu.RLock()
	defer m.ackedMu.RUnlock()
	return m.acked
}

func (m *Message) setAcked() {
	m.ackedMu.Lock()
	defer m.ackedMu.Unlock()
	m.acked = true
}
