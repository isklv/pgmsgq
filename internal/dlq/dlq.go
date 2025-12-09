package dlq

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

type Manager struct {
	db    pgx.Executable
	table string
}

func New(db pgx.Executable, table string) *Manager {
	return &Manager{db: db, table: table}
}

type Message struct {
	ID         int64
	QueueName  string
	Payload    []byte
	FailedAt   time.Time
	Error      string
	OriginalID int64
}

func (m *Manager) List(ctx context.Context, queue string, limit int) ([]*Message, error) {
	// ... как раньше, но без Ack/Requeue методов (они в pkg/pgmsgq/dlq.go)
	rows, err := m.db.Query(ctx,
		`SELECT id, queue_name, payload, failed_at, error, original_id
		 FROM `+m.table+`
		 WHERE queue_name = $1
		 ORDER BY failed_at DESC
		 LIMIT $2`,
		queue, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var msg Message
		if err := rows.Scan(&msg.ID, &msg.QueueName, &msg.Payload, &msg.FailedAt, &msg.Error, &msg.OriginalID); err != nil {
			return nil, err
		}
		msgs = append(msgs, &msg)
	}
	return msgs, rows.Err()
}

func (m *Manager) Insert(ctx context.Context, queue string, payload []byte, reason string, originalID int64) error {
	_, err := m.db.Exec(ctx,
		`INSERT INTO `+m.table+`
			(queue_name, payload, error, original_id)
			VALUES ($1, $2, $3, $4)`,
		queue, payload, reason, originalID,
	)
	return err
}

func (m *Manager) Delete(ctx context.Context, id int64) error {
	_, err := m.db.Exec(ctx, `DELETE FROM `+m.table+` WHERE id = $1`, id)
	return err
}