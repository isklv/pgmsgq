package pgmsgq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Subscription represents an active LISTEN session.
type Subscription struct {
	queue  *Queue
	cancel context.CancelFunc
	closed chan struct{}
	wg     sync.WaitGroup
}

// Subscribe starts a push-based listener.
// Must call sub.Close() to stop.
func (q *Queue) Subscribe(ctx context.Context, handler func(*Message)) (*Subscription, error) {
	if !q.cfg.PushEnabled {
		return nil, errors.New("config.PushEnabled must be true")
	}

	subCtx, cancel := context.WithCancel(ctx)
	sub := &Subscription{
		queue:  q,
		cancel: cancel,
		closed: make(chan struct{}),
	}

	conn, err := q.db.Acquire(subCtx)
	if err != nil {
		cancel()
		return nil, err
	}
	defer conn.Release()

	channel := q.cfg.PushChannelName + "_q_" + q.name
	if _, err := conn.Exec(subCtx, "LISTEN "+pg.QuoteIdentifier(channel)); err != nil {
		cancel()
		return nil, err
	}

	sub.wg.Add(1)
	go sub.run(conn.Conn(), subCtx, handler)

	return sub, nil
}

func (s *Subscription) run(conn *pgx.Conn, ctx context.Context, handler func(*Message)) {
	defer s.wg.Done()
	defer close(s.closed)

	// Drain existing messages first
	s.consumeAll(ctx, handler)

	for {
		notif, err := s.waitForNotification(ctx, conn)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.queue.cfg.Metrics.OnSubscribeError(s.queue.name)
			}
			return
		}
		if notif == nil {
			continue
		}
		s.consumeAll(ctx, handler)
	}
}

func (s *Subscription) waitForNotification(ctx context.Context, conn *pgx.Conn) (*pgconn.Notification, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if !conn.IsClosed() {
			conn.PgConn().ProcessAcknowledgements()
		}

		notif, err := conn.WaitForNotification(ctx)
		if err != nil {
			if pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return nil, err
		}
		return notif, nil
	}
}

func (s *Subscription) consumeAll(ctx context.Context, handler func(*Message)) {
	for {
		msg, err := s.queue.Consume(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				s.queue.cfg.Metrics.OnConsumeError(s.queue.name)
			}
			return
		}
		if msg == nil {
			break
		}

		go func(m *Message) {
			defer func() {
				if r := recover(); r != nil {
					_ = m.Nack("panic_in_handler")
				}
			}()
			handler(m)
		}(msg)
	}
}

// Close stops the subscription.
func (s *Subscription) Close() error {
	s.cancel()
	<-s.closed
	return nil
}