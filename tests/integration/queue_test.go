package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/isklv/pgmsgq/internal/testutil"
	"github.com/isklv/pgmsgq/pkg/pgmsgq"
)

func TestPublishConsume(t *testing.T) {
	pool := testutil.NewTestPool(t)
	queue := pgmsgq.New(pool, testutil.UniqueQueueName(t), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := queue.Publish(ctx, map[string]string{"test": "ok"})
	if err != nil {
		t.Fatal(err)
	}

	msg, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if msg == nil {
		t.Fatal("expected message")
	}

	var payload map[string]string
	if err := msg.DecodeJSON(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["test"] != "ok" {
		t.Errorf("got %v, want ok", payload)
	}

	if err := msg.Ack(); err != nil {
		t.Fatal(err)
	}

	// Ensure gone
	msg2, _ := queue.Consume(ctx)
	if msg2 != nil {
		t.Error("second message should not exist")
	}
}

func TestNackRetryDLQ(t *testing.T) {
	pool := testutil.NewTestPool(t)
	queue := pgmsgq.New(pool, testutil.UniqueQueueName(t), &pgmsgq.Config{
		MaxRetries:      1,
		DelayAfterRetry: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := queue.Publish(ctx, "retry_me")
	if err != nil {
		t.Fatal(err)
	}

	// First attempt
	msg, _ := queue.Consume(ctx)
	if msg == nil {
		t.Fatal("first consume nil")
	}
	if err := msg.Nack("test"); err != nil {
		t.Fatal(err)
	}

	// Wait for delayed release
	time.Sleep(50 * time.Millisecond)

	// Retry
	msg2, _ := queue.Consume(ctx)
	if msg2 == nil {
		t.Fatal("retry consume nil")
	}
	if msg2.RetryCount() != 1 {
		t.Errorf("retry count = %d, want 1", msg2.RetryCount())
	}
	if err := msg2.Nack("fail again"); err != nil {
		t.Fatal(err)
	}

	// Should be in DLQ
	dlq, err := queue.ListDLQ(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dlq) != 1 {
		t.Fatalf("dlq len = %d, want 1", len(dlq))
	}
	if string(dlq[0].Payload) != `"retry_me"` {
		t.Errorf("dlq payload = %s", dlq[0].Payload)
	}
}

func TestSubscribePush(t *testing.T) {
	pool := testutil.NewTestPool(t)
	queue := pgmsgq.New(pool, testutil.UniqueQueueName(t), &pgmsgq.Config{
		PushEnabled: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	received := make(chan string, 1)
	sub, err := queue.Subscribe(ctx, func(msg *pgmsgq.Message) {
		var s string
		_ = json.Unmarshal(msg.Payload(), &s)
		received <- s
		_ = msg.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()

	err = queue.Publish(ctx, "pushed!")
	if err != nil {
		t.Fatal(err)
	}

	select {
	case s := <-received:
		if s != "pushed!" {
			t.Errorf("got %q, want 'pushed!'", s)
		}
	case <-time.After(2 * time.Second):
		t.Error("timeout waiting for push")
	}
}

func TestDedup(t *testing.T) {
	pool := testutil.NewTestPool(t)
	queue := pgmsgq.New(pool, testutil.UniqueQueueName(t), nil)

	ctx := context.Background()

	err := queue.Publish(ctx, "first", pgmsgq.WithDedupID("id1"))
	if err != nil {
		t.Fatal(err)
	}

	err = queue.Publish(ctx, "second", pgmsgq.WithDedupID("id1"))
	if err != nil {
		t.Fatal(err)
	}

	// Only one message
	msg, _ := queue.Consume(ctx)
	if msg == nil {
		t.Fatal("no message")
	}
	_ = msg.Ack()

	msg2, _ := queue.Consume(ctx)
	if msg2 != nil {
		t.Error("second message should be deduped")
	}
}

func TestBatch(t *testing.T) {
	pool := testutil.NewTestPool(t)
	queue := pgmsgq.New(pool, testutil.UniqueQueueName(t), nil)

	ctx := context.Background()

	payloads := []any{"a", "b", "c"}
	err := queue.BatchPublish(ctx, payloads)
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := queue.BatchConsume(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d msgs, want 3", len(msgs))
	}

	for _, m := range msgs {
		_ = m.Ack()
	}
}
