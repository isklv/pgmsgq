package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/isklv/pgmsgq/pkg/pgmsgq"
)

type Task struct {
	ID   int    `json:"id"`
	Op   string `json:"op"`
	Data string `json:"data"`
}

func main() {
	log.Println("🔔 Starting push-based (LISTEN/NOTIFY) example...")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://postgres:pass@localhost:5432/pgmsgq_test?sslmode=disable")
	if err != nil {
		log.Fatal("DB connect:", err)
	}
	defer pool.Close()

	queue := pgmsgq.New(pool, "tasks", &pgmsgq.Config{
		PushEnabled: true,
	})

	go queue.StartDelayedReleaser(ctx, 2*time.Second)

	sub, err := queue.Subscribe(ctx, func(msg *pgmsgq.Message) {
		var t Task
		if err := msg.DecodeJSON(&t); err != nil {
			_ = msg.Nack("decode")
			return
		}

		log.Printf("⚡ Push-received task %d: %s(%q)", t.ID, t.Op, t.Data)
		time.Sleep(300 * time.Millisecond)

		if t.ID%3 == 0 {
			_ = msg.Nack("retry_me")
			return
		}

		_ = msg.Ack()
	})
	if err != nil {
		log.Fatal("Subscribe failed:", err)
	}
	defer sub.Close()

	// Publish
	for i := 1; i <= 5; i++ {
		task := Task{ID: i, Op: "process", Data: "item-" + string(rune('A'+i-1))}
		if err := queue.Publish(ctx, task, pgmsgq.WithPriority(255-i*10)); err != nil {
			log.Printf("Publish error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	log.Println("📬 Messages published. Waiting...")

	select {
	case <-ctx.Done():
	case <-time.After(10 * time.Second):
	}
	log.Println("🛑 Done.")
}