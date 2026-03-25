package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/isklv/pgmsgq/pkg/pgmsgq"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Greeting struct {
	Name string `json:"name"`
}

func main() {
	log.Println("🚀 Starting basic pgmsgq example...")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, "postgres://postgres:pass@127.0.0.1:5432/pgmsgq_test?sslmode=disable")
	if err != nil {
		log.Fatal("DB connect:", err)
	}
	defer pool.Close()

	queue := pgmsgq.New(pool, "greetings", &pgmsgq.Config{
		MaxRetries:      2,
		DelayAfterRetry: 2 * time.Second,
	})

	if err := queue.InitDB(ctx); err != nil {
		log.Fatal("Init tables:", err)
	}

	go queue.StartDelayedReleaser(ctx, 3*time.Second)

	// Publish
	g := Greeting{Name: "User" + string(rune('A'))}
	if err := queue.Publish(ctx, g,
		pgmsgq.WithPriority(200),
		pgmsgq.WithDedupID("greet-"+g.Name),
	); err != nil {
		log.Printf("Publish error: %v", err)
	} else {
		log.Printf("✅ Published: %+v", g)
	}
	time.Sleep(3 * time.Second)

	msg, err := queue.Consume(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		log.Printf("Consume error: %v", err)
		time.Sleep(time.Second)
	}

	if msg == nil {
		log.Println("🛑 Msg is nil")
		return
	}

	var gr Greeting
	if err := msg.DecodeJSON(&gr); err != nil {
		log.Printf("Decode error: %v", err)
		_ = msg.Nack("decode_failed")
	}

	log.Printf("📨 Processing: Hello, %s!", gr.Name)
	time.Sleep(3 * time.Second)

	if g.Name == "UserB" {
		log.Printf("❌ Simulating failure for %s", gr.Name)
		_ = msg.Nack("simulated_error")
	} else {
		log.Printf("✅ Acking %s", gr.Name)
		err = msg.Ack()
		if err != nil {
			log.Printf("Acking error: %v", err)
		}
	}

	log.Println("🛑 Shutdown...")
}
