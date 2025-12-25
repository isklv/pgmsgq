```
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

	pool, err := pgxpool.New(ctx, "postgres://postgres:pass@localhost:5432/pgmsgq_test?sslmode=disable")
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
		<-ctx.Done()
	}

	go queue.StartDelayedReleaser(ctx, 3*time.Second)

	// Publish
	for i := 1; i <= 3; i++ {
		g := Greeting{Name: "User" + string(rune('A'+i-1))}
		if err := queue.Publish(ctx, g,
			pgmsgq.WithPriority(200),
			pgmsgq.WithDedupID("greet-"+g.Name),
		); err != nil {
			log.Printf("Publish error: %v", err)
		} else {
			log.Printf("✅ Published: %+v", g)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Consume
	log.Println("👷 Starting consumer...")
	go func() {
		for {
			msg, err := queue.Consume(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consume error: %v", err)
				time.Sleep(time.Second)
				continue
			}
			if msg == nil {
				continue
			}

			var g Greeting
			if err := msg.DecodeJSON(&g); err != nil {
				log.Printf("Decode error: %v", err)
				_ = msg.Nack("decode_failed")
				continue
			}

			log.Printf("📨 Processing: Hello, %s!", g.Name)
			time.Sleep(500 * time.Millisecond)

			if g.Name == "UserB" {
				log.Printf("❌ Simulating failure for %s", g.Name)
				_ = msg.Nack("simulated_error")
			} else {
				log.Printf("✅ Acking %s", g.Name)
				_ = msg.Ack()
			}
		}
	}()

	<-ctx.Done()
	log.Println("🛑 Shutdown...")
}
```