package testutil

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const TestDBURL = "postgres://postgres:pass@localhost:5432/pgmsgq_test?sslmode=disable"

func NewTestPool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()

	if os.Getenv("INTEGRATION") == "" {
		tb.Skip("skipping integration test; set INTEGRATION=1 to run")
	}

	pool, err := pgxpool.New(context.Background(), TestDBURL)
	if err != nil {
		tb.Fatalf("failed to connect to test DB: %v", err)
	}

	// Cleanup before test
	_, _ = pool.Exec(context.Background(), "TRUNCATE TABLE pgmsgq_messages, pgmsgq_dlq RESTART IDENTITY")

	tb.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func UniqueQueueName(tb testing.TB) string {
	tb.Helper()
	return fmt.Sprintf("testq_%d_%d", os.Getpid(), tb.TempDir())
}