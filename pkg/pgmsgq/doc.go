// Package pgmsgq provides a reliable message queue over PostgreSQL.
//
// It supports:
//   - Pull-based consumption (Consume, BatchConsume)
//   - Push-based via LISTEN/NOTIFY (Subscribe)
//   - Retries, dead-letter queue, priorities, idempotency
//   - Prometheus metrics
//
// Quick start:
//
//	queue := pgmsgq.New(pool, "my_queue", nil)
//	queue.Publish(ctx, "hello")
//
//	msg, _ := queue.Consume(ctx)
//	msg.Ack()
//
package pgmsgq