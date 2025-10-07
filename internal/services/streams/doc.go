// Package streamsvc implements the Streams facade on top of the internal
// EventLog. It provides publish/subscribe, idempotency, retry backoff, and
// DLQ behavior consumed by gRPC/HTTP transports.
//
// Example:
//
//	svc := streamsvc.New(rt)
//	// Publish with key and idempotency
//	id, _ := svc.Publish(ctx, "default", "orders", []byte("payload"), map[string]string{"idempotencyKey":"pub-1"}, "user:123")
//	_ = svc.Ack(ctx, "default", "orders", "workers", id)
//	// Subscribe using sink
//	_ = svc.StreamSubscribe(ctx, "default", "orders", "workers", nil, mySink)
package streamsvc

// Performance notes
//
// Publish
//   - Storage fsync policy is configured at server start. For lowest latency
//     with durability tradeoffs use: --fsync never (dev only). For a balanced
//     throughput/latency profile use: --fsync interval --fsync-interval-ms 5
//     which enables Pebble WAL group-commit with a small 5ms window.
//
// Subscribe
//   - FLO_SUB_FLUSH_MS / --sub-flush-ms: optional flush window in ms for the
//     per-subscriber writer. Small windows (2–5ms) coalesce network writes and
//     reduce flush overhead without adding noticeable latency.
//   - FLO_SUB_BUF / --sub-buf: buffered queue length per subscriber. Increase
//     for bursty producers or slow clients to avoid backpressure.
//   - Multi-partition fan-in reads partitions concurrently and feeds a single
//     subscriber writer queue to avoid head-of-line blocking across partitions.
//
// Observability
//   - streams.publish emits per-publish latency (dur_ms) and metadata.
//   - streams.deliver emits per-flush delivery_ms, end_to_end_ms (avg),
//     batch_n, and the subscriber queue depth/capacity to surface backpressure.
