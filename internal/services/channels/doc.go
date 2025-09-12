// Package channelsvc implements the Channels facade on top of the internal
// EventLog. It provides publish/subscribe, idempotency, retry backoff, and
// DLQ behavior consumed by gRPC/HTTP transports.
//
// Example:
//
//	svc := channelsvc.New(rt)
//	// Publish with key and idempotency
//	id, _ := svc.Publish(ctx, "default", "orders", []byte("payload"), map[string]string{"idempotencyKey":"pub-1"}, "user:123")
//	_ = svc.Ack(ctx, "default", "orders", "workers", id)
//	// Subscribe using sink
//	_ = svc.StreamSubscribe(ctx, "default", "orders", "workers", nil, mySink)
package channelsvc
