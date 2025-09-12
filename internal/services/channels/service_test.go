package channelsvc

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/eventlog"
	"github.com/rzbill/flo/internal/runtime"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func newServiceForTest(t *testing.T) (*Service, *runtime.Runtime) {
	t.Helper()
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return New(rt), rt
}

func TestPublishIdempotency(t *testing.T) {
	svc, rt := newServiceForTest(t)
	headers := map[string]string{"idempotencyKey": "pub-1"}
	id1, err := svc.Publish(context.Background(), "default", "orders", []byte("hello"), headers, "")
	if err != nil {
		t.Fatalf("publish1: %v", err)
	}
	id2, err := svc.Publish(context.Background(), "default", "orders", []byte("hello"), headers, "")
	if err != nil {
		t.Fatalf("publish2: %v", err)
	}
	if string(id1) != string(id2) {
		t.Fatalf("idempotency failed: %x vs %x", id1, id2)
	}
	// Ensure only one entry in partition 0
	log, err := rt.OpenLog("default", "orders", 0)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	items, _ := log.Read(eventlog.ReadOptions{Limit: 100})
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}

func TestNackAttemptsAndDLQ(t *testing.T) {
	svc, rt := newServiceForTest(t)
	// Nack 5 times to trigger DLQ
	id := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	for i := 0; i < 5; i++ {
		if err := svc.Nack(context.Background(), "default", "orders", "workers", id); err != nil {
			t.Fatalf("nack: %v", err)
		}
	}
	dlq, err := rt.OpenLog("default", "dlq/orders/workers", 0)
	if err != nil {
		t.Fatalf("open dlq: %v", err)
	}
	items, _ := dlq.Read(eventlog.ReadOptions{Limit: 10})
	if len(items) == 0 {
		t.Fatalf("expected dlq item")
	}
}

func TestRetryBackoffAppends(t *testing.T) {
	svc, rt := newServiceForTest(t)
	id := []byte{0, 0, 0, 0, 0, 0, 0, 2}
	start := time.Now().UnixMilli()
	if err := svc.Nack(context.Background(), "default", "orders", "workers", id); err != nil {
		t.Fatalf("nack: %v", err)
	}
	retry, err := rt.OpenLog("default", "retry/orders/workers", 0)
	if err != nil {
		t.Fatalf("open retry: %v", err)
	}
	items, _ := retry.Read(eventlog.ReadOptions{Limit: 10})
	if len(items) == 0 {
		t.Fatalf("expected retry item")
	}
	if len(items[0].Header) < 8 {
		t.Fatalf("missing retryAt header")
	}
	retryAt := binary.BigEndian.Uint64(items[0].Header[:8])
	if retryAt < uint64(start) || retryAt > uint64(time.Now().Add(5*time.Second).UnixMilli()) {
		t.Fatalf("retryAt out of range: %d", retryAt)
	}
}

type sink struct {
	ctx    context.Context
	itemsC chan SubscribeItem
}

func (s sink) Send(it SubscribeItem) error {
	select {
	case s.itemsC <- it:
	default:
	}
	return nil
}
func (s sink) Context() context.Context { return s.ctx }
func (s sink) Flush() error             { return nil }

func TestStreamSubscribeReceivesAppend(t *testing.T) {
	t.Skip("flaky under CI; revisit with deterministic harness")
}
