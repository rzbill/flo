package eventlog

import (
	"context"
	"testing"
	"time"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func TestWaitForAppendWake(t *testing.T) {
	dir := t.TempDir()
	db, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	l, err := OpenLog(db, "ns", "t", 1)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	done := make(chan struct{})
	go func() {
		ok := l.WaitForAppend(500 * time.Millisecond)
		if !ok {
			t.Errorf("expected wake by append")
		}
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	if _, err := l.Append(context.Background(), []AppendRecord{{Payload: []byte("x")}}); err != nil {
		t.Fatalf("append: %v", err)
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatalf("timeout waiting for waiter to wake")
	}
}

func TestWaitForAppendTimeout(t *testing.T) {
	dir := t.TempDir()
	db, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	l, err := OpenLog(db, "ns", "t", 1)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	ok := l.WaitForAppend(50 * time.Millisecond)
	if ok {
		t.Fatalf("expected timeout")
	}
}
