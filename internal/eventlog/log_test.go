package eventlog

import (
	"context"
	"testing"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func newTestLog(t *testing.T) *Log {
	t.Helper()
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
	return l
}

func TestAppendAssignsSequential(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()
	seqs, err := l.Append(ctx, []AppendRecord{{Header: []byte("h1"), Payload: []byte("p1")}, {Header: []byte("h2"), Payload: []byte("p2")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(seqs) != 2 {
		t.Fatalf("want 2 seqs, got %d", len(seqs))
	}
	if !(seqs[0] < seqs[1]) {
		t.Fatalf("expected increasing seqs: %v", seqs)
	}
}

func TestAppendDurableAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	l, err := OpenLog(db, "ns", "t", 1)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	ctx := context.Background()
	seqs, err := l.Append(ctx, []AppendRecord{{Payload: []byte("x")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if len(seqs) != 1 {
		t.Fatalf("want one seq")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// reopen and ensure lastSeq is restored via meta
	db2, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	t.Cleanup(func() { _ = db2.Close() })
	l2, err := OpenLog(db2, "ns", "t", 1)
	if err != nil {
		t.Fatalf("open log2: %v", err)
	}
	seqs2, err := l2.Append(ctx, []AppendRecord{{Payload: []byte("y")}})
	if err != nil {
		t.Fatalf("append2: %v", err)
	}
	if !(seqs[0] < seqs2[0]) {
		t.Fatalf("expected next seq > previous: prev=%d next=%d", seqs[0], seqs2[0])
	}
}
