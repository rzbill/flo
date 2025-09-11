package eventlog

import (
	"context"
	"testing"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func newLogForCursor(t *testing.T) (*Log, func()) {
	dir := t.TempDir()
	db, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	l, err := OpenLog(db, "ns", "t", 1)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	return l, func() { _ = db.Close() }
}

func TestCommitCursorIdempotent(t *testing.T) {
	l, cleanup := newLogForCursor(t)
	defer cleanup()
	// seed two records
	seqs, err := l.Append(context.Background(), []AppendRecord{{Payload: []byte("a")}, {Payload: []byte("b")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	tok1 := tokenFromSeq(seqs[0])
	tok2 := tokenFromSeq(seqs[1])

	if err := l.CommitCursor("g1", tok1); err != nil {
		t.Fatalf("commit1: %v", err)
	}
	if got, ok := l.GetCursor("g1"); !ok || got.Seq() != tok1.Seq() {
		t.Fatalf("cursor mismatch")
	}

	// committing same or lower should be no-op
	if err := l.CommitCursor("g1", tok1); err != nil {
		t.Fatalf("commit same: %v", err)
	}
	if err := l.CommitCursor("g1", tokenFromSeq(tok1.Seq()-1)); err != nil {
		t.Fatalf("commit lower: %v", err)
	}
	if got, ok := l.GetCursor("g1"); !ok || got.Seq() != tok1.Seq() {
		t.Fatalf("cursor regressed")
	}

	// committing higher should advance
	if err := l.CommitCursor("g1", tok2); err != nil {
		t.Fatalf("commit2: %v", err)
	}
	if got, _ := l.GetCursor("g1"); got.Seq() != tok2.Seq() {
		t.Fatalf("did not advance")
	}
}

func TestCursorPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	l, err := OpenLog(db, "ns", "t", 1)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	seqs, err := l.Append(context.Background(), []AppendRecord{{Payload: []byte("a")}})
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := l.CommitCursor("g1", tokenFromSeq(seqs[0])); err != nil {
		t.Fatalf("commit: %v", err)
	}
	_ = db.Close()

	db2, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("reopen pebble: %v", err)
	}
	defer db2.Close()
	l2, err := OpenLog(db2, "ns", "t", 1)
	if err != nil {
		t.Fatalf("open log2: %v", err)
	}
	if got, ok := l2.GetCursor("g1"); !ok || got.Seq() != seqs[0] {
		t.Fatalf("cursor not persisted")
	}
}
