package eventlog

import (
	"context"
	"testing"
	"time"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

type captureArchiver struct {
	ns, t    string
	p        uint32
	min, max uint64
	called   bool
}

func (c *captureArchiver) EmitTrimRange(ns, t string, p uint32, minSeq, maxSeq uint64) {
	c.ns, c.t, c.p, c.min, c.max, c.called = ns, t, p, minSeq, maxSeq, true
}

func tsFromHeader(h []byte) (int64, bool) {
	if len(h) < 8 {
		return 0, false
	}
	// simple big-endian int64 in first 8 bytes
	v := int64(uint64(h[0])<<56 | uint64(h[1])<<48 | uint64(h[2])<<40 | uint64(h[3])<<32 | uint64(h[4])<<24 | uint64(h[5])<<16 | uint64(h[6])<<8 | uint64(h[7]))
	return v, true
}

func TestTrimOlderThanByTimestamp(t *testing.T) {
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

	now := time.Now().UnixMilli()
	recs := []AppendRecord{
		{Header: makeTs(now - 10_000), Payload: []byte("a")},
		{Header: makeTs(now - 5_000), Payload: []byte("b")},
		{Header: makeTs(now), Payload: []byte("c")},
	}
	if _, err := l.Append(context.Background(), recs); err != nil {
		t.Fatalf("append: %v", err)
	}

	del, last, err := l.TrimOlderThan(context.Background(), now-1, 10, 0, tsFromHeader)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if del != 2 {
		t.Fatalf("expected 2 deleted, got %d", del)
	}
	if last == 0 {
		return
	}
}

func makeTs(ms int64) []byte {
	b := make([]byte, 8)
	b[0] = byte(uint64(ms) >> 56)
	b[1] = byte(uint64(ms) >> 48)
	b[2] = byte(uint64(ms) >> 40)
	b[3] = byte(uint64(ms) >> 32)
	b[4] = byte(uint64(ms) >> 24)
	b[5] = byte(uint64(ms) >> 16)
	b[6] = byte(uint64(ms) >> 8)
	b[7] = byte(uint64(ms))
	return b
}

func TestTrimToMaxBytes(t *testing.T) {
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

	// append three payloads of size ~10 each
	for i := 0; i < 3; i++ {
		if _, err := l.Append(context.Background(), []AppendRecord{{Payload: []byte("0123456789")}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	// Trim down to ~1.5 payloads; should delete at least one
	del, err := l.TrimToMaxBytes(context.Background(), 15, 10, 0)
	if err != nil {
		t.Fatalf("trim bytes: %v", err)
	}
	if del < 1 {
		t.Fatalf("expected at least 1 deletion")
	}
}

func TestArchiverHookEmittedOnTrim(t *testing.T) {
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
	cap := &captureArchiver{}
	l.archiver = cap

	now := time.Now().UnixMilli()
	_, _ = l.Append(context.Background(), []AppendRecord{{Header: makeTs(now - 10_000)}, {Header: makeTs(now)}})
	_, _, err = l.TrimOlderThan(context.Background(), now-1, 10, 0, tsFromHeader)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if !cap.called || cap.min == 0 || cap.max == 0 {
		t.Fatalf("expected archiver hook called with range")
	}
}

func TestArchiverHookEmittedOnBytesTrim(t *testing.T) {
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
	cap := &captureArchiver{}
	l.archiver = cap

	for i := 0; i < 3; i++ {
		if _, err := l.Append(context.Background(), []AppendRecord{{Payload: []byte("0123456789")}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	_, err = l.TrimToMaxBytes(context.Background(), 15, 10, 0)
	if err != nil {
		t.Fatalf("trim: %v", err)
	}
	if !cap.called || cap.min == 0 || cap.max == 0 {
		t.Fatalf("expected archiver hook called on bytes trim")
	}
}
