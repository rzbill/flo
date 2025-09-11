package eventlog

import (
	"context"
	"testing"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func seedLog(t *testing.T, n int) (*Log, []uint64) {
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
	recs := make([]AppendRecord, n)
	for i := 0; i < n; i++ {
		recs[i] = AppendRecord{Payload: []byte{byte(i)}}
	}
	seqs, err := l.Append(context.Background(), recs)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return l, seqs
}

func TestReadForward(t *testing.T) {
	l, seqs := seedLog(t, 5)
	items, _ := l.Read(ReadOptions{Limit: 3})
	if len(items) != 3 {
		t.Fatalf("want 3 items, got %d", len(items))
	}
	if items[0].Seq != seqs[0] || items[2].Seq != seqs[2] {
		t.Fatalf("unexpected seqs")
	}
}

func TestReadReverse(t *testing.T) {
	l, seqs := seedLog(t, 4)
	items, _ := l.Read(ReadOptions{Reverse: true, Limit: 2})
	if len(items) != 2 {
		t.Fatalf("want 2, got %d", len(items))
	}
	if !(items[0].Seq == seqs[3] && items[1].Seq == seqs[2]) {
		t.Fatalf("unexpected reverse order")
	}
}

func TestSeekByToken(t *testing.T) {
	l, seqs := seedLog(t, 4)
	tok := tokenFromSeq(seqs[2])
	items, _ := l.Read(ReadOptions{Start: tok, Limit: 2})
	if len(items) == 0 || items[0].Seq != seqs[2] {
		t.Fatalf("seek failed")
	}
}
