package eventlog

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

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

func TestFirstSeq(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()

	// Empty log should return 0
	if seq := l.FirstSeq(); seq != 0 {
		t.Fatalf("empty log should return 0, got %d", seq)
	}

	// Add some records
	seqs, err := l.Append(ctx, []AppendRecord{
		{Header: []byte("h1"), Payload: []byte("p1")},
		{Header: []byte("h2"), Payload: []byte("p2")},
		{Header: []byte("h3"), Payload: []byte("p3")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// FirstSeq should return the first sequence number
	if seq := l.FirstSeq(); seq != seqs[0] {
		t.Fatalf("expected first seq %d, got %d", seqs[0], seq)
	}
}

func TestLastSeq(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()

	// Empty log should return 0
	if seq := l.LastSeq(); seq != 0 {
		t.Fatalf("empty log should return 0, got %d", seq)
	}

	// Add some records
	seqs, err := l.Append(ctx, []AppendRecord{
		{Header: []byte("h1"), Payload: []byte("p1")},
		{Header: []byte("h2"), Payload: []byte("p2")},
		{Header: []byte("h3"), Payload: []byte("p3")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// LastSeq should return the last sequence number
	if seq := l.LastSeq(); seq != seqs[len(seqs)-1] {
		t.Fatalf("expected last seq %d, got %d", seqs[len(seqs)-1], seq)
	}
}

func TestStats(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()

	// Empty log stats
	firstSeq, lastSeq, count, bytes, err := l.Stats()
	if err != nil {
		t.Fatalf("stats error: %v", err)
	}
	if firstSeq != 0 || lastSeq != 0 || count != 0 || bytes != 0 {
		t.Fatalf("empty log should have zero stats, got first=%d last=%d count=%d bytes=%d", firstSeq, lastSeq, count, bytes)
	}

	// Add some records
	records := []AppendRecord{
		{Header: []byte("h1"), Payload: []byte("payload1")},
		{Header: []byte("h2"), Payload: []byte("payload2")},
		{Header: []byte("h3"), Payload: []byte("payload3")},
	}
	seqs, err := l.Append(ctx, records)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Check stats
	firstSeq, lastSeq, count, bytes, err = l.Stats()
	if err != nil {
		t.Fatalf("stats error: %v", err)
	}
	if firstSeq != seqs[0] {
		t.Fatalf("expected first seq %d, got %d", seqs[0], firstSeq)
	}
	if lastSeq != seqs[len(seqs)-1] {
		t.Fatalf("expected last seq %d, got %d", seqs[len(seqs)-1], lastSeq)
	}
	if count != uint64(len(records)) {
		t.Fatalf("expected count %d, got %d", len(records), count)
	}

	// Calculate expected bytes (encoded record sizes including varint header length + header + payload + crc32)
	expectedBytes := uint64(0)
	for _, r := range records {
		// EncodeRecord stores: varint(headerLen) + header + payload + crc32(4 bytes)
		headerLen := len(r.Header)
		varintLen := 1 // minimum varint length for small headers
		if headerLen > 127 {
			varintLen = 2
		}
		expectedBytes += uint64(varintLen + headerLen + len(r.Payload) + 4) // +4 for CRC
	}
	if bytes != expectedBytes {
		t.Fatalf("expected bytes %d, got %d", expectedBytes, bytes)
	}
}

func TestLastPublishMs(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()

	// Empty log should return 0
	if ms := l.LastPublishMs(); ms != 0 {
		t.Fatalf("empty log should return 0, got %d", ms)
	}

	// Add records with timestamps
	now := time.Now().UnixMilli()
	records := []AppendRecord{
		{Header: createTimestampHeader(now - 1000), Payload: []byte("p1")},
		{Header: createTimestampHeader(now - 500), Payload: []byte("p2")},
		{Header: createTimestampHeader(now), Payload: []byte("p3")},
	}
	_, err := l.Append(ctx, records)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// LastPublishMs should return the timestamp of the last record
	if ms := l.LastPublishMs(); ms != uint64(now) {
		t.Fatalf("expected last publish ms %d, got %d", now, ms)
	}
}

func TestLastPublishMsNoTimestamp(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()

	// Add record without timestamp header
	_, err := l.Append(ctx, []AppendRecord{
		{Header: []byte("short"), Payload: []byte("p1")},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// Should return 0 when no timestamp header
	if ms := l.LastPublishMs(); ms != 0 {
		t.Fatalf("expected 0 for no timestamp, got %d", ms)
	}
}

func createTimestampHeader(timestampMs int64) []byte {
	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, uint64(timestampMs))
	return header
}
