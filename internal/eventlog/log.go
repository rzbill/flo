package eventlog

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

// AppendRecord represents a single appendable event.
type AppendRecord struct {
	Header  []byte
	Payload []byte
}

// Log provides append-only operations for a namespace/topic/partition.
type Log struct {
	db        *pebblestore.DB
	namespace string
	topic     string
	part      uint32

	mu       sync.Mutex
	lastSeq  uint64
	notifyCh chan struct{}
	archiver ArchiverHook
}

// OpenLog initializes a Log and loads the last sequence from metadata (if any).
func OpenLog(db *pebblestore.DB, namespace, topic string, partition uint32) (*Log, error) {
	l := &Log{db: db, namespace: namespace, topic: topic, part: partition, notifyCh: make(chan struct{}), archiver: noopArchiver{}}
	// Load lastSeq from meta if present
	metaKey := KeyLogMeta(namespace, topic, partition)
	meta, err := db.Get(metaKey)
	if err == nil && len(meta) >= 8 {
		l.lastSeq = binary.BigEndian.Uint64(meta[:8])
	}
	return l, nil
}

// Append appends the provided records as a single atomic batch. Returns assigned seq numbers.
func (l *Log) Append(ctx context.Context, recs []AppendRecord) ([]uint64, error) {
	if len(recs) == 0 {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.db.NewBatch()
	defer b.Close()

	seqs := make([]uint64, len(recs))
	for i, r := range recs {
		l.lastSeq++
		seq := l.lastSeq
		val := EncodeRecord(r.Header, r.Payload)
		if err := b.Set(KeyLogEntry(l.namespace, l.topic, l.part, seq), val, nil); err != nil {
			return nil, err
		}
		seqs[i] = seq
	}

	// Update metadata with lastSeq
	var meta [8]byte
	binary.BigEndian.PutUint64(meta[:], l.lastSeq)
	if err := b.Set(KeyLogMeta(l.namespace, l.topic, l.part), meta[:], nil); err != nil {
		return nil, err
	}

	if err := l.db.CommitBatch(ctx, b); err != nil {
		return nil, err
	}
	// notify waiters
	close(l.notifyCh)
	l.notifyCh = make(chan struct{})
	return seqs, nil
}

var ErrNotFound = errors.New("event not found")
