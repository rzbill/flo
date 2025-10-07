package eventlog

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"

	"github.com/cockroachdb/pebble"
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

// FirstSeq returns the first existing sequence number for this partition, if any.
// Returns 0 when empty.
func (l *Log) FirstSeq() uint64 {
	low := KeyLogEntry(l.namespace, l.topic, l.part, 0)
	hi := KeyLogEntry(l.namespace, l.topic, l.part, ^uint64(0))
	iter, err := l.db.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: append(hi, 0x00)})
	if err != nil {
		return 0
	}
	defer iter.Close()
	if !iter.First() {
		return 0
	}
	return binary.BigEndian.Uint64(iter.Key()[len(low)-8:])
}

// LastSeq returns the last existing sequence number for this partition, if any.
// Returns 0 when empty.
func (l *Log) LastSeq() uint64 {
	low := KeyLogEntry(l.namespace, l.topic, l.part, 0)
	hi := KeyLogEntry(l.namespace, l.topic, l.part, ^uint64(0))
	iter, err := l.db.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: append(hi, 0x00)})
	if err != nil {
		return 0
	}
	defer iter.Close()
	if !iter.Last() {
		return 0
	}
	return binary.BigEndian.Uint64(iter.Key()[len(low)-8:])
}

// Stats summarizes message count and total bytes for this log partition.
// Bytes is the sum of value lengths across entries in the partition.
func (l *Log) Stats() (firstSeq uint64, lastSeq uint64, count uint64, bytes uint64, err error) {
	low := KeyLogEntry(l.namespace, l.topic, l.part, 0)
	hi := KeyLogEntry(l.namespace, l.topic, l.part, ^uint64(0))
	iter, ierr := l.db.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: append(hi, 0x00)})
	if ierr != nil {
		err = ierr
		return
	}
	defer iter.Close()
	if !iter.First() {
		// empty
		return
	}
	for ok := true; ok; ok = iter.Next() {
		seq := binary.BigEndian.Uint64(iter.Key()[len(low)-8:])
		if count == 0 {
			firstSeq = seq
		}
		lastSeq = seq
		bytes += uint64(len(iter.Value()))
		count++
	}
	return
}

// LastPublishMs returns the header timestamp (ms) of the latest record in this partition.
// When the partition is empty, or the last record lacks an 8-byte header timestamp, returns 0.
func (l *Log) LastPublishMs() uint64 {
	items, _ := l.Read(ReadOptions{Reverse: true, Limit: 1})
	if len(items) == 0 {
		return 0
	}
	if len(items[0].Header) >= 8 {
		return binary.BigEndian.Uint64(items[0].Header[:8])
	}
	return 0
}
