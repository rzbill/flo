package eventlog

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/cockroachdb/pebble"
)

// HeaderTimestampExtractor extracts a write timestamp (ms) from an event header.
// Returns (ms, true) if present and valid.
type HeaderTimestampExtractor func(header []byte) (int64, bool)

// TrimOlderThan deletes entries with header timestamp < cutoffMs.
// Deletes are committed in batches of up to batchLimit keys with an optional throttle between commits.
// Returns number of deleted entries and the last deleted sequence (0 if none).
func (l *Log) TrimOlderThan(ctx context.Context, cutoffMs int64, batchLimit int, throttle time.Duration, tsx HeaderTimestampExtractor) (int, uint64, error) {
	if batchLimit <= 0 {
		batchLimit = 1024
	}

	low := KeyLogEntry(l.namespace, l.topic, l.part, 0)
	hi := KeyLogEntry(l.namespace, l.topic, l.part, ^uint64(0))
	iter, err := l.db.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: append(hi, 0x00)})
	if err != nil {
		return 0, 0, err
	}
	defer iter.Close()

	deleted := 0
	var lastSeq uint64
	var minSeq uint64
	firstDeleted := true
	for ok := iter.First(); ok; {
		b := l.db.NewBatch()
		n := 0
		for ok && n < batchLimit {
			seq := binary.BigEndian.Uint64(iter.Key()[len(low)-8:])
			dec, okDec := DecodeRecord(iter.Value())
			if okDec {
				if ms, okTs := tsx(dec.Header); okTs && ms < cutoffMs {
					if err := b.Delete(iter.Key(), nil); err != nil {
						b.Close()
						return deleted, lastSeq, err
					}
					deleted++
					lastSeq = seq
					if firstDeleted {
						minSeq = seq
						firstDeleted = false
					}
					n++
					ok = iter.Next()
					continue
				}
			}
			// stop when we reach an entry newer than cutoff
			ok = false
			break
		}
		if n > 0 {
			if err := l.db.CommitBatch(ctx, b); err != nil {
				b.Close()
				return deleted, lastSeq, err
			}
			b.Close()
			if deleted > 0 && !firstDeleted {
				l.archiver.EmitTrimRange(l.namespace, l.topic, l.part, minSeq, lastSeq)
			}
			if throttle > 0 {
				time.Sleep(throttle)
			}
		} else {
			b.Close()
		}
	}
	return deleted, lastSeq, nil
}

// TrimToMaxBytes approximates retention by total value bytes.
// If current bytes <= maxBytes, it is a no-op. Otherwise, deletes the oldest entries
// until total bytes <= maxBytes. Batched and throttled like TrimOlderThan.
func (l *Log) TrimToMaxBytes(ctx context.Context, maxBytes int64, batchLimit int, throttle time.Duration) (int, error) {
	if batchLimit <= 0 {
		batchLimit = 1024
	}
	if maxBytes < 0 {
		return 0, nil
	}

	low := KeyLogEntry(l.namespace, l.topic, l.part, 0)
	hi := KeyLogEntry(l.namespace, l.topic, l.part, ^uint64(0))
	iter, err := l.db.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: append(hi, 0x00)})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	// compute total bytes
	var total int64
	for ok := iter.First(); ok; ok = iter.Next() {
		total += int64(len(iter.Value()))
	}
	if total <= maxBytes {
		return 0, nil
	}

	// delete from oldest until under maxBytes
	deleted := 0
	var minSeq uint64
	var lastSeq uint64
	firstDeleted := true
	for ok := iter.First(); ok && total > maxBytes; {
		b := l.db.NewBatch()
		n := 0
		for ok && n < batchLimit && total > maxBytes {
			valLen := int64(len(iter.Value()))
			seq := binary.BigEndian.Uint64(iter.Key()[len(low)-8:])
			if err := b.Delete(iter.Key(), nil); err != nil {
				b.Close()
				return deleted, err
			}
			total -= valLen
			deleted++
			n++
			lastSeq = seq
			if firstDeleted {
				minSeq = seq
				firstDeleted = false
			}
			ok = iter.Next()
		}
		if n > 0 {
			if err := l.db.CommitBatch(ctx, b); err != nil {
				b.Close()
				return deleted, err
			}
			b.Close()
			if deleted > 0 && !firstDeleted {
				l.archiver.EmitTrimRange(l.namespace, l.topic, l.part, minSeq, lastSeq)
			}
			if throttle > 0 {
				time.Sleep(throttle)
			}
		} else {
			b.Close()
		}
	}
	return deleted, nil
}
