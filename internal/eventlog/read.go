package eventlog

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble"
)

// Token encodes the starting position as seq (8 bytes big-endian).
type Token [8]byte

func tokenFromSeq(seq uint64) Token { var t Token; binary.BigEndian.PutUint64(t[:], seq); return t }
func (t Token) Seq() uint64         { return binary.BigEndian.Uint64(t[:]) }

type ReadOptions struct {
	Start   Token // if zero, begin from the first entry
	Limit   int
	Reverse bool
}

type Item struct {
	Seq     uint64
	Header  []byte
	Payload []byte
}

// Read returns up to Limit items starting at Start (inclusive). Reverse scans descending.
func (l *Log) Read(opts ReadOptions) ([]Item, Token) {
	startSeq := opts.Start.Seq()
	startKey := KeyLogEntry(l.namespace, l.topic, l.part, startSeq)
	// Range bounds: prefix of entries for this partition
	// Build smallest and largest possible entry keys for SeekGE/SeekLT
	low := KeyLogEntry(l.namespace, l.topic, l.part, 0)
	hi := KeyLogEntry(l.namespace, l.topic, l.part, ^uint64(0))

	iter, err := l.db.NewIter(&pebble.IterOptions{LowerBound: low, UpperBound: append(hi, 0x00)})
	// prepare outputs before early returns
	items := make([]Item, 0, max(1, opts.Limit))
	var next Token
	if err != nil {
		return items, next
	}
	defer iter.Close()

	if opts.Reverse {
		if startSeq == 0 {
			// start from last
			if !iter.Last() {
				return items, next
			}
		} else {
			if !iter.SeekLT(startKey) {
				if !iter.Last() {
					return items, next
				}
			}
		}
		for iter.Valid() && (opts.Limit == 0 || len(items) < opts.Limit) {
			seq := binary.BigEndian.Uint64(iter.Key()[len(startKey)-8:])
			dec, ok := DecodeRecord(iter.Value())
			if ok {
				items = append(items, Item{Seq: seq, Header: dec.Header, Payload: dec.Payload})
			}
			if !iter.Prev() {
				break
			}
		}
		if iter.Valid() {
			// Next token for reverse could be current seq
			copy(next[:], iter.Key()[len(startKey)-8:len(startKey)])
		}
		return items, next
	}

	// Forward scan
	if startSeq == 0 {
		if !iter.First() {
			return items, next
		}
	} else {
		if !iter.SeekGE(startKey) {
			return items, next
		}
	}
	for iter.Valid() && (opts.Limit == 0 || len(items) < opts.Limit) {
		seq := binary.BigEndian.Uint64(iter.Key()[len(startKey)-8:])
		dec, ok := DecodeRecord(iter.Value())
		if ok {
			items = append(items, Item{Seq: seq, Header: dec.Header, Payload: dec.Payload})
		}
		if !iter.Next() {
			break
		}
	}
	if iter.Valid() {
		copy(next[:], iter.Key()[len(startKey)-8:len(startKey)])
	}
	return items, next
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
