package id

import (
	"encoding/binary"
	"math"
	"sync"
	"time"
)

// ID is a 128-bit, lexicographically sortable identifier encoded as 16 bytes
// big-endian: [8 bytes ms_timestamp][8 bytes sequence].
type ID [16]byte

// Bytes returns the raw 16-byte representation.
func (i ID) Bytes() []byte { b := make([]byte, 16); copy(b, i[:]); return b }

// String returns a hex string.
func (i ID) String() string { return fmtHex(i[:]) }

// Compare returns -1, 0, 1 based on lexical comparison.
func (i ID) Compare(other ID) int {
	for idx := 0; idx < 16; idx++ {
		if i[idx] < other[idx] {
			return -1
		}
		if i[idx] > other[idx] {
			return 1
		}
	}
	return 0
}

// Generator produces monotonically increasing IDs per process.
type Generator struct {
	mu       sync.Mutex
	lastMs   int64
	sequence uint64
}

// NewGenerator creates a new Generator.
func NewGenerator() *Generator { return &Generator{} }

// NowMs returns current time in milliseconds since Unix epoch.
var NowMs = func() int64 { return time.Now().UnixMilli() }

// Next returns a new ID. If clock goes backwards, it uses lastMs and increments sequence.
// If sequence overflows within the same millisecond, it busy-waits for next ms.
func (g *Generator) Next() ID {
	g.mu.Lock()
	defer g.mu.Unlock()

	ms := NowMs()
	if ms < g.lastMs {
		ms = g.lastMs
	}

	if ms == g.lastMs {
		if g.sequence == math.MaxUint64 {
			// wait until next ms to avoid overflow
			for {
				ms = NowMs()
				if ms > g.lastMs {
					break
				}
				time.Sleep(time.Millisecond / 8)
			}
			g.sequence = 0
		} else {
			g.sequence++
		}
	} else {
		g.sequence = 0
	}

	g.lastMs = ms
	return makeID(ms, g.sequence)
}

func makeID(ms int64, seq uint64) ID {
	var id ID
	binary.BigEndian.PutUint64(id[0:8], uint64(ms))
	binary.BigEndian.PutUint64(id[8:16], seq)
	return id
}

// fmtHex is a small, allocation-lean hex encoder for fixed-size IDs.
func fmtHex(b []byte) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, v := range b {
		out[i*2] = hexdigits[v>>4]
		out[i*2+1] = hexdigits[v&0x0f]
	}
	return string(out)
}
