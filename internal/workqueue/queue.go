package workqueue

import (
	"context"
	"encoding/binary"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

// WorkQueue provides enqueue operations with priority and delay.
type WorkQueue struct {
	db        *pebblestore.DB
	namespace string
	queue     string
	part      uint32

	mu      sync.Mutex
	lastSeq uint64

	// backpressure
	maxAvailable  int
	throttleSleep time.Duration

	// sweeper controls
	sweepStop    chan struct{}
	sweepEnabled bool
	sweepIntv    time.Duration
	sweepMax     int
	sweepPrio    uint32
}

type QueueOptions struct {
	MaxAvailable  int           // throttle when available items > MaxAvailable (0 disables)
	ThrottleSleep time.Duration // sleep between retries when throttled
}

// OpenQueue initializes a WorkQueue and restores lastSeq from metadata if present.
func OpenQueue(db *pebblestore.DB, namespace, queue string, partition uint32) (*WorkQueue, error) {
	q := &WorkQueue{db: db, namespace: namespace, queue: queue, part: partition, throttleSleep: 10 * time.Millisecond}
	if meta, err := db.Get(MetaKey(namespace, queue, partition)); err == nil && len(meta) >= 8 {
		q.lastSeq = binary.BigEndian.Uint64(meta[:8])
	}
	return q, nil
}

func (q *WorkQueue) WithOptions(opts QueueOptions) *WorkQueue {
	q.maxAvailable = opts.MaxAvailable
	if opts.ThrottleSleep > 0 {
		q.throttleSleep = opts.ThrottleSleep
	}
	return q
}

func seqToMsgID(seq uint64) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], seq)
	return id
}

// ReclaimExpired scans lease index and returns expired items to availability with default priority.
// TODO: (FLO-OPS) Remaining production-grade tasks:
//   - Maintain a checkpoint across runs to resume from last scanned expiry
//   - Emit metrics/logs and expose admin controls externally
//   - Robust orphan cleanup across {q_lease, q_lease_idx, msg}
func (q *WorkQueue) ReclaimExpired(ctx context.Context, nowMs int64, max int, defaultPriority uint32) (int, error) {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	prefix := LeaseIdxPrefix(q.namespace, q.queue)
	hi := append(append([]byte{}, prefix...), 0xFF)
	iter, err := q.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return 0, nil
	}
	defer iter.Close()

	b := q.db.NewBatch()
	reclaimed := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		k := iter.Key()
		if len(k) < len(prefix)+8+1+16 {
			continue
		}
		exp := int64(binary.BigEndian.Uint64(k[len(prefix) : len(prefix)+8]))
		if exp > nowMs {
			break
		}
		idOff := len(k) - 16
		var id [16]byte
		copy(id[:], k[idOff:])
		// parse seq from ID high 8 bytes we stored
		seq := binary.BigEndian.Uint64(id[8:])
		// remove lease idx and lease record
		_ = b.Delete(k, nil)
		_ = b.Delete(LeaseKey(q.namespace, q.queue, "", id), nil) // group-agnostic reclaim best-effort
		// re-add to priority with default priority
		if err := b.Set(PrioKey(q.namespace, q.queue, defaultPriority, seq), nil, nil); err != nil {
			return reclaimed, err
		}
		reclaimed++
		if max > 0 && reclaimed >= max {
			break
		}
	}
	if reclaimed > 0 {
		if err := q.db.CommitBatch(ctx, b); err != nil {
			return reclaimed, err
		}
		// compaction hint after large sweep
		if reclaimed >= 4096 {
			_ = q.db.CompactRange(prefix, hi)
		}
	}
	return reclaimed, nil
}

// Enqueue inserts a message with priority and optional delay.
// If nowMs <= 0, time.Now().UnixMilli() is used.
func (q *WorkQueue) Enqueue(ctx context.Context, header, payload []byte, priority uint32, delayMs int64, nowMs int64) (uint64, error) {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}

	// simple throttle when available exceeds threshold
	if q.maxAvailable > 0 {
		for {
			avail := q.availableCount()
			if avail < q.maxAvailable {
				break
			}
			time.Sleep(q.throttleSleep)
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	b := q.db.NewBatch()
	defer b.Close()

	q.lastSeq++
	seq := q.lastSeq
	val := EncodeMessage(header, payload)
	if err := b.Set(MsgKey(q.namespace, q.queue, q.part, seq), val, nil); err != nil {
		return 0, err
	}
	// delay index if requested; otherwise index into priority for immediate availability
	if delayMs > 0 {
		fire := uint64(nowMs + delayMs)
		id := seqToMsgID(seq)
		var buf [12]byte
		binary.BigEndian.PutUint32(buf[0:4], priority)
		binary.BigEndian.PutUint64(buf[4:12], seq)
		if err := b.Set(DelayKey(q.namespace, q.queue, fire, id), buf[:], nil); err != nil {
			return 0, err
		}
	} else {
		// priority index (lower sorts first)
		if err := b.Set(PrioKey(q.namespace, q.queue, priority, seq), nil, nil); err != nil {
			return 0, err
		}
	}
	// update metadata: lastSeq (8B) | availableCount (4B)
	var meta [12]byte
	binary.BigEndian.PutUint64(meta[0:8], q.lastSeq)
	avail := q.availableCount()
	if delayMs == 0 {
		avail++
	}
	binary.BigEndian.PutUint32(meta[8:12], uint32(avail))
	if err := b.Set(MetaKey(q.namespace, q.queue, q.part), meta[:], nil); err != nil {
		return 0, err
	}

	if err := q.db.CommitBatch(ctx, b); err != nil {
		return 0, err
	}
	return seq, nil
}

func (q *WorkQueue) availableCount() int {
	meta, err := q.db.Get(MetaKey(q.namespace, q.queue, q.part))
	if err != nil || len(meta) < 12 {
		return 0
	}
	return int(binary.BigEndian.Uint32(meta[8:12]))
}

// LeasedMessage represents a dequeued message under a lease.
type LeasedMessage struct {
	Seq      uint64
	Header   []byte
	Payload  []byte
	ExpiryMs int64
}

// promoteDue moves delayed messages that are due into the priority index.
func (q *WorkQueue) promoteDue(ctx context.Context, nowMs int64, max int) error {
	prefix := DelayPrefix(q.namespace, q.queue)
	hi := append(append([]byte{}, prefix...), 0xFF)
	iter, err := q.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return nil
	}
	defer iter.Close()

	b := q.db.NewBatch()
	promoted := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		key := iter.Key()
		if len(key) < len(prefix)+8+1+16 {
			continue
		}
		fire := int64(binary.BigEndian.Uint64(key[len(prefix) : len(prefix)+8]))
		if fire > nowMs {
			break
		}
		val := iter.Value()
		if len(val) < 12 {
			continue
		}
		prio := binary.BigEndian.Uint32(val[0:4])
		seq := binary.BigEndian.Uint64(val[4:12])
		if err := b.Delete(key, nil); err != nil {
			return err
		}
		if err := b.Set(PrioKey(q.namespace, q.queue, prio, seq), nil, nil); err != nil {
			return err
		}
		promoted++
		if max > 0 && promoted >= max {
			break
		}
	}
	if promoted > 0 {
		// increment availableCount in meta
		meta, _ := q.db.Get(MetaKey(q.namespace, q.queue, q.part))
		var m2 [12]byte
		if len(meta) >= 8 {
			copy(m2[0:8], meta[0:8])
		} else {
			binary.BigEndian.PutUint64(m2[0:8], q.lastSeq)
		}
		avail := 0
		if len(meta) >= 12 {
			avail = int(binary.BigEndian.Uint32(meta[8:12]))
		}
		avail += promoted
		binary.BigEndian.PutUint32(m2[8:12], uint32(avail))
		_ = b.Set(MetaKey(q.namespace, q.queue, q.part), m2[:], nil)
		if err := q.db.CommitBatch(ctx, b); err != nil {
			return err
		}
	}
	return nil
}

// Dequeue acquires up to count messages ordered by priority, creating leases.
func (q *WorkQueue) Dequeue(ctx context.Context, group string, count int, leaseMs int64, nowMs int64) ([]LeasedMessage, error) {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	if count <= 0 {
		count = 1
	}
	if leaseMs <= 0 {
		leaseMs = 30_000
	}
	// Promote due delays first
	_ = q.promoteDue(ctx, nowMs, count*4)

	prefix := PrioPrefix(q.namespace, q.queue)
	hi := append(append([]byte{}, prefix...), 0xFF)
	iter, err := q.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return nil, nil
	}
	defer iter.Close()

	b := q.db.NewBatch()
	defer b.Close()
	msgs := make([]LeasedMessage, 0, count)
	acquired := 0
	for ok := iter.First(); ok && acquired < count; ok = iter.Next() {
		k := iter.Key()
		if len(k) < len(prefix)+4+1+8 {
			continue
		}
		seq := binary.BigEndian.Uint64(k[len(k)-8:])
		// Load message
		val, errGet := q.db.Get(MsgKey(q.namespace, q.queue, q.part, seq))
		if errGet != nil {
			_ = b.Delete(k, nil)
			continue
		}
		dec, okDec := DecodeMessage(val)
		if !okDec {
			_ = b.Delete(k, nil)
			continue
		}
		// Create lease with attempts=0
		exp := nowMs + leaseMs
		var lbuf [12]byte
		binary.BigEndian.PutUint64(lbuf[0:8], uint64(exp))
		binary.BigEndian.PutUint32(lbuf[8:12], 0)
		id := seqToMsgID(seq)
		if err := b.Set(LeaseKey(q.namespace, q.queue, group, id), lbuf[:], nil); err != nil {
			return nil, err
		}
		if err := b.Set(LeaseIdxKey(q.namespace, q.queue, uint64(exp), id), nil, nil); err != nil {
			return nil, err
		}
		// Remove from availability index
		if err := b.Delete(k, nil); err != nil {
			return nil, err
		}
		msgs = append(msgs, LeasedMessage{Seq: seq, Header: dec.Header, Payload: dec.Payload, ExpiryMs: exp})
		acquired++
	}
	if acquired > 0 {
		// decrement available count
		meta, _ := q.db.Get(MetaKey(q.namespace, q.queue, q.part))
		var avail int
		if len(meta) >= 12 {
			avail = int(binary.BigEndian.Uint32(meta[8:12]))
		}
		if avail < acquired {
			avail = 0
		} else {
			avail -= acquired
		}
		var m2 [12]byte
		if len(meta) >= 8 {
			copy(m2[0:8], meta[0:8])
		} else {
			binary.BigEndian.PutUint64(m2[0:8], q.lastSeq)
		}
		binary.BigEndian.PutUint32(m2[8:12], uint32(avail))
		_ = b.Set(MetaKey(q.namespace, q.queue, q.part), m2[:], nil)
		if err := q.db.CommitBatch(ctx, b); err != nil {
			return nil, err
		}
	}
	return msgs, nil
}

// ExtendLease extends leases for the provided sequences by leaseMs (preserving attempts).
func (q *WorkQueue) ExtendLease(ctx context.Context, group string, seqs []uint64, leaseMs int64, nowMs int64) error {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	if leaseMs <= 0 {
		leaseMs = 30_000
	}
	b := q.db.NewBatch()
	defer b.Close()
	for _, seq := range seqs {
		id := seqToMsgID(seq)
		exp := nowMs + leaseMs
		attempts := uint32(0)
		if existing, err := q.db.Get(LeaseKey(q.namespace, q.queue, group, id)); err == nil && len(existing) >= 12 {
			attempts = binary.BigEndian.Uint32(existing[8:12])
		}
		var lbuf [12]byte
		binary.BigEndian.PutUint64(lbuf[0:8], uint64(exp))
		binary.BigEndian.PutUint32(lbuf[8:12], attempts)
		if err := b.Set(LeaseKey(q.namespace, q.queue, group, id), lbuf[:], nil); err != nil {
			return err
		}
		if err := b.Set(LeaseIdxKey(q.namespace, q.queue, uint64(exp), id), nil, nil); err != nil {
			return err
		}
	}
	return q.db.CommitBatch(ctx, b)
}

// Complete removes messages from lease state and deletes the message payload.
func (q *WorkQueue) Complete(ctx context.Context, group string, seqs []uint64) error {
	b := q.db.NewBatch()
	defer b.Close()
	for _, seq := range seqs {
		id := seqToMsgID(seq)
		if err := b.Delete(LeaseKey(q.namespace, q.queue, group, id), nil); err != nil {
			return err
		}
		if err := b.Delete(MsgKey(q.namespace, q.queue, q.part, seq), nil); err != nil {
			return err
		}
	}
	return q.db.CommitBatch(ctx, b)
}

// Fail handles retry-after or DLQ routing, and increments attempts.
func (q *WorkQueue) Fail(ctx context.Context, group string, seqs []uint64, retryAfterMs int64, toDLQ bool, nowMs int64) error {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	b := q.db.NewBatch()
	defer b.Close()
	for _, seq := range seqs {
		id := seqToMsgID(seq)
		attempts := uint32(0)
		if existing, err := q.db.Get(LeaseKey(q.namespace, q.queue, group, id)); err == nil && len(existing) >= 12 {
			attempts = binary.BigEndian.Uint32(existing[8:12])
		}
		attempts++
		// remove lease records
		_ = b.Delete(LeaseKey(q.namespace, q.queue, group, id), nil)
		if toDLQ {
			// write to DLQ and delete message
			val, err := q.db.Get(MsgKey(q.namespace, q.queue, q.part, seq))
			if err == nil {
				if err := b.Set(DLQKey(q.namespace, q.queue, group, id), val, nil); err != nil {
					return err
				}
			}
			if err := b.Delete(MsgKey(q.namespace, q.queue, q.part, seq), nil); err != nil {
				return err
			}
		} else {
			// retry after: push back to delay with default priority (e.g., 10)
			prio := uint32(10)
			var buf [12]byte
			binary.BigEndian.PutUint32(buf[0:4], prio)
			binary.BigEndian.PutUint64(buf[4:12], seq)
			fire := uint64(nowMs + retryAfterMs)
			if err := b.Set(DelayKey(q.namespace, q.queue, fire, id), buf[:], nil); err != nil {
				return err
			}
		}
		// store attempts back into lease for observability
		var lbuf [12]byte
		binary.BigEndian.PutUint64(lbuf[0:8], uint64(nowMs))
		binary.BigEndian.PutUint32(lbuf[8:12], attempts)
		_ = b.Set(LeaseKey(q.namespace, q.queue, group, id), lbuf[:], nil)
	}
	return q.db.CommitBatch(ctx, b)
}

// ConfigureSweeper sets background sweeper options.
func (q *WorkQueue) ConfigureSweeper(interval time.Duration, maxPerTick int, defaultPriority uint32) {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if maxPerTick <= 0 {
		maxPerTick = 1024
	}
	q.sweepIntv = interval
	q.sweepMax = maxPerTick
	q.sweepPrio = defaultPriority
}

// SetSweeperEnabled starts or stops the background sweeper according to the flag.
func (q *WorkQueue) SetSweeperEnabled(enabled bool) {
	q.sweepEnabled = enabled
	if enabled {
		q.StartSweeper(q.sweepIntv, q.sweepMax, q.sweepPrio)
	} else {
		q.StopSweeper()
	}
}

// StartSweeper runs a background loop to reclaim expired leases.
func (q *WorkQueue) StartSweeper(interval time.Duration, maxPerTick int, defaultPriority uint32) {
	if q.sweepStop != nil {
		return
	}
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if maxPerTick <= 0 {
		maxPerTick = 1024
	}
	q.sweepStop = make(chan struct{})
	go func() {
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		for {
			select {
			case <-q.sweepStop:
				return
			case <-time.After(interval + time.Duration(rng.Int63n(int64(interval/10+1)))):
				if !q.sweepEnabled {
					continue
				}
				_, _ = q.ReclaimExpired(context.Background(), time.Now().UnixMilli(), maxPerTick, defaultPriority)
			}
		}
	}()
}

// StopSweeper stops the background sweeper.
func (q *WorkQueue) StopSweeper() {
	if q.sweepStop != nil {
		close(q.sweepStop)
		q.sweepStop = nil
	}
}
