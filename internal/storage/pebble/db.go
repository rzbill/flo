package pebblestore

import (
	"context"
	"errors"
	"time"

	"github.com/cockroachdb/pebble"
)

// FsyncMode defines durability behavior for write operations.
type FsyncMode int

const (
	FsyncModeUnspecified FsyncMode = iota
	// FsyncModeAlways requests a WAL fsync on each committed batch/write.
	FsyncModeAlways
	// FsyncModeInterval enables group-commit by allowing Pebble to coalesce WAL
	// syncs for operations within the configured interval.
	FsyncModeInterval
	// FsyncModeNever avoids forcing WAL syncs from the application. Pebble may
	// still sync based on its own policies. This mode trades durability latency
	// for throughput and should be used with care.
	FsyncModeNever
)

// Options configures the Pebble store wrapper.
type Options struct {
	// DataDir is the path to the Pebble database directory.
	DataDir string
	// Fsync determines when to sync the WAL.
	Fsync FsyncMode
	// FsyncInterval controls group-commit when Fsync=FsyncModeInterval.
	FsyncInterval time.Duration
	// PebbleOptions allows advanced tuning of Pebble. If nil, sensible defaults are used.
	PebbleOptions *pebble.Options
	// Metrics allows observing read/write/commit latencies and sizes. Optional.
	Metrics MetricsHook
}

// MetricsHook is a minimal hook surface for storage observations.
type MetricsHook interface {
	ObserveWrite(elapsed time.Duration, bytes int)
	ObserveRead(elapsed time.Duration, bytes int)
	ObserveBatchCommit(elapsed time.Duration, numOps int, bytes int)
}

// NoopMetrics is used when no metrics hook is provided.
type NoopMetrics struct{}

func (NoopMetrics) ObserveWrite(time.Duration, int)            {}
func (NoopMetrics) ObserveRead(time.Duration, int)             {}
func (NoopMetrics) ObserveBatchCommit(time.Duration, int, int) {}

// DB wraps a Pebble database instance with fsync policy and basic helpers.
type DB struct {
	inner     *pebble.DB
	writeSync bool
	metrics   MetricsHook
}

// Open creates or opens a Pebble database with the provided options.
func Open(opts Options) (*DB, error) {
	if opts.DataDir == "" {
		return nil, errors.New("pebble: Options.DataDir is required")
	}

	po := opts.PebbleOptions
	if po == nil {
		po = &pebble.Options{}
	}

	// Configure group-commit via WALMinSyncInterval when desired.
	switch opts.Fsync {
	case FsyncModeAlways:
		// Force Sync on each write. WALMinSyncInterval left at default (0).
		// We'll pass WriteOptions{Sync:true} on commits.
	case FsyncModeInterval:
		if opts.FsyncInterval <= 0 {
			opts.FsyncInterval = 5 * time.Millisecond
		}
		po.WALMinSyncInterval = func() time.Duration { return opts.FsyncInterval }
	case FsyncModeNever:
		// Neither set WALMinSyncInterval nor Sync on writes.
	default:
		// Default to small group-commit for reasonable latency/throughput tradeoff.
		po.WALMinSyncInterval = func() time.Duration { return 5 * time.Millisecond }
	}

	inner, err := pebble.Open(opts.DataDir, po)
	if err != nil {
		return nil, err
	}

	metrics := opts.Metrics
	if metrics == nil {
		metrics = NoopMetrics{}
	}

	db := &DB{
		inner:     inner,
		writeSync: opts.Fsync == FsyncModeAlways,
		metrics:   metrics,
	}
	return db, nil
}

// Close closes the Pebble database.
func (db *DB) Close() error {
	if db == nil || db.inner == nil {
		return nil
	}
	return db.inner.Close()
}

// NewSnapshot creates a consistent view of the database. Caller must Close the snapshot.
func (db *DB) NewSnapshot() *pebble.Snapshot {
	return db.inner.NewSnapshot()
}

// NewBatch creates a new batch for atomic multi-key updates.
func (db *DB) NewBatch() *pebble.Batch {
	return db.inner.NewBatch()
}

// CommitBatch commits the provided batch with the configured fsync policy.
func (db *DB) CommitBatch(ctx context.Context, b *pebble.Batch) error {
	if b == nil {
		return errors.New("pebble: nil batch")
	}
	start := time.Now()
	size := b.Len()
	defer db.metrics.ObserveBatchCommit(time.Since(start), 0, size)

	syncMode := pebble.NoSync
	if db.writeSync {
		syncMode = pebble.Sync
	}
	return b.Commit(syncMode)
}

// Set sets a key to a value using a small internal batch respecting fsync policy.
func (db *DB) Set(key, value []byte) error {
	b := db.inner.NewBatch()
	defer b.Close()
	if err := b.Set(key, value, nil); err != nil {
		return err
	}
	return db.CommitBatch(context.Background(), b)
}

// Delete removes a key using a small internal batch respecting fsync policy.
func (db *DB) Delete(key []byte) error {
	b := db.inner.NewBatch()
	defer b.Close()
	if err := b.Delete(key, nil); err != nil {
		return err
	}
	return db.CommitBatch(context.Background(), b)
}

// Get copies the value for the given key.
func (db *DB) Get(key []byte) ([]byte, error) {
	start := time.Now()
	val, closer, err := db.inner.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, err
		}
		return nil, err
	}
	defer closer.Close()
	buf := append([]byte(nil), val...)
	db.metrics.ObserveRead(time.Since(start), len(buf))
	return buf, nil
}

// NewIter creates a raw Pebble iterator with the provided options.
func (db *DB) NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return db.inner.NewIter(opts)
}

// CompactRange requests compaction of the key range [start, end).
func (db *DB) CompactRange(start, end []byte) error {
	return db.inner.Compact(start, end, true)
}
