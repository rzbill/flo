package pebblestore

import (
	"context"
	"testing"
	"time"
)

type testMetrics struct {
	wrote        int
	read         int
	batchCommits int
	batchBytes   int
}

func (m *testMetrics) ObserveWrite(d time.Duration, bytes int) { m.wrote += bytes }
func (m *testMetrics) ObserveRead(d time.Duration, bytes int)  { m.read += bytes }
func (m *testMetrics) ObserveBatchCommit(d time.Duration, numOps int, bytes int) {
	m.batchCommits++
	m.batchBytes += bytes
}

func newTestDB(t *testing.T) (*DB, *testMetrics) {
	t.Helper()
	dir := t.TempDir()
	metrics := &testMetrics{}
	db, err := Open(Options{
		DataDir:       dir,
		Fsync:         FsyncModeInterval,
		FsyncInterval: 2 * time.Millisecond,
		Metrics:       metrics,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, metrics
}

func TestCRUD(t *testing.T) {
	db, metrics := newTestDB(t)

	key := []byte("k1")
	val := []byte("v1")
	if err := db.Set(key, val); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(val) {
		t.Fatalf("got %q want %q", got, val)
	}

	if metrics.read == 0 {
		t.Fatalf("expected read metrics to record bytes")
	}

	if err := db.Delete(key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := db.Get(key); err == nil {
		t.Fatalf("expected not found after delete")
	}
}

func TestBatchCommitMetrics(t *testing.T) {
	db, metrics := newTestDB(t)

	b := db.NewBatch()
	if err := b.Set([]byte("a"), []byte("1"), nil); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if err := b.Set([]byte("b"), []byte("2"), nil); err != nil {
		t.Fatalf("batch set: %v", err)
	}
	if err := db.CommitBatch(context.Background(), b); err != nil {
		t.Fatalf("commit: %v", err)
	}
	b.Close()

	if metrics.batchCommits != 1 {
		t.Fatalf("want 1 batch commit, got %d", metrics.batchCommits)
	}
	if metrics.batchBytes <= 0 {
		t.Fatalf("expected positive batch bytes")
	}
}

func TestSnapshotConsistency(t *testing.T) {
	db, _ := newTestDB(t)

	key := []byte("k2")
	if err := db.Set(key, []byte("old")); err != nil {
		t.Fatalf("set: %v", err)
	}
	snap := db.NewSnapshot()
	defer snap.Close()

	// mutate after snapshot
	if err := db.Set(key, []byte("new")); err != nil {
		t.Fatalf("set: %v", err)
	}

	// read via snapshot should see old
	valOld, closer, err := snap.Get(key)
	if err != nil {
		t.Fatalf("snap get: %v", err)
	}
	if string(valOld) != "old" {
		t.Fatalf("snapshot saw %q want %q", valOld, "old")
	}
	closer.Close()

	// read via DB should see new
	valNew, err := db.Get(key)
	if err != nil {
		t.Fatalf("db get: %v", err)
	}
	if string(valNew) != "new" {
		t.Fatalf("db saw %q want %q", valNew, "new")
	}
}
