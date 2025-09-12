package namespace

import (
	"testing"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func TestEnsureNamespaceIdempotent(t *testing.T) {
	dir := t.TempDir()
	db, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	m1, err := EnsureNamespace(db, "default")
	if err != nil {
		t.Fatalf("ensure1: %v", err)
	}
	m2, err := EnsureNamespace(db, "default")
	if err != nil {
		t.Fatalf("ensure2: %v", err)
	}
	if m1.Name != m2.Name || m1.CreatedAtMs != m2.CreatedAtMs {
		t.Fatalf("not idempotent: %+v vs %+v", m1, m2)
	}
}
