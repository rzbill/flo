package runtime

import (
	"context"
	"testing"

	cfgpkg "github.com/rzbill/flo/internal/config"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func TestOpenCloseHealth(t *testing.T) {
	dir := t.TempDir()
	rt, err := Open(Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()
	if err := rt.CheckHealth(context.Background()); err != nil {
		t.Fatalf("health: %v", err)
	}
}

func TestEnsureAndOpen(t *testing.T) {
	dir := t.TempDir()
	rt, err := Open(Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer rt.Close()
	if _, err := rt.EnsureNamespace("default"); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	if _, err := rt.OpenLog("default", "orders", 0); err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := rt.OpenQueue("default", "jobs", 0); err != nil {
		t.Fatalf("open queue: %v", err)
	}
}
