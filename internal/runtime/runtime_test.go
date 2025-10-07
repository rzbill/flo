package runtime

import (
	"context"
	"sync"
	"testing"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/eventlog"
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

func TestLogCaching(t *testing.T) {
	dir := t.TempDir()
	rt, err := Open(Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// Open the same log multiple times
	log1, err := rt.OpenLog("default", "orders", 0)
	if err != nil {
		t.Fatalf("open log 1: %v", err)
	}
	log2, err := rt.OpenLog("default", "orders", 0)
	if err != nil {
		t.Fatalf("open log 2: %v", err)
	}

	// Should return the same instance (cached)
	if log1 != log2 {
		t.Fatalf("expected same log instance, got different instances")
	}
}

func TestLogCachingConcurrent(t *testing.T) {
	dir := t.TempDir()
	rt, err := Open(Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// Test concurrent access to the same log
	const numGoroutines = 10
	var wg sync.WaitGroup
	logs := make([]*eventlog.Log, numGoroutines)
	errors := make([]error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log, err := rt.OpenLog("default", "orders", 0)
			logs[i] = log
			errors[i] = err
		}(i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			t.Fatalf("goroutine %d error: %v", i, err)
		}
	}

	// All logs should be the same instance
	firstLog := logs[0]
	for i, log := range logs {
		if log != firstLog {
			t.Fatalf("goroutine %d got different log instance", i)
		}
	}
}

func TestLogCachingDifferentPartitions(t *testing.T) {
	dir := t.TempDir()
	rt, err := Open(Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// Open logs for different partitions
	log0, err := rt.OpenLog("default", "orders", 0)
	if err != nil {
		t.Fatalf("open log partition 0: %v", err)
	}
	log1, err := rt.OpenLog("default", "orders", 1)
	if err != nil {
		t.Fatalf("open log partition 1: %v", err)
	}

	// Should be different instances
	if log0 == log1 {
		t.Fatalf("expected different log instances for different partitions")
	}
}

func TestLogCachingDifferentTopics(t *testing.T) {
	dir := t.TempDir()
	rt, err := Open(Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// Open logs for different topics
	log1, err := rt.OpenLog("default", "orders", 0)
	if err != nil {
		t.Fatalf("open log orders: %v", err)
	}
	log2, err := rt.OpenLog("default", "payments", 0)
	if err != nil {
		t.Fatalf("open log payments: %v", err)
	}

	// Should be different instances
	if log1 == log2 {
		t.Fatalf("expected different log instances for different topics")
	}
}

func TestLogCachingDifferentNamespaces(t *testing.T) {
	dir := t.TempDir()
	rt, err := Open(Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	defer rt.Close()

	// Ensure both namespaces exist
	if _, err := rt.EnsureNamespace("ns1"); err != nil {
		t.Fatalf("ensure ns1: %v", err)
	}
	if _, err := rt.EnsureNamespace("ns2"); err != nil {
		t.Fatalf("ensure ns2: %v", err)
	}

	// Open logs for different namespaces
	log1, err := rt.OpenLog("ns1", "orders", 0)
	if err != nil {
		t.Fatalf("open log ns1: %v", err)
	}
	log2, err := rt.OpenLog("ns2", "orders", 0)
	if err != nil {
		t.Fatalf("open log ns2: %v", err)
	}

	// Should be different instances
	if log1 == log2 {
		t.Fatalf("expected different log instances for different namespaces")
	}
}

func TestLogKeyGeneration(t *testing.T) {
	rt := &Runtime{}

	// Test log key generation
	key1 := rt.logKey("ns1", "topic1", 0)
	key2 := rt.logKey("ns1", "topic1", 1)
	key3 := rt.logKey("ns1", "topic2", 0)
	key4 := rt.logKey("ns2", "topic1", 0)

	// Different partitions should have different keys
	if key1 == key2 {
		t.Fatalf("expected different keys for different partitions")
	}

	// Different topics should have different keys
	if key1 == key3 {
		t.Fatalf("expected different keys for different topics")
	}

	// Different namespaces should have different keys
	if key1 == key4 {
		t.Fatalf("expected different keys for different namespaces")
	}

	// Same parameters should have same key
	key1Again := rt.logKey("ns1", "topic1", 0)
	if key1 != key1Again {
		t.Fatalf("expected same key for same parameters")
	}
}
