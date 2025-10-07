package runtime

import (
	"context"
	"errors"
	"sync"
	"time"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/eventlog"
	"github.com/rzbill/flo/internal/namespace"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
	"github.com/rzbill/flo/internal/workqueue"
)

// Options for building the Runtime.
type Options struct {
	DataDir       string
	Fsync         pebblestore.FsyncMode
	FsyncInterval time.Duration
	Config        cfgpkg.Config
}

// Runtime wires storage, config, and facades for a single-node instance.
type Runtime struct {
	db     *pebblestore.DB
	config cfgpkg.Config
	logsMu sync.Mutex
	logs   map[string]*eventlog.Log
}

// Open initializes the underlying storage and returns a Runtime.
func Open(opts Options) (*Runtime, error) {
	db, err := pebblestore.Open(pebblestore.Options{DataDir: opts.DataDir, Fsync: opts.Fsync, FsyncInterval: opts.FsyncInterval})
	if err != nil {
		return nil, err
	}
	rt := &Runtime{db: db, config: opts.Config, logs: make(map[string]*eventlog.Log)}
	return rt, nil
}

// Close closes underlying resources.
func (r *Runtime) Close() error {
	if r.db == nil {
		return nil
	}
	return r.db.Close()
}

// CheckHealth performs a simple health check.
func (r *Runtime) CheckHealth(ctx context.Context) error {
	if r.db == nil {
		return errors.New("db not open")
	}
	it, err := r.db.NewIter(nil)
	if err != nil {
		return err
	}
	it.Close()
	return nil
}

// EnsureNamespace creates a namespace record if absent.
func (r *Runtime) EnsureNamespace(name string) (namespace.Meta, error) {
	return namespace.EnsureNamespace(r.db, name)
}

// OpenLog opens an event log for given namespace/topic/partition.
func (r *Runtime) OpenLog(ns, topic string, partition uint32) (*eventlog.Log, error) {
	key := r.logKey(ns, topic, partition)
	r.logsMu.Lock()
	if l, ok := r.logs[key]; ok {
		r.logsMu.Unlock()
		return l, nil
	}
	r.logsMu.Unlock()
	// Create outside the lock to avoid blocking
	l, err := eventlog.OpenLog(r.db, ns, topic, partition)
	if err != nil {
		return nil, err
	}
	r.logsMu.Lock()
	// Double-check in case another goroutine created it
	if existing, ok := r.logs[key]; ok {
		r.logsMu.Unlock()
		return existing, nil
	}
	r.logs[key] = l
	r.logsMu.Unlock()
	return l, nil
}

// OpenQueue opens a work queue for given namespace/queue/partition.
func (r *Runtime) OpenQueue(ns, queue string, partition uint32) (*workqueue.WorkQueue, error) {
	return workqueue.OpenQueue(r.db, ns, queue, partition)
}

// DB exposes the underlying DB for advanced operations (internal use only).
func (r *Runtime) DB() *pebblestore.DB { return r.db }

// Config returns the runtime configuration.
func (r *Runtime) Config() cfgpkg.Config { return r.config }

func (r *Runtime) logKey(ns, topic string, partition uint32) string {
	// ns|topic|part
	// Allocate small buffer
	return ns + "|" + topic + "|" + string(rune(partition))
}
