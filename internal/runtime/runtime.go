package runtime

import (
	"context"
	"errors"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/eventlog"
	"github.com/rzbill/flo/internal/namespace"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
	"github.com/rzbill/flo/internal/workqueue"
)

// Options for building the Runtime.
type Options struct {
	DataDir string
	Fsync   pebblestore.FsyncMode
	Config  cfgpkg.Config
}

// Runtime wires storage, config, and facades for a single-node instance.
type Runtime struct {
	db     *pebblestore.DB
	config cfgpkg.Config
}

// Open initializes the underlying storage and returns a Runtime.
func Open(opts Options) (*Runtime, error) {
	db, err := pebblestore.Open(pebblestore.Options{DataDir: opts.DataDir, Fsync: opts.Fsync})
	if err != nil {
		return nil, err
	}
	rt := &Runtime{db: db, config: opts.Config}
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
	return eventlog.OpenLog(r.db, ns, topic, partition)
}

// OpenQueue opens a work queue for given namespace/queue/partition.
func (r *Runtime) OpenQueue(ns, queue string, partition uint32) (*workqueue.WorkQueue, error) {
	return workqueue.OpenQueue(r.db, ns, queue, partition)
}

// DB exposes the underlying DB for advanced operations (internal use only).
func (r *Runtime) DB() *pebblestore.DB { return r.db }

// Config returns the runtime configuration.
func (r *Runtime) Config() cfgpkg.Config { return r.config }
