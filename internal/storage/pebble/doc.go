// Package pebblestore provides a thin wrapper around Pebble with fsync policy,
// snapshots, batches, and minimal metrics hooks.
//
// Usage:
//
//	db, err := pebblestore.Open(pebblestore.Options{
//	    DataDir: "./data",
//	    Fsync:   pebblestore.FsyncModeInterval,
//	})
//	if err != nil { /* handle */ }
//	defer db.Close()
//
//	// Atomic updates with batches
//	b := db.NewBatch()
//	_ = b.Set([]byte("k"), []byte("v"), nil)
//	_ = db.CommitBatch(context.Background(), b)
//	b.Close()
//
//	// Point ops
//	_ = db.Set([]byte("k2"), []byte("v2"))
//	v, _ := db.Get([]byte("k2"))
package pebblestore
