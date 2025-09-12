// Package runtime wires storage, config, and facades into a single-node
// Flo instance. It exposes Open/Close, basic health checks, and helpers
// to open internal components used by higher-level services.
//
// Example:
//
//	cfg := config.Default()
//	rt, _ := runtime.Open(runtime.Options{DataDir: "./data", Fsync: pebblestore.FsyncModeAlways, Config: cfg})
//	defer rt.Close()
//	// Health
//	_ = rt.CheckHealth(context.Background())
//	// Open a log and append
//	log, _ := rt.OpenLog("default", "orders", 0)
//	_, _ = log.Append(context.Background(), []eventlog.AppendRecord{{Payload: []byte("hello")}})
package runtime
