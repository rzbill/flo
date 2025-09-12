// Package httpserver provides a minimal REST gateway for Flo with SSE
// subscribe support and JSON endpoints mirroring the Channels surface.
//
// Example:
//
//	rt, _ := runtime.Open(runtime.Options{DataDir: "./data", Fsync: pebblestore.FsyncModeAlways, Config: config.Default()})
//	s := httpserver.New(rt)
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	_ = s.ListenAndServe(ctx, ":8080")
package httpserver
