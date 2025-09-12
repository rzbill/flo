// Package config provides loading and environment overlay for Flo runtime
// configuration. It exposes a Default() baseline and helpers to construct
// an Options struct for the runtime and servers.
//
// Example:
//
//	cfg := config.Default()
//	// Optionally load from file and overlay env vars
//	if fileCfg, err := config.Load("/etc/flo.json"); err == nil {
//	    cfg = fileCfg
//	}
//	config.FromEnv(&cfg)
//	// Pass cfg into runtime.Options
//	rt, _ := runtime.Open(runtime.Options{DataDir: "/var/lib/flo", Fsync: pebblestore.FsyncModeAlways, Config: cfg})
//	defer rt.Close()
package config
