// Package serverrun exposes a shared Run entrypoint used by the CLI to start
// the Flo runtime with gRPC and HTTP servers, handling lifecycle and shutdown.
//
// Example:
//
//	opts := serverrun.Options{DataDir: "./data", GRPCAddr: ":50051", HTTPAddr: ":8080", Fsync: pebblestore.FsyncModeAlways, Config: config.Default()}
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	_ = serverrun.Run(ctx, opts)
package serverrun
