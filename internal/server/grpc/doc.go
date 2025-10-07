// Package grpcserver hosts the gRPC server for Flo, registering Health and
// Streams services and delegating to the shared services layer.
//
// Example:
//
//	rt, _ := runtime.Open(runtime.Options{DataDir: "./data", Fsync: pebblestore.FsyncModeAlways, Config: config.Default()})
//	s := grpcserver.New(rt)
//	ctx, cancel := context.WithCancel(context.Background())
//	defer cancel()
//	_ = s.ListenAndServe(ctx, ":50051")
package grpcserver
