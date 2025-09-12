package serverrun

import (
	"context"
	"log"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/runtime"
	grpcserver "github.com/rzbill/flo/internal/server/grpc"
	httpserver "github.com/rzbill/flo/internal/server/http"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

type Options struct {
	DataDir  string
	GRPCAddr string
	HTTPAddr string
	Fsync    pebblestore.FsyncMode
	Config   cfgpkg.Config
}

// Run starts gRPC and HTTP servers and blocks until ctx is cancelled.
func Run(ctx context.Context, opts Options) error {
	rt, err := runtime.Open(runtime.Options{DataDir: opts.DataDir, Fsync: opts.Fsync, Config: opts.Config})
	if err != nil {
		return err
	}
	defer rt.Close()

	gsrv := grpcserver.New(rt)
	go func() {
		if err := gsrv.ListenAndServe(ctx, opts.GRPCAddr); err != nil && ctx.Err() == nil {
			log.Printf("grpc error: %v", err)
		}
	}()

	hsrv := httpserver.New(rt)
	go func() {
		if err := hsrv.ListenAndServe(ctx, opts.HTTPAddr); err != nil && ctx.Err() == nil {
			log.Printf("http error: %v", err)
		}
	}()

	<-ctx.Done()
	return nil
}
