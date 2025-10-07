package serverrun

import (
	"context"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/runtime"
	grpcserver "github.com/rzbill/flo/internal/server/grpc"
	httpserver "github.com/rzbill/flo/internal/server/http"
	streamsvc "github.com/rzbill/flo/internal/services/streams"
	workqueuesvc "github.com/rzbill/flo/internal/services/workqueues"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
	logpkg "github.com/rzbill/flo/pkg/log"
)

func getenvDefault(key, def string) string {
	if v := func() string { return getenv(key) }(); v != "" {
		return v
	}
	return def
}

// small wrapper to allow testing; replaced by os.Getenv at build time
var getenv = func(key string) string { return os.Getenv(key) }

type Options struct {
	DataDir       string
	GRPCAddr      string
	HTTPAddr      string
	UIAddr        string
	UIBase        string
	Fsync         pebblestore.FsyncMode
	FsyncInterval time.Duration
	Config        cfgpkg.Config
}

// Run starts gRPC and HTTP servers and blocks until ctx is cancelled.
func Run(ctx context.Context, opts Options) error {
	// Be robust to callers that don't pass a signal-aware context
	// or if signal delivery needs to be observed here. We layer a
	// local signal context over the provided one.
	sctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if opts.DataDir == "" {
		opts.DataDir = cfgpkg.DefaultDataDir()
	}
	storeDir := filepath.Join(opts.DataDir, "store")
	rt, err := runtime.Open(runtime.Options{DataDir: storeDir, Fsync: opts.Fsync, FsyncInterval: opts.FsyncInterval, Config: opts.Config})
	if err != nil {
		return err
	}
	defer rt.Close()

	// Build process-wide logger using env/ApplyConfig; defaults: level=info, format=text
	cfg := &logpkg.Config{
		Level:  getenvDefault("FLO_LOG_LEVEL", "info"),
		Format: getenvDefault("FLO_LOG_FORMAT", "text"),
	}
	procLogger, err := logpkg.ApplyConfig(cfg)
	if err != nil {
		// Fallback to a sane default
		lvl := logpkg.InfoLevel
		if l, e := logpkg.ParseLevel(cfg.Level); e == nil {
			lvl = l
		}
		procLogger = logpkg.NewLogger(logpkg.WithLevel(lvl), logpkg.WithFormatter(&logpkg.TextFormatter{}))
	}

	// Redirect stdlib logs (e.g., Pebble) to our logger
	logpkg.RedirectStdLog(procLogger)

	// Log startup with unified logger/format and subscriber tunables
	procLogger.Info("Starting FLO server",
		logpkg.Str("grpc", opts.GRPCAddr),
		logpkg.Str("http", opts.HTTPAddr),
		logpkg.Str("ui", opts.UIAddr),
		logpkg.Str("ui_base", opts.UIBase),
		logpkg.Str("level", cfg.Level),
		logpkg.Str("format", cfg.Format),
		logpkg.Str("sub_flush_ms", getenvDefault("FLO_SUB_FLUSH_MS", "0")),
		logpkg.Str("sub_buf", getenvDefault("FLO_SUB_BUF", "1024")),
	)

	// Create shared service instances for both transports
	streamsSvc := streamsvc.NewWithLogger(rt, procLogger.With(logpkg.Component("streams")))
	workQueuesSvc := workqueuesvc.NewWithLogger(rt, procLogger.With(logpkg.Component("workqueues")))
	gsrv := grpcserver.NewWithService(rt, streamsSvc, workQueuesSvc, procLogger)
	hsrv := httpserver.NewWithService(rt, streamsSvc, workQueuesSvc, procLogger)
	hsrv.SetUIBase(opts.UIBase)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := gsrv.ListenAndServe(sctx, opts.GRPCAddr); err != nil && sctx.Err() == nil {
			log.Printf("grpc error: %v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := hsrv.ListenAndServe(sctx, opts.HTTPAddr); err != nil && sctx.Err() == nil {
			log.Printf("http error: %v", err)
		}
	}()

	var uiSrv *httpserver.Server
	if opts.UIAddr != "" && opts.UIAddr != opts.HTTPAddr {
		uiSrv = httpserver.NewUIOnly(rt)
		uiSrv.SetUIBase(opts.UIBase)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := uiSrv.ListenAndServe(sctx, opts.UIAddr); err != nil && sctx.Err() == nil {
				log.Printf("ui http error: %v", err)
			}
		}()
	}

	<-sctx.Done()
	// Initiate graceful shutdown of servers before closing the runtime/DB to avoid races.
	gsrv.Close()
	hsrv.Close()
	if uiSrv != nil {
		uiSrv.Close()
	}
	wg.Wait()
	return nil
}
