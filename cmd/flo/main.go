package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	clientcmd "github.com/rzbill/flo/internal/cmd/client"
	serverrun "github.com/rzbill/flo/internal/cmd/server"
	cfgpkg "github.com/rzbill/flo/internal/config"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
	logpkg "github.com/rzbill/flo/pkg/log"
	"github.com/spf13/cobra"
)

func main() {
	// initialize logger for CLI
	// Respect FLO_LOG_LEVEL for both CLI and server start output
	level := os.Getenv("FLO_LOG_LEVEL")
	parsed, err := logpkg.ParseLevel(level)
	if err != nil || level == "" {
		parsed = logpkg.InfoLevel
	}
	logger := logpkg.NewLogger(
		logpkg.WithLevel(parsed),
		logpkg.WithFormatter(&logpkg.TextFormatter{}),
		logpkg.WithOutput(logpkg.NewConsoleOutput()),
	)

	// Redirect standard library logs (used by Pebble) to our logger
	logpkg.RedirectStdLog(logger)

	rootCmd := &cobra.Command{
		Use:   "flo",
		Short: "Flo runtime CLI",
		Long:  "Flo is a single-binary runtime. This CLI manages the server and basic operations.",
	}

	// init
	initCmd := &cobra.Command{
		Use:   "init",
		Short: "Initialize flo",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("flo init: nothing to do yet")
		},
	}
	rootCmd.AddCommand(initCmd)

	// server start
	serverCmd := &cobra.Command{Use: "server", Short: "Server commands"}
	serverStartCmd := &cobra.Command{
		Use:     "start",
		Short:   "Start flo server (gRPC and HTTP)",
		Aliases: []string{"run"},
		RunE: func(cmd *cobra.Command, args []string) error {
			dataDir, _ := cmd.Flags().GetString("data-dir")
			grpcAddr, _ := cmd.Flags().GetString("grpc")
			httpAddr, _ := cmd.Flags().GetString("http")
			uiAddr, _ := cmd.Flags().GetString("ui")
			uiBase, _ := cmd.Flags().GetString("ui-base")
			fsyncMode, _ := cmd.Flags().GetString("fsync")
			fsyncIntervalMs, _ := cmd.Flags().GetInt("fsync-interval-ms")
			logLevel, _ := cmd.Flags().GetString("log-level")
			logFormat, _ := cmd.Flags().GetString("log-format")
			subFlushMs, _ := cmd.Flags().GetInt("sub-flush-ms")
			subBuf, _ := cmd.Flags().GetInt("sub-buf")

			mode := pebblestore.FsyncModeAlways
			switch fsyncMode {
			case "never":
				mode = pebblestore.FsyncModeNever
			case "interval":
				mode = pebblestore.FsyncModeInterval
			case "always":
				mode = pebblestore.FsyncModeAlways
			default:
				return fmt.Errorf("invalid --fsync; use always|interval|never")
			}

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			cfg := cfgpkg.Default()
			if logLevel != "" {
				_ = os.Setenv("FLO_LOG_LEVEL", logLevel)
			}
			if logFormat != "" {
				_ = os.Setenv("FLO_LOG_FORMAT", logFormat)
			}
			if subFlushMs > 0 {
				_ = os.Setenv("FLO_SUB_FLUSH_MS", fmt.Sprintf("%d", subFlushMs))
			}
			if subBuf > 0 {
				_ = os.Setenv("FLO_SUB_BUF", fmt.Sprintf("%d", subBuf))
			}
			if err := serverrun.Run(ctx, serverrun.Options{
				DataDir:       dataDir,
				GRPCAddr:      grpcAddr,
				HTTPAddr:      httpAddr,
				UIAddr:        uiAddr,
				UIBase:        uiBase,
				Fsync:         mode,
				FsyncInterval: time.Duration(fsyncIntervalMs) * time.Millisecond,
				Config:        cfg,
			}); err != nil {
				return fmt.Errorf("server error: %w", err)
			}
			// brief delay to allow logs flush
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}
	serverStartCmd.Flags().String("data-dir", "", "Data directory (if not specified, uses OS-specific application data directory)")
	serverStartCmd.Flags().String("grpc", ":50051", "gRPC listen address")
	serverStartCmd.Flags().String("http", ":8080", "HTTP listen address (API + UI if --ui not set)")
	serverStartCmd.Flags().String("ui", os.Getenv("FLO_UI"), "UI listen address (optional, separate from --http)")
	serverStartCmd.Flags().String("ui-base", os.Getenv("FLO_UI_BASE"), "UI base path (default /ui; use / to serve at root)")
	serverStartCmd.Flags().String("fsync", "always", "Fsync mode: always|interval|never")
	serverStartCmd.Flags().Int("fsync-interval-ms", 5, "When --fsync=interval, group-commit window in ms (default 5)")
	serverStartCmd.Flags().String("log-level", os.Getenv("FLO_LOG_LEVEL"), "Log level: debug|info|warn|error")
	serverStartCmd.Flags().String("log-format", os.Getenv("FLO_LOG_FORMAT"), "Log format: text|json (default text)")
	serverStartCmd.Flags().Int("sub-flush-ms", func() int { v, _ := strconv.Atoi(os.Getenv("FLO_SUB_FLUSH_MS")); return v }(), "Subscribe flush window in ms (default 0)")
	serverStartCmd.Flags().Int("sub-buf", func() int {
		v, _ := strconv.Atoi(os.Getenv("FLO_SUB_BUF"))
		if v == 0 {
			return 1024
		}
		return v
	}(), "Subscribe buffer size per stream (default 1024)")
	serverCmd.AddCommand(serverStartCmd)
	rootCmd.AddCommand(serverCmd)

	// ns create
	nsCmd := &cobra.Command{Use: "namespace", Short: "Namespace operations"}
	nsCreateCmd := &cobra.Command{
		Use:   "create",
		Short: "Create namespace",
		RunE: func(cmd *cobra.Command, args []string) error {
			name, _ := cmd.Flags().GetString("name")
			body := map[string]string{"namespace": name}
			b, _ := json.Marshal(body)
			resp, err := http.Post(apiURL()+"/v1/ns/create", "application/json", bytes.NewReader(b))
			if err != nil {
				return err
			}
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			fmt.Println("status:", resp.Status)
			return nil
		},
	}
	nsCreateCmd.Flags().String("name", "default", "Namespace name")
	nsCmd.AddCommand(nsCreateCmd)
	rootCmd.AddCommand(nsCmd)

	// stream commands (migrated into internal/cmd/client)
	streamCmd := clientcmd.NewStreamCommand(apiURL)
	rootCmd.AddCommand(streamCmd)

	// workqueue commands
	workqueueCmd := clientcmd.NewWorkQueueCommand()
	rootCmd.AddCommand(workqueueCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func apiURL() string {
	if v := os.Getenv("FLO_HTTP"); v != "" {
		return v
	}
	return "http://127.0.0.1:8080"
}
