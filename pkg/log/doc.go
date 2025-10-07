// Package log provides Flo's structured logging facade and utilities.
//
// # Overview
//
// The package exposes a small Logger interface with leveled methods and a
// simple Field type for structured context. Internally it is backed by Go's
// standard library slog via a custom handler that preserves our existing
// formatter/hooks/outputs pipeline. This allows adoption of the slog ecosystem
// while keeping consistent output and behavior across the codebase.
//
// Quick start
//
//	l := log.NewLogger(
//	    log.WithLevel(log.InfoLevel),
//	    log.WithFormatter(&log.TextFormatter{}),
//	    log.WithOutput(log.NewConsoleOutput()),
//	)
//	l = l.With(log.Component("server"), log.Str("ns", "default"))
//	l.Info("server started", log.Int("port", 8080))
//
// # Configuration
//
// Use ApplyConfig to build a logger from a declarative Config, supporting JSON
// or text formatting and multiple outputs (console, file, null). Hooks allow
// redaction and sampling.
//
// # Interop
//
// To integrate with libraries expecting *log.Logger, use ToStdLogger or
// RedirectStdLog. To interop with slog directly, obtain slog.Logger via
// the facade using GetDefaultLogger().(*BaseLogger).slogLogger when necessary,
// though most code should remain against this facade.
package log
