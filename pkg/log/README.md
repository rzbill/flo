# Go Structured Logging (pkg/log)

A small, fast, structured logging library for Go with typed fields, flexible formatting, and pluggable outputs. Internally it uses Go's slog APIs via a custom handler, so you get modern patterns with a stable, ergonomic facade.

## Key Features

- **Structured logging**: Log entries are key-value pairs with a simple field API
- **Typed fields**: Helpers like `Str`, `Int`, `Bool`, `Err`, `Time`, `Duration`, `Any`
- **Log levels**: `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`
- **Formats**: JSON and human-friendly text (with optional colors)
- **Outputs**: Console and file (with rotation); easily extendable
- **Privacy and performance**: Redaction and sampling supported via configuration
- **Interop**: Redirect standard `log` output or adapt to `*log.Logger`

## Quick Start

```go
import (
    "time"
    log "github.com/rzbill/flo/pkg/log"
)

logger := log.NewLogger(
    log.WithLevel(log.InfoLevel),
    log.WithFormatter(&log.TextFormatter{DisableColors: false}),
    log.WithOutput(log.NewConsoleOutput()),
)

logger.Info("server started", log.Int("port", 8080))
```

### Component-specific loggers

```go
svc := logger.With(log.Component("api"), log.Str("ns", "default"))
svc.Info("route registered", log.Str("path", "/users"))
```

### Context propagation

`WithContext` captures known IDs from `context.Context` using shared keys. Add values to the context using the provided constants, then merge:

```go
ctx := context.WithValue(r.Context(), log.RequestIDKey, reqID)
child := logger.WithContext(ctx)
child.Info("request started", log.Str("method", r.Method))
```

## Configuration

Build a logger from a declarative config, including outputs, redaction, and sampling:

```go
cfg := &log.Config{
    Level:  "info",
    Format: "json", // or "text"
    Outputs: []log.OutputConfig{
        {
            Type: "file",
            Options: map[string]interface{}{
                "filename":    "/var/log/app/service.log",
                "max_size":    "100MB",
                "max_backups": 5,
            },
        },
        { Type: "console", Options: map[string]interface{}{"error_to_stderr": true} },
    },
    RedactedFields: []string{"password", "api_key", "token"},
    Sampling: &log.SamplingConfig{Initial: 10, Thereafter: 100},
}

logger, err := log.ApplyConfig(cfg)
if err != nil {
    panic(err)
}
```

## Field Helpers

```go
log.Str("key", "value")
log.Int("key", 123)
log.Int64("key", 123)
log.Float64("key", 1.23)
log.Bool("key", true)
log.Err(err)
log.Time("ts", time.Now())
log.Duration("took", time.Since(start))
log.Any("key", value)
log.F("key", value) // alias of Any

// Contextual helpers
log.Component("component-name")
log.RequestID("req-id")
log.TraceID("trace-id")
log.SpanID("span-id")
```

## Interop

- Standard library redirect:

```go
restore := log.RedirectStdLog(logger)
defer restore()
```

- Adapt to `*log.Logger`:

```go
std := log.ToStdLogger(logger, "", 0)
std.Println("hello")
```

## Notes

- No global default logger is provided. Construct a `Logger` and pass it explicitly.
- Redaction and sampling are applied via internal slog handler wrappers configured by `Config`.

## Tips

- Use appropriate levels (`DEBUG` for development; `INFO`+ for production)
- Prefer the typed field API for consistency and queryability
- Use `RedactedFields` for sensitive data and `Sampling` for high-volume logs
- Close file outputs gracefully on shutdown