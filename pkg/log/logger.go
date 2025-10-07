// Package log provides a structured logging system for Rune services.
package log

import (
	"context"
	"log/slog"
	"time"
)

// Level represents the severity level of a log message.
type Level int

// Log levels
const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
	FatalLevel
)

// String returns the string representation of the log level.
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// Fields is a map of field names to values.
type Fields map[string]interface{}

// Context keys for propagating logging context
const (
	RequestIDKey = "request_id"
	TraceIDKey   = "trace_id"
	SpanIDKey    = "span_id"
	ComponentKey = "component"
	OperationKey = "operation"
)

// Entry represents a single log entry.
type Entry struct {
	Level     Level
	Message   string
	Fields    Fields
	Timestamp time.Time
	Caller    string
	Error     error
}

// Logger defines the core logging interface for Rune components.
type Logger interface {
	// Standard logging methods with structured context (Field-based API)
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)
	Fatal(msg string, fields ...Field)

	// Standard logging methods with key-value pairs (for backward compatibility)
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
	Fatalf(msg string, args ...interface{})

	// Field creation methods (for backward compatibility)
	WithField(key string, value interface{}) Logger
	WithFields(fields Fields) Logger
	WithError(err error) Logger

	// With adds multiple fields to the logger (for new Field-based API)
	With(fields ...Field) Logger

	// WithContext adds request context to the Logger
	WithContext(ctx context.Context) Logger

	// WithComponent tags logs with a component name
	WithComponent(component string) Logger

	// SetLevel sets the minimum log level
	SetLevel(level Level)

	// GetLevel returns the current minimum log level
	GetLevel() Level
}

// Formatter defines the interface for formatting log entries.
type Formatter interface {
	Format(entry *Entry) ([]byte, error)
}

// Output defines the interface for log outputs.
type Output interface {
	Write(entry *Entry, formattedEntry []byte) error
	Close() error
}

// LoggerOption is a function that configures a logger.
type LoggerOption func(*BaseLogger)

// BaseLogger implements the Logger interface.
type BaseLogger struct {
	level      Level
	fields     Fields
	formatter  Formatter
	outputs    []Output
	slogLogger *slog.Logger
}

// Hooks are no longer used; prefer slog handler wrappers for cross-cutting concerns.

// ContextExtractor extracts logging context from a context.Context.
func ContextExtractor(ctx context.Context) Fields {
	if ctx == nil {
		return Fields{}
	}

	fields := Fields{}

	// Extract standard context values
	if v := ctx.Value(RequestIDKey); v != nil {
		fields[RequestIDKey] = v
	}
	if v := ctx.Value(TraceIDKey); v != nil {
		fields[TraceIDKey] = v
	}
	if v := ctx.Value(SpanIDKey); v != nil {
		fields[SpanIDKey] = v
	}
	if v := ctx.Value(ComponentKey); v != nil {
		fields[ComponentKey] = v
	}
	if v := ctx.Value(OperationKey); v != nil {
		fields[OperationKey] = v
	}

	// Extract custom field keys (injected by ContextInjector)
	// We need to scan all context keys to find our custom fieldKeyType keys
	// This is a limitation of Go's context package - we can't enumerate all keys
	// For now, we'll rely on the standard keys above and any custom extraction logic

	return fields
}

// ContextInjector removed; prefer passing fields with Logger.With().
// FromContext removed; pass Logger explicitly via dependency injection.
// Deprecated context helpers removed.
// Global default logger removed; construct and pass Logger instances explicitly.
// Global helper functions removed; prefer using a concrete Logger instance.
// NewLogger creates a new logger with the given options.
func NewLogger(options ...LoggerOption) Logger {
	logger := &BaseLogger{
		level:     InfoLevel,
		fields:    Fields{},
		formatter: &JSONFormatter{},
		outputs:   []Output{},
	}

	// Apply options
	for _, option := range options {
		option(logger)
	}

	// Add default output if none specified
	if len(logger.outputs) == 0 {
		logger.outputs = append(logger.outputs, &ConsoleOutput{})
	}

	// Initialize slog with our bridge handler
	logger.slogLogger = slog.New(newBridgeHandler(logger))

	return logger
}

// WithLevel sets the minimum log level.
func WithLevel(level Level) LoggerOption {
	return func(l *BaseLogger) {
		l.level = level
	}
}

// WithFormatter sets the log formatter.
func WithFormatter(formatter Formatter) LoggerOption {
	return func(l *BaseLogger) {
		l.formatter = formatter
	}
}

// WithOutput adds an output to the logger.
func WithOutput(output Output) LoggerOption {
	return func(l *BaseLogger) {
		l.outputs = append(l.outputs, output)
	}
}
