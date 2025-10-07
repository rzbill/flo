package log

import (
	"context"
	"log/slog"
	"runtime"
	"strconv"
)

// bridgeHandler is a slog.Handler that routes records through our existing
// formatter/hooks/outputs pipeline.
type bridgeHandler struct {
	logger     *BaseLogger
	attrs      []slog.Attr
	group      string
	redactions map[string]struct{}
	sampler    *sampler
}

func newBridgeHandler(logger *BaseLogger) *bridgeHandler {
	return &bridgeHandler{logger: logger}
}

// Enabled gates by the BaseLogger level.
func (h *bridgeHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.logger.level <= fromSlogLevel(level)
}

// Handle converts the slog record to our Entry and writes it using the logger's
// formatter, hooks, and outputs.
func (h *bridgeHandler) Handle(_ context.Context, r slog.Record) error {
	// Merge base attrs and record attrs into Fields
	fields := Fields{}
	// Base attrs from handler
	for i := range h.attrs {
		a := h.attrs[i]
		fields[a.Key] = a.Value.Any()
	}
	// Record attrs
	r.Attrs(func(a slog.Attr) bool {
		// Optional redaction
		if h.redactions != nil {
			if _, ok := h.redactions[a.Key]; ok {
				fields[a.Key] = "[REDACTED]"
				return true
			}
		}
		fields[a.Key] = a.Value.Any()
		return true
	})

	// Optional sampling: if sampler decides to drop, stop here
	if h.sampler != nil && !h.sampler.allow(r.Level, r.Message) {
		return nil
	}

	// Determine caller from slog.Record's PC when present
	caller := ""
	if pc := r.PC; pc != 0 {
		if fn := runtime.FuncForPC(pc); fn != nil {
			file, line := fn.FileLine(pc)
			caller = file + ":" + itoa(line)
		}
	} else {
		// Fallback depth approximated for calls through BaseLogger
		if _, file, line, ok := runtime.Caller(5); ok {
			caller = file + ":" + itoa(line)
		}
	}

	// Build Entry
	entry := &Entry{
		Level:     fromSlogLevel(r.Level),
		Message:   r.Message,
		Fields:    fields,
		Timestamp: r.Time,
		Caller:    caller,
		Error:     nil,
	}

	// Hook system removed; use handler wrappers instead.

	// Format and write
	formatted, err := h.logger.formatter.Format(entry)
	if err != nil {
		return err
	}
	for _, out := range h.logger.outputs {
		_ = out.Write(entry, formatted)
	}
	return nil
}

// WithAttrs returns a copy of the handler with additional base attributes.
func (h *bridgeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	nh := *h
	if len(attrs) > 0 {
		nh.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	}
	return &nh
}

// WithGroup returns a copy of the handler; grouping is stored but otherwise
// not used by our pipeline.
func (h *bridgeHandler) WithGroup(name string) slog.Handler {
	nh := *h
	nh.group = name
	return &nh
}

// withRedactions returns a copy of the handler that redacts the provided keys.
func (h *bridgeHandler) withRedactions(keys []string) *bridgeHandler {
	if len(keys) == 0 {
		return h
	}
	nh := *h
	nh.redactions = make(map[string]struct{}, len(keys))
	for _, k := range keys {
		nh.redactions[k] = struct{}{}
	}
	return &nh
}

// withSampler returns a copy of the handler with a sampling policy.
func (h *bridgeHandler) withSampler(initial, thereafter int) *bridgeHandler {
	if thereafter <= 0 {
		return h
	}
	nh := *h
	nh.sampler = newSampler(initial, thereafter)
	return &nh
}

// sampler implements simple per-message sampling.
type sampler struct {
	initial    uint64
	thereafter uint64
	counts     map[string]uint64
}

func newSampler(initial, thereafter int) *sampler {
	if initial < 0 {
		initial = 0
	}
	if thereafter <= 0 {
		thereafter = 1
	}
	return &sampler{
		initial:    uint64(initial),
		thereafter: uint64(thereafter),
		counts:     make(map[string]uint64),
	}
}

func (s *sampler) allow(level slog.Level, message string) bool {
	key := strconv.Itoa(int(level)) + ":" + message
	n := s.counts[key]
	s.counts[key] = n + 1
	if n < s.initial {
		return true
	}
	return (n-s.initial)%s.thereafter == 0
}

// Helper: map our Level to slog.Level
func toSlogLevel(level Level) slog.Level {
	switch level {
	case DebugLevel:
		return slog.LevelDebug
	case InfoLevel:
		return slog.LevelInfo
	case WarnLevel:
		return slog.LevelWarn
	case ErrorLevel, FatalLevel:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Helper: map slog.Level to our Level
func fromSlogLevel(level slog.Level) Level {
	switch {
	case level <= slog.LevelDebug:
		return DebugLevel
	case level == slog.LevelInfo:
		return InfoLevel
	case level == slog.LevelWarn:
		return WarnLevel
	default:
		return ErrorLevel
	}
}

// Helper: convert map fields to slog attrs
func attrsFromMap(m Fields) []slog.Attr {
	if len(m) == 0 {
		return nil
	}
	attrs := make([]slog.Attr, 0, len(m))
	for k, v := range m {
		attrs = append(attrs, slog.Any(k, v))
	}
	return attrs
}

// Helper: convert Field slice to slog attrs
func attrsFromFieldSlice(fields []Field) []slog.Attr {
	if len(fields) == 0 {
		return nil
	}
	attrs := make([]slog.Attr, 0, len(fields))
	for _, f := range fields {
		attrs = append(attrs, slog.Any(f.Key, f.Value))
	}
	return attrs
}

// argsToAttrs converts key-value variadic args (k1, v1, k2, v2, ...) to slog.Attr.
func argsToAttrs(args []interface{}) []slog.Attr {
	if len(args) == 0 {
		return nil
	}
	attrs := make([]slog.Attr, 0, len(args)/2+1)
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			if key, ok := args[i].(string); ok {
				attrs = append(attrs, slog.Any(key, args[i+1]))
			} else {
				attrs = append(attrs, slog.Any("arg"+strconv.Itoa(i), args[i+1]))
			}
		} else {
			attrs = append(attrs, slog.Any("arg"+strconv.Itoa(i), args[i]))
		}
	}
	return attrs
}

// attrsToAny converts []slog.Attr to []any for slog.Logger.With.
func attrsToAny(attrs []slog.Attr) []any {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]any, len(attrs))
	for i := range attrs {
		out[i] = attrs[i]
	}
	return out
}

// itoa is a small fast int to string for non-negative numbers.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	// Max 20 digits for int64, int is enough
	var buf [20]byte
	bp := len(buf)
	for i > 0 {
		bp--
		buf[bp] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[bp:])
}
