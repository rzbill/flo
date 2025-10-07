package streamsvc

import (
	"encoding/binary"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
)

// celFilter wraps a compiled CEL program and provides a common evaluator used by
// subscribe streaming and historical search. When disabled, Eval always returns true.
type celFilter struct {
	prog    cel.Program
	enabled bool
}

func newCELFilter(expr string) (celFilter, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return celFilter{enabled: false}, nil
	}
	env, err := cel.NewEnv(
		cel.Variable("partition", cel.IntType),
		cel.Variable("sequence", cel.IntType),
		cel.Variable("ts_ms", cel.IntType),
		cel.Variable("size", cel.IntType),
		cel.Variable("text", cel.StringType),
		// Expose parsed JSON payload (map/list/values) for field filtering
		cel.Variable("json", cel.DynType),
		// Expose user headers map (from record header JSON)
		cel.Variable("headers", cel.MapType(cel.StringType, cel.StringType)),
		// Current time in ms for windowed filters
		cel.Variable("now_ms", cel.IntType),
	)
	if err != nil {
		return celFilter{}, err
	}
	ast, iss := env.Parse(expr)
	if iss != nil && iss.Err() != nil {
		return celFilter{}, iss.Err()
	}
	checked, iss2 := env.Check(ast)
	if iss2 != nil && iss2.Err() != nil {
		return celFilter{}, iss2.Err()
	}
	prog, err := env.Program(checked)
	if err != nil {
		return celFilter{}, err
	}
	return celFilter{prog: prog, enabled: true}, nil
}

// Eval evaluates the compiled expression against a message. When disabled, returns true.
func (f celFilter) Eval(partition int, sequence uint64, header []byte, payload []byte) bool {
	if !f.enabled {
		return true
	}
	var ts int64
	if len(header) >= 8 {
		ts = int64(binary.BigEndian.Uint64(header[:8]))
	}
	var jsonObj any
	_ = json.Unmarshal(payload, &jsonObj)
	// Decode headers JSON from header bytes after the first 8-byte timestamp
	headers := map[string]string{}
	if len(header) > 8 {
		var hm map[string]string
		if err := json.Unmarshal(header[8:], &hm); err == nil {
			headers = hm
		}
	}
	out, _, err := f.prog.Eval(map[string]any{
		"partition": int64(partition),
		"sequence":  int64(sequence),
		"ts_ms":     ts,
		"size":      int64(len(payload)),
		"text":      string(payload),
		"json":      jsonObj,
		"headers":   headers,
		"now_ms":    time.Now().UnixMilli(),
	})
	if err != nil {
		return false
	}
	b, ok := out.Value().(bool)
	return ok && b
}
