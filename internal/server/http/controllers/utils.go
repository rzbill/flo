package controllers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// Helper functions for common HTTP responses

// writeError writes an error response with the given status code and message.
func writeError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// writeJSON writes a JSON response with the given data.
func writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(data)
}

// writeNoContent writes a 204 No Content response.
func writeNoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

// writeCreated writes a 201 Created response.
func writeCreated(w http.ResponseWriter) {
	w.WriteHeader(http.StatusCreated)
}

// decodeID converts various ID formats to []byte.
//
// It supports string (base64 or raw), []byte, and []any (byte array) formats.
func decodeID(v any) []byte {
	switch t := v.(type) {
	case string:
		b, err := base64.StdEncoding.DecodeString(t)
		if err == nil {
			return b
		}
		return []byte(t)
	case []byte:
		return t
	case []any:
		// treat as byte array numbers
		buf := make([]byte, 0, len(t))
		for _, e := range t {
			if f, ok := e.(float64); ok {
				buf = append(buf, byte(f))
			}
		}
		return buf
	default:
		return nil
	}
}

// parseLimit parses a limit string and returns a valid limit value.
//
// Returns 0 for empty strings or invalid values.
func parseLimit(limitStr string) int {
	if limitStr == "" {
		return 0
	}
	if limit, err := strconv.Atoi(limitStr); err == nil && limit > 0 {
		return limit
	}
	return 0
}

// parseTimestamp parses a timestamp string and returns Unix milliseconds.
//
// Supports both RFC3339 format and raw millisecond timestamps.
// Returns 0 for empty strings or invalid values.
func parseTimestamp(ts string) int64 {
	if ts == "" {
		return 0
	}
	// Try parsing as milliseconds first
	if ms, err := strconv.ParseInt(ts, 10, 64); err == nil {
		return ms
	}
	// Try parsing as RFC3339
	if t, err := time.Parse(time.RFC3339, ts); err == nil {
		return t.UnixMilli()
	}
	return 0
}

// parseBool parses a boolean string and returns the boolean value.
//
// Returns true for "true" or "1", false otherwise.
func parseBool(s string) bool {
	return s == "true" || s == "1"
}
