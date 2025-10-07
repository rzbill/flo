package controllers

import (
	"context"
	"encoding/json"
	"net/http"

	streamsvc "github.com/rzbill/flo/internal/services/streams"
)

// sseSink implements the SubscribeSink interface for Server-Sent Events.
//
// It formats stream messages as SSE data events for real-time streaming
// to web clients.
type sseSink struct {
	w http.ResponseWriter
	r *http.Request
}

// Send formats and sends a stream message as an SSE data event.
//
// The message is JSON-encoded and sent with the "data: " prefix followed by
// two newlines as required by the SSE specification.
func (s sseSink) Send(it streamsvc.SubscribeItem) error {
	b, _ := json.Marshal(map[string]any{"id": it.ID, "payload": it.Payload})
	if _, err := s.w.Write([]byte("data: ")); err != nil {
		return err
	}
	if _, err := s.w.Write(b); err != nil {
		return err
	}
	if _, err := s.w.Write([]byte("\n\n")); err != nil {
		return err
	}
	return nil
}

// Context returns the request context for cancellation.
func (s sseSink) Context() context.Context {
	return s.r.Context()
}

// Flush flushes the HTTP response writer if it supports flushing.
//
// This ensures that SSE events are immediately sent to the client.
func (s sseSink) Flush() error {
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}
