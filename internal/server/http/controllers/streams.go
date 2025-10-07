package controllers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rzbill/flo/internal/runtime"
	streamsvc "github.com/rzbill/flo/internal/services/streams"
)

// StreamsController handles all stream-related HTTP endpoints.
//
// It provides a RESTful interface to the Flo Streams service, including
// stream management, message publishing/consumption, consumer groups,
// retry/DLQ operations, and real-time streaming via Server-Sent Events.
type StreamsController struct {
	rt *runtime.Runtime
	st *streamsvc.Service
}

// NewStreamsController creates a new streams controller.
//
// The controller requires both a runtime instance for configuration and
// a streams service for business logic operations.
func NewStreamsController(rt *runtime.Runtime, svc *streamsvc.Service) *StreamsController {
	return &StreamsController{
		rt: rt,
		st: svc,
	}
}

// RegisterRoutes registers all stream-related routes with the given mux.
//
// This method sets up all HTTP endpoints for stream operations including:
// - Stream management (list, create, delete)
// - Message operations (publish, list, search, get by ID)
// - Streaming (subscribe, tail)
// - Consumer group operations (ack, nack)
// - Statistics and monitoring
// - Retry and DLQ management
func (c *StreamsController) RegisterRoutes(mux *http.ServeMux) {
	// Stream management
	mux.HandleFunc("/v1/streams", c.handleListStreams)
	mux.HandleFunc("/v1/streams/create", c.handleCreate)
	mux.HandleFunc("/v1/streams/publish", c.handlePublish)
	mux.HandleFunc("/v1/streams/flush", c.handleFlushStream)
	mux.HandleFunc("/v1/streams/delete", c.handleDeleteMessage)

	// Message operations
	mux.HandleFunc("/v1/streams/messages", c.handleListMessages)
	mux.HandleFunc("/v1/streams/message/", c.handleGetMessage)
	mux.HandleFunc("/v1/streams/search", c.handleSearch)

	// Streaming
	mux.HandleFunc("/v1/streams/subscribe", c.handleSubscribeSSE)
	mux.HandleFunc("/v1/streams/tail", c.handleTailSSE)

	// Consumer group operations
	mux.HandleFunc("/v1/streams/ack", c.handleAck)
	mux.HandleFunc("/v1/streams/nack", c.handleNack)

	// Statistics and monitoring
	mux.HandleFunc("/v1/streams/metrics", c.handleStreamMetrics)
	mux.HandleFunc("/v1/streams/stats", c.handleStreamStats)
	mux.HandleFunc("/v1/streams/groups", c.handleStreamGroups)

	// Retry and DLQ
	mux.HandleFunc("/v1/streams/retry", c.handleListRetryMessages)
	mux.HandleFunc("/v1/streams/dlq", c.handleListDLQMessages)
	mux.HandleFunc("/v1/streams/retry-dlq-stats", c.handleRetryDLQStats)
	mux.HandleFunc("/v1/streams/message-delivery", c.handleMessageDelivery)
}

// handleListStreams lists all streams in a namespace.
func (c *StreamsController) handleListStreams(w http.ResponseWriter, r *http.Request) {
	ns := r.URL.Query().Get("namespace")
	list, err := c.st.ListStreams(r.Context(), ns)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to list streams")
		return
	}
	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}
	writeJSON(w, map[string]any{"namespace": ns, "streams": list})
}

// handleCreate creates a new stream.
func (c *StreamsController) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if err := c.st.CreateStream(r.Context(), req.Namespace, req.Stream, req.Partitions); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to create stream")
		return
	}
	writeCreated(w)
}

// handlePublish publishes a message to a stream.
func (c *StreamsController) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	start := time.Now()
	var req publishReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if _, err := c.st.EnsureNamespace(r.Context(), req.Namespace); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to ensure namespace")
		return
	}
	if _, err := c.st.Publish(r.Context(), req.Namespace, req.Stream, req.Payload, req.Headers, req.Key); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to publish message")
		return
	}
	// Expose basic timing for client-side debugging
	w.Header().Set("X-Publish-Latency-Ms", strconv.FormatInt(time.Since(start).Milliseconds(), 10))
	w.WriteHeader(http.StatusAccepted)
}

// handleFlushStream flushes all messages from a stream (or specific partition).
func (c *StreamsController) handleFlushStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	partStr := r.URL.Query().Get("partition")

	if st == "" {
		writeError(w, http.StatusBadRequest, "Stream parameter is required")
		return
	}

	var partition *uint32
	if partStr != "" {
		if v, err := strconv.Atoi(partStr); err == nil && v >= 0 {
			p := uint32(v)
			partition = &p
		} else {
			writeError(w, http.StatusBadRequest, "Invalid partition parameter")
			return
		}
	}

	deletedCount, err := c.st.FlushStream(r.Context(), ns, st, partition)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]any{
		"namespace":     ns,
		"stream":        st,
		"deleted_count": deletedCount,
		"partition":     partition,
	})
}

// handleDeleteMessage deletes a specific message by ID from a stream.
func (c *StreamsController) handleDeleteMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	messageIDStr := r.URL.Query().Get("message_id")

	if st == "" || messageIDStr == "" {
		writeError(w, http.StatusBadRequest, "Stream and message_id parameters are required")
		return
	}

	// Decode base64 message ID
	messageID, err := base64.StdEncoding.DecodeString(messageIDStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid message_id format")
		return
	}

	if err := c.st.DeleteMessage(r.Context(), ns, st, messageID); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeNoContent(w)
}

// handleAck acknowledges a message.
func (c *StreamsController) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	var req ackReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	id := decodeID(req.ID)
	if err := c.st.Ack(r.Context(), req.Namespace, req.Stream, req.Group, id); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to ack message")
		return
	}
	writeNoContent(w)
}

// handleNack negatively acknowledges a message.
func (c *StreamsController) handleNack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	var req nackReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	id := decodeID(req.ID)
	if err := c.st.Nack(r.Context(), req.Namespace, req.Stream, req.Group, id, req.Error); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to nack message")
		return
	}
	writeNoContent(w)
}

// handleListMessages lists messages from a stream partition.
func (c *StreamsController) handleListMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	partStr := r.URL.Query().Get("partition")
	startB64 := r.URL.Query().Get("start_token")
	limitStr := r.URL.Query().Get("limit")
	reverseStr := r.URL.Query().Get("reverse")

	var part uint32
	if partStr != "" {
		if v, err := strconv.Atoi(partStr); err == nil && v >= 0 {
			part = uint32(v)
		}
	}
	var start []byte
	if startB64 != "" {
		if b, err := base64.StdEncoding.DecodeString(startB64); err == nil {
			start = b
		}
	}
	limit := 100
	if limitStr != "" {
		if v, err := strconv.Atoi(limitStr); err == nil && v > 0 {
			limit = v
		}
	}
	reverse := parseBool(reverseStr)

	items, next, err := c.st.ListMessages(r.Context(), ns, st, part, start, limit, reverse)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to list messages")
		return
	}

	out := struct {
		Namespace string                 `json:"namespace"`
		Stream    string                 `json:"stream"`
		Partition uint32                 `json:"partition"`
		Items     []listMessagesRespItem `json:"items"`
		NextToken string                 `json:"next_token"`
	}{Namespace: ns, Stream: st, Partition: part}

	out.Items = make([]listMessagesRespItem, 0, len(items))
	for _, it := range items {
		out.Items = append(out.Items, listMessagesRespItem{
			ID:        it.ID,
			Payload:   it.Payload,
			Header:    it.Header,
			Seq:       it.Seq,
			Partition: it.Partition,
		})
	}
	if len(next) > 0 {
		out.NextToken = base64.StdEncoding.EncodeToString(next)
	}
	writeJSON(w, out)
}

// handleGetMessage gets a specific message by ID.
func (c *StreamsController) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	// Extract message ID from URL path: /v1/streams/message/{id}
	path := strings.TrimPrefix(r.URL.Path, "/v1/streams/message/")
	if path == "" {
		writeError(w, http.StatusBadRequest, "Message ID is required")
		return
	}

	// Decode base64 message ID
	id, err := base64.StdEncoding.DecodeString(path)
	if err != nil || len(id) != 8 {
		writeError(w, http.StatusBadRequest, "Invalid message ID format")
		return
	}

	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")

	if st == "" {
		writeError(w, http.StatusBadRequest, "Stream parameter is required")
		return
	}

	msg, err := c.st.GetMessageByID(r.Context(), ns, st, id)
	if err != nil {
		writeError(w, http.StatusNotFound, "Message not found")
		return
	}

	out := struct {
		ID        []byte `json:"id"`
		Payload   []byte `json:"payload"`
		Header    []byte `json:"header"`
		Partition uint32 `json:"partition"`
		Seq       uint64 `json:"seq"`
	}{
		ID:        msg.ID,
		Payload:   msg.Payload,
		Header:    msg.Header,
		Partition: msg.Partition,
		Seq:       msg.Seq,
	}

	writeJSON(w, out)
}

// handleSearch searches messages with various filters.
func (c *StreamsController) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	from := r.URL.Query().Get("from")
	at := r.URL.Query().Get("at")
	reverse := r.URL.Query().Get("reverse")
	limitStr := r.URL.Query().Get("limit")
	nextB64 := r.URL.Query().Get("next_token")
	filter := r.URL.Query().Get("filter")

	atMs := parseTimestamp(at)
	var nextTok []byte
	if nextB64 != "" {
		if b, err := base64.StdEncoding.DecodeString(nextB64); err == nil {
			nextTok = b
		}
	}
	lim := 100
	if v, err := strconv.Atoi(limitStr); err == nil && v > 0 && v <= 500 {
		lim = v
	}
	opts := streamsvc.SearchOptions{
		From:    from,
		AtMs:    atMs,
		Reverse: parseBool(reverse),
		Limit:   lim,
		Filter:  filter,
	}
	items, next, err := c.st.Search(r.Context(), ns, st, nextTok, opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to search messages")
		return
	}

	out := struct {
		Namespace string                 `json:"namespace"`
		Stream    string                 `json:"stream"`
		Items     []listMessagesRespItem `json:"items"`
		NextToken string                 `json:"next_token"`
	}{Namespace: ns, Stream: st}

	out.Items = make([]listMessagesRespItem, 0, len(items))
	for _, it := range items {
		out.Items = append(out.Items, listMessagesRespItem{
			ID:        it.ID,
			Payload:   it.Payload,
			Header:    it.Header,
			Seq:       it.Seq,
			Partition: it.Partition,
		})
	}
	if len(next) == 8 {
		out.NextToken = base64.StdEncoding.EncodeToString(next)
	}
	writeJSON(w, out)
}

// handleListRetryMessages lists retry messages for a stream and group.
func (c *StreamsController) handleListRetryMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	group := r.URL.Query().Get("group")
	limitStr := r.URL.Query().Get("limit")
	reverseStr := r.URL.Query().Get("reverse")
	startTokenStr := r.URL.Query().Get("start_token")

	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	reverse := parseBool(reverseStr)

	var startToken []byte
	if startTokenStr != "" {
		if b, err := base64.StdEncoding.DecodeString(startTokenStr); err == nil {
			startToken = b
		}
	}

	items, next, err := c.st.ListRetryMessages(r.Context(), ns, st, group, startToken, limit, reverse)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to JSON response
	response := map[string]any{
		"items":      items,
		"next_token": next,
	}

	writeJSON(w, response)
}

// handleListDLQMessages lists DLQ messages for a stream and group.
func (c *StreamsController) handleListDLQMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	group := r.URL.Query().Get("group")
	limitStr := r.URL.Query().Get("limit")
	reverseStr := r.URL.Query().Get("reverse")
	startTokenStr := r.URL.Query().Get("start_token")

	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	reverse := parseBool(reverseStr)

	var startToken []byte
	if startTokenStr != "" {
		if b, err := base64.StdEncoding.DecodeString(startTokenStr); err == nil {
			startToken = b
		}
	}

	items, next, err := c.st.ListDLQMessages(r.Context(), ns, st, group, startToken, limit, reverse)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Convert to JSON response
	response := map[string]any{
		"items":      items,
		"next_token": next,
	}

	writeJSON(w, response)
}

// handleRetryDLQStats returns retry and DLQ statistics.
func (c *StreamsController) handleRetryDLQStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ns := r.URL.Query().Get("namespace")
	stream := r.URL.Query().Get("stream")
	group := r.URL.Query().Get("group")

	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	stats, err := c.st.GetRetryDLQStats(r.Context(), ns, stream, group)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Transform to new concise schema: groups array with per-group stats + summary
	type groupStat struct {
		Name         string  `json:"name"`
		RetryCount   uint32  `json:"retry_count"`
		DLQCount     uint32  `json:"dlq_count"`
		TotalRetries uint32  `json:"total_retries"`
		SuccessRate  float64 `json:"success_rate"`
	}

	groups := make([]groupStat, 0, len(stats))
	var sumRetry, sumDLQ, sumAttempts uint32
	for _, st := range stats {
		groups = append(groups, groupStat{
			Name:         st.Group,
			RetryCount:   st.RetryCount,
			DLQCount:     st.DLQCount,
			TotalRetries: st.TotalRetries,
			SuccessRate:  st.SuccessRate,
		})
		sumRetry += st.RetryCount
		sumDLQ += st.DLQCount
		sumAttempts += st.TotalRetries
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })

	// Compute overall success rate across groups
	overallMessages := float64(sumRetry + sumDLQ)
	overallSuccess := 0.0
	if overallMessages > 0 {
		overallSuccess = float64(sumDLQ) / overallMessages * 100.0
	}

	resp := map[string]any{
		"namespace": ns,
		"stream":    stream,
		"groups":    groups,
		"summary": map[string]any{
			"total_groups":  len(groups),
			"total_retry":   sumRetry,
			"total_dlq":     sumDLQ,
			"total_retries": sumAttempts,
			"success_rate":  overallSuccess,
		},
	}

	writeJSON(w, resp)
}

// handleMessageDelivery returns message delivery status.
func (c *StreamsController) handleMessageDelivery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}

	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	messageID := r.URL.Query().Get("message_id")

	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	if st == "" || messageID == "" {
		writeError(w, http.StatusBadRequest, "Stream and message_id are required")
		return
	}

	id, err := base64.StdEncoding.DecodeString(messageID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "Invalid message_id format")
		return
	}

	statuses, err := c.st.GetMessageDeliveryStatus(r.Context(), ns, st, id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]any{
		"namespace":  ns,
		"stream":     st,
		"message_id": messageID,
		"delivery_status": func() any {
			if statuses == nil {
				return []any{}
			}
			return statuses
		}(),
	})
}

// handleSubscribeSSE streams messages over SSE with consumer group support.
// Query params: namespace, stream, group, from=latest|earliest, at=ms|RFC3339, start_token, filter, limit
func (c *StreamsController) handleSubscribeSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	stream := r.URL.Query().Get("stream")
	group := r.URL.Query().Get("group")
	from := r.URL.Query().Get("from") // latest|earliest
	at := r.URL.Query().Get("at")     // RFC3339 or ms
	startB64 := r.URL.Query().Get("start_token")
	filter := r.URL.Query().Get("filter")
	limitStr := r.URL.Query().Get("limit")

	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	var start []byte
	if startB64 != "" {
		if b, err := base64.StdEncoding.DecodeString(startB64); err == nil {
			start = b
		}
	}

	// Resolve options
	var opts streamsvc.SubscribeOptions
	if from == "earliest" {
		opts.From = "earliest"
	}
	if at != "" {
		opts.AtMs = parseTimestamp(at)
	}
	if filter != "" {
		// bound filter length to 2KiB to avoid abuse
		if len(filter) > 2048 {
			writeError(w, http.StatusBadRequest, "Filter too long")
			return
		}
		opts.Filter = filter
	}
	if limitStr != "" {
		if limit := parseLimit(limitStr); limit > 0 {
			opts.Limit = limit
		}
	}

	// Optional: policy params via query; if provided, pass through in SubscribeOptions.
	if pol, pace, ok := parsePolicyFromQuery(r); ok {
		opts.Policy = &pol
		opts.RetryPace = &pace
	}

	if err := c.st.StreamSubscribe(r.Context(), ns, stream, group, start, opts, sseSink{w: w, r: r}); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to subscribe")
		return
	}
}

// parsePolicyFromQuery parses optional retry policy fields from query params.
// Returns (policy, pace, ok) where ok indicates that any field was present.
func parsePolicyFromQuery(r *http.Request) (streamsvc.RetryPolicy, time.Duration, bool) {
	q := r.URL.Query()
	var pol streamsvc.RetryPolicy
	var pace time.Duration
	seen := false

	if t := q.Get("policy_type"); t != "" {
		switch t {
		case string(streamsvc.BackoffExp), string(streamsvc.BackoffExpJitter), string(streamsvc.BackoffFixed), string(streamsvc.BackoffNone):
			pol.Type = streamsvc.BackoffType(t)
			seen = true
		}
	}
	if v := q.Get("policy_base_ms"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms >= 0 {
			pol.Base = time.Duration(ms) * time.Millisecond
			seen = true
		}
	}
	if v := q.Get("policy_factor"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 {
			pol.Factor = f
			seen = true
		}
	}
	if v := q.Get("policy_cap_ms"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms >= 0 {
			pol.Cap = time.Duration(ms) * time.Millisecond
			seen = true
		}
	}
	if v := q.Get("policy_max_attempts"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			pol.MaxAttempts = uint32(n)
			seen = true
		}
	}
	if v := q.Get("retry_pace_ms"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms >= 0 {
			pace = time.Duration(ms) * time.Millisecond
			seen = true
		}
	}
	return pol, pace, seen
}

// handleTailSSE streams raw messages over SSE without requiring a consumer group or acks.
// Query params: namespace, stream, from=latest|earliest, at=ms|RFC3339, start_token, filter, limit
func (c *StreamsController) handleTailSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	from := r.URL.Query().Get("from")
	at := r.URL.Query().Get("at")
	startB64 := r.URL.Query().Get("start_token")
	filter := r.URL.Query().Get("filter")
	limitStr := r.URL.Query().Get("limit")

	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	var start []byte
	if startB64 != "" {
		if b, err := base64.StdEncoding.DecodeString(startB64); err == nil {
			start = b
		}
	}

	var opts streamsvc.SubscribeOptions
	if from == "earliest" {
		opts.From = "earliest"
	}
	if at != "" {
		opts.AtMs = parseTimestamp(at)
	}
	if filter != "" {
		if len(filter) > 2048 {
			writeError(w, http.StatusBadRequest, "Filter too long")
			return
		}
		opts.Filter = filter
	}
	if limitStr != "" {
		if limit := parseLimit(limitStr); limit > 0 {
			opts.Limit = limit
		}
	}

	if err := c.st.StreamTail(r.Context(), ns, st, start, opts, sseSink{w: w, r: r}); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to tail")
		return
	}
}

// handleStreamStats returns stream statistics.
func (c *StreamsController) handleStreamStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	stream := r.URL.Query().Get("stream")
	all := r.URL.Query().Get("all")

	var (
		resp any
		err  error
	)
	switch {
	case all == "1" || all == "true":
		if stream == "" {
			resp, err = c.buildAllNamespacesAllStreams(r.Context())
		} else {
			resp, err = c.buildAllNamespacesForStream(r.Context(), stream)
		}
	case stream == "":
		resp, err = c.buildNamespaceSummary(r.Context(), ns)
	default:
		resp, err = c.buildSingleStreamStats(r.Context(), ns, stream)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to get stream stats")
		return
	}
	writeJSON(w, resp)
}

func (c *StreamsController) handleStreamMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	q := r.URL.Query()
	ns := q.Get("namespace")
	st := q.Get("stream")
	metric := q.Get("type")
	// Note: ns can be empty for system-wide metrics across all namespaces
	if metric == "" {
		writeError(w, http.StatusBadRequest, "type is required")
		return
	}

	// Resolve time window
	nowMs := time.Now().UnixMilli()
	var startMs, endMs int64
	if v := q.Get("end_ms"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			endMs = ms
		}
	}
	if v := q.Get("start_ms"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			startMs = ms
		}
	}
	if startMs == 0 || endMs == 0 {
		switch q.Get("range") {
		case "7d":
			endMs = nowMs
			startMs = endMs - int64(7*24*time.Hour/time.Millisecond)
		case "30d":
			endMs = nowMs
			startMs = endMs - int64(30*24*time.Hour/time.Millisecond)
		default: // 90d
			endMs = nowMs
			startMs = endMs - int64(90*24*time.Hour/time.Millisecond)
		}
	}
	if endMs <= startMs {
		writeError(w, http.StatusBadRequest, "invalid time window")
		return
	}

	// step_ms with clamps
	stepMs := int64(4 * time.Hour / time.Millisecond)
	if v := q.Get("step_ms"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil && ms > 0 {
			stepMs = ms
		}
	}
	minStep := int64(time.Minute / time.Millisecond)
	maxStep := int64(24 * time.Hour / time.Millisecond)
	if stepMs < minStep {
		stepMs = minStep
	}
	if stepMs > maxStep {
		stepMs = maxStep
	}

	// Cap bucket count
	window := endMs - startMs
	if window/stepMs > 500 {
		stepMs = (window / 500) + 1
	}

	byPartition := parseBool(q.Get("by_partition"))
	fill := q.Get("fill") // "", "zero", "null", "ffill"

	// Map type -> unit
	unit := "events"
	switch metric {
	case "publish_rate":
		unit = "events/sec"
	case "bytes_rate":
		unit = "bytes/sec"
	case "publish_count", "ack_count":
		unit = "events"
	default:
		writeError(w, http.StatusBadRequest, "invalid type")
		return
	}

	// Handle system-wide metrics when no stream specified
	if st == "" {
		resp, err := c.st.SystemWideMetrics(r.Context(), ns, streamsvc.Metric(metric), startMs, endMs, stepMs, byPartition, fill, unit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, resp)
		return
	}

	// Delegate to service for specific stream
	series, err := c.st.Metrics(r.Context(), ns, st, streamsvc.Metric(metric), startMs, endMs, stepMs, byPartition, fill)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := map[string]any{
		"namespace": ns,
		"stream":    st,
		"metric":    metric,
		"unit":      unit,
		"start_ms":  startMs,
		"end_ms":    endMs,
		"step_ms":   stepMs,
		"series":    series, // []{label, points:[[ms,val],...]}
	}
	writeJSON(w, resp)
}

// handleStreamGroups returns stream group information.
func (c *StreamsController) handleStreamGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	ns := r.URL.Query().Get("namespace")
	st := r.URL.Query().Get("stream")
	// Effective namespace: default when empty
	effNS := ns
	if effNS == "" {
		effNS = c.rt.Config().DefaultNamespaceName
	}

	if st == "" {
		writeError(w, http.StatusBadRequest, "Stream parameter is required")
		return
	}

	// Get groups with cursor keys (have processed messages at some point)
	cursorGroups, err := c.st.GetWithCursorGroups(r.Context(), effNS, st)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to get cursor groups")
		return
	}

	// Get groups with retry/DLQ data (may not be currently active)
	retryGroups := c.st.DiscoverGroupsWithRetryData(effNS, st)

	// Combine all groups and mark their status
	allGroups := make(map[string]map[string]interface{})

	// Mark groups with cursor keys (have processed messages)
	for _, group := range cursorGroups {
		allGroups[group] = map[string]interface{}{
			"name":       group,
			"has_cursor": true,
		}
	}

	// Mark retry groups (may overlap with cursor groups)
	for _, group := range retryGroups {
		if existing, exists := allGroups[group]; exists {
			// Group has both cursor and retry data
			existing["has_retry_data"] = true
		} else {
			// Group has retry data but no cursor (unusual case)
			allGroups[group] = map[string]interface{}{
				"name":           group,
				"has_cursor":     false,
				"has_retry_data": true,
			}
		}
	}

	// Optionally enrich with detailed subscription info when requested
	// Always include full subscription details
	subsByGroup := map[string]streamsvc.SubscriptionInfo{}
	if subs, err := c.st.GetSubscriptions(r.Context(), effNS, st); err == nil {
		for _, s := range subs {
			subsByGroup[s.Group] = s
		}
	}

	// Convert to slice and enrich if available
	groups := make([]map[string]interface{}, 0, len(allGroups))
	for name, groupInfo := range allGroups {
		if s, ok := subsByGroup[name]; ok {
			groupInfo["active_subscribers"] = s.ActiveSubscribers
			groupInfo["last_delivered_ms"] = s.LastDeliveredMs
			groupInfo["total_lag"] = s.TotalLag
			// Convert partitions
			parts := make([]map[string]interface{}, 0, len(s.Partitions))
			for _, p := range s.Partitions {
				parts = append(parts, map[string]interface{}{
					"partition":  p.Partition,
					"cursor_seq": p.CursorSeq,
					"end_seq":    p.EndSeq,
					"lag":        p.Lag,
				})
			}
			groupInfo["partitions"] = parts
		}
		groups = append(groups, groupInfo)
	}

	resp := map[string]interface{}{
		"namespace": effNS,
		"stream":    st,
		"groups":    groups,
		"summary": map[string]interface{}{
			"total":           len(groups),
			"with_cursor":     len(cursorGroups),
			"with_retry_data": len(retryGroups),
		},
	}

	writeJSON(w, resp)
}

// buildAllNamespacesAllStreams returns a flat list of streams across all namespaces with totals.
func (c *StreamsController) buildAllNamespacesAllStreams(ctx context.Context) (any, error) {
	nsList, err := c.st.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	type streamRow struct {
		Namespace         string               `json:"namespace"`
		Stream            string               `json:"stream"`
		Partitions        []partitionStatsJSON `json:"partitions"`
		TotalCount        uint64               `json:"total_count"`
		TotalBytes        uint64               `json:"total_bytes"`
		ActiveSubscribers int                  `json:"active_subscribers"`
		LastPublishMs     uint64               `json:"last_publish_ms"`
		LastDeliveredMs   uint64               `json:"last_delivered_ms"`
		GroupsCount       int                  `json:"groups_count"`
	}
	out := struct {
		Streams           []streamRow `json:"streams"`
		StreamsCnt        int         `json:"streams_count"`
		TotalCount        uint64      `json:"total_count"`
		TotalBytes        uint64      `json:"total_bytes"`
		ActiveSubscribers int         `json:"active_subscribers"`
	}{}
	out.Streams = make([]streamRow, 0, 128)
	var sumActive int
	for _, n := range nsList {
		streams, err := c.st.ListStreams(ctx, n)
		if err != nil {
			return nil, err
		}
		for _, stream := range streams {
			parts, total, err := c.st.StreamStats(ctx, n, stream)
			if err != nil {
				return nil, err
			}
			row := streamRow{Namespace: n, Stream: stream, TotalCount: total}
			var bytesSum uint64
			row.Partitions = make([]partitionStatsJSON, 0, len(parts))
			var lastPubMax uint64
			for _, ps := range parts {
				row.Partitions = append(row.Partitions, partitionStatsJSON{
					Partition:     ps.Partition,
					FirstSeq:      ps.FirstSeq,
					LastSeq:       ps.LastSeq,
					Count:         ps.Count,
					Bytes:         ps.Bytes,
					LastPublishMs: ps.LastPublishMs,
				})
				bytesSum += ps.Bytes
				if ps.LastPublishMs > lastPubMax {
					lastPubMax = ps.LastPublishMs
				}
			}
			row.TotalBytes = bytesSum
			row.ActiveSubscribers = c.st.ActiveSubscribersCount(n, stream)
			sumActive += row.ActiveSubscribers
			row.LastPublishMs = lastPubMax
			if ts, _ := c.st.LastDeliveredMs(ctx, n, stream); ts > 0 {
				row.LastDeliveredMs = ts
			}
			if gc, err := c.st.GroupsCount(ctx, n, stream); err == nil {
				row.GroupsCount = gc
			}
			out.TotalCount += total
			out.TotalBytes += bytesSum
			out.Streams = append(out.Streams, row)
		}
	}
	out.StreamsCnt = len(out.Streams)
	out.ActiveSubscribers = sumActive
	return out, nil
}

// buildAllNamespacesForStream aggregates a specific stream across all namespaces.
func (c *StreamsController) buildAllNamespacesForStream(ctx context.Context, stream string) (any, error) {
	nsList, err := c.st.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	type nsStats struct {
		Namespace  string               `json:"namespace"`
		Partitions []partitionStatsJSON `json:"partitions"`
		TotalCount uint64               `json:"total_count"`
	}
	agg := struct {
		Stream     string    `json:"stream"`
		Namespaces []nsStats `json:"namespaces"`
		GrandTotal uint64    `json:"grand_total"`
	}{Stream: stream}
	for _, n := range nsList {
		parts, total, err := c.st.StreamStats(ctx, n, stream)
		if err != nil {
			return nil, err
		}
		row := nsStats{Namespace: n, TotalCount: total}
		row.Partitions = make([]partitionStatsJSON, 0, len(parts))
		for _, ps := range parts {
			row.Partitions = append(row.Partitions, partitionStatsJSON{
				Partition:     ps.Partition,
				FirstSeq:      ps.FirstSeq,
				LastSeq:       ps.LastSeq,
				Count:         ps.Count,
				Bytes:         ps.Bytes,
				LastPublishMs: ps.LastPublishMs,
			})
		}
		agg.Namespaces = append(agg.Namespaces, row)
		agg.GrandTotal += total
	}
	return agg, nil
}

// buildNamespaceSummary returns per-stream stats within a namespace with totals.
func (c *StreamsController) buildNamespaceSummary(ctx context.Context, ns string) (any, error) {
	streams, err := c.st.ListStreams(ctx, ns)
	if err != nil {
		return nil, err
	}
	type streamRow struct {
		Stream            string               `json:"stream"`
		Partitions        []partitionStatsJSON `json:"partitions"`
		TotalCount        uint64               `json:"total_count"`
		TotalBytes        uint64               `json:"total_bytes"`
		ActiveSubscribers int                  `json:"active_subscribers"`
		LastPublishMs     uint64               `json:"last_publish_ms"`
		LastDeliveredMs   uint64               `json:"last_delivered_ms"`
		GroupsCount       int                  `json:"groups_count"`
	}
	out := struct {
		Namespace         string      `json:"namespace"`
		Streams           []streamRow `json:"streams"`
		StreamsCnt        int         `json:"streams_count"`
		TotalCount        uint64      `json:"total_count"`
		TotalBytes        uint64      `json:"total_bytes"`
		ActiveSubscribers int         `json:"active_subscribers"`
	}{Namespace: ns}
	out.Streams = make([]streamRow, 0, len(streams))
	var sumActive int
	for _, cname := range streams {
		parts, total, err := c.st.StreamStats(ctx, ns, cname)
		if err != nil {
			return nil, err
		}
		row := streamRow{Stream: cname, TotalCount: total}
		var bytesSum uint64
		row.Partitions = make([]partitionStatsJSON, 0, len(parts))
		var lastPubMax uint64
		for _, ps := range parts {
			row.Partitions = append(row.Partitions, partitionStatsJSON{
				Partition:     ps.Partition,
				FirstSeq:      ps.FirstSeq,
				LastSeq:       ps.LastSeq,
				Count:         ps.Count,
				Bytes:         ps.Bytes,
				LastPublishMs: ps.LastPublishMs,
			})
			bytesSum += ps.Bytes
			if ps.LastPublishMs > lastPubMax {
				lastPubMax = ps.LastPublishMs
			}
		}
		row.TotalBytes = bytesSum
		row.ActiveSubscribers = c.st.ActiveSubscribersCount(ns, cname)
		sumActive += row.ActiveSubscribers
		row.LastPublishMs = lastPubMax
		if ts, _ := c.st.LastDeliveredMs(ctx, ns, cname); ts > 0 {
			row.LastDeliveredMs = ts
		}
		if gc, err := c.st.GroupsCount(ctx, ns, cname); err == nil {
			row.GroupsCount = gc
		}
		out.TotalCount += total
		out.TotalBytes += bytesSum
		out.Streams = append(out.Streams, row)
	}
	out.StreamsCnt = len(out.Streams)
	out.ActiveSubscribers = sumActive
	return out, nil
}

// buildSingleStreamStats returns detailed stats for a single stream in a namespace.
func (c *StreamsController) buildSingleStreamStats(ctx context.Context, ns, stream string) (any, error) {
	parts, total, err := c.st.StreamStats(ctx, ns, stream)
	if err != nil {
		return nil, err
	}
	out := singleStreamStatsJSON{Namespace: ns, Stream: stream, TotalCount: total}
	out.Partitions = make([]partitionStatsJSON, 0, len(parts))
	var lastPubMax uint64
	var totalBytes uint64
	for _, ps := range parts {
		out.Partitions = append(out.Partitions, partitionStatsJSON{
			Partition:     ps.Partition,
			FirstSeq:      ps.FirstSeq,
			LastSeq:       ps.LastSeq,
			Count:         ps.Count,
			Bytes:         ps.Bytes,
			LastPublishMs: ps.LastPublishMs,
		})
		totalBytes += ps.Bytes
		if ps.LastPublishMs > lastPubMax {
			lastPubMax = ps.LastPublishMs
		}
	}
	out.TotalBytes = totalBytes
	out.ActiveSubscribers = c.st.ActiveSubscribersCount(ns, stream)
	out.LastPublishMs = lastPubMax
	if ts, _ := c.st.LastDeliveredMs(ctx, ns, stream); ts > 0 {
		out.LastDeliveredMs = ts
	}
	if gc, err := c.st.GroupsCount(ctx, ns, stream); err == nil {
		out.GroupsCount = gc
	}
	return out, nil
}
