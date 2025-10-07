package controllers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	"github.com/rzbill/flo/internal/runtime"
	workqueuesvc "github.com/rzbill/flo/internal/services/workqueues"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// WorkQueuesController handles all work queue-related HTTP endpoints.
//
// It provides a RESTful interface to the Flo WorkQueues service, including
// queue management, message enqueue/dequeue, consumer management,
// lease operations, and admin operations.
type WorkQueuesController struct {
	rt *runtime.Runtime
	wq *workqueuesvc.Service
}

// NewWorkQueuesController creates a new work queues controller.
func NewWorkQueuesController(rt *runtime.Runtime, svc *workqueuesvc.Service) *WorkQueuesController {
	return &WorkQueuesController{
		rt: rt,
		wq: svc,
	}
}

// writeProtoJSON writes a protobuf message as JSON using protojson marshaler.
func writeProtoJSON(w http.ResponseWriter, msg proto.Message) {
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
	}
	data, err := marshaler.Marshal(msg)
	if err != nil {
		http.Error(w, "Failed to marshal response", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// RegisterRoutes registers all work queue-related routes with the given mux.
func (c *WorkQueuesController) RegisterRoutes(mux *http.ServeMux) {
	// Queue management
	mux.HandleFunc("/v1/workqueues", c.handleListWorkQueues)
	mux.HandleFunc("/v1/workqueues/groups", c.handleListGroups)
	mux.HandleFunc("/v1/workqueues/create", c.handleCreate)

	// Core operations
	mux.HandleFunc("/v1/workqueues/enqueue", c.handleEnqueue)
	mux.HandleFunc("/v1/workqueues/dequeue", c.handleDequeue)
	mux.HandleFunc("/v1/workqueues/complete", c.handleComplete)
	mux.HandleFunc("/v1/workqueues/fail", c.handleFail)
	mux.HandleFunc("/v1/workqueues/extend-lease", c.handleExtendLease)

	// Consumer registry
	mux.HandleFunc("/v1/workqueues/consumers/register", c.handleRegisterConsumer)
	mux.HandleFunc("/v1/workqueues/consumers/heartbeat", c.handleHeartbeat)
	mux.HandleFunc("/v1/workqueues/consumers/unregister", c.handleUnregisterConsumer)
	mux.HandleFunc("/v1/workqueues/consumers/list", c.handleListConsumers)

	// PEL & Claims
	mux.HandleFunc("/v1/workqueues/pending", c.handleListPending)
	mux.HandleFunc("/v1/workqueues/claim", c.handleClaim)

	// Admin & Stats
	mux.HandleFunc("/v1/workqueues/stats", c.handleGetStats)
	mux.HandleFunc("/v1/workqueues/metrics", c.handleMetrics)
	mux.HandleFunc("/v1/workqueues/ready", c.handleListReadyMessages)
	mux.HandleFunc("/v1/workqueues/dlq", c.handleListDLQ)
	mux.HandleFunc("/v1/workqueues/completed", c.handleListCompleted)
	mux.HandleFunc("/v1/workqueues/flush", c.handleFlush)
}

// handleListWorkQueues lists all work queues in a namespace.
// GET /v1/workqueues?namespace=<namespace>
func (c *WorkQueuesController) handleListWorkQueues(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	list, err := c.wq.ListWorkQueues(r.Context(), ns)
	if err != nil {
		http.Error(w, "Failed to list work queues", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"namespace":  ns,
		"workqueues": list,
	})
}

// handleListGroups lists all consumer groups for a work queue.
// GET /v1/workqueues/groups?namespace=<namespace>&name=<name>
func (c *WorkQueuesController) handleListGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = c.rt.Config().DefaultNamespaceName
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		http.Error(w, "name parameter required", http.StatusBadRequest)
		return
	}

	groups, err := c.wq.ListGroups(r.Context(), ns, name)
	if err != nil {
		http.Error(w, "Failed to list groups", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"namespace": ns,
		"name":      name,
		"groups":    groups,
	})
}

// handleCreate creates a new work queue.
// POST /v1/workqueues/create
func (c *WorkQueuesController) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.CreateWorkQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.CreateWorkQueue(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleEnqueue adds a message to the queue.
// POST /v1/workqueues/enqueue
func (c *WorkQueuesController) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.EnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.Enqueue(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleDequeue retrieves messages from the queue.
// POST /v1/workqueues/dequeue
// Note: HTTP version is non-streaming. Returns a batch of messages as JSON.
// For streaming dequeue, use the gRPC API.
func (c *WorkQueuesController) handleDequeue(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// TODO: Implement HTTP-friendly dequeue that doesn't require gRPC streaming
	// For now, return a placeholder response indicating to use gRPC
	http.Error(w, "Dequeue streaming is only supported via gRPC. Use /v1/workqueues/pending for HTTP queries.", http.StatusNotImplemented)
}

// handleComplete marks a message as successfully processed.
// POST /v1/workqueues/complete
func (c *WorkQueuesController) handleComplete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.CompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.Complete(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleFail marks a message as failed and schedules retry.
// POST /v1/workqueues/fail
func (c *WorkQueuesController) handleFail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.FailRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.Fail(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleExtendLease extends the lease on a message.
// POST /v1/workqueues/extend-lease
func (c *WorkQueuesController) handleExtendLease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.ExtendLeaseRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.ExtendLease(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleRegisterConsumer registers a consumer.
// POST /v1/workqueues/consumers/register
func (c *WorkQueuesController) handleRegisterConsumer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.RegisterConsumerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.RegisterConsumer(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleHeartbeat sends a consumer heartbeat.
// POST /v1/workqueues/consumers/heartbeat
func (c *WorkQueuesController) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.HeartbeatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.Heartbeat(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleUnregisterConsumer unregisters a consumer.
// POST /v1/workqueues/consumers/unregister
func (c *WorkQueuesController) handleUnregisterConsumer(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.UnregisterConsumerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.UnregisterConsumer(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleListConsumers lists consumers in a group.
// GET /v1/workqueues/consumers/list?namespace=...&name=...&group=...
func (c *WorkQueuesController) handleListConsumers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	req := &flov1.ListConsumersRequest{
		Namespace: r.URL.Query().Get("namespace"),
		Name:      r.URL.Query().Get("name"),
		Group:     r.URL.Query().Get("group"),
	}

	resp, err := c.wq.ListConsumers(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleListPending lists pending (in-flight) messages.
// GET /v1/workqueues/pending?namespace=...&name=...&group=...&consumer_id=...&limit=...
func (c *WorkQueuesController) handleListPending(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)

	req := &flov1.ListPendingRequest{
		Namespace:  r.URL.Query().Get("namespace"),
		Name:       r.URL.Query().Get("name"),
		Group:      r.URL.Query().Get("group"),
		ConsumerId: r.URL.Query().Get("consumer_id"),
		Limit:      int32(limit),
	}

	resp, err := c.wq.ListPending(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleClaim manually reassigns messages to a consumer.
// POST /v1/workqueues/claim
func (c *WorkQueuesController) handleClaim(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.ClaimRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.Claim(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleGetStats returns queue statistics.
// GET /v1/workqueues/stats?namespace=...&name=...&group=...
func (c *WorkQueuesController) handleGetStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	req := &flov1.GetWorkQueueStatsRequest{
		Namespace: r.URL.Query().Get("namespace"),
		Name:      r.URL.Query().Get("name"),
		Group:     r.URL.Query().Get("group"),
	}

	resp, err := c.wq.GetWorkQueueStats(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleListDLQ lists messages in the dead letter queue.
// GET /v1/workqueues/dlq?namespace=...&name=...&group=...&limit=...
func (c *WorkQueuesController) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)

	req := &flov1.ListWorkQueueDLQRequest{
		Namespace: r.URL.Query().Get("namespace"),
		Name:      r.URL.Query().Get("name"),
		Group:     r.URL.Query().Get("group"),
		Limit:     int32(limit),
	}

	resp, err := c.wq.ListDLQMessages(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleListCompleted lists recently completed messages with execution metadata.
// GET /v1/workqueues/completed?namespace=<ns>&name=<name>&group=<group>&partition=<p>&limit=<n>
func (c *WorkQueuesController) handleListCompleted(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	partition, _ := strconv.ParseInt(r.URL.Query().Get("partition"), 10, 32)
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)

	req := &flov1.ListCompletedRequest{
		Namespace: r.URL.Query().Get("namespace"),
		Name:      r.URL.Query().Get("name"),
		Group:     r.URL.Query().Get("group"),
		Partition: int32(partition),
		Limit:     int32(limit),
	}

	resp, err := c.wq.ListCompleted(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleListReadyMessages lists messages waiting in the priority queue (ready to be dequeued).
// GET /v1/workqueues/ready?namespace=<ns>&name=<name>&partition=<p>&limit=<n>&include_payload=<bool>
func (c *WorkQueuesController) handleListReadyMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	partition, _ := strconv.ParseInt(r.URL.Query().Get("partition"), 10, 32)
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)
	includePayload := r.URL.Query().Get("include_payload") == "true"

	req := &flov1.ListReadyMessagesRequest{
		Namespace:      r.URL.Query().Get("namespace"),
		Name:           r.URL.Query().Get("name"),
		Partition:      int32(partition),
		Limit:          int32(limit),
		IncludePayload: includePayload,
	}

	resp, err := c.wq.ListReadyMessages(r.Context(), req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleFlush clears queues.
// POST /v1/workqueues/flush
func (c *WorkQueuesController) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req flov1.FlushWorkQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := c.wq.Flush(r.Context(), &req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	writeProtoJSON(w, resp)
}

// handleMetrics returns time-series metrics for a workqueue.
// GET /v1/workqueues/metrics?namespace=<ns>&name=<wq>&type=<metric>&range=<7d|30d|90d>&step_ms=<ms>
func (c *WorkQueuesController) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	ns := q.Get("namespace")
	name := q.Get("name")
	metricType := q.Get("type")

	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}
	if metricType == "" {
		http.Error(w, "type is required", http.StatusBadRequest)
		return
	}

	// Parse time window
	nowMs := int64(time.Now().UnixMilli())
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
		http.Error(w, "invalid time window", http.StatusBadRequest)
		return
	}

	// Parse step_ms with clamps
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

	// Validate metric type
	var metric workqueuesvc.Metric
	switch metricType {
	case "enqueue_count":
		metric = workqueuesvc.MetricEnqueueCount
	case "dequeue_count":
		metric = workqueuesvc.MetricDequeueCount
	case "complete_count":
		metric = workqueuesvc.MetricCompleteCount
	case "fail_count":
		metric = workqueuesvc.MetricFailCount
	case "enqueue_rate":
		metric = workqueuesvc.MetricEnqueueRate
	case "dequeue_rate":
		metric = workqueuesvc.MetricDequeueRate
	case "complete_rate":
		metric = workqueuesvc.MetricCompleteRate
	case "queue_depth":
		metric = workqueuesvc.MetricQueueDepth
	default:
		http.Error(w, "invalid metric type", http.StatusBadRequest)
		return
	}

	// Get metrics from service
	series, err := c.wq.Metrics(r.Context(), ns, name, metric, startMs, endMs, stepMs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Build response
	response := map[string]interface{}{
		"series":      series,
		"start_ms":    startMs,
		"end_ms":      endMs,
		"step_ms":     stepMs,
		"metric_type": metricType,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
