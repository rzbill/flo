package grpcserver

import (
	"context"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	workqueuesvc "github.com/rzbill/flo/internal/services/workqueues"
)

// workQueuesSvc wraps the workqueues service for gRPC.
type workQueuesSvc struct {
	flov1.UnimplementedWorkQueuesServiceServer
	svc *workqueuesvc.Service
}

// CreateWorkQueue initializes a new work queue.
func (w *workQueuesSvc) CreateWorkQueue(ctx context.Context, req *flov1.CreateWorkQueueRequest) (*flov1.CreateWorkQueueResponse, error) {
	return w.svc.CreateWorkQueue(ctx, req)
}

// Enqueue adds a message to the queue.
func (w *workQueuesSvc) Enqueue(ctx context.Context, req *flov1.EnqueueRequest) (*flov1.EnqueueResponse, error) {
	return w.svc.Enqueue(ctx, req)
}

// Dequeue retrieves messages from the queue (streaming).
func (w *workQueuesSvc) Dequeue(req *flov1.DequeueRequest, stream flov1.WorkQueuesService_DequeueServer) error {
	return w.svc.Dequeue(req, stream)
}

// Complete marks a message as successfully processed.
func (w *workQueuesSvc) Complete(ctx context.Context, req *flov1.CompleteRequest) (*flov1.CompleteResponse, error) {
	return w.svc.Complete(ctx, req)
}

// Fail marks a message as failed and schedules retry.
func (w *workQueuesSvc) Fail(ctx context.Context, req *flov1.FailRequest) (*flov1.FailResponse, error) {
	return w.svc.Fail(ctx, req)
}

// ExtendLease extends the lease on a message.
func (w *workQueuesSvc) ExtendLease(ctx context.Context, req *flov1.ExtendLeaseRequest) (*flov1.ExtendLeaseResponse, error) {
	return w.svc.ExtendLease(ctx, req)
}

// RegisterConsumer registers a consumer with the registry.
func (w *workQueuesSvc) RegisterConsumer(ctx context.Context, req *flov1.RegisterConsumerRequest) (*flov1.RegisterConsumerResponse, error) {
	return w.svc.RegisterConsumer(ctx, req)
}

// Heartbeat updates consumer liveness.
func (w *workQueuesSvc) Heartbeat(ctx context.Context, req *flov1.HeartbeatRequest) (*flov1.HeartbeatResponse, error) {
	return w.svc.Heartbeat(ctx, req)
}

// UnregisterConsumer removes a consumer from the registry.
func (w *workQueuesSvc) UnregisterConsumer(ctx context.Context, req *flov1.UnregisterConsumerRequest) (*flov1.UnregisterConsumerResponse, error) {
	return w.svc.UnregisterConsumer(ctx, req)
}

// ListConsumers returns all consumers in a group.
func (w *workQueuesSvc) ListConsumers(ctx context.Context, req *flov1.ListConsumersRequest) (*flov1.ListConsumersResponse, error) {
	return w.svc.ListConsumers(ctx, req)
}

// ListPending returns pending (in-flight) messages.
func (w *workQueuesSvc) ListPending(ctx context.Context, req *flov1.ListPendingRequest) (*flov1.ListPendingResponse, error) {
	return w.svc.ListPending(ctx, req)
}

// Claim manually reassigns messages to a different consumer.
func (w *workQueuesSvc) Claim(ctx context.Context, req *flov1.ClaimRequest) (*flov1.ClaimResponse, error) {
	return w.svc.Claim(ctx, req)
}

// GetWorkQueueStats returns queue statistics.
func (w *workQueuesSvc) GetWorkQueueStats(ctx context.Context, req *flov1.GetWorkQueueStatsRequest) (*flov1.GetWorkQueueStatsResponse, error) {
	return w.svc.GetWorkQueueStats(ctx, req)
}

// ListDLQMessages returns messages in the dead letter queue.
func (w *workQueuesSvc) ListDLQMessages(ctx context.Context, req *flov1.ListWorkQueueDLQRequest) (*flov1.ListWorkQueueDLQResponse, error) {
	return w.svc.ListDLQMessages(ctx, req)
}

// ListCompleted returns recently completed messages with execution metadata.
func (w *workQueuesSvc) ListCompleted(ctx context.Context, req *flov1.ListCompletedRequest) (*flov1.ListCompletedResponse, error) {
	return w.svc.ListCompleted(ctx, req)
}

// ListReadyMessages returns messages waiting in the priority queue.
func (w *workQueuesSvc) ListReadyMessages(ctx context.Context, req *flov1.ListReadyMessagesRequest) (*flov1.ListReadyMessagesResponse, error) {
	return w.svc.ListReadyMessages(ctx, req)
}

// Flush clears queues (for testing/cleanup).
func (w *workQueuesSvc) Flush(ctx context.Context, req *flov1.FlushWorkQueueRequest) (*flov1.FlushWorkQueueResponse, error) {
	return w.svc.Flush(ctx, req)
}
