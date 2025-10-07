package workqueues

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	flov1 "github.com/rzbill/flo/api/flo/v1"
	"github.com/rzbill/flo/internal/runtime"
	"github.com/rzbill/flo/internal/workqueue"
	logpkg "github.com/rzbill/flo/pkg/log"
)

// Service provides WorkQueue operations over gRPC.
// It coordinates WorkQueue, LeaseManager, ConsumerRegistry, and AutoClaimScanner.
type Service struct {
	flov1.UnimplementedWorkQueuesServiceServer
	rt     *runtime.Runtime
	logger logpkg.Logger

	// Defaults
	defaultPartitions  int32
	defaultLeaseMs     int64
	defaultBlockMs     int64
	defaultHeartbeatMs int64
}

// New creates a new WorkQueues service with default settings.
func New(rt *runtime.Runtime) *Service {
	logger := logpkg.NewLogger(logpkg.WithLevel(logpkg.InfoLevel))
	logger = logger.With(logpkg.F("component", "workqueues"))
	return NewWithLogger(rt, logger)
}

// NewWithLogger creates a new WorkQueues service with a custom logger.
func NewWithLogger(rt *runtime.Runtime, logger logpkg.Logger) *Service {
	if logger == nil {
		logger = logpkg.NewLogger(logpkg.WithLevel(logpkg.InfoLevel))
		logger = logger.With(logpkg.F("component", "workqueues"))
	}

	return &Service{
		rt:                 rt,
		logger:             logger,
		defaultPartitions:  16,
		defaultLeaseMs:     30000, // 30 seconds
		defaultBlockMs:     5000,  // 5 seconds
		defaultHeartbeatMs: 15000, // 15 seconds
	}
}

// ListWorkQueues returns all workqueue names in the given namespace.
// It scans the Pebble database for workqueue metadata keys.
func (s *Service) ListWorkQueues(_ context.Context, ns string) ([]string, error) {
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}

	db := s.rt.DB()
	set := make(map[string]struct{})

	// Scan for workqueue meta keys: ns/<namespace>/wq/<name>/p<partition>/meta
	prefix := []byte(fmt.Sprintf("ns/%s/wq/", ns))
	hi := append(append([]byte{}, prefix...), 0xFF)

	it, err := db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: hi})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		// Keys are of the form: ns/<namespace>/wq/<name>/p<partition>/meta
		// We want to extract the <name> part
		parts := bytes.Split(k, []byte{'/'})
		if len(parts) >= 4 {
			name := string(parts[3])
			set[name] = struct{}{}
		}
	}

	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	sort.Strings(names)

	return names, nil
}

// ListGroups returns all consumer group names for a given workqueue.
// It scans the Pebble database for consumer registry and lease keys.
func (s *Service) ListGroups(_ context.Context, namespace, name string) ([]string, error) {
	if namespace == "" {
		namespace = s.rt.Config().DefaultNamespaceName
	}

	db := s.rt.DB()
	set := make(map[string]struct{})

	// Scan consumer registry keys: ns/<namespace>/wq/<name>/cons/<group>/<consumer_id>
	consPrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/cons/", namespace, name))
	consHi := append(append([]byte{}, consPrefix...), 0xFF)

	it, err := db.NewIter(&pebble.IterOptions{LowerBound: consPrefix, UpperBound: consHi})
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}
	defer func() { _ = it.Close() }()

	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		// Keys are of the form: ns/<namespace>/wq/<name>/cons/<group>/<consumer_id>
		// We want to extract the <group> part
		parts := bytes.Split(k, []byte{'/'})
		if len(parts) >= 6 {
			group := string(parts[5])
			set[group] = struct{}{}
		}
	}

	// Also scan lease keys: ns/<namespace>/wq/<name>/lease/<group>/<id>
	leasePrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/lease/", namespace, name))
	leaseHi := append(append([]byte{}, leasePrefix...), 0xFF)

	it2, err := db.NewIter(&pebble.IterOptions{LowerBound: leasePrefix, UpperBound: leaseHi})
	if err != nil {
		return nil, fmt.Errorf("failed to create lease iterator: %w", err)
	}
	defer func() { _ = it2.Close() }()

	for ok := it2.First(); ok; ok = it2.Next() {
		k := it2.Key()
		// Keys are of the form: ns/<namespace>/wq/<name>/lease/<group>/<id>
		parts := bytes.Split(k, []byte{'/'})
		if len(parts) >= 6 {
			group := string(parts[5])
			set[group] = struct{}{}
		}
	}

	// Always include "default" group
	set["default"] = struct{}{}

	groups := make([]string, 0, len(set))
	for g := range set {
		groups = append(groups, g)
	}
	sort.Strings(groups)

	return groups, nil
}

// ============================================================================
// Core Operations
// ============================================================================

// CreateWorkQueue initializes a new work queue.
func (s *Service) CreateWorkQueue(ctx context.Context, req *flov1.CreateWorkQueueRequest) (*flov1.CreateWorkQueueResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}

	partitions := req.Partitions
	if partitions == 0 {
		partitions = s.defaultPartitions
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Initialize queues for all partitions
	for p := int32(0); p < partitions; p++ {
		_, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, uint32(p))
		if err != nil {
			s.logger.Error("failed to create workqueue partition",
				logpkg.F("namespace", req.Namespace),
				logpkg.F("name", req.Name),
				logpkg.F("partition", p),
				logpkg.Err(err),
			)
			return nil, fmt.Errorf("create partition %d: %w", p, err)
		}
	}

	s.logger.Info("created workqueue",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("partitions", partitions),
	)

	return &flov1.CreateWorkQueueResponse{}, nil
}

// Enqueue adds a message to the queue with optional priority and delay.
func (s *Service) Enqueue(ctx context.Context, req *flov1.EnqueueRequest) (*flov1.EnqueueResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Determine partition from key
	partition := s.selectPartition(req.Key, s.defaultPartitions)

	// Open queue for partition
	q, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, uint32(partition))
	if err != nil {
		return nil, fmt.Errorf("open queue: %w", err)
	}

	// Encode headers
	headerData, err := encodeHeaders(req.Headers)
	if err != nil {
		return nil, fmt.Errorf("encode headers: %w", err)
	}

	// Enqueue message
	priority := uint32(req.Priority)
	delayMs := req.DelayMs
	nowMs := time.Now().UnixMilli()
	seq, err := q.Enqueue(ctx, headerData, req.Payload, priority, delayMs, nowMs)
	if err != nil {
		return nil, fmt.Errorf("enqueue: %w", err)
	}

	// Generate message ID (16 bytes: 8 bytes zero + 8 bytes sequence)
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], seq)

	s.logger.Debug("enqueued message",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("partition", partition),
		logpkg.F("seq", seq),
		logpkg.F("priority", priority),
		logpkg.F("delay_ms", delayMs),
	)

	return &flov1.EnqueueResponse{
		Id: id[:],
	}, nil
}

// Dequeue retrieves messages from the queue with lease management.
// This is a streaming RPC that can block waiting for new messages.
func (s *Service) Dequeue(req *flov1.DequeueRequest, stream flov1.WorkQueuesService_DequeueServer) error {
	if req.Namespace == "" {
		return fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return fmt.Errorf("name required")
	}
	if req.Group == "" {
		return fmt.Errorf("group required")
	}
	if req.ConsumerId == "" {
		return fmt.Errorf("consumer_id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return fmt.Errorf("ensure namespace: %w", err)
	}

	// Defaults
	count := int(req.Count)
	if count == 0 {
		count = 1
	}
	if count > 100 {
		count = 100
	}

	leaseMs := req.LeaseMs
	if leaseMs == 0 {
		leaseMs = s.defaultLeaseMs
	}

	blockMs := req.BlockMs
	if blockMs == 0 {
		blockMs = s.defaultBlockMs
	}

	// Try dequeue with blocking
	ctx := stream.Context()
	deadline := time.Now().Add(time.Duration(blockMs) * time.Millisecond)

	for {
		nowMs := time.Now().UnixMilli()

		// Try to dequeue from all partitions
		for p := int32(0); p < s.defaultPartitions; p++ {
			q, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, uint32(p))
			if err != nil {
				s.logger.Error("failed to open queue",
					logpkg.F("partition", p),
					logpkg.Err(err),
				)
				continue
			}

			// Try dequeue
			items, err := q.Dequeue(ctx, req.Group, count, leaseMs, nowMs)
			if err != nil {
				s.logger.Error("dequeue failed",
					logpkg.F("partition", p),
					logpkg.Err(err),
				)
				continue
			}

			// Register leases in PEL and send messages
			if len(items) > 0 {
				leaseMgr := workqueue.NewLeaseManager(s.rt.DB(), req.Namespace, req.Name)

				for _, item := range items {
					var msgID [16]byte
					binary.BigEndian.PutUint64(msgID[8:], item.Seq)

					// Register lease in PEL
					_, err := leaseMgr.AcquireLease(ctx, req.Group, msgID[:], req.ConsumerId, leaseMs)
					if err != nil {
						s.logger.Warn("failed to add to PEL",
							logpkg.F("msg_id", msgID),
							logpkg.Err(err),
						)
						// Continue anyway since the lease is already created by q.Dequeue
					}

					// Get lease to retrieve delivery count
					lease, err := leaseMgr.GetLease(ctx, req.Group, msgID[:])
					deliveryCount := int32(1) // Default to 1 if we can't get lease
					enqueuedAtMs := nowMs     // Approximate as current time if not tracked
					if err == nil && lease != nil {
						deliveryCount = lease.DeliveryCount
					}

					resp := &flov1.DequeueResponse{
						Id:               msgID[:],
						Payload:          item.Payload,
						Headers:          decodeHeadersFromBytes(item.Header),
						Partition:        p,
						EnqueuedAtMs:     enqueuedAtMs, // TODO M4: Store actual enqueue time in message encoding
						DeliveryCount:    deliveryCount,
						LeaseExpiresAtMs: item.ExpiryMs,
					}

					if err := stream.Send(resp); err != nil {
						return fmt.Errorf("send response: %w", err)
					}
				}
			}

			// If we got messages, return
			if len(items) > 0 {
				s.logger.Debug("dequeued messages",
					logpkg.F("namespace", req.Namespace),
					logpkg.F("name", req.Name),
					logpkg.F("group", req.Group),
					logpkg.F("consumer_id", req.ConsumerId),
					logpkg.F("count", len(items)),
				)
				return nil
			}
		}

		// Check if we should keep waiting
		if time.Now().After(deadline) {
			break
		}

		// Check context
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			// Continue loop
		}
	}

	// No messages available
	s.logger.Debug("dequeue timeout - no messages",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("group", req.Group),
	)

	return nil
}

// Complete marks a message as successfully processed and removes it.
func (s *Service) Complete(ctx context.Context, req *flov1.CompleteRequest) (*flov1.CompleteResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}
	if len(req.Id) == 0 {
		return nil, fmt.Errorf("id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Extract sequence from ID
	if len(req.Id) < 16 {
		return nil, fmt.Errorf("invalid id length")
	}
	var id [16]byte
	copy(id[:], req.Id)
	seq := binary.BigEndian.Uint64(id[8:])

	// Get lease info for tracking
	leaseMgr := workqueue.NewLeaseManager(s.rt.DB(), req.Namespace, req.Name)
	lease, err := leaseMgr.GetLease(ctx, req.Group, req.Id)
	var dequeuedAtMs int64
	var deliveryCount int32
	var consumerID string
	if err == nil && lease != nil {
		dequeuedAtMs = lease.LastDeliveryMs
		deliveryCount = lease.DeliveryCount
		consumerID = lease.ConsumerID
	}

	// Determine partition (TODO: store partition in message metadata)
	// For now, try all partitions
	completedMgr := workqueue.NewCompletedManager(s.rt.DB(), req.Namespace, req.Name)
	nowMs := time.Now().UnixMilli()

	for p := int32(0); p < s.defaultPartitions; p++ {
		q, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, uint32(p))
		if err != nil {
			continue
		}

		err = q.Complete(ctx, req.Group, []uint64{seq})
		if err == nil {
			// Add to completed buffer
			completedEntry := &workqueue.CompletedEntry{
				ID:            req.Id,
				Seq:           seq,
				Partition:     uint32(p),
				Group:         req.Group,
				ConsumerID:    consumerID,
				EnqueuedAtMs:  0, // Not tracked yet
				DequeuedAtMs:  dequeuedAtMs,
				CompletedAtMs: nowMs,
				Duration:      nowMs - dequeuedAtMs,
				DeliveryCount: deliveryCount,
				PayloadSize:   0, // Not tracked in this version
			}

			if err := completedMgr.AddCompleted(ctx, completedEntry); err != nil {
				s.logger.Warn("failed to add completed entry",
					logpkg.F("namespace", req.Namespace),
					logpkg.F("name", req.Name),
					logpkg.Err(err),
				)
				// Don't fail the completion if we can't track it
			}

			s.logger.Debug("completed message",
				logpkg.F("namespace", req.Namespace),
				logpkg.F("name", req.Name),
				logpkg.F("group", req.Group),
				logpkg.F("seq", seq),
			)
			return &flov1.CompleteResponse{}, nil
		}
	}

	return nil, fmt.Errorf("message not found or already completed")
}

// Fail marks a message as failed and schedules retry or moves to DLQ.
func (s *Service) Fail(ctx context.Context, req *flov1.FailRequest) (*flov1.FailResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}
	if len(req.Id) == 0 {
		return nil, fmt.Errorf("id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Extract sequence from ID
	if len(req.Id) < 16 {
		return nil, fmt.Errorf("invalid id length")
	}
	var id [16]byte
	copy(id[:], req.Id)
	seq := binary.BigEndian.Uint64(id[8:])

	// Retry after default or specified
	retryAfterMs := req.RetryAfterMs
	if retryAfterMs == 0 {
		retryAfterMs = 5000 // 5 seconds default
	}

	nowMs := time.Now().UnixMilli()

	// Check delivery count from lease to determine if we should route to DLQ
	leaseMgr := workqueue.NewLeaseManager(s.rt.DB(), req.Namespace, req.Name)
	lease, err := leaseMgr.GetLease(ctx, req.Group, req.Id)

	maxAttempts := int32(5) // Default: 5 attempts before DLQ
	toDLQ := false
	if err == nil && lease != nil {
		if lease.DeliveryCount >= maxAttempts {
			toDLQ = true // Exceeded max attempts, route to DLQ
		}
	}

	// Try all partitions
	for p := int32(0); p < s.defaultPartitions; p++ {
		q, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, uint32(p))
		if err != nil {
			continue
		}

		// Fail with retry or DLQ
		err = q.Fail(ctx, req.Group, []uint64{seq}, retryAfterMs, toDLQ, nowMs)
		if err == nil {
			if toDLQ {
				s.logger.Info("message moved to DLQ after max attempts",
					logpkg.F("namespace", req.Namespace),
					logpkg.F("name", req.Name),
					logpkg.F("group", req.Group),
					logpkg.F("seq", seq),
					logpkg.F("delivery_count", lease.DeliveryCount),
					logpkg.F("max_attempts", maxAttempts),
					logpkg.F("error", req.Error),
				)
			} else {
				s.logger.Debug("failed message - scheduled retry",
					logpkg.F("namespace", req.Namespace),
					logpkg.F("name", req.Name),
					logpkg.F("group", req.Group),
					logpkg.F("seq", seq),
					logpkg.F("retry_after_ms", retryAfterMs),
					logpkg.F("error", req.Error),
				)
			}
			return &flov1.FailResponse{}, nil
		}
	}

	return nil, fmt.Errorf("message not found")
}

// ExtendLease extends the lease on a message being processed.
func (s *Service) ExtendLease(ctx context.Context, req *flov1.ExtendLeaseRequest) (*flov1.ExtendLeaseResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}
	if len(req.Id) == 0 {
		return nil, fmt.Errorf("id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Extract sequence from ID
	if len(req.Id) < 16 {
		return nil, fmt.Errorf("invalid id length")
	}
	var id [16]byte
	copy(id[:], req.Id)
	seq := binary.BigEndian.Uint64(id[8:])

	// Extension duration
	extensionMs := req.ExtensionMs
	if extensionMs == 0 {
		extensionMs = s.defaultLeaseMs
	}

	// Try all partitions
	for p := int32(0); p < s.defaultPartitions; p++ {
		q, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, uint32(p))
		if err != nil {
			continue
		}

		nowMs := time.Now().UnixMilli()
		err = q.ExtendLease(ctx, req.Group, []uint64{seq}, extensionMs, nowMs)
		if err == nil {
			newExpiresAt := nowMs + extensionMs
			s.logger.Debug("extended lease",
				logpkg.F("namespace", req.Namespace),
				logpkg.F("name", req.Name),
				logpkg.F("group", req.Group),
				logpkg.F("seq", seq),
				logpkg.F("extension_ms", extensionMs),
			)
			return &flov1.ExtendLeaseResponse{
				NewExpiresAtMs: newExpiresAt,
			}, nil
		}
	}

	return nil, fmt.Errorf("lease not found or expired")
}

// ============================================================================
// Consumer Registry
// ============================================================================

// RegisterConsumer registers a consumer with the registry.
func (s *Service) RegisterConsumer(ctx context.Context, req *flov1.RegisterConsumerRequest) (*flov1.RegisterConsumerResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Create consumer registry
	ttlMs := req.HeartbeatTtlMs
	if ttlMs == 0 {
		ttlMs = s.defaultHeartbeatMs
	}
	ttl := time.Duration(ttlMs) * time.Millisecond

	registry := workqueue.NewConsumerRegistry(s.rt.DB(), req.Namespace, req.Name, ttl)

	// Convert labels to metadata
	metadata := req.Labels
	if metadata == nil {
		metadata = make(map[string]string)
	}
	metadata["capacity"] = fmt.Sprintf("%d", req.Capacity)

	// Register
	_, err2 := registry.Register(ctx, req.Group, req.ConsumerId, metadata)
	if err2 != nil {
		return nil, fmt.Errorf("register consumer: %w", err2)
	}

	s.logger.Info("registered consumer",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("group", req.Group),
		logpkg.F("consumer_id", req.ConsumerId),
		logpkg.F("capacity", req.Capacity),
	)

	return &flov1.RegisterConsumerResponse{}, nil
}

// Heartbeat updates consumer liveness.
func (s *Service) Heartbeat(ctx context.Context, req *flov1.HeartbeatRequest) (*flov1.HeartbeatResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Create consumer registry
	registry := workqueue.NewConsumerRegistry(s.rt.DB(), req.Namespace, req.Name, 15*time.Second)

	// Heartbeat
	expiresAt, err2 := registry.Heartbeat(ctx, req.Group, req.ConsumerId)
	if err2 != nil {
		return nil, fmt.Errorf("heartbeat: %w", err2)
	}

	s.logger.Debug("consumer heartbeat",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("group", req.Group),
		logpkg.F("consumer_id", req.ConsumerId),
		logpkg.F("in_flight", req.InFlight),
	)

	return &flov1.HeartbeatResponse{
		ExpiresAtMs: expiresAt,
	}, nil
}

// UnregisterConsumer removes a consumer from the registry.
func (s *Service) UnregisterConsumer(ctx context.Context, req *flov1.UnregisterConsumerRequest) (*flov1.UnregisterConsumerResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Create consumer registry
	registry := workqueue.NewConsumerRegistry(s.rt.DB(), req.Namespace, req.Name, 15*time.Second)

	// Unregister
	err2 := registry.Unregister(ctx, req.Group, req.ConsumerId)
	if err2 != nil {
		return nil, fmt.Errorf("unregister: %w", err2)
	}

	s.logger.Info("unregistered consumer",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("group", req.Group),
		logpkg.F("consumer_id", req.ConsumerId),
	)

	return &flov1.UnregisterConsumerResponse{}, nil
}

// ListConsumers returns all consumers in a group.
func (s *Service) ListConsumers(ctx context.Context, req *flov1.ListConsumersRequest) (*flov1.ListConsumersResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Create consumer registry
	registry := workqueue.NewConsumerRegistry(s.rt.DB(), req.Namespace, req.Name, 15*time.Second)

	// List consumers
	consumers, err2 := registry.List(ctx, req.Group, 1000)
	if err2 != nil {
		return nil, fmt.Errorf("list consumers: %w", err2)
	}

	// Get PEL to count in-flight messages per consumer
	leaseMgr := workqueue.NewLeaseManager(s.rt.DB(), req.Namespace, req.Name)
	allLeases, err3 := leaseMgr.ListPending(ctx, req.Group, "", 10000)
	if err3 != nil {
		s.logger.Warn("failed to list pending for consumer in-flight count",
			logpkg.F("namespace", req.Namespace),
			logpkg.F("name", req.Name),
			logpkg.F("group", req.Group),
			logpkg.Err(err3),
		)
		allLeases = []*workqueue.Lease{} // Continue with empty list
	}

	// Build per-consumer in-flight count
	inFlightCount := make(map[string]int32)
	for _, lease := range allLeases {
		inFlightCount[lease.ConsumerID]++
	}

	// Convert to proto
	now := time.Now().UnixMilli()
	resp := &flov1.ListConsumersResponse{
		Consumers: make([]*flov1.ConsumerInfo, 0, len(consumers)),
	}

	for _, c := range consumers {
		// Extract capacity from metadata
		capacity := int32(1) // Default capacity
		if capStr, ok := c.Metadata["capacity"]; ok {
			if capInt, err := strconv.Atoi(capStr); err == nil && capInt > 0 {
				capacity = int32(capInt)
			}
		}

		info := &flov1.ConsumerInfo{
			ConsumerId:  c.ID,
			Capacity:    capacity,
			InFlight:    inFlightCount[c.ID],
			Labels:      c.Metadata,
			LastSeenMs:  c.LastHeartbeat,
			ExpiresAtMs: c.ExpiresAtMs,
			IsAlive:     c.ExpiresAtMs > now,
		}
		resp.Consumers = append(resp.Consumers, info)
	}

	return resp, nil
}

// ============================================================================
// PEL & Claims
// ============================================================================

// ListPending returns pending (in-flight) messages.
func (s *Service) ListPending(ctx context.Context, req *flov1.ListPendingRequest) (*flov1.ListPendingResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Create lease manager
	leaseMgr := workqueue.NewLeaseManager(s.rt.DB(), req.Namespace, req.Name)

	// List pending
	limit := int(req.Limit)
	if limit == 0 {
		limit = 100
	}

	leases, err := leaseMgr.ListPending(ctx, req.Group, req.ConsumerId, limit)
	if err != nil {
		return nil, fmt.Errorf("list pending: %w", err)
	}

	// Convert to proto
	now := time.Now().UnixMilli()
	resp := &flov1.ListPendingResponse{
		Entries: make([]*flov1.PendingEntry, 0, len(leases)),
	}

	for _, lease := range leases {
		entry := &flov1.PendingEntry{
			Id:               lease.MessageID,
			ConsumerId:       lease.ConsumerID,
			DeliveryCount:    lease.DeliveryCount,
			LastDeliveryMs:   lease.LastDeliveryMs,
			LeaseExpiresAtMs: lease.ExpiresAtMs,
			IdleMs:           now - lease.LastDeliveryMs,
		}
		resp.Entries = append(resp.Entries, entry)
	}

	return resp, nil
}

// Claim manually reassigns messages to a different consumer.
func (s *Service) Claim(ctx context.Context, req *flov1.ClaimRequest) (*flov1.ClaimResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}
	if req.NewConsumerId == "" {
		return nil, fmt.Errorf("new_consumer_id required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Create lease manager
	leaseMgr := workqueue.NewLeaseManager(s.rt.DB(), req.Namespace, req.Name)

	// Lease duration
	leaseMs := req.LeaseMs
	if leaseMs == 0 {
		leaseMs = s.defaultLeaseMs
	}

	// Claim each message
	claimed := int32(0)
	for _, idBytes := range req.Ids {
		err := leaseMgr.ClaimLease(ctx, req.Group, idBytes, req.NewConsumerId, leaseMs)
		if err != nil {
			s.logger.Warn("failed to claim message",
				logpkg.F("id", fmt.Sprintf("%x", idBytes)),
				logpkg.Err(err),
			)
			continue
		}
		claimed++
	}

	s.logger.Info("claimed messages",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("group", req.Group),
		logpkg.F("new_consumer_id", req.NewConsumerId),
		logpkg.F("requested", len(req.Ids)),
		logpkg.F("claimed", claimed),
	)

	return &flov1.ClaimResponse{
		ClaimedCount: claimed,
	}, nil
}

// ============================================================================
// Admin & Stats
// ============================================================================

// GetWorkQueueStats returns queue statistics.
func (s *Service) GetWorkQueueStats(ctx context.Context, req *flov1.GetWorkQueueStatsRequest) (*flov1.GetWorkQueueStatsResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Aggregate stats across all partitions
	var totalReady int64 = 0
	var totalPending int64 = 0
	var totalCompleted int64 = 0

	// Count ready messages across all partitions
	for p := int32(0); p < s.defaultPartitions; p++ {
		q, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, uint32(p))
		if err == nil {
			// List ready messages (limit to max to get count)
			readyMsgs, _ := q.ListQueuedMessages(ctx, 10000, false)
			totalReady += int64(len(readyMsgs))
		}
	}

	// Count pending messages in PEL (across all groups or specific group)
	leaseMgr := workqueue.NewLeaseManager(s.rt.DB(), req.Namespace, req.Name)

	// If specific group requested
	if req.Group != "" {
		pendingEntries, err := leaseMgr.ListPending(ctx, req.Group, "", 10000)
		if err == nil {
			totalPending = int64(len(pendingEntries))
		}
	} else {
		// Count across all groups (scan lease keys)
		// For now, just scan default group as an approximation
		pendingEntries, err := leaseMgr.ListPending(ctx, "default", "", 10000)
		if err == nil {
			totalPending = int64(len(pendingEntries))
		}
	}

	// Count completed messages across all partitions
	completedMgr := workqueue.NewCompletedManager(s.rt.DB(), req.Namespace, req.Name)
	for p := int32(0); p < s.defaultPartitions; p++ {
		completedStats, err := completedMgr.GetStats(uint32(p))
		if err == nil && completedStats != nil {
			totalCompleted += int64(completedStats.Count)
		}
	}

	// Count retry messages (delayed messages waiting for retry)
	var totalRetry int64 = 0
	delayPrefix := fmt.Sprintf("ns/%s/wq/%s/delay_idx/", req.Namespace, req.Name)
	retryIter, err := s.rt.DB().NewIter(&pebble.IterOptions{
		LowerBound: []byte(delayPrefix),
		UpperBound: []byte(delayPrefix + "\xff"),
	})
	if err == nil {
		defer retryIter.Close()
		for retryIter.First(); retryIter.Valid(); retryIter.Next() {
			totalRetry++
		}
	}

	// Count DLQ messages (permanently failed)
	var totalDLQ int64 = 0
	if req.Group != "" {
		// Count for specific group
		dlqPrefix := fmt.Sprintf("ns/%s/wq/%s/dlq/%s/", req.Namespace, req.Name, req.Group)
		dlqIter, err := s.rt.DB().NewIter(&pebble.IterOptions{
			LowerBound: []byte(dlqPrefix),
			UpperBound: []byte(dlqPrefix + "\xff"),
		})
		if err == nil {
			defer dlqIter.Close()
			for dlqIter.First(); dlqIter.Valid(); dlqIter.Next() {
				totalDLQ++
			}
		}
	} else {
		// Count across all groups (scan all DLQ keys)
		dlqPrefix := fmt.Sprintf("ns/%s/wq/%s/dlq/", req.Namespace, req.Name)
		dlqIter, err := s.rt.DB().NewIter(&pebble.IterOptions{
			LowerBound: []byte(dlqPrefix),
			UpperBound: []byte(dlqPrefix + "\xff"),
		})
		if err == nil {
			defer dlqIter.Close()
			for dlqIter.First(); dlqIter.Valid(); dlqIter.Next() {
				totalDLQ++
			}
		}
	}

	// Total failed = DLQ (permanently failed messages)
	totalFailed := totalDLQ

	// Build response
	resp := &flov1.GetWorkQueueStatsResponse{
		TotalEnqueued:  totalReady + totalPending + totalCompleted + totalDLQ, // Include DLQ in total
		TotalCompleted: totalCompleted,
		TotalFailed:    totalFailed,
		PendingCount:   totalReady,   // Ready = Pending (waiting to be dequeued)
		InFlightCount:  totalPending, // In-flight = PEL
		RetryCount:     totalRetry,   // Delayed messages waiting for retry
		DlqCount:       totalDLQ,     // Permanently failed messages
		Groups:         make([]*flov1.GroupStats, 0),
	}

	return resp, nil
}

// ListDLQMessages returns messages in the dead letter queue.
func (s *Service) ListDLQMessages(ctx context.Context, req *flov1.ListWorkQueueDLQRequest) (*flov1.ListWorkQueueDLQResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}
	if req.Group == "" {
		return nil, fmt.Errorf("group required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Get limit (default 100, max 1000)
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// Scan DLQ keys for the group
	dlqPrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/dlq/%s/", req.Namespace, req.Name, req.Group))
	dlqHi := append(append([]byte{}, dlqPrefix...), 0xFF)

	it, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: dlqPrefix, UpperBound: dlqHi})
	if err != nil {
		return nil, fmt.Errorf("failed to create DLQ iterator: %w", err)
	}
	defer func() { _ = it.Close() }()

	entries := make([]*flov1.DLQEntry, 0, limit)
	count := 0

	for ok := it.First(); ok && count < limit; ok = it.Next() {
		k := it.Key()
		v := it.Value()

		// Extract sequence from key
		// DLQ key format: ns/<namespace>/wq/<name>/dlq/<group>/<seq>
		parts := bytes.Split(k, []byte{'/'})
		if len(parts) < 6 {
			continue
		}

		seqStr := string(parts[len(parts)-1])
		seq, err := strconv.ParseUint(seqStr, 10, 64)
		if err != nil {
			continue
		}

		// Generate message ID from sequence
		var msgID [16]byte
		binary.BigEndian.PutUint64(msgID[8:], seq)

		// Decode DLQ entry value (contains failure info)
		// For now, just return basic info
		entry := &flov1.DLQEntry{
			Id:            msgID[:],
			Seq:           seq,
			Partition:     0,                      // TODO: Track partition in DLQ
			FailedAtMs:    time.Now().UnixMilli(), // TODO: Store actual failure time
			DeliveryCount: 5,                      // TODO: Store actual delivery count
			LastError:     string(v),              // Error message stored in value
			Group:         req.Group,
		}

		entries = append(entries, entry)
		count++
	}

	return &flov1.ListWorkQueueDLQResponse{
		Entries: entries,
	}, nil
}

// ListCompleted returns recently completed messages with execution metadata.
func (s *Service) ListCompleted(ctx context.Context, req *flov1.ListCompletedRequest) (*flov1.ListCompletedResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Get partition (default to 0)
	partition := uint32(req.Partition)
	if partition < 0 {
		partition = 0
	}

	// Get limit (default 100, max 1000)
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// List completed messages
	completedMgr := workqueue.NewCompletedManager(s.rt.DB(), req.Namespace, req.Name)
	entries, err := completedMgr.ListCompleted(ctx, partition, limit, req.Group)
	if err != nil {
		return nil, fmt.Errorf("list completed: %w", err)
	}

	// Convert to proto
	protoEntries := make([]*flov1.CompletedEntry, 0, len(entries))
	for _, entry := range entries {
		protoEntry := &flov1.CompletedEntry{
			Id:            entry.ID,
			Seq:           entry.Seq,
			Partition:     entry.Partition,
			Group:         entry.Group,
			ConsumerId:    entry.ConsumerID,
			EnqueuedAtMs:  entry.EnqueuedAtMs,
			DequeuedAtMs:  entry.DequeuedAtMs,
			CompletedAtMs: entry.CompletedAtMs,
			DurationMs:    entry.Duration,
			DeliveryCount: entry.DeliveryCount,
			PayloadSize:   entry.PayloadSize,
			Headers:       entry.Headers,
		}
		protoEntries = append(protoEntries, protoEntry)
	}

	return &flov1.ListCompletedResponse{
		Entries: protoEntries,
	}, nil
}

// ListReadyMessages returns messages waiting in the priority queue (ready to be dequeued).
func (s *Service) ListReadyMessages(ctx context.Context, req *flov1.ListReadyMessagesRequest) (*flov1.ListReadyMessagesResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	// Get partition (default to 0)
	partition := uint32(req.Partition)
	if partition < 0 {
		partition = 0
	}

	// Get limit (default 100, max 1000)
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	// Open queue for this partition
	q, err := workqueue.OpenQueue(s.rt.DB(), req.Namespace, req.Name, partition)
	if err != nil {
		return nil, fmt.Errorf("open queue: %w", err)
	}

	// List queued messages
	messages, err := q.ListQueuedMessages(ctx, limit, req.IncludePayload)
	if err != nil {
		return nil, fmt.Errorf("list queued messages: %w", err)
	}

	// Convert to proto
	protoMessages := make([]*flov1.ReadyMessage, 0, len(messages))
	for _, msg := range messages {
		// Decode headers
		headers := decodeHeadersFromBytes(msg.Header)

		protoMsg := &flov1.ReadyMessage{
			Id:           msg.ID,
			Seq:          msg.Seq,
			Partition:    msg.Partition,
			Priority:     msg.Priority,
			EnqueuedAtMs: msg.EnqueuedAtMs,
			DelayUntilMs: msg.DelayUntilMs,
			Headers:      headers,
			PayloadSize:  msg.PayloadSize,
		}

		// Include payload if requested
		if req.IncludePayload && msg.Payload != nil {
			protoMsg.Payload = msg.Payload
		}

		protoMessages = append(protoMessages, protoMsg)
	}

	return &flov1.ListReadyMessagesResponse{
		Messages: protoMessages,
	}, nil
}

// Flush clears queues (for testing/cleanup).
func (s *Service) Flush(ctx context.Context, req *flov1.FlushWorkQueueRequest) (*flov1.FlushWorkQueueResponse, error) {
	if req.Namespace == "" {
		return nil, fmt.Errorf("namespace required")
	}
	if req.Name == "" {
		return nil, fmt.Errorf("name required")
	}

	// Ensure namespace exists
	_, err := s.rt.EnsureNamespace(req.Namespace)
	if err != nil {
		return nil, fmt.Errorf("ensure namespace: %w", err)
	}

	flushedCount := int64(0)

	// Flush priority queue and messages for all partitions
	for p := int32(0); p < s.defaultPartitions; p++ {
		// Clear priority index
		prioPrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/p%d/prio/", req.Namespace, req.Name, p))
		prioHi := append(append([]byte{}, prioPrefix...), 0xFF)

		it, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: prioPrefix, UpperBound: prioHi})
		if err == nil {
			b := s.rt.DB().NewBatch()
			for ok := it.First(); ok; ok = it.Next() {
				_ = b.Delete(it.Key(), nil)
				flushedCount++
			}
			_ = it.Close()
			_ = s.rt.DB().CommitBatch(ctx, b)
		}

		// Clear message data
		msgPrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/p%d/msg/", req.Namespace, req.Name, p))
		msgHi := append(append([]byte{}, msgPrefix...), 0xFF)

		it2, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: msgPrefix, UpperBound: msgHi})
		if err == nil {
			b := s.rt.DB().NewBatch()
			for ok := it2.First(); ok; ok = it2.Next() {
				_ = b.Delete(it2.Key(), nil)
			}
			_ = it2.Close()
			_ = s.rt.DB().CommitBatch(ctx, b)
		}
	}

	// Flush PEL (Pending Entries List) if group specified or all groups
	leasePrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/lease/", req.Namespace, req.Name))
	if req.Group != "" {
		leasePrefix = []byte(fmt.Sprintf("ns/%s/wq/%s/lease/%s/", req.Namespace, req.Name, req.Group))
	}
	leaseHi := append(append([]byte{}, leasePrefix...), 0xFF)

	it, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: leasePrefix, UpperBound: leaseHi})
	if err == nil {
		b := s.rt.DB().NewBatch()
		for ok := it.First(); ok; ok = it.Next() {
			_ = b.Delete(it.Key(), nil)
			flushedCount++
		}
		_ = it.Close()
		_ = s.rt.DB().CommitBatch(ctx, b)
	}

	// Flush DLQ if requested
	if req.FlushDlq {
		dlqPrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/dlq/", req.Namespace, req.Name))
		if req.Group != "" {
			dlqPrefix = []byte(fmt.Sprintf("ns/%s/wq/%s/dlq/%s/", req.Namespace, req.Name, req.Group))
		}
		dlqHi := append(append([]byte{}, dlqPrefix...), 0xFF)

		itDLQ, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: dlqPrefix, UpperBound: dlqHi})
		if err == nil {
			b := s.rt.DB().NewBatch()
			for ok := itDLQ.First(); ok; ok = itDLQ.Next() {
				_ = b.Delete(itDLQ.Key(), nil)
				flushedCount++
			}
			_ = itDLQ.Close()
			_ = s.rt.DB().CommitBatch(ctx, b)
		}
	}

	// Flush completed entries
	completedPrefix := []byte(fmt.Sprintf("ns/%s/wq/%s/completed/", req.Namespace, req.Name))
	if req.Group != "" {
		completedPrefix = []byte(fmt.Sprintf("ns/%s/wq/%s/completed/%s/", req.Namespace, req.Name, req.Group))
	}
	completedHi := append(append([]byte{}, completedPrefix...), 0xFF)

	itCompleted, err := s.rt.DB().NewIter(&pebble.IterOptions{LowerBound: completedPrefix, UpperBound: completedHi})
	if err == nil {
		b := s.rt.DB().NewBatch()
		for ok := itCompleted.First(); ok; ok = itCompleted.Next() {
			_ = b.Delete(itCompleted.Key(), nil)
			flushedCount++
		}
		_ = itCompleted.Close()
		_ = s.rt.DB().CommitBatch(ctx, b)
	}

	s.logger.Info("workqueue flushed",
		logpkg.F("namespace", req.Namespace),
		logpkg.F("name", req.Name),
		logpkg.F("group", req.Group),
		logpkg.F("flush_dlq", req.FlushDlq),
		logpkg.F("flushed_count", flushedCount),
	)

	return &flov1.FlushWorkQueueResponse{
		FlushedCount: flushedCount,
	}, nil
}

// ============================================================================
// Helper Functions
// ============================================================================

// selectPartition determines which partition to use based on key.
func (s *Service) selectPartition(key string, partitions int32) int32 {
	if key == "" {
		return 0
	}
	hash := crc32.ChecksumIEEE([]byte(key))
	return int32(hash % uint32(partitions))
}

// encodeHeaders encodes headers into a byte slice.
func encodeHeaders(headers map[string]string) ([]byte, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	// Simple encoding: key1\0value1\0key2\0value2\0...
	var result []byte
	for k, v := range headers {
		result = append(result, []byte(k)...)
		result = append(result, 0)
		result = append(result, []byte(v)...)
		result = append(result, 0)
	}
	return result, nil
}

// decodeMessage splits encoded message into headers and payload.
// decodeHeadersFromBytes decodes headers from a byte slice.
func decodeHeadersFromBytes(headerBytes []byte) map[string]string {
	headers := make(map[string]string)
	if len(headerBytes) == 0 {
		return headers
	}

	// Parse null-delimited key-value pairs
	parts := []string{}
	start := 0
	for i, b := range headerBytes {
		if b == 0 {
			parts = append(parts, string(headerBytes[start:i]))
			start = i + 1
		}
	}

	// Pair up keys and values
	for i := 0; i+1 < len(parts); i += 2 {
		headers[parts[i]] = parts[i+1]
	}

	return headers
}

// Metrics computes time-series metrics for a workqueue.
// For now, this returns simulated data based on current stats.
// TODO: Implement proper time-series storage for accurate historical metrics.
func (s *Service) Metrics(ctx context.Context, ns, name string, metric Metric, startMs, endMs, stepMs int64) ([]Series, error) {
	// Ensure namespace exists
	metaNS, err := s.rt.EnsureNamespace(ns)
	if err != nil {
		return nil, err
	}

	// Get current stats to derive approximate metrics
	stats, err := s.GetWorkQueueStats(ctx, &flov1.GetWorkQueueStatsRequest{
		Namespace: ns,
		Name:      name,
	})
	if err != nil {
		return nil, err
	}

	// Calculate number of buckets
	bucketCount := int((endMs - startMs) / stepMs)
	if bucketCount < 1 {
		bucketCount = 1
	}
	if bucketCount > 500 {
		bucketCount = 500
		stepMs = (endMs - startMs) / int64(bucketCount)
	}

	buckets := make([]float64, bucketCount)

	// For MVP, generate approximate time-series from current stats
	// In a real implementation, we would store counters over time
	switch metric {
	case MetricEnqueueCount:
		// Distribute total enqueued across time window
		avg := float64(stats.TotalEnqueued) / float64(bucketCount)
		for i := range buckets {
			buckets[i] = avg
		}
	case MetricDequeueCount:
		// Distribute total dequeued (completed + failed + in_flight) across time window
		totalDequeued := stats.TotalCompleted + stats.TotalFailed + stats.InFlightCount
		avg := float64(totalDequeued) / float64(bucketCount)
		for i := range buckets {
			buckets[i] = avg
		}
	case MetricCompleteCount:
		// Distribute total completed across time window
		avg := float64(stats.TotalCompleted) / float64(bucketCount)
		for i := range buckets {
			buckets[i] = avg
		}
	case MetricFailCount:
		// Distribute total failed across time window
		avg := float64(stats.TotalFailed) / float64(bucketCount)
		for i := range buckets {
			buckets[i] = avg
		}
	case MetricEnqueueRate:
		// Convert counts to rate (events/sec)
		seconds := float64(stepMs) / 1000.0
		avg := float64(stats.TotalEnqueued) / float64(bucketCount) / seconds
		for i := range buckets {
			buckets[i] = avg
		}
	case MetricDequeueRate:
		// Convert counts to rate (events/sec)
		seconds := float64(stepMs) / 1000.0
		totalDequeued := stats.TotalCompleted + stats.TotalFailed + stats.InFlightCount
		avg := float64(totalDequeued) / float64(bucketCount) / seconds
		for i := range buckets {
			buckets[i] = avg
		}
	case MetricCompleteRate:
		// Convert counts to rate (events/sec)
		seconds := float64(stepMs) / 1000.0
		avg := float64(stats.TotalCompleted) / float64(bucketCount) / seconds
		for i := range buckets {
			buckets[i] = avg
		}
	case MetricQueueDepth:
		// Use current pending count as approximate queue depth
		for i := range buckets {
			buckets[i] = float64(stats.PendingCount)
		}
	default:
		return nil, fmt.Errorf("unsupported metric: %s", metric)
	}

	// Build points from buckets
	points := make([][2]float64, bucketCount)
	for i := 0; i < bucketCount; i++ {
		t := startMs + int64(i)*stepMs
		points[i] = [2]float64{float64(t), buckets[i]}
	}

	return []Series{{Label: metaNS.Name + "/" + name, Points: points}}, nil
}
