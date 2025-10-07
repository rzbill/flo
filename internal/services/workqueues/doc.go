// Package workqueues provides the gRPC service layer for WorkQueue operations.
//
// # Overview
//
// The WorkQueues service implements one-of-N task queue semantics where each message
// is delivered to exactly one worker. This is in contrast to Streams (pub/sub) where
// messages are fanned out to all subscribers.
//
// # Core Concepts
//
//   - Lease: Temporary exclusive ownership of a message (default 30s)
//   - Consumer: A worker that dequeues and processes messages
//   - Group: Isolated queue for consumers; messages are distributed across consumers in a group
//   - PEL (Pending Entries List): In-flight messages currently leased
//   - Auto-Claim: Automatic reassignment of expired leases to active consumers
//   - DLQ (Dead Letter Queue): Failed messages after max retry attempts
//
// # Service Architecture
//
// The service is a thin gRPC layer that coordinates:
//   - workqueue.WorkQueue: Core queue operations (enqueue/dequeue/complete/fail)
//   - workqueue.LeaseManager: Lease lifecycle management
//   - workqueue.ConsumerRegistry: Consumer tracking with heartbeats
//   - workqueue.AutoClaimScanner: Background recovery of expired leases
//
// # Message Flow
//
//  1. Producer → Enqueue → Priority/Delay queues
//  2. Consumer → Dequeue → Acquire lease → Process
//  3. Consumer → Complete → Release lease → Message deleted
//  4. [OR] Consumer → Fail → Retry schedule or DLQ
//  5. [OR] Lease expires → Auto-claim → Reassign to active consumer
//
// # Retry & DLQ
//
//   - Configurable retry policy per group (exponential backoff with jitter)
//   - Failed messages retry with delay
//   - After max attempts, messages move to DLQ
//   - DLQ messages can be inspected, replayed, or flushed
//
// # Consumer Liveness
//
//   - Consumers register with metadata (capacity, labels)
//   - Heartbeat required every ~5s (TTL 15s)
//   - Expired consumers removed automatically
//   - Messages from failed consumers auto-claimed
//
// # Performance
//
//   - Priority-based dequeue (lower values first)
//   - Delayed message support (scheduled delivery)
//   - Batched dequeue (up to 100 messages)
//   - Streaming dequeue with blocking
//   - Partition-aware for parallelism
//
// # Admin Operations
//
//   - GetWorkQueueStats: Queue metrics and group stats
//   - ListPending: View in-flight messages (PEL)
//   - ListConsumers: Active workers per group
//   - ListDLQMessages: Inspect failed messages
//   - Claim: Manual message reassignment
//   - Flush: Clear queues for testing/cleanup
package workqueues
