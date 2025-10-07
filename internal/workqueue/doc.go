// Package workqueue implements a one-of-N task queue with lease-based delivery.
//
// Unlike EventLog (used by Streams for fanout pub/sub), WorkQueue ensures that
// each message is delivered to exactly one consumer in a group. This is achieved
// through:
//
// - Lease-based ownership: Messages are leased to consumers for a duration
// - Pending Entries List (PEL): Tracks in-flight messages per consumer
// - Auto-claim: Automatically reassigns stalled messages from dead consumers
// - Priority ordering: Messages can have priorities for processing order
// - Delayed delivery: Messages can be held until a specific time
// - Retry & DLQ: Failed messages retry with backoff, then move to DLQ
//
// # Keyspace
//
// All keys are prefixed with ns/{namespace}/wq/{name}/:
//
//	msg/{id}                           - Message data
//	priority_idx/{priority}/{id}       - Priority index for dequeue
//	delay_idx/{ready_at_ms}/{id}       - Delayed message index
//	lease/{group}/{id}                 - Active lease (expires_at_ms, consumer_id, deliveries)
//	lease_idx/{group}/{expires_ms}/{id}- Lease expiry index for scanning
//	pel/{group}/{consumer}/{id}        - Pending Entries List
//	retry/{group}/{ready_at_ms}/{id}   - Retry schedule
//	dlq/{group}/{id}                   - Dead Letter Queue
//	cons/{group}/{consumer_id}         - Consumer registry
//	cons_idx/{group}/{expires_ms}/{cid}- Consumer expiry index
//	groupcfg/{group}                   - Group configuration (retry policy)
//
// # Message Lifecycle
//
//  1. Enqueue: msg written, indexed by priority/delay
//  2. Dequeue: msg leased to consumer, lease written, PEL updated
//  3. Processing:
//     - Extend: lease extended via heartbeat
//     - Complete: msg deleted, lease removed, PEL cleared
//     - Fail: retry scheduled or DLQ'd, lease removed, PEL updated
//  4. Expiry: lease expires → msg available for re-dequeue
//  5. Auto-claim: stalled msgs from dead consumers reassigned
//
// # At-Least-Once Semantics
//
// Messages are delivered at-least-once. Duplicates can occur if:
// - Consumer crashes after processing but before Complete
// - Lease expires while processing
// - Network issues cause retries
//
// Consumers should be idempotent or use deduplication caches.
//
// # Comparison with Streams
//
//	| Aspect           | Streams        | WorkQueues        |
//	|------------------|----------------|-------------------|
//	| Pattern          | Pub/Sub        | Task Queue        |
//	| Delivery         | Everyone       | One-of-N          |
//	| State            | Cursors        | Leases            |
//	| Ordering         | Per partition  | Priority-based    |
//	| Use Case         | Events/Logs    | Jobs/Tasks        |
package workqueue
