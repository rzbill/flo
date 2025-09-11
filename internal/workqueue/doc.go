// Package workqueue defines the internal keyspace, record format, and basic
// queue operations (enqueue, dequeue with leases, extend, complete, fail) for
// Flo's WorkQueue.
//
// # Keys (namespace-scoped)
//
//   - ns/{ns}/q/{queue}/{part_be4}/msg/{seq_be8}           (message payload)
//   - ns/{ns}/q/{queue}/{part_be4}/m                      (partition metadata)
//   - ns/{ns}/q_prio/{queue}/{priority_be4}/{seq_be8}     (availability index)
//   - ns/{ns}/q_delay/{queue}/{fire_ms_be8}/{msg_id_be16} (delay index)
//   - ns/{ns}/q_lease/{queue}/{group}/{msg_id_be16}       (lease record)
//   - ns/{ns}/q_lease_idx/{queue}/{expiry_ms_be8}/{msg_id_be16} (lease expiry idx)
//   - ns/{ns}/q_dlq/{queue}/{group}/{msg_id_be16}         (dead-letter)
//
// Partition metadata stores: lastSeq (8B) | availableCount (4B) for simple
// backpressure accounting.
//
// # Records
//
// Message: headerLen(4B BE) | header | payload | crc32c(header|payload)
// Lease:   expiryMs(8B BE) | attempts(4B BE)
// Delay value: priority(4B BE) | seq(8B BE)
//
// # API (internal)
//
//	q, _ := OpenQueue(db, ns, name, part).WithOptions(QueueOptions{MaxAvailable: 10})
//	// Enqueue with priority and optional delay
//	seq, _ := q.Enqueue(ctx, hdr, body, 5 /*prio*/, 0 /*delayMs*/, 0)
//
//	// Dequeue creates leases ordered by priority (lower first)
//	msgs, _ := q.Dequeue(ctx, "groupA", 10, 30_000, 0)
//	_ = q.ExtendLease(ctx, "groupA", []uint64{msgs[0].Seq}, 30_000, 0)
//	_ = q.Complete(ctx, "groupA", []uint64{msgs[0].Seq})
//
//	// Fail with retry-after or DLQ
//	_ = q.Fail(ctx, "groupA", []uint64{seq}, 5_000 /*retryAfterMs*/, false /*toDLQ*/, 0)
//
// # Backpressure
//
// If QueueOptions.MaxAvailable > 0, Enqueue throttles when the simple
// availableCount exceeds the threshold. This is a basic mechanism intended as a
// starting point; production systems should rely on richer metrics/limits.
package workqueue
