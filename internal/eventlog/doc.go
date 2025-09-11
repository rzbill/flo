// Package eventlog implements Flo's internal append-only EventLog.
//
// # Overview
//
// The log is partitioned by namespace/topic/partition and persisted in Pebble.
// Keys are lexicographically ordered for efficient range scans:
//   - ns/{ns}/log/{topic}/{part_be4}/m           (partition metadata: lastSeq)
//   - ns/{ns}/log/{topic}/{part_be4}/e/{seq_be8} (entries)
//   - ns/{ns}/cursor/{topic}/{group}/{part_be4}  (durable group cursors)
//
// Records are stored as: headerLen(4B BE) | header | payload | crc32c(header|payload).
//
// API surface (internal)
//
//	l, _ := OpenLog(db, ns, topic, part)
//	// Append a batch atomically; returns assigned seq numbers
//	seqs, _ := l.Append(ctx, []AppendRecord{{Header: h, Payload: p}})
//
//	// Read forward/reverse with an optional start token and limit
//	items, next := l.Read(ReadOptions{Start: tokenFromSeq(seqs[0]), Limit: 100})
//	_ = next // resume position
//
//	// Blocking wait/notify
//	woke := l.WaitForAppend(200 * time.Millisecond)
//	_ = woke
//
//	// Durable consumer cursor commits (idempotent, no regression)
//	_ = l.CommitCursor("groupA", tokenFromSeq(seqs[len(seqs)-1]))
//
//	// Trims (approximate):
//	//  - by age using header timestamps
//	//  - by total bytes budget
//	// Both support batching and throttling and emit archiver ranges via ArchiverHook
//	_, _, _ = l.TrimOlderThan(ctx, cutoffMs, 1024, 0, tsExtractor)
//	_, _ = l.TrimToMaxBytes(ctx, maxBytes, 1024, 0)
//
// # Archiver integration
//
// A minimal ArchiverHook seam is provided. When trims delete entries, the hook
// is called with a best-effort contiguous range {minSeq, maxSeq} for the batch.
// The default implementation is a no-op. Archiver components can set
// l.archiver to capture trim ranges and enqueue export jobs per FLO_ARCHIVER.md.
package eventlog
