package workqueue

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

// CompletedEntry represents a completed message with execution metadata.
type CompletedEntry struct {
	ID            []byte            // Message ID
	Seq           uint64            // Sequence number
	Partition     uint32            // Partition number
	Group         string            // Consumer group
	ConsumerID    string            // Consumer that processed it
	EnqueuedAtMs  int64             // When message was enqueued
	DequeuedAtMs  int64             // When message was dequeued
	CompletedAtMs int64             // When message was completed
	Duration      int64             // Processing duration (CompletedAtMs - DequeuedAtMs)
	DeliveryCount int32             // Number of delivery attempts
	PayloadSize   int32             // Size of the payload in bytes
	Headers       map[string]string // Optional headers (if configured)
}

// CompletedMeta tracks metadata for completed messages in a partition.
type CompletedMeta struct {
	FirstSeq   uint64 // Oldest completed seq in buffer
	LastSeq    uint64 // Newest completed seq in buffer
	Count      int32  // Number of completed messages
	TotalBytes int64  // Total bytes of completed messages
}

// CompletedManager manages the completed messages buffer.
type CompletedManager struct {
	db              *pebblestore.DB
	namespace       string
	name            string
	maxPerPartition int32 // Max completed messages to keep per partition
	maxAgeMs        int64 // Max age in milliseconds
}

// NewCompletedManager creates a new completed messages manager.
func NewCompletedManager(db *pebblestore.DB, namespace, name string) *CompletedManager {
	return &CompletedManager{
		db:              db,
		namespace:       namespace,
		name:            name,
		maxPerPartition: 1000,             // Default: keep last 1000 per partition
		maxAgeMs:        24 * 3600 * 1000, // Default: 24 hours
	}
}

// SetRetention configures retention limits.
func (cm *CompletedManager) SetRetention(maxMessages int32, maxAgeMs int64) {
	if maxMessages > 0 {
		cm.maxPerPartition = maxMessages
	}
	if maxAgeMs > 0 {
		cm.maxAgeMs = maxAgeMs
	}
}

// AddCompleted adds a completed message entry and enforces retention limits.
func (cm *CompletedManager) AddCompleted(ctx context.Context, entry *CompletedEntry) error {
	if entry == nil {
		return fmt.Errorf("entry is nil")
	}

	// Encode entry
	value, err := encodeCompletedEntry(entry)
	if err != nil {
		return fmt.Errorf("encode entry: %w", err)
	}

	// Write entry
	key := completedKey(cm.namespace, cm.name, entry.Partition, entry.Seq)
	batch := cm.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(key, value, nil); err != nil {
		return fmt.Errorf("set completed entry: %w", err)
	}

	// Update metadata
	if err := cm.updateMetadata(batch, entry.Partition, entry.Seq, int32(len(value))); err != nil {
		return fmt.Errorf("update metadata: %w", err)
	}

	// Commit batch
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	// Check if we need to trim
	meta, err := cm.getMetadata(entry.Partition)
	if err == nil && meta.Count > cm.maxPerPartition {
		// Trim in background (don't block completion)
		go func() {
			trimCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = cm.TrimOldest(trimCtx, entry.Partition, int(cm.maxPerPartition))
		}()
	}

	return nil
}

// ListCompleted returns completed messages for a partition in reverse order (newest first).
func (cm *CompletedManager) ListCompleted(ctx context.Context, partition uint32, limit int, group string) ([]*CompletedEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}

	prefix := completedPrefix(cm.namespace, cm.name, partition)
	hi := append(append([]byte{}, prefix...), 0xFF)

	iter, err := cm.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: hi,
	})
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	entries := make([]*CompletedEntry, 0, limit)
	now := time.Now().UnixMilli()

	// Iterate in reverse (newest first)
	for ok := iter.Last(); ok && len(entries) < limit; ok = iter.Prev() {
		value := iter.Value()
		entry, err := decodeCompletedEntry(value)
		if err != nil {
			continue
		}

		// Filter by group if specified
		if group != "" && entry.Group != group {
			continue
		}

		// Filter by age
		if cm.maxAgeMs > 0 && (now-entry.CompletedAtMs) > cm.maxAgeMs {
			continue
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

// TrimOldest removes the oldest completed messages, keeping only 'keepCount' most recent.
// Returns the trimmed entries for potential archiving.
func (cm *CompletedManager) TrimOldest(ctx context.Context, partition uint32, keepCount int) ([]*CompletedEntry, error) {
	if keepCount < 0 {
		keepCount = int(cm.maxPerPartition)
	}

	// Get all completed entries
	prefix := completedPrefix(cm.namespace, cm.name, partition)
	hi := append(append([]byte{}, prefix...), 0xFF)

	iter, err := cm.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: hi,
	})
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	// Collect all entries with their keys
	type entryWithKey struct {
		key   []byte
		entry *CompletedEntry
	}
	all := make([]entryWithKey, 0)

	for ok := iter.First(); ok; ok = iter.Next() {
		keyCopy := append([]byte(nil), iter.Key()...)
		value := iter.Value()
		entry, err := decodeCompletedEntry(value)
		if err != nil {
			continue
		}
		all = append(all, entryWithKey{key: keyCopy, entry: entry})
	}

	// If count <= keepCount, nothing to trim
	if len(all) <= keepCount {
		return nil, nil
	}

	// Trim oldest (first N entries, since they're in ascending seq order)
	toTrim := all[:len(all)-keepCount]
	trimmed := make([]*CompletedEntry, 0, len(toTrim))

	batch := cm.db.NewBatch()
	defer batch.Close()

	for _, item := range toTrim {
		if err := batch.Delete(item.key, nil); err != nil {
			return nil, fmt.Errorf("delete entry: %w", err)
		}
		trimmed = append(trimmed, item.entry)
	}

	// Update metadata
	if len(all) > len(toTrim) {
		firstSeq := all[len(toTrim)].entry.Seq
		lastSeq := all[len(all)-1].entry.Seq
		newCount := int32(keepCount)

		meta := &CompletedMeta{
			FirstSeq: firstSeq,
			LastSeq:  lastSeq,
			Count:    newCount,
		}
		metaKey := completedMetaKey(cm.namespace, cm.name, partition)
		metaValue, _ := encodeCompletedMeta(meta)
		if err := batch.Set(metaKey, metaValue, nil); err != nil {
			return nil, fmt.Errorf("update metadata: %w", err)
		}
	}

	// Commit
	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, fmt.Errorf("commit trim batch: %w", err)
	}

	return trimmed, nil
}

// GetStats returns statistics for completed messages in a partition.
func (cm *CompletedManager) GetStats(partition uint32) (*CompletedMeta, error) {
	return cm.getMetadata(partition)
}

// updateMetadata updates the completed metadata for a partition.
func (cm *CompletedManager) updateMetadata(batch *pebble.Batch, partition uint32, seq uint64, size int32) error {
	metaKey := completedMetaKey(cm.namespace, cm.name, partition)

	// Get existing metadata
	existing, err := cm.db.Get(metaKey)
	var meta *CompletedMeta
	if err == nil && len(existing) > 0 {
		meta, _ = decodeCompletedMeta(existing)
	}
	if meta == nil {
		meta = &CompletedMeta{}
	}

	// Update metadata
	if meta.Count == 0 {
		meta.FirstSeq = seq
	}
	meta.LastSeq = seq
	meta.Count++
	meta.TotalBytes += int64(size)

	// Write metadata
	value, err := encodeCompletedMeta(meta)
	if err != nil {
		return err
	}
	return batch.Set(metaKey, value, nil)
}

// getMetadata retrieves metadata for a partition.
func (cm *CompletedManager) getMetadata(partition uint32) (*CompletedMeta, error) {
	metaKey := completedMetaKey(cm.namespace, cm.name, partition)
	value, err := cm.db.Get(metaKey)
	if err != nil {
		if err == pebble.ErrNotFound {
			return &CompletedMeta{}, nil
		}
		return nil, err
	}
	return decodeCompletedMeta(value)
}

// encodeCompletedEntry serializes a CompletedEntry to bytes.
func encodeCompletedEntry(entry *CompletedEntry) ([]byte, error) {
	return json.Marshal(entry)
}

// decodeCompletedEntry deserializes bytes to a CompletedEntry.
func decodeCompletedEntry(data []byte) (*CompletedEntry, error) {
	var entry CompletedEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// encodeCompletedMeta serializes CompletedMeta to bytes (compact binary format).
func encodeCompletedMeta(meta *CompletedMeta) ([]byte, error) {
	buf := make([]byte, 8+8+4+8) // firstSeq + lastSeq + count + totalBytes
	binary.BigEndian.PutUint64(buf[0:8], meta.FirstSeq)
	binary.BigEndian.PutUint64(buf[8:16], meta.LastSeq)
	binary.BigEndian.PutUint32(buf[16:20], uint32(meta.Count))
	binary.BigEndian.PutUint64(buf[20:28], uint64(meta.TotalBytes))
	return buf, nil
}

// decodeCompletedMeta deserializes bytes to CompletedMeta.
func decodeCompletedMeta(data []byte) (*CompletedMeta, error) {
	if len(data) < 28 {
		return nil, fmt.Errorf("invalid metadata length: %d", len(data))
	}
	meta := &CompletedMeta{
		FirstSeq:   binary.BigEndian.Uint64(data[0:8]),
		LastSeq:    binary.BigEndian.Uint64(data[8:16]),
		Count:      int32(binary.BigEndian.Uint32(data[16:20])),
		TotalBytes: int64(binary.BigEndian.Uint64(data[20:28])),
	}
	return meta, nil
}
