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

// ConsumerRegistry manages consumer registration, heartbeats, and TTL.
type ConsumerRegistry struct {
	db        *pebblestore.DB
	namespace string
	name      string
	ttl       time.Duration
}

// Consumer represents a registered consumer/worker.
type Consumer struct {
	ID            string
	Group         string
	RegisteredMs  int64
	LastHeartbeat int64
	ExpiresAtMs   int64
	Metadata      map[string]string // Optional: hostname, version, etc.
}

// NewConsumerRegistry creates a new ConsumerRegistry.
func NewConsumerRegistry(db *pebblestore.DB, namespace, name string, ttl time.Duration) *ConsumerRegistry {
	if ttl == 0 {
		ttl = 15 * time.Second // Default TTL
	}
	return &ConsumerRegistry{
		db:        db,
		namespace: namespace,
		name:      name,
		ttl:       ttl,
	}
}

// Register adds or updates a consumer registration.
func (cr *ConsumerRegistry) Register(ctx context.Context, group, consumerID string, metadata map[string]string) (*Consumer, error) {
	now := time.Now().UnixMilli()
	expiresAt := now + cr.ttl.Milliseconds()

	consumer := &Consumer{
		ID:            consumerID,
		Group:         group,
		RegisteredMs:  now,
		LastHeartbeat: now,
		ExpiresAtMs:   expiresAt,
		Metadata:      metadata,
	}

	// Check if consumer already exists
	consKey := consumerKey(cr.namespace, cr.name, group, consumerID)
	existing, err := cr.db.Get(consKey)
	if err == nil && len(existing) > 0 {
		// Update existing consumer
		var existingConsumer Consumer
		if json.Unmarshal(existing, &existingConsumer) == nil {
			consumer.RegisteredMs = existingConsumer.RegisteredMs // Keep original registration time
		}
	}

	consData, err := json.Marshal(consumer)
	if err != nil {
		return nil, fmt.Errorf("marshal consumer: %w", err)
	}

	batch := cr.db.NewBatch()
	defer batch.Close()

	// Write consumer
	if err := batch.Set(consKey, consData, pebble.Sync); err != nil {
		return nil, fmt.Errorf("write consumer: %w", err)
	}

	// Write consumer TTL index for expiry scanning
	consIdxKey := consumerIndexKey(cr.namespace, cr.name, group, expiresAt, consumerID)
	if err := batch.Set(consIdxKey, []byte(consumerID), pebble.Sync); err != nil {
		return nil, fmt.Errorf("write consumer index: %w", err)
	}

	// Remove old index entry if this is an update
	if existing != nil && len(existing) > 0 {
		var old Consumer
		if json.Unmarshal(existing, &old) == nil && old.ExpiresAtMs != expiresAt {
			oldIdxKey := consumerIndexKey(cr.namespace, cr.name, group, old.ExpiresAtMs, consumerID)
			_ = batch.Delete(oldIdxKey, pebble.Sync) // Best effort
		}
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, fmt.Errorf("commit consumer registration: %w", err)
	}

	return consumer, nil
}

// Heartbeat updates the consumer's last heartbeat and extends TTL.
func (cr *ConsumerRegistry) Heartbeat(ctx context.Context, group, consumerID string) (int64, error) {
	now := time.Now().UnixMilli()
	expiresAt := now + cr.ttl.Milliseconds()

	consKey := consumerKey(cr.namespace, cr.name, group, consumerID)
	existing, err := cr.db.Get(consKey)
	if err != nil {
		return 0, fmt.Errorf("consumer not found: %w", err)
	}

	var consumer Consumer
	if err := json.Unmarshal(existing, &consumer); err != nil {
		return 0, fmt.Errorf("unmarshal consumer: %w", err)
	}

	oldExpiresAt := consumer.ExpiresAtMs
	consumer.LastHeartbeat = now
	consumer.ExpiresAtMs = expiresAt

	consData, err := json.Marshal(consumer)
	if err != nil {
		return 0, fmt.Errorf("marshal consumer: %w", err)
	}

	batch := cr.db.NewBatch()
	defer batch.Close()

	// Update consumer
	if err := batch.Set(consKey, consData, pebble.Sync); err != nil {
		return 0, fmt.Errorf("write consumer: %w", err)
	}

	// Remove old index entry
	oldIdxKey := consumerIndexKey(cr.namespace, cr.name, group, oldExpiresAt, consumerID)
	if err := batch.Delete(oldIdxKey, pebble.Sync); err != nil {
		return 0, fmt.Errorf("delete old consumer index: %w", err)
	}

	// Write new index entry
	newIdxKey := consumerIndexKey(cr.namespace, cr.name, group, expiresAt, consumerID)
	if err := batch.Set(newIdxKey, []byte(consumerID), pebble.Sync); err != nil {
		return 0, fmt.Errorf("write consumer index: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, fmt.Errorf("commit heartbeat: %w", err)
	}

	return expiresAt, nil
}

// Unregister removes a consumer registration.
func (cr *ConsumerRegistry) Unregister(ctx context.Context, group, consumerID string) error {
	consKey := consumerKey(cr.namespace, cr.name, group, consumerID)

	// Load consumer to get expiry time for index cleanup
	existing, err := cr.db.Get(consKey)
	if err != nil {
		// Already unregistered
		return nil
	}

	var consumer Consumer
	if err := json.Unmarshal(existing, &consumer); err != nil {
		return fmt.Errorf("unmarshal consumer: %w", err)
	}

	batch := cr.db.NewBatch()
	defer batch.Close()

	// Delete consumer
	if err := batch.Delete(consKey, pebble.Sync); err != nil {
		return fmt.Errorf("delete consumer: %w", err)
	}

	// Delete consumer index
	consIdxKey := consumerIndexKey(cr.namespace, cr.name, group, consumer.ExpiresAtMs, consumerID)
	if err := batch.Delete(consIdxKey, pebble.Sync); err != nil {
		return fmt.Errorf("delete consumer index: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit unregister: %w", err)
	}

	return nil
}

// Get retrieves a consumer by ID.
func (cr *ConsumerRegistry) Get(ctx context.Context, group, consumerID string) (*Consumer, error) {
	consKey := consumerKey(cr.namespace, cr.name, group, consumerID)
	data, err := cr.db.Get(consKey)
	if err != nil {
		return nil, fmt.Errorf("consumer not found: %w", err)
	}

	var consumer Consumer
	if err := json.Unmarshal(data, &consumer); err != nil {
		return nil, fmt.Errorf("unmarshal consumer: %w", err)
	}

	return &consumer, nil
}

// List returns all consumers in a group.
func (cr *ConsumerRegistry) List(ctx context.Context, group string, limit int) ([]*Consumer, error) {
	prefix := consumerPrefix(cr.namespace, cr.name, group)

	iter, err := cr.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	var consumers []*Consumer
	for iter.First(); iter.Valid() && len(consumers) < limit; iter.Next() {
		var consumer Consumer
		if err := json.Unmarshal(iter.Value(), &consumer); err != nil {
			continue
		}
		consumers = append(consumers, &consumer)
	}

	return consumers, nil
}

// ListExpired returns consumers that have expired (missed heartbeats).
func (cr *ConsumerRegistry) ListExpired(ctx context.Context, group string, limit int) ([]*Consumer, error) {
	now := time.Now().UnixMilli()
	prefix := workQueuePrefix(cr.namespace, cr.name) + prefixConsIdx + group + "/"

	iter, err := cr.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	var consumers []*Consumer
	for iter.First(); iter.Valid() && len(consumers) < limit; iter.Next() {
		key := iter.Key()

		// Extract expiry time from key
		// Format: prefix + expiry(8B) + consumerID
		if len(key) < len(prefix)+8 {
			continue
		}

		expiryMs := int64(binary.BigEndian.Uint64(key[len(prefix) : len(prefix)+8]))
		if expiryMs > now {
			// Not expired yet, and since index is sorted, we're done
			break
		}

		// Extract consumer ID
		consumerID := string(key[len(prefix)+8:])

		// Load full consumer
		consumer, err := cr.Get(ctx, group, consumerID)
		if err != nil {
			continue
		}

		consumers = append(consumers, consumer)
	}

	return consumers, nil
}

// CleanupExpired removes expired consumers.
// Should be called periodically by a background sweeper.
func (cr *ConsumerRegistry) CleanupExpired(ctx context.Context, group string, limit int) (int, error) {
	expired, err := cr.ListExpired(ctx, group, limit)
	if err != nil {
		return 0, fmt.Errorf("list expired: %w", err)
	}

	count := 0
	for _, consumer := range expired {
		if err := cr.Unregister(ctx, group, consumer.ID); err != nil {
			// Log but continue
			continue
		}
		count++
	}

	return count, nil
}

// IsActive checks if a consumer is registered and not expired.
func (cr *ConsumerRegistry) IsActive(ctx context.Context, group, consumerID string) bool {
	consumer, err := cr.Get(ctx, group, consumerID)
	if err != nil {
		return false
	}

	now := time.Now().UnixMilli()
	return consumer.ExpiresAtMs > now
}

// SelectConsumer selects a consumer for auto-claim using round-robin or random strategy.
// Returns empty string if no active consumers are available.
func (cr *ConsumerRegistry) SelectConsumer(ctx context.Context, group string) (string, error) {
	consumers, err := cr.List(ctx, group, 100) // Get up to 100 consumers
	if err != nil {
		return "", fmt.Errorf("list consumers: %w", err)
	}

	// Filter active consumers
	now := time.Now().UnixMilli()
	var active []string
	for _, c := range consumers {
		if c.ExpiresAtMs > now {
			active = append(active, c.ID)
		}
	}

	if len(active) == 0 {
		return "", fmt.Errorf("no active consumers")
	}

	// Simple random selection
	// TODO: Could implement round-robin with state tracking
	idx := time.Now().UnixNano() % int64(len(active))
	return active[idx], nil
}
