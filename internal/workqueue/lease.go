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

// LeaseManager handles lease acquisition, extension, and expiry for WorkQueue messages.
type LeaseManager struct {
	db        *pebblestore.DB
	namespace string
	name      string
}

// Lease represents an active lease on a message.
type Lease struct {
	MessageID      []byte
	ConsumerID     string
	Group          string
	ExpiresAtMs    int64
	DeliveryCount  int32
	LastDeliveryMs int64
}

// NewLeaseManager creates a new LeaseManager.
func NewLeaseManager(db *pebblestore.DB, namespace, name string) *LeaseManager {
	return &LeaseManager{
		db:        db,
		namespace: namespace,
		name:      name,
	}
}

// AcquireLease attempts to acquire a lease on a message.
// Returns error if message is already leased and not expired.
func (lm *LeaseManager) AcquireLease(ctx context.Context, group string, msgID []byte, consumerID string, leaseMs int64) (*Lease, error) {
	now := time.Now().UnixMilli()
	expiresAt := now + leaseMs

	// Check if message already has an active lease
	leaseKey := leaseKey(lm.namespace, lm.name, group, msgID)
	existing, err := lm.db.Get(leaseKey)

	if err == nil && len(existing) > 0 {
		// Lease exists, check if expired
		var existingLease Lease
		if err := json.Unmarshal(existing, &existingLease); err == nil {
			if existingLease.ExpiresAtMs > now {
				// Lease still active
				return nil, fmt.Errorf("message already leased by %s until %d", existingLease.ConsumerID, existingLease.ExpiresAtMs)
			}
			// Lease expired, we can acquire (increment delivery count)
		}
	}

	// Load existing delivery count
	deliveryCount := int32(1)
	if existing != nil && len(existing) > 0 {
		var prev Lease
		if json.Unmarshal(existing, &prev) == nil {
			deliveryCount = prev.DeliveryCount + 1
		}
	}

	lease := &Lease{
		MessageID:      msgID,
		ConsumerID:     consumerID,
		Group:          group,
		ExpiresAtMs:    expiresAt,
		DeliveryCount:  deliveryCount,
		LastDeliveryMs: now,
	}

	// Write lease
	leaseData, err := json.Marshal(lease)
	if err != nil {
		return nil, fmt.Errorf("marshal lease: %w", err)
	}

	batch := lm.db.NewBatch()
	defer batch.Close()

	// Write lease
	if err := batch.Set(leaseKey, leaseData, pebble.Sync); err != nil {
		return nil, fmt.Errorf("write lease: %w", err)
	}

	// Write lease index for expiry scanning
	leaseIdxKey := leaseIndexKey(lm.namespace, lm.name, group, expiresAt, msgID)
	if err := batch.Set(leaseIdxKey, msgID, pebble.Sync); err != nil {
		return nil, fmt.Errorf("write lease index: %w", err)
	}

	// Update PEL (Pending Entries List)
	pelK := pelKey(lm.namespace, lm.name, group, consumerID, msgID)
	pelData, _ := json.Marshal(map[string]interface{}{
		"deliveries": deliveryCount,
		"last_ms":    now,
		"expires_ms": expiresAt,
	})
	if err := batch.Set(pelK, pelData, pebble.Sync); err != nil {
		return nil, fmt.Errorf("write PEL: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return nil, fmt.Errorf("commit lease: %w", err)
	}

	return lease, nil
}

// ExtendLease extends an existing lease.
func (lm *LeaseManager) ExtendLease(ctx context.Context, group string, msgID []byte, consumerID string, extensionMs int64) (int64, error) {
	now := time.Now().UnixMilli()
	leaseKey := leaseKey(lm.namespace, lm.name, group, msgID)

	// Load existing lease
	existing, err := lm.db.Get(leaseKey)
	if err != nil {
		return 0, fmt.Errorf("lease not found: %w", err)
	}

	var lease Lease
	if err := json.Unmarshal(existing, &lease); err != nil {
		return 0, fmt.Errorf("unmarshal lease: %w", err)
	}

	// Verify consumer owns this lease
	if lease.ConsumerID != consumerID {
		return 0, fmt.Errorf("lease owned by %s, not %s", lease.ConsumerID, consumerID)
	}

	// Check if already expired
	if lease.ExpiresAtMs < now {
		return 0, fmt.Errorf("lease expired at %d", lease.ExpiresAtMs)
	}

	// Extend lease
	newExpiresAt := now + extensionMs
	oldExpiresAt := lease.ExpiresAtMs
	lease.ExpiresAtMs = newExpiresAt

	// Write updated lease
	leaseData, err := json.Marshal(lease)
	if err != nil {
		return 0, fmt.Errorf("marshal lease: %w", err)
	}

	batch := lm.db.NewBatch()
	defer batch.Close()

	// Update lease
	if err := batch.Set(leaseKey, leaseData, pebble.Sync); err != nil {
		return 0, fmt.Errorf("write lease: %w", err)
	}

	// Remove old lease index entry
	oldLeaseIdxKey := leaseIndexKey(lm.namespace, lm.name, group, oldExpiresAt, msgID)
	if err := batch.Delete(oldLeaseIdxKey, pebble.Sync); err != nil {
		return 0, fmt.Errorf("delete old lease index: %w", err)
	}

	// Write new lease index entry
	newLeaseIdxKey := leaseIndexKey(lm.namespace, lm.name, group, newExpiresAt, msgID)
	if err := batch.Set(newLeaseIdxKey, msgID, pebble.Sync); err != nil {
		return 0, fmt.Errorf("write new lease index: %w", err)
	}

	// Update PEL
	pelK := pelKey(lm.namespace, lm.name, group, consumerID, msgID)
	pelData, _ := json.Marshal(map[string]interface{}{
		"deliveries": lease.DeliveryCount,
		"last_ms":    lease.LastDeliveryMs,
		"expires_ms": newExpiresAt,
	})
	if err := batch.Set(pelK, pelData, pebble.Sync); err != nil {
		return 0, fmt.Errorf("write PEL: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return 0, fmt.Errorf("commit lease extension: %w", err)
	}

	return newExpiresAt, nil
}

// ReleaseLease removes a lease (on Complete or after Fail processing).
func (lm *LeaseManager) ReleaseLease(ctx context.Context, group string, msgID []byte, consumerID string) error {
	leaseKey := leaseKey(lm.namespace, lm.name, group, msgID)

	// Load existing lease to get expiry time
	existing, err := lm.db.Get(leaseKey)
	if err != nil {
		// Already released or never existed
		return nil
	}

	var lease Lease
	if err := json.Unmarshal(existing, &lease); err != nil {
		return fmt.Errorf("unmarshal lease: %w", err)
	}

	// Verify ownership (optional - could allow force release)
	if lease.ConsumerID != consumerID {
		return fmt.Errorf("lease owned by %s, not %s", lease.ConsumerID, consumerID)
	}

	batch := lm.db.NewBatch()
	defer batch.Close()

	// Delete lease
	if err := batch.Delete(leaseKey, pebble.Sync); err != nil {
		return fmt.Errorf("delete lease: %w", err)
	}

	// Delete lease index
	leaseIdxKey := leaseIndexKey(lm.namespace, lm.name, group, lease.ExpiresAtMs, msgID)
	if err := batch.Delete(leaseIdxKey, pebble.Sync); err != nil {
		return fmt.Errorf("delete lease index: %w", err)
	}

	// Delete PEL entry
	pelK := pelKey(lm.namespace, lm.name, group, consumerID, msgID)
	if err := batch.Delete(pelK, pebble.Sync); err != nil {
		return fmt.Errorf("delete PEL: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit lease release: %w", err)
	}

	return nil
}

// GetLease retrieves the current lease for a message, if any.
func (lm *LeaseManager) GetLease(ctx context.Context, group string, msgID []byte) (*Lease, error) {
	leaseKey := leaseKey(lm.namespace, lm.name, group, msgID)
	data, err := lm.db.Get(leaseKey)
	if err != nil {
		return nil, fmt.Errorf("lease not found: %w", err)
	}

	var lease Lease
	if err := json.Unmarshal(data, &lease); err != nil {
		return nil, fmt.Errorf("unmarshal lease: %w", err)
	}

	return &lease, nil
}

// ListExpiredLeases returns leases that have expired.
// Used by auto-claim scanner.
func (lm *LeaseManager) ListExpiredLeases(ctx context.Context, group string, limit int) ([]*Lease, error) {
	now := time.Now().UnixMilli()
	prefix := workQueuePrefix(lm.namespace, lm.name) + prefixLeaseIdx + group + "/"

	iter, err := lm.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	var leases []*Lease
	for iter.First(); iter.Valid() && len(leases) < limit; iter.Next() {
		key := iter.Key()

		// Extract expiry time from key
		// Format: prefix + expiry(8B) + msgID
		if len(key) < len(prefix)+8 {
			continue
		}

		expiryMs := int64(binary.BigEndian.Uint64(key[len(prefix) : len(prefix)+8]))
		if expiryMs > now {
			// Not expired yet, and since index is sorted, we're done
			break
		}

		// Extract message ID
		msgID := key[len(prefix)+8:]

		// Load full lease
		lease, err := lm.GetLease(ctx, group, msgID)
		if err != nil {
			continue // Skip if can't load
		}

		leases = append(leases, lease)
	}

	return leases, nil
}

// ListPending returns all pending (leased) messages for a group or consumer.
func (lm *LeaseManager) ListPending(ctx context.Context, group, consumerID string, limit int) ([]*Lease, error) {
	var prefix string
	if consumerID != "" {
		// Specific consumer
		prefix = pelConsumerPrefix(lm.namespace, lm.name, group, consumerID)
	} else {
		// All consumers in group
		prefix = pelPrefix(lm.namespace, lm.name, group)
	}

	iter, err := lm.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefix),
		UpperBound: []byte(prefix + "\xff"),
	})
	if err != nil {
		return nil, fmt.Errorf("create iterator: %w", err)
	}
	defer iter.Close()

	var leases []*Lease
	for iter.First(); iter.Valid() && len(leases) < limit; iter.Next() {
		// PEL key format: prefix + consumerID + "/" + msgID
		// We need to extract msgID and load the full lease
		key := iter.Key()

		// Find last "/" to get msgID
		lastSlash := -1
		for i := len(key) - 1; i >= 0; i-- {
			if key[i] == '/' {
				lastSlash = i
				break
			}
		}
		if lastSlash == -1 {
			continue
		}

		msgID := key[lastSlash+1:]

		// Load full lease
		lease, err := lm.GetLease(ctx, group, msgID)
		if err != nil {
			continue
		}

		leases = append(leases, lease)
	}

	return leases, nil
}

// ClaimLease reassigns a lease to a new consumer.
// Used for manual claims and auto-claim.
func (lm *LeaseManager) ClaimLease(ctx context.Context, group string, msgID []byte, newConsumerID string, leaseMs int64) error {
	// Load existing lease
	leaseKey := leaseKey(lm.namespace, lm.name, group, msgID)
	existing, err := lm.db.Get(leaseKey)
	if err != nil {
		return fmt.Errorf("lease not found: %w", err)
	}

	var oldLease Lease
	if err := json.Unmarshal(existing, &oldLease); err != nil {
		return fmt.Errorf("unmarshal lease: %w", err)
	}

	oldConsumerID := oldLease.ConsumerID
	now := time.Now().UnixMilli()
	newExpiresAt := now + leaseMs

	// Create new lease
	newLease := &Lease{
		MessageID:      msgID,
		ConsumerID:     newConsumerID,
		Group:          group,
		ExpiresAtMs:    newExpiresAt,
		DeliveryCount:  oldLease.DeliveryCount + 1, // Increment delivery count
		LastDeliveryMs: now,
	}

	leaseData, err := json.Marshal(newLease)
	if err != nil {
		return fmt.Errorf("marshal lease: %w", err)
	}

	batch := lm.db.NewBatch()
	defer batch.Close()

	// Update lease
	if err := batch.Set(leaseKey, leaseData, pebble.Sync); err != nil {
		return fmt.Errorf("write lease: %w", err)
	}

	// Remove old lease index
	oldLeaseIdxKey := leaseIndexKey(lm.namespace, lm.name, group, oldLease.ExpiresAtMs, msgID)
	if err := batch.Delete(oldLeaseIdxKey, pebble.Sync); err != nil {
		return fmt.Errorf("delete old lease index: %w", err)
	}

	// Write new lease index
	newLeaseIdxKey := leaseIndexKey(lm.namespace, lm.name, group, newExpiresAt, msgID)
	if err := batch.Set(newLeaseIdxKey, msgID, pebble.Sync); err != nil {
		return fmt.Errorf("write new lease index: %w", err)
	}

	// Remove old PEL entry
	oldPelK := pelKey(lm.namespace, lm.name, group, oldConsumerID, msgID)
	if err := batch.Delete(oldPelK, pebble.Sync); err != nil {
		return fmt.Errorf("delete old PEL: %w", err)
	}

	// Write new PEL entry
	newPelK := pelKey(lm.namespace, lm.name, group, newConsumerID, msgID)
	pelData, _ := json.Marshal(map[string]interface{}{
		"deliveries": newLease.DeliveryCount,
		"last_ms":    now,
		"expires_ms": newExpiresAt,
	})
	if err := batch.Set(newPelK, pelData, pebble.Sync); err != nil {
		return fmt.Errorf("write new PEL: %w", err)
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("commit claim: %w", err)
	}

	return nil
}
