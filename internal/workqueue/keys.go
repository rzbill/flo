package workqueue

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// Key prefixes for WorkQueue data structures
const (
	prefixMsg       = "msg/"          // Message data
	prefixPriority  = "priority_idx/" // Priority index
	prefixDelay     = "delay_idx/"    // Delayed message index
	prefixLease     = "lease/"        // Active leases
	prefixLeaseIdx  = "lease_idx/"    // Lease expiry index
	prefixPEL       = "pel/"          // Pending Entries List
	prefixRetry     = "retry/"        // Retry schedule
	prefixDLQ       = "dlq/"          // Dead Letter Queue
	prefixCons      = "cons/"         // Consumer registry
	prefixConsIdx   = "cons_idx/"     // Consumer expiry index
	prefixGroupCfg  = "groupcfg/"     // Group configuration
	prefixCompleted = "completed/"    // Completed messages buffer
)

// workQueuePrefix returns the base prefix for a work queue.
// Format: ns/{namespace}/wq/{name}/
func workQueuePrefix(namespace, name string) string {
	return fmt.Sprintf("ns/%s/wq/%s/", namespace, name)
}

// Partition-aware helper functions for queue.go compatibility

// MetaKey returns the metadata key for a partition.
// Format: ns/{ns}/wq/{name}/meta/{partition}
func MetaKey(namespace, name string, partition uint32) []byte {
	s := fmt.Sprintf("ns/%s/wq/%s/meta/%d", namespace, name, partition)
	return []byte(s)
}

// MsgKey returns the message key with partition and sequence.
// Format: ns/{ns}/wq/{name}/msg/{partition}/{seq}
func MsgKey(namespace, name string, partition uint32, seq uint64) []byte {
	s := fmt.Sprintf("ns/%s/wq/%s/msg/%d/%d", namespace, name, partition, seq)
	return []byte(s)
}

// PrioKey returns the priority index key.
// Format: ns/{ns}/wq/{name}/priority_idx/{priority}/{seq}
// Lower priority values are dequeued first (ascending order)
func PrioKey(namespace, name string, priority uint32, seq uint64) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixPriority

	var idBuf [16]byte
	binary.BigEndian.PutUint64(idBuf[8:], seq)

	key := make([]byte, len(prefix)+4+16)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], priority)
	copy(key[len(prefix)+4:], idBuf[:])
	return key
}

// DelayKey returns the delay index key.
// Format: ns/{ns}/wq/{name}/delay_idx/{ready_at_ms}/{id}
func DelayKey(namespace, name string, readyAtMs uint64, id [16]byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixDelay
	key := make([]byte, len(prefix)+8+16)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], readyAtMs)
	copy(key[len(prefix)+8:], id[:])
	return key
}

// LeaseKey returns the lease key.
// Format: ns/{ns}/wq/{name}/lease/{group}/{id}
func LeaseKey(namespace, name, group string, id [16]byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixLease + group + "/"
	key := make([]byte, len(prefix)+16)
	copy(key, prefix)
	copy(key[len(prefix):], id[:])
	return key
}

// LeaseIdxPrefix returns the prefix for lease index scanning.
// Format: ns/{ns}/wq/{name}/lease_idx/
func LeaseIdxPrefix(namespace, name string) []byte {
	s := workQueuePrefix(namespace, name) + prefixLeaseIdx
	return []byte(s)
}

// DelayPrefix returns the prefix for delay index scanning.
// Format: ns/{ns}/wq/{name}/delay_idx/
func DelayPrefix(namespace, name string) []byte {
	s := workQueuePrefix(namespace, name) + prefixDelay
	return []byte(s)
}

// PrioPrefix returns the prefix for priority index scanning.
// Format: ns/{ns}/wq/{name}/priority_idx/
func PrioPrefix(namespace, name string) []byte {
	s := workQueuePrefix(namespace, name) + prefixPriority
	return []byte(s)
}

// LeaseIdxKey returns the lease index key for a specific expiry time and message.
// Format: ns/{ns}/wq/{name}/lease_idx/{expires_ms}/{id}
func LeaseIdxKey(namespace, name string, expiresMs uint64, id [16]byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixLeaseIdx
	key := make([]byte, len(prefix)+8+16)
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], expiresMs)
	copy(key[len(prefix)+8:], id[:])
	return key
}

// DLQKey returns the DLQ key for a message.
// Format: ns/{ns}/wq/{name}/dlq/{group}/{id}
func DLQKey(namespace, name, group string, id [16]byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixDLQ + group + "/"
	key := make([]byte, len(prefix)+16)
	copy(key, prefix)
	copy(key[len(prefix):], id[:])
	return key
}

// LeasePrefix returns the prefix for lease scanning in a group.
// Format: ns/{ns}/wq/{name}/lease/{group}/
func LeasePrefix(namespace, name, group string) []byte {
	s := workQueuePrefix(namespace, name) + prefixLease + group + "/"
	return []byte(s)
}

// msgKey returns the key for a message.
// Format: ns/{ns}/wq/{name}/msg/{id}
func msgKey(namespace, name string, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixMsg
	key := make([]byte, len(prefix)+len(id))
	copy(key, prefix)
	copy(key[len(prefix):], id)
	return key
}

// priorityIndexKey returns the key for priority index.
// Format: ns/{ns}/wq/{name}/priority_idx/{priority}/{id}
// Priority is int32, higher values first (we use ^priority for descending order)
func priorityIndexKey(namespace, name string, priority int32, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixPriority
	// Invert priority for descending order (higher priority dequeued first)
	invertedPriority := ^uint32(priority)

	key := make([]byte, len(prefix)+4+len(id))
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], invertedPriority)
	copy(key[len(prefix)+4:], id)
	return key
}

// delayIndexKey returns the key for delayed message index.
// Format: ns/{ns}/wq/{name}/delay_idx/{ready_at_ms}/{id}
func delayIndexKey(namespace, name string, readyAtMs int64, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixDelay
	key := make([]byte, len(prefix)+8+len(id))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], uint64(readyAtMs))
	copy(key[len(prefix)+8:], id)
	return key
}

// leaseKey returns the key for a lease.
// Format: ns/{ns}/wq/{name}/lease/{group}/{id}
func leaseKey(namespace, name, group string, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixLease + group + "/"
	key := make([]byte, len(prefix)+len(id))
	copy(key, prefix)
	copy(key[len(prefix):], id)
	return key
}

// leaseIndexKey returns the key for lease expiry index.
// Format: ns/{ns}/wq/{name}/lease_idx/{group}/{expires_ms}/{id}
func leaseIndexKey(namespace, name, group string, expiresMs int64, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixLeaseIdx + group + "/"
	key := make([]byte, len(prefix)+8+len(id))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], uint64(expiresMs))
	copy(key[len(prefix)+8:], id)
	return key
}

// pelKey returns the key for Pending Entries List.
// Format: ns/{ns}/wq/{name}/pel/{group}/{consumer}/{id}
func pelKey(namespace, name, group, consumerID string, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixPEL + group + "/" + consumerID + "/"
	key := make([]byte, len(prefix)+len(id))
	copy(key, prefix)
	copy(key[len(prefix):], id)
	return key
}

// retryKey returns the key for retry schedule.
// Format: ns/{ns}/wq/{name}/retry/{group}/{ready_at_ms}/{id}
func retryKey(namespace, name, group string, readyAtMs int64, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixRetry + group + "/"
	key := make([]byte, len(prefix)+8+len(id))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], uint64(readyAtMs))
	copy(key[len(prefix)+8:], id)
	return key
}

// dlqKey returns the key for Dead Letter Queue.
// Format: ns/{ns}/wq/{name}/dlq/{group}/{id}
func dlqKey(namespace, name, group string, id []byte) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixDLQ + group + "/"
	key := make([]byte, len(prefix)+len(id))
	copy(key, prefix)
	copy(key[len(prefix):], id)
	return key
}

// consumerKey returns the key for consumer registry.
// Format: ns/{ns}/wq/{name}/cons/{group}/{consumer_id}
func consumerKey(namespace, name, group, consumerID string) []byte {
	s := workQueuePrefix(namespace, name) + prefixCons + group + "/" + consumerID
	return []byte(s)
}

// consumerIndexKey returns the key for consumer expiry index.
// Format: ns/{ns}/wq/{name}/cons_idx/{group}/{expires_ms}/{consumer_id}
func consumerIndexKey(namespace, name, group string, expiresMs int64, consumerID string) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixConsIdx + group + "/"
	cidBytes := []byte(consumerID)
	key := make([]byte, len(prefix)+8+len(cidBytes))
	copy(key, prefix)
	binary.BigEndian.PutUint64(key[len(prefix):], uint64(expiresMs))
	copy(key[len(prefix)+8:], cidBytes)
	return key
}

// groupConfigKey returns the key for group configuration.
// Format: ns/{ns}/wq/{name}/groupcfg/{group}
func groupConfigKey(namespace, name, group string) []byte {
	s := workQueuePrefix(namespace, name) + prefixGroupCfg + group
	return []byte(s)
}

// completedKey returns the key for a completed message entry.
// Format: ns/{ns}/wq/{name}/completed/{partition}/{seq}
func completedKey(namespace, name string, partition uint32, seq uint64) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixCompleted
	key := make([]byte, len(prefix)+4+8)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], partition)
	binary.BigEndian.PutUint64(key[len(prefix)+4:], seq)
	return key
}

// completedPrefix returns the prefix for scanning all completed messages in a partition.
// Format: ns/{ns}/wq/{name}/completed/{partition}/
func completedPrefix(namespace, name string, partition uint32) []byte {
	prefix := workQueuePrefix(namespace, name) + prefixCompleted
	key := make([]byte, len(prefix)+4)
	copy(key, prefix)
	binary.BigEndian.PutUint32(key[len(prefix):], partition)
	return key
}

// completedMetaKey returns the metadata key for completed messages in a partition.
// Format: ns/{ns}/wq/{name}/completed_meta/{partition}
func completedMetaKey(namespace, name string, partition uint32) []byte {
	s := fmt.Sprintf("%scompleted_meta/%d", workQueuePrefix(namespace, name), partition)
	return []byte(s)
}

// keyRange returns start and end keys for scanning with a prefix.
// The end key is exclusive (prefix + 0xFF suffix).
func keyRange(prefix string) ([]byte, []byte) {
	start := []byte(prefix)
	end := make([]byte, len(prefix)+1)
	copy(end, prefix)
	end[len(prefix)] = 0xFF
	return start, end
}

// parseMessageIDFromKey extracts message ID from index keys.
// Works for priority_idx, delay_idx, lease_idx, retry keys.
func parseMessageIDFromKey(key []byte, idLen int) []byte {
	if len(key) < idLen {
		return nil
	}
	return key[len(key)-idLen:]
}

// Helper to scan prefixes
func scanPrefix(namespace, name, prefix string) ([]byte, []byte) {
	p := workQueuePrefix(namespace, name) + prefix
	return keyRange(p)
}

// leasePrefix returns the prefix for all leases in a group.
func leasePrefix(namespace, name, group string) string {
	return workQueuePrefix(namespace, name) + prefixLease + group + "/"
}

// pelPrefix returns the prefix for all PEL entries in a group.
func pelPrefix(namespace, name, group string) string {
	return workQueuePrefix(namespace, name) + prefixPEL + group + "/"
}

// pelConsumerPrefix returns the prefix for a consumer's PEL entries.
func pelConsumerPrefix(namespace, name, group, consumerID string) string {
	return workQueuePrefix(namespace, name) + prefixPEL + group + "/" + consumerID + "/"
}

// consumerPrefix returns the prefix for all consumers in a group.
// Format: ns/{ns}/wq/{name}/cons/{group}/
func consumerPrefix(namespace, name, group string) string {
	return workQueuePrefix(namespace, name) + prefixCons + group + "/"
}

// parseGroupFromKey extracts group name from keys like lease/{group}/{id}
func parseGroupFromKey(key []byte, basePrefix string) string {
	s := string(key)
	if !strings.Contains(s, basePrefix) {
		return ""
	}
	after := s[strings.Index(s, basePrefix)+len(basePrefix):]
	parts := strings.Split(after, "/")
	if len(parts) > 0 {
		return parts[0]
	}
	return ""
}
