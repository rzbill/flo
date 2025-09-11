package workqueue

import "encoding/binary"

// Keyspace (namespace-scoped):
// - ns/{ns}/q/{queue}/{part_be4}/msg/{seq_be8}
// - ns/{ns}/q/{queue}/{part_be4}/m (partition metadata)
// - ns/{ns}/q_delay/{queue}/{fire_ms_be8}/{msg_id_be16}
// - ns/{ns}/q_lease/{queue}/{group}/{msg_id_be16}
// - ns/{ns}/q_lease_idx/{queue}/{expiry_ms_be8}/{msg_id_be16}
// - ns/{ns}/q_dlq/{queue}/{group}/{msg_id_be16}
// - ns/{ns}/q_prio/{queue}/{priority_be4}/{seq_be8}

var (
	sep        = byte('/')
	nsPrefix   = []byte("ns/")
	qSeg       = []byte("/q/")
	qDelaySeg  = []byte("/q_delay/")
	qLeaseSeg  = []byte("/q_lease/")
	qLeaseIdx  = []byte("/q_lease_idx/")
	qDLQSeg    = []byte("/q_dlq/")
	msgSeg     = []byte("/msg/")
	metaSuffix = []byte("/m")
	prioSeg    = []byte("/q_prio/")
)

func be4(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}
func be8(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

// MsgKey: primary message storage key.
func MsgKey(namespace, queue string, part uint32, seq uint64) []byte {
	k := make([]byte, 0, len(namespace)+len(queue)+48)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, qSeg...)
	k = append(k, queue...)
	k = append(k, sep)
	k = be4(k, part)
	k = append(k, msgSeg...)
	k = be8(k, seq)
	return k
}

// MetaKey: queue partition metadata (e.g., last sequence)
func MetaKey(namespace, queue string, part uint32) []byte {
	k := make([]byte, 0, len(namespace)+len(queue)+32)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, qSeg...)
	k = append(k, queue...)
	k = append(k, sep)
	k = be4(k, part)
	k = append(k, metaSuffix...)
	return k
}

// DelayKey: time-index for delayed delivery
func DelayKey(namespace, queue string, fireMs uint64, msgID [16]byte) []byte {
	k := make([]byte, 0, len(namespace)+len(queue)+64)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, qDelaySeg...)
	k = append(k, queue...)
	k = append(k, sep)
	k = be8(k, fireMs)
	k = append(k, sep)
	k = append(k, msgID[:]...)
	return k
}

// LeaseKey: lease record per group
func LeaseKey(namespace, queue, group string, msgID [16]byte) []byte {
	k := make([]byte, 0, len(namespace)+len(queue)+len(group)+64)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, qLeaseSeg...)
	k = append(k, queue...)
	k = append(k, sep)
	k = append(k, group...)
	k = append(k, sep)
	k = append(k, msgID[:]...)
	return k
}

// LeasePrefix returns the prefix for all leases for a given queue and group.
func LeasePrefix(namespace, queue, group string) []byte {
	p := make([]byte, 0, len(namespace)+len(queue)+len(group)+16)
	p = append(p, nsPrefix...)
	p = append(p, namespace...)
	p = append(p, qLeaseSeg...)
	p = append(p, queue...)
	p = append(p, sep)
	p = append(p, group...)
	p = append(p, sep)
	return p
}

// LeaseIdxKey: expiry index to scan for expired leases
func LeaseIdxKey(namespace, queue string, expiryMs uint64, msgID [16]byte) []byte {
	k := make([]byte, 0, len(namespace)+len(queue)+64)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, qLeaseIdx...)
	k = append(k, queue...)
	k = append(k, sep)
	k = be8(k, expiryMs)
	k = append(k, sep)
	k = append(k, msgID[:]...)
	return k
}

// LeaseIdxPrefix returns the prefix to scan all lease expiries for a queue.
func LeaseIdxPrefix(namespace, queue string) []byte {
	p := make([]byte, 0, len(namespace)+len(queue)+16)
	p = append(p, nsPrefix...)
	p = append(p, namespace...)
	p = append(p, qLeaseIdx...)
	p = append(p, queue...)
	p = append(p, sep)
	return p
}

// DLQKey: dead-letter queue storage
func DLQKey(namespace, queue, group string, msgID [16]byte) []byte {
	k := make([]byte, 0, len(namespace)+len(queue)+len(group)+64)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, qDLQSeg...)
	k = append(k, queue...)
	k = append(k, sep)
	k = append(k, group...)
	k = append(k, sep)
	k = append(k, msgID[:]...)
	return k
}

// PrioKey: priority index for dequeue ordering (lower first).
func PrioKey(namespace, queue string, priority uint32, seq uint64) []byte {
	k := make([]byte, 0, len(namespace)+len(queue)+48)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, prioSeg...)
	k = append(k, queue...)
	k = append(k, sep)
	k = be4(k, priority)
	k = append(k, sep)
	k = be8(k, seq)
	return k
}

// DelayPrefix returns the prefix for delay index keys for a queue: ns/{ns}/q_delay/{queue}/
func DelayPrefix(namespace, queue string) []byte {
	p := make([]byte, 0, len(namespace)+len(queue)+16)
	p = append(p, nsPrefix...)
	p = append(p, namespace...)
	p = append(p, qDelaySeg...)
	p = append(p, queue...)
	p = append(p, sep)
	return p
}

// PrioPrefix returns the prefix for priority index keys for a queue: ns/{ns}/q_prio/{queue}/
func PrioPrefix(namespace, queue string) []byte {
	p := make([]byte, 0, len(namespace)+len(queue)+16)
	p = append(p, nsPrefix...)
	p = append(p, namespace...)
	p = append(p, prioSeg...)
	p = append(p, queue...)
	p = append(p, sep)
	return p
}
