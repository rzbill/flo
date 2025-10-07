package eventlog

import (
	"encoding/binary"
)

// Keyspace helpers for Pebble keys.
//
// Layout (byte-wise, lexicographically sortable):
// - ns/{ns}/log/{topic}/{part_be4}/m
// - ns/{ns}/log/{topic}/{part_be4}/e/{seq_be8}
// - ns/{ns}/cursor/{topic}/{group}/{part_be4}

var (
	sep        = byte('/')
	nsPrefix   = []byte("ns/")
	logSeg     = []byte("/log/")
	cursorSeg  = []byte("/cursor/")
	metaSuffix = []byte("/m")
	entrySeg   = []byte("/e/")
	ldSuffix   = []byte("/ts")
)

func appendBE4(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendBE8(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

// KeyLogMeta builds the partition metadata key.
func KeyLogMeta(namespace, topic string, partition uint32) []byte {
	k := make([]byte, 0, len(namespace)+len(topic)+32)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, logSeg...)
	k = append(k, topic...)
	k = append(k, sep)
	k = appendBE4(k, partition)
	k = append(k, metaSuffix...)
	return k
}

// KeyLogEntry builds the entry key with a big-endian sequence for proper ordering.
func KeyLogEntry(namespace, topic string, partition uint32, seq uint64) []byte {
	k := make([]byte, 0, len(namespace)+len(topic)+48)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, logSeg...)
	k = append(k, topic...)
	k = append(k, sep)
	k = appendBE4(k, partition)
	k = append(k, entrySeg...)
	k = appendBE8(k, seq)
	return k
}

// KeyCursor builds the durable cursor key for a group and partition.
func KeyCursor(namespace, topic, group string, partition uint32) []byte {
	k := make([]byte, 0, len(namespace)+len(topic)+len(group)+48)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, cursorSeg...)
	k = append(k, topic...)
	k = append(k, sep)
	k = append(k, group...)
	k = append(k, sep)
	k = appendBE4(k, partition)
	return k
}

// KeyCursorLastDelivered builds a key colocated with cursor to store last-delivered time (ms).
// Layout: ns/{ns}/cursor/{topic}/{group}/{part_be4}/ts
func KeyCursorLastDelivered(namespace, topic, group string, partition uint32) []byte {
	k := KeyCursor(namespace, topic, group, partition)
	k = append(k, ldSuffix...)
	return k
}

// KeyCursorLastDeliveredPrefix returns a range prefix to scan all last-delivered keys for a topic across groups/partitions.
// Layout prefix: ns/{ns}/cursor/{topic}/
func KeyCursorLastDeliveredPrefix(namespace, topic string) []byte {
	k := make([]byte, 0, len(namespace)+len(topic)+24)
	k = append(k, nsPrefix...)
	k = append(k, namespace...)
	k = append(k, cursorSeg...)
	k = append(k, topic...)
	k = append(k, sep)
	return k
}
