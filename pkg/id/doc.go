// Package id provides a 128-bit, lexicographically sortable identifier.
//
// # Format
//
// The ID is 16 bytes big-endian: [8 bytes ms_timestamp][8 bytes sequence].
// This guarantees that byte-wise comparison preserves chronological order,
// and that IDs generated within the same millisecond remain strictly
// increasing by sequence.
//
// # Monotonicity
//
// The Generator ensures per-process monotonicity:
//   - If the system clock regresses, it pins to the last seen millisecond and
//     increments the sequence to avoid going backwards.
//   - If the sequence would overflow within a millisecond, it waits for the
//     next millisecond before emitting the next ID.
//
// Usage
//
//	g := id.NewGenerator()
//	newID := g.Next()
//	b := newID.Bytes()   // 16-byte representation
//	s := newID.String()  // hex string
package id
