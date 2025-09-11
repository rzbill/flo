package eventlog

import (
	"encoding/binary"
)

// CommitCursor stores the last processed token for a group/partition idempotently.
// If the provided token is lower than the stored one, the commit is ignored.
func (l *Log) CommitCursor(group string, tok Token) error {
	key := KeyCursor(l.namespace, l.topic, group, l.part)
	// Read existing
	cur, err := l.db.Get(key)
	if err == nil && len(cur) >= 8 {
		prev := binary.BigEndian.Uint64(cur[:8])
		if tok.Seq() <= prev {
			return nil
		}
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], tok.Seq())
	return l.db.Set(key, b[:])
}

// GetCursor loads the current cursor token for a group/partition.
func (l *Log) GetCursor(group string) (Token, bool) {
	key := KeyCursor(l.namespace, l.topic, group, l.part)
	cur, err := l.db.Get(key)
	if err != nil || len(cur) < 8 {
		return Token{}, false
	}
	var t Token
	copy(t[:], cur[:8])
	return t, true
}
