package eventlog

import (
	"bytes"
	"testing"
)

func TestKeyOrderingEntries(t *testing.T) {
	a := KeyLogEntry("ns", "topic", 1, 10)
	b := KeyLogEntry("ns", "topic", 1, 11)
	if !bytes.HasPrefix(a, KeyLogMeta("ns", "topic", 1)[:len(a)-8-3]) {
		t.Fatalf("entry key should share prefix with meta")
	}
	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("expected seq 10 < seq 11")
	}
}

func TestCursorKey(t *testing.T) {
	k := KeyCursor("ns", "t", "g", 7)
	if !bytes.Contains(k, []byte("ns/ns/cursor/t/g/")) {
		t.Fatalf("unexpected cursor layout: %q", string(k))
	}
}
