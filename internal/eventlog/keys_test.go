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

func TestKeyCursorLastDelivered(t *testing.T) {
	k := KeyCursorLastDelivered("ns", "topic", "group", 5)

	// Should contain the base cursor key
	baseKey := KeyCursor("ns", "topic", "group", 5)
	if !bytes.HasPrefix(k, baseKey) {
		t.Fatalf("last delivered key should start with cursor key")
	}

	// Should have the timestamp suffix
	if !bytes.HasSuffix(k, []byte("/ts")) {
		t.Fatalf("last delivered key should end with /ts, got %q", string(k))
	}

	// Should be longer than base key
	if len(k) <= len(baseKey) {
		t.Fatalf("last delivered key should be longer than base cursor key")
	}
}

func TestKeyCursorLastDeliveredPrefix(t *testing.T) {
	k := KeyCursorLastDeliveredPrefix("ns", "topic")

	// Should contain namespace and topic
	if !bytes.Contains(k, []byte("ns/ns/cursor/topic/")) {
		t.Fatalf("unexpected prefix layout: %q", string(k))
	}

	// Should not contain group or partition
	if bytes.Contains(k, []byte("group")) {
		t.Fatalf("prefix should not contain group")
	}
}

func TestKeyOrderingLastDelivered(t *testing.T) {
	// Test that last delivered keys sort correctly
	a := KeyCursorLastDelivered("ns", "topic", "group1", 1)
	b := KeyCursorLastDelivered("ns", "topic", "group2", 1)
	c := KeyCursorLastDelivered("ns", "topic", "group1", 2)

	// Different groups should sort differently
	if bytes.Compare(a, b) == 0 {
		t.Fatalf("different groups should have different keys")
	}

	// Different partitions should sort differently
	if bytes.Compare(a, c) == 0 {
		t.Fatalf("different partitions should have different keys")
	}
}

func TestKeyLogMetaLayout(t *testing.T) {
	k := KeyLogMeta("ns", "topic", 7)
	if !bytes.Equal(k, []byte("ns/ns/log/topic/\x00\x00\x00\x07/m")) {
		t.Fatalf("unexpected KeyLogMeta: %q", string(k))
	}
}

func TestKeyCursorLastDeliveredPrefixLayout(t *testing.T) {
	p := KeyCursorLastDeliveredPrefix("ns", "topic")
	if !bytes.Equal(p, []byte("ns/ns/cursor/topic/")) {
		t.Fatalf("unexpected KeyCursorLastDeliveredPrefix: %q", string(p))
	}
}
