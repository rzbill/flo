package workqueue

import (
	"bytes"
	"testing"
)

func TestMsgKeyOrdering(t *testing.T) {
	a := MsgKey("ns", "q", 1, 10)
	b := MsgKey("ns", "q", 1, 11)
	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("expected seq ordering")
	}
}

func TestLeaseIdxOrdering(t *testing.T) {
	id := [16]byte{1}
	a := LeaseIdxKey("ns", "q", 100, id)
	b := LeaseIdxKey("ns", "q", 200, id)
	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("expected expiry ordering")
	}
}

func TestPrioKeyOrdering(t *testing.T) {
	a := PrioKey("ns", "q", 1, 100)
	b := PrioKey("ns", "q", 2, 50)
	if bytes.Compare(a, b) >= 0 {
		t.Fatalf("expected lower priority to sort first")
	}
}

func TestLeasePrefixLayout(t *testing.T) {
	p := LeasePrefix("ns", "queue", "group")
	expected := "ns/ns/wq/queue/lease/group/"
	if !bytes.Equal(p, []byte(expected)) {
		t.Fatalf("unexpected LeasePrefix: got %q, want %q", string(p), expected)
	}
}

func TestDelayPrefixLayout(t *testing.T) {
	p := DelayPrefix("ns", "queue")
	expected := "ns/ns/wq/queue/delay_idx/"
	if !bytes.Equal(p, []byte(expected)) {
		t.Fatalf("unexpected DelayPrefix: got %q, want %q", string(p), expected)
	}
}

func TestPrioPrefixLayout(t *testing.T) {
	p := PrioPrefix("ns", "queue")
	expected := "ns/ns/wq/queue/priority_idx/"
	if !bytes.Equal(p, []byte(expected)) {
		t.Fatalf("unexpected PrioPrefix: got %q, want %q", string(p), expected)
	}
}
