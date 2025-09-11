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
