package streamsvc

import (
	"testing"
)

func TestErrorKey(t *testing.T) {
	got := errorKey("default", "orders", "workers", []byte("id"))
	want := []byte("ns/default/stream/orders/error/workers/id")
	if string(got) != string(want) {
		t.Fatalf("errorKey mismatch:\n got=%q\nwant=%q", string(got), string(want))
	}
}

func TestFailureTimeKey(t *testing.T) {
	got := failureTimeKey("default", "orders", "workers", []byte("abc"))
	want := []byte("ns/default/stream/orders/failure/workers/abc")
	if string(got) != string(want) {
		t.Fatalf("failureTimeKey mismatch:\n got=%q\nwant=%q", string(got), string(want))
	}
}

func TestIdemKey(t *testing.T) {
	got := idemKey("default", "orders", "k1")
	want := []byte("ns/default/stream/orders/idem/k1")
	if string(got) != string(want) {
		t.Fatalf("idemKey mismatch:\n got=%q\nwant=%q", string(got), string(want))
	}
}

func TestStreamMetaKey(t *testing.T) {
	got := streamMetaKey("default", "orders")
	want := []byte("ns/default/stream/orders/meta")
	if string(got) != string(want) {
		t.Fatalf("streamMetaKey mismatch:\n got=%q\nwant=%q", string(got), string(want))
	}
}

func TestAttemptsKey(t *testing.T) {
	got := attemptsKey("default", "orders", "workers", []byte("xyz"))
	want := []byte("ns/default/stream/orders/attempts/workers/xyz")
	if string(got) != string(want) {
		t.Fatalf("attemptsKey mismatch:\n got=%q\nwant=%q", string(got), string(want))
	}
}

func TestRetryNextKey(t *testing.T) {
	got := retryNextKey("ns1", "ch1", "g1", []byte("id123"))
	want := []byte("ns/ns1/stream/ch1/retry_next/g1/id123")
	if string(got) != string(want) {
		t.Fatalf("retryNextKey mismatch:\n got=%q\nwant=%q", string(got), string(want))
	}
}

func TestDeliveryKeyAndPrefix(t *testing.T) {
	id := []byte("id16")
	gotK := deliveryKey("nsA", "streamB", id, "grpC")
	wantK := []byte("ns/nsA/delivery/streamB/id16/grpC")
	if string(gotK) != string(wantK) {
		t.Fatalf("deliveryKey mismatch:\n got=%q\nwant=%q", string(gotK), string(wantK))
	}

	gotP := deliveryPrefix("nsA", "streamB", id)
	wantP := []byte("ns/nsA/delivery/streamB/id16/")
	if string(gotP) != string(wantP) {
		t.Fatalf("deliveryPrefix mismatch:\n got=%q\nwant=%q", string(gotP), string(wantP))
	}
}
