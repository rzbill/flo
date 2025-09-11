package id

import (
	"testing"
	"time"
)

func TestOrderingMonotonic(t *testing.T) {
	g := NewGenerator()
	NowMs = func() int64 { return 1000 }
	defer func() { NowMs = func() int64 { return time.Now().UnixMilli() } }()

	a := g.Next()
	b := g.Next()
	if a.Compare(b) >= 0 {
		t.Fatalf("expected a<b")
	}
}

func TestClockRegressionGuard(t *testing.T) {
	g := NewGenerator()
	seq := int64(1000)
	NowMs = func() int64 { return seq }
	defer func() { NowMs = func() int64 { return time.Now().UnixMilli() } }()

	a := g.Next() // uses 1000
	seq = 900     // clock went backwards
	b := g.Next() // should still be >= a
	if a.Compare(b) >= 0 {
		t.Fatalf("expected b>a despite clock regression")
	}
}

func TestSequenceOverflowWaitsNextMs(t *testing.T) {
	g := NewGenerator()
	NowMs = func() int64 { return 2000 }
	defer func() { NowMs = func() int64 { return time.Now().UnixMilli() } }()

	// Simulate near-overflow
	g.lastMs = 2000
	g.sequence = ^uint64(0) - 1

	_ = g.Next() // seq becomes MaxUint64

	done := make(chan struct{})
	go func() {
		_ = g.Next() // should wait for next ms and reset seq
		close(done)
	}()

	// Advance time after a brief moment to let goroutine reach wait loop
	time.AfterFunc(10*time.Millisecond, func() { NowMs = func() int64 { return 2001 } })

	select {
	case <-done:
		// ok
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for overflow handling")
	}
}
