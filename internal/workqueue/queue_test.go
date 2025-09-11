package workqueue

import (
	"context"
	"testing"
	"time"

	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func openTestQueue(t *testing.T) *WorkQueue {
	t.Helper()
	dir := t.TempDir()
	db, err := pebblestore.Open(pebblestore.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways})
	if err != nil {
		t.Fatalf("open pebble: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	q, err := OpenQueue(db, "ns", "q", 1)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	return q
}

func TestEnqueueUpdatesIndices(t *testing.T) {
	q := openTestQueue(t)
	ctx := context.Background()
	seq, err := q.Enqueue(ctx, []byte("h"), []byte("p"), 5, 1000, 1000)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if seq == 0 {
		t.Fatalf("want seq > 0")
	}
}

func TestDequeueRespectsPriorityAndDelay(t *testing.T) {
	q := openTestQueue(t)
	ctx := context.Background()
	// immediate low priority
	s1, _ := q.Enqueue(ctx, nil, []byte("a"), 10, 0, 1000)
	// delayed higher priority becomes available after promote
	s2, _ := q.Enqueue(ctx, nil, []byte("b"), 1, 200, 1000)
	if s1 == 0 || s2 == 0 {
		t.Fatalf("seqs")
	}

	// before delay due, dequeue should return only s1
	msgs, err := q.Dequeue(ctx, "g", 1, 1000, 1100)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Seq != s1 {
		t.Fatalf("expected s1 first before promote")
	}

	// after due, next dequeue should get s2
	msgs, err = q.Dequeue(ctx, "g", 1, 1000, 1300)
	if err != nil {
		t.Fatalf("dequeue2: %v", err)
	}
	if len(msgs) != 1 || msgs[0].Seq != s2 {
		t.Fatalf("expected s2 after delay due")
	}
}

func TestExtendAndComplete(t *testing.T) {
	q := openTestQueue(t)
	ctx := context.Background()
	s, _ := q.Enqueue(ctx, nil, []byte("x"), 5, 0, 1000)
	msgs, err := q.Dequeue(ctx, "g", 1, 1000, 1100)
	if err != nil || len(msgs) != 1 || msgs[0].Seq != s {
		t.Fatalf("dequeue: %v", err)
	}
	// extend
	if err := q.ExtendLease(ctx, "g", []uint64{s}, 2000, 1200); err != nil {
		t.Fatalf("extend: %v", err)
	}
	// complete
	if err := q.Complete(ctx, "g", []uint64{s}); err != nil {
		t.Fatalf("complete: %v", err)
	}
}

func TestFailRetryAndDLQ(t *testing.T) {
	q := openTestQueue(t)
	ctx := context.Background()
	s, _ := q.Enqueue(ctx, nil, []byte("x"), 5, 0, 1000)
	_, _ = q.Dequeue(ctx, "g", 1, 1000, 1100)
	// retry after
	if err := q.Fail(ctx, "g", []uint64{s}, 200, false, 1100); err != nil {
		t.Fatalf("fail retry: %v", err)
	}
	// not immediately available
	msgs, _ := q.Dequeue(ctx, "g", 1, 1000, 1150)
	if len(msgs) != 0 {
		t.Fatalf("should not dequeue before retry after")
	}
	msgs, _ = q.Dequeue(ctx, "g", 1, 1000, 1400)
	if len(msgs) != 1 || msgs[0].Seq != s {
		t.Fatalf("should dequeue after retry delay")
	}

	// DLQ flow
	s2, _ := q.Enqueue(ctx, nil, []byte("y"), 5, 0, 2000)
	_, _ = q.Dequeue(ctx, "g", 1, 1000, 2100)
	if err := q.Fail(ctx, "g", []uint64{s2}, 0, true, 2100); err != nil {
		t.Fatalf("fail dlq: %v", err)
	}
}

func TestBackpressureThrottle(t *testing.T) {
	q := openTestQueue(t).WithOptions(QueueOptions{MaxAvailable: 0, ThrottleSleep: 1})
	ctx := context.Background()
	// With MaxAvailable=0, any immediate enqueue should block briefly but still succeed
	_, err := q.Enqueue(ctx, nil, []byte("bp"), 5, 0, 1000)
	if err != nil {
		t.Fatalf("enqueue under bp: %v", err)
	}
}

func TestReclaimExpired(t *testing.T) {
	q := openTestQueue(t)
	ctx := context.Background()
	s, _ := q.Enqueue(ctx, nil, []byte("x"), 5, 0, 1000)
	// lease it
	_, _ = q.Dequeue(ctx, "g", 1, 50, 1000)
	// reclaim after expiry
	n, err := q.ReclaimExpired(ctx, 1100, 10, 5)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if n < 1 {
		t.Fatalf("expected reclaim >=1")
	}
	// should be available again
	msgs, _ := q.Dequeue(ctx, "g2", 1, 1000, 1200)
	if len(msgs) != 1 || msgs[0].Seq != s {
		t.Fatalf("expected reclaimed seq")
	}
}

func TestSweeperBackground(t *testing.T) {
	q := openTestQueue(t)
	ctx := context.Background()
	_, _ = q.Enqueue(ctx, nil, []byte("x"), 5, 0, 1000)
	_, _ = q.Dequeue(ctx, "g", 1, 50, 1000)
	q.ConfigureSweeper(50*time.Millisecond, 32, 5)
	q.SetSweeperEnabled(true)
	defer q.StopSweeper()
	// sweeper should reclaim shortly
	time.Sleep(400 * time.Millisecond)
	msgs, _ := q.Dequeue(ctx, "g2", 1, 1000, 1400)
	if len(msgs) != 1 {
		t.Fatalf("expected a reclaimed item dequeued by background sweeper")
	}
}
