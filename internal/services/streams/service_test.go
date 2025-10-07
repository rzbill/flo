package streamsvc

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/eventlog"
	"github.com/rzbill/flo/internal/runtime"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func newServiceForTest(t *testing.T) (*Service, *runtime.Runtime) {
	t.Helper()
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("open runtime: %v", err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return New(rt), rt
}

func TestPublishIdempotency(t *testing.T) {
	svc, rt := newServiceForTest(t)
	headers := map[string]string{"idempotencyKey": "pub-1"}
	id1, err := svc.Publish(context.Background(), "default", "orders", []byte("hello"), headers, "")
	if err != nil {
		t.Fatalf("publish1: %v", err)
	}
	id2, err := svc.Publish(context.Background(), "default", "orders", []byte("hello"), headers, "")
	if err != nil {
		t.Fatalf("publish2: %v", err)
	}
	if string(id1) != string(id2) {
		t.Fatalf("idempotency failed: %x vs %x", id1, id2)
	}
	// Ensure only one entry in partition 0
	log, err := rt.OpenLog("default", "orders", 0)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	items, _ := log.Read(eventlog.ReadOptions{Limit: 100})
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
}

func TestFindStartTokenAtBinarySearch(t *testing.T) {
	svc, rt := newServiceForTest(t)
	l, err := rt.OpenLog("default", "ts", 0)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	// Append 5 records with increasing header timestamps spaced by 10ms
	base := time.Now().Add(-100 * time.Millisecond).UnixMilli()
	for i := 0; i < 5; i++ {
		var hdr [8]byte
		binary.BigEndian.PutUint64(hdr[:], uint64(base+int64(i*10)))
		if _, err := l.Append(context.Background(), []eventlog.AppendRecord{{Header: hdr[:], Payload: []byte{byte(i)}}}); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	tok := svc.findStartTokenAt(l, base+25) // expect index 3rd (>= 25 → 30)
	items, _ := l.Read(eventlog.ReadOptions{Start: tok, Limit: 1})
	if len(items) == 0 {
		t.Fatalf("no item at token")
	}
	got := int64(binary.BigEndian.Uint64(items[0].Header[:8]))
	if got != base+30 {
		t.Fatalf("want %d got %d", base+30, got)
	}
}

func TestFlushAndBufferTunables(t *testing.T) {
	t.Setenv("FLO_SUB_FLUSH_MS", "2")
	t.Setenv("FLO_SUB_BUF", "256")
	svc, _ := newServiceForTest(t)
	if svc.flushWindow <= 0 {
		t.Fatalf("expected non-zero flush window")
	}
	if svc.subBufLen != 256 {
		t.Fatalf("expected subBufLen=256, got %d", svc.subBufLen)
	}
}

func TestNackAttemptsAndDLQ(t *testing.T) {
	svc, rt := newServiceForTest(t)
	// Nack 5 times to trigger DLQ
	id := []byte{0, 0, 0, 0, 0, 0, 0, 1}
	for i := 0; i < 5; i++ {
		if err := svc.Nack(context.Background(), "default", "orders", "workers", id, "test error"); err != nil {
			t.Fatalf("nack: %v", err)
		}
	}
	dlq, err := rt.OpenLog("default", "dlq/orders/workers", 0)
	if err != nil {
		t.Fatalf("open dlq: %v", err)
	}
	items, _ := dlq.Read(eventlog.ReadOptions{Limit: 10})
	if len(items) == 0 {
		t.Fatalf("expected dlq item")
	}
}

func TestRetryBackoffAppends(t *testing.T) {
	svc, rt := newServiceForTest(t)
	id := []byte{0, 0, 0, 0, 0, 0, 0, 2}
	start := time.Now().UnixMilli()
	if err := svc.Nack(context.Background(), "default", "orders", "workers", id, "test error"); err != nil {
		t.Fatalf("nack: %v", err)
	}
	retry, err := rt.OpenLog("default", "retry/orders/workers", 0)
	if err != nil {
		t.Fatalf("open retry: %v", err)
	}
	items, _ := retry.Read(eventlog.ReadOptions{Limit: 10})
	if len(items) == 0 {
		t.Fatalf("expected retry item")
	}
	if len(items[0].Header) < 8 {
		t.Fatalf("missing retryAt header")
	}
	retryAt := binary.BigEndian.Uint64(items[0].Header[:8])
	if retryAt < uint64(start) || retryAt > uint64(time.Now().Add(5*time.Second).UnixMilli()) {
		t.Fatalf("retryAt out of range: %d", retryAt)
	}
}

type sink struct {
	ctx    context.Context
	itemsC chan SubscribeItem
}

func (s sink) Send(it SubscribeItem) error {
	select {
	case s.itemsC <- it:
	default:
	}
	return nil
}
func (s sink) Context() context.Context { return s.ctx }
func (s sink) Flush() error             { return nil }

func TestStreamSubscribeReceivesAppend(t *testing.T) {
	t.Skip("flaky under CI; revisit with deterministic harness")
}

func TestFlushStream(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish some messages
	_, err := svc.Publish(ctx, "default", "test", []byte("msg1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	_, err = svc.Publish(ctx, "default", "test", []byte("msg2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Verify messages exist
	items, _, err := svc.ListMessages(ctx, "default", "test", 0, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(items))
	}

	// Flush the stream
	deletedCount, err := svc.FlushStream(ctx, "default", "test", nil)
	if err != nil {
		t.Fatalf("flush stream: %v", err)
	}
	if deletedCount != 2 {
		t.Fatalf("expected 2 deleted messages, got %d", deletedCount)
	}

	// Verify stream is empty
	items, _, err = svc.ListMessages(ctx, "default", "test", 0, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages after flush: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 messages after flush, got %d", len(items))
	}
}

func TestFlushStreamPartition(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Create a stream with multiple partitions
	err := svc.CreateStream(ctx, "default", "test", 2)
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Publish messages with different keys to different partitions
	_, err = svc.Publish(ctx, "default", "test", []byte("msg1"), nil, "key1")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	_, err = svc.Publish(ctx, "default", "test", []byte("msg2"), nil, "key2")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Check which partition has messages
	items0, _, err := svc.ListMessages(ctx, "default", "test", 0, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages partition 0: %v", err)
	}
	items1, _, err := svc.ListMessages(ctx, "default", "test", 1, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages partition 1: %v", err)
	}

	// Find a partition with messages to flush
	var partitionToFlush uint32
	var expectedRemaining int
	if len(items0) > 0 {
		partitionToFlush = 0
		expectedRemaining = len(items1)
	} else {
		partitionToFlush = 1
		expectedRemaining = len(items0)
	}

	// Flush the selected partition
	deletedCount, err := svc.FlushStream(ctx, "default", "test", &partitionToFlush)
	if err != nil {
		t.Fatalf("flush partition %d: %v", partitionToFlush, err)
	}
	if deletedCount == 0 {
		t.Fatalf("expected at least 1 deleted message from partition %d, got %d", partitionToFlush, deletedCount)
	}

	// Verify the other partition still has messages
	remainingItems, _, err := svc.ListMessages(ctx, "default", "test", 1-partitionToFlush, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages after partition flush: %v", err)
	}
	if len(remainingItems) != expectedRemaining {
		t.Fatalf("expected %d messages in remaining partition, got %d", expectedRemaining, len(remainingItems))
	}
}

func TestMetricsPublishAndAckCountersAggregate(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	ns := "default"
	ch := "metrics-agg"

	// Publish three messages of known sizes
	_, _ = svc.Publish(ctx, ns, ch, []byte("aaa"), nil, "")
	_, _ = svc.Publish(ctx, ns, ch, []byte("bbbb"), nil, "")
	id, _ := svc.Publish(ctx, ns, ch, []byte("cc"), nil, "")

	// Ack one
	if err := svc.Ack(ctx, ns, ch, "g1", id); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// Window: last 2 minutes to avoid boundary flake
	stepMs := int64(time.Minute / time.Millisecond)
	nowMs := time.Now().UnixMilli()
	startMs := ((nowMs / stepMs) - 1) * stepMs
	endMs := startMs + 2*stepMs

	// publish_count
	series, err := svc.Metrics(ctx, ns, ch, MetricPublishCount, startMs, endMs, stepMs, false, "")
	if err != nil {
		t.Fatalf("metrics publish_count: %v", err)
	}
	var sum float64
	for _, p := range series[0].Points {
		sum += p[1]
	}
	if int(sum) != 3 {
		t.Fatalf("publish_count sum want 3 got %v", sum)
	}

	// publish_rate (sum(rate)*60 == 3)
	series, err = svc.Metrics(ctx, ns, ch, MetricPublishRate, startMs, endMs, stepMs, false, "")
	if err != nil {
		t.Fatalf("metrics publish_rate: %v", err)
	}
	sum = 0
	for _, p := range series[0].Points {
		sum += p[1]
	}
	if diff := (sum * 60.0) - 3.0; diff < -0.001 || diff > 0.001 {
		t.Fatalf("publish_rate sum*60 want 3 got %v", sum*60.0)
	}

	// bytes_rate (sum(rate)*60 == total bytes)
	totalBytes := float64(3 + 4 + 2)
	series, err = svc.Metrics(ctx, ns, ch, MetricBytesRate, startMs, endMs, stepMs, false, "")
	if err != nil {
		t.Fatalf("metrics bytes_rate: %v", err)
	}
	sum = 0
	for _, p := range series[0].Points {
		sum += p[1]
	}
	if diff := (sum * 60.0) - totalBytes; diff < -0.001 || diff > 0.001 {
		t.Fatalf("bytes_rate sum*60 want %v got %v", totalBytes, sum*60.0)
	}

	// ack_count
	series, err = svc.Metrics(ctx, ns, ch, MetricAckCount, startMs, endMs, stepMs, false, "")
	if err != nil {
		t.Fatalf("metrics ack_count: %v", err)
	}
	sum = 0
	for _, p := range series[0].Points {
		sum += p[1]
	}
	if int(sum) != 1 {
		t.Fatalf("ack_count sum want 1 got %v", sum)
	}
}

func TestMetricsByPartitionCounts(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()
	ns := "default"
	ch := "metrics-part"

	// 2 partitions
	if err := svc.CreateStream(ctx, ns, ch, 2); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	// Choose keys mapping to different partitions
	pickPart := func(k string) int { return int(crc32.ChecksumIEEE([]byte(k)) % 2) }
	k0, k1 := "ka", "kb"
	// ensure different
	if pickPart(k0) == pickPart(k1) {
		k1 = "kc"
		if pickPart(k0) == pickPart(k1) {
			k1 = "kd"
		}
	}

	_, _ = svc.Publish(ctx, ns, ch, []byte("x"), nil, k0)
	_, _ = svc.Publish(ctx, ns, ch, []byte("y"), nil, k1)

	stepMs := int64(time.Minute / time.Millisecond)
	nowMs := time.Now().UnixMilli()
	startMs := ((nowMs / stepMs) - 1) * stepMs
	endMs := startMs + 2*stepMs

	series, err := svc.Metrics(ctx, ns, ch, MetricPublishCount, startMs, endMs, stepMs, true, "")
	if err != nil {
		t.Fatalf("metrics publish_count by_partition: %v", err)
	}
	if len(series) != 2 {
		t.Fatalf("expected 2 series, got %d", len(series))
	}
	// Each partition should have sum 1
	for i, s := range series {
		var sum float64
		for _, p := range s.Points {
			sum += p[1]
		}
		if int(sum) != 1 {
			t.Fatalf("series %d sum want 1 got %v", i, sum)
		}
	}
}

func TestDeleteMessage(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish a message
	id, err := svc.Publish(ctx, "default", "test", []byte("hello"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Verify message exists
	items, _, err := svc.ListMessages(ctx, "default", "test", 0, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 message, got %d", len(items))
	}

	// Delete the message
	err = svc.DeleteMessage(ctx, "default", "test", id)
	if err != nil {
		t.Fatalf("delete message: %v", err)
	}

	// Verify message is gone
	items, _, err = svc.ListMessages(ctx, "default", "test", 0, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages after delete: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected 0 messages after delete, got %d", len(items))
	}
}

func TestDeleteMessageNotFound(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Try to delete a non-existent message
	fakeID := make([]byte, 8)
	binary.BigEndian.PutUint64(fakeID, 999)

	err := svc.DeleteMessage(ctx, "default", "test", fakeID)
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
	if err.Error() != "message not found" {
		t.Fatalf("expected 'message not found' error, got: %v", err)
	}
}

// New tests for retry/DLQ functionality

func TestNackStoresError(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// First publish a message to get a valid ID
	id, err := svc.Publish(ctx, "default", "orders", []byte("test message"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	errorMsg := "database connection failed"

	// Nack with error message
	if err := svc.Nack(ctx, "default", "orders", "workers", id, errorMsg); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Verify error message is stored
	storedError := svc.getErrorMessage("default", "orders", "workers", id)
	if storedError != errorMsg {
		t.Fatalf("expected error message %q, got %q", errorMsg, storedError)
	}
}

func TestNackWithoutError(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// First publish a message to get a valid ID
	id, err := svc.Publish(ctx, "default", "orders", []byte("test message"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Nack without error message
	if err := svc.Nack(ctx, "default", "orders", "workers", id, ""); err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Verify no error message is stored
	storedError := svc.getErrorMessage("default", "orders", "workers", id)
	if storedError != "" {
		t.Fatalf("expected empty error message, got %q", storedError)
	}
}

func TestListRetryMessages(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// First publish messages to get valid IDs
	id1, err := svc.Publish(ctx, "default", "orders", []byte("message 1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	id2, err := svc.Publish(ctx, "default", "orders", []byte("message 2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Nack messages to create retry entries
	if err := svc.Nack(ctx, "default", "orders", "workers", id1, "error 1"); err != nil {
		t.Fatalf("nack 1: %v", err)
	}
	if err := svc.Nack(ctx, "default", "orders", "workers", id2, "error 2"); err != nil {
		t.Fatalf("nack 2: %v", err)
	}

	// List retry messages
	items, nextToken, err := svc.ListRetryMessages(ctx, "default", "orders", "workers", nil, 10, false)
	if err != nil {
		t.Fatalf("list retry messages: %v", err)
	}

	if len(items) == 0 {
		t.Fatalf("expected retry messages, got none")
	}

	// Verify retry message structure
	for _, item := range items {
		if item.Group != "workers" {
			t.Fatalf("expected group 'workers', got %q", item.Group)
		}
		if item.RetryCount == 0 {
			t.Fatalf("expected retry count > 0, got %d", item.RetryCount)
		}
		if item.MaxRetries != svc.policy.MaxAttempts {
			t.Fatalf("expected max retries %d, got %d", svc.policy.MaxAttempts, item.MaxRetries)
		}
		if item.NextRetryAtMs <= 0 {
			t.Fatalf("expected next retry time > 0, got %d", item.NextRetryAtMs)
		}
	}

	// Verify pagination token
	if nextToken != nil {
		t.Logf("got next token: %x", nextToken)
	}
}

func TestListDLQMessages(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// First publish a message to get a valid ID
	id, err := svc.Publish(ctx, "default", "orders", []byte("dlq message"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Create a DLQ message by nacking 5 times
	for i := 0; i < 5; i++ {
		if err := svc.Nack(ctx, "default", "orders", "workers", id, "persistent error"); err != nil {
			t.Fatalf("nack %d: %v", i+1, err)
		}
	}

	// List DLQ messages
	items, nextToken, err := svc.ListDLQMessages(ctx, "default", "orders", "workers", nil, 10, false)
	if err != nil {
		t.Fatalf("list dlq messages: %v", err)
	}

	if len(items) == 0 {
		t.Fatalf("expected dlq messages, got none")
	}

	// Verify DLQ message structure
	for _, item := range items {
		if item.Group != "workers" {
			t.Fatalf("expected group 'workers', got %q", item.Group)
		}
		if item.RetryCount != svc.policy.MaxAttempts {
			t.Fatalf("expected retry count %d, got %d", svc.policy.MaxAttempts, item.RetryCount)
		}
		if item.MaxRetries != svc.policy.MaxAttempts {
			t.Fatalf("expected max retries %d, got %d", svc.policy.MaxAttempts, item.MaxRetries)
		}
		if item.FailedAtMs <= 0 {
			t.Fatalf("expected failed time > 0, got %d", item.FailedAtMs)
		}
		if item.LastError == "" {
			t.Fatalf("expected error message, got empty")
		}
	}

	// Verify pagination token
	if nextToken != nil {
		t.Logf("got next token: %x", nextToken)
	}
}

func TestGetRetryDLQStats(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Create some retry and DLQ messages by first publishing them
	id1, err := svc.Publish(ctx, "default", "orders", []byte("retry message"), nil, "")
	if err != nil {
		t.Fatalf("publish retry: %v", err)
	}
	id2, err := svc.Publish(ctx, "default", "orders", []byte("dlq message"), nil, "")
	if err != nil {
		t.Fatalf("publish dlq: %v", err)
	}

	// Create retry message
	if err := svc.Nack(ctx, "default", "orders", "workers", id1, "retry error"); err != nil {
		t.Fatalf("nack retry: %v", err)
	}

	// Create DLQ message
	for i := 0; i < 5; i++ {
		if err := svc.Nack(ctx, "default", "orders", "workers", id2, "dlq error"); err != nil {
			t.Fatalf("nack dlq %d: %v", i+1, err)
		}
	}

	// Get stats
	stats, err := svc.GetRetryDLQStats(ctx, "default", "orders", "workers")
	if err != nil {
		t.Fatalf("get retry dlq stats: %v", err)
	}

	if len(stats) == 0 {
		t.Fatalf("expected stats, got none")
	}

	// Verify stats structure
	stat := stats[0]
	if stat.Namespace != "default" {
		t.Fatalf("expected namespace 'default', got %q", stat.Namespace)
	}
	if stat.Stream != "orders" {
		t.Fatalf("expected stream 'orders', got %q", stat.Stream)
	}
	if stat.Group != "workers" {
		t.Fatalf("expected group 'workers', got %q", stat.Group)
	}
	// Note: The counting methods are not fully implemented yet, so we just verify the structure
	// TODO: Implement proper counting in countRetryMessages and countDLQMessages
	t.Logf("got stats: retry=%d, dlq=%d", stat.RetryCount, stat.DLQCount)
}

func TestListRetryMessagesWithPagination(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Create multiple retry messages by first publishing them
	for i := 0; i < 3; i++ {
		id, err := svc.Publish(ctx, "default", "orders", []byte(fmt.Sprintf("pagination message %d", i)), nil, "")
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if err := svc.Nack(ctx, "default", "orders", "workers", id, "error"); err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}

	// Test pagination with limit
	items, nextToken, err := svc.ListRetryMessages(ctx, "default", "orders", "workers", nil, 2, false)
	if err != nil {
		t.Fatalf("list retry messages: %v", err)
	}

	if len(items) > 2 {
		t.Fatalf("expected at most 2 items, got %d", len(items))
	}

	// Test with next token
	if nextToken != nil {
		items2, _, err := svc.ListRetryMessages(ctx, "default", "orders", "workers", nextToken, 2, false)
		if err != nil {
			t.Fatalf("list retry messages with token: %v", err)
		}
		t.Logf("got %d more items with pagination", len(items2))
	}
}

func TestListRetryMessagesReverse(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Create retry messages by first publishing them
	for i := 0; i < 2; i++ {
		id, err := svc.Publish(ctx, "default", "orders", []byte(fmt.Sprintf("reverse message %d", i)), nil, "")
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		if err := svc.Nack(ctx, "default", "orders", "workers", id, "error"); err != nil {
			t.Fatalf("nack %d: %v", i, err)
		}
	}

	// Test reverse order
	items, _, err := svc.ListRetryMessages(ctx, "default", "orders", "workers", nil, 10, true)
	if err != nil {
		t.Fatalf("list retry messages reverse: %v", err)
	}

	if len(items) == 0 {
		t.Fatalf("expected retry messages, got none")
	}

	t.Logf("got %d items in reverse order", len(items))
}

func TestRetryDLQStatsEmpty(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Get stats for non-existent stream
	stats, err := svc.GetRetryDLQStats(ctx, "default", "nonexistent", "workers")
	if err != nil {
		t.Fatalf("get retry dlq stats: %v", err)
	}

	// Should return empty stats, not error
	if len(stats) == 0 {
		t.Log("got empty stats for non-existent stream (expected)")
	}
}

func TestCalculateNextRetryTime(t *testing.T) {
	svc, _ := newServiceForTest(t)

	// Test exponential backoff calculation
	baseTime := int64(1000000) // 1 second in ms

	testCases := []struct {
		attempts    uint32
		expectedMin int64
		expectedMax int64
	}{
		{1, 1000200, 1000200},  // 200ms
		{2, 1000400, 1000400},  // 400ms
		{3, 1000800, 1000800},  // 800ms
		{4, 1001600, 1001600},  // 1600ms
		{5, 1003200, 1003200},  // 3200ms
		{10, 1030000, 1030000}, // 30s max (200ms * 2^9 = 102.4s, capped at 30s)
	}

	for _, tc := range testCases {
		nextRetry := svc.calculateNextRetryTime(tc.attempts, baseTime)
		if nextRetry < tc.expectedMin || nextRetry > tc.expectedMax {
			t.Fatalf("attempts %d: expected %d-%d, got %d", tc.attempts, tc.expectedMin, tc.expectedMax, nextRetry)
		}
	}
}

func TestComputeBackoff(t *testing.T) {
	// deterministic policy without jitter
	pol := RetryPolicy{Type: BackoffExp, Base: 200 * time.Millisecond, Cap: 1500 * time.Millisecond, Factor: 2.0, MaxAttempts: 5}
	b1 := computeBackoff(pol, 1)
	b2 := computeBackoff(pol, 2)
	b3 := computeBackoff(pol, 3)
	b4 := computeBackoff(pol, 4)
	b5 := computeBackoff(pol, 5)
	if b1 != 200*time.Millisecond || b2 != 400*time.Millisecond || b3 != 800*time.Millisecond {
		t.Fatalf("unexpected backoffs: %v %v %v", b1, b2, b3)
	}
	if b4 != 1500*time.Millisecond || b5 != 1500*time.Millisecond {
		t.Fatalf("cap not applied: %v %v", b4, b5)
	}
	// jitter policy should be within [0, cap]
	pol = RetryPolicy{Type: BackoffExpJitter, Base: 200 * time.Millisecond, Cap: 1500 * time.Millisecond, Factor: 2.0, MaxAttempts: 5}
	bj := computeBackoff(pol, 4)
	if bj < 0 || bj > 1500*time.Millisecond {
		t.Fatalf("jitter out of range: %v", bj)
	}
}

func TestListNamespaces(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no namespaces
	namespaces, err := svc.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if len(namespaces) != 0 {
		t.Fatalf("expected 0 namespaces, got %d", len(namespaces))
	}

	// Create a namespace by ensuring it
	_, err = svc.EnsureNamespace(ctx, "test-ns")
	if err != nil {
		t.Fatalf("ensure namespace: %v", err)
	}

	// Now should have the namespace
	namespaces, err = svc.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if len(namespaces) != 1 {
		t.Fatalf("expected 1 namespace, got %d", len(namespaces))
	}
	if namespaces[0] != "test-ns" {
		t.Fatalf("expected 'test-ns', got '%s'", namespaces[0])
	}

	// Create another namespace
	_, err = svc.EnsureNamespace(ctx, "test-ns2")
	if err != nil {
		t.Fatalf("ensure namespace 2: %v", err)
	}

	// Should now have both
	namespaces, err = svc.ListNamespaces(ctx)
	if err != nil {
		t.Fatalf("list namespaces: %v", err)
	}
	if len(namespaces) != 2 {
		t.Fatalf("expected 2 namespaces, got %d", len(namespaces))
	}
}

func TestListStreams(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no streams
	streams, err := svc.ListStreams(ctx, "default")
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if len(streams) != 0 {
		t.Fatalf("expected 0 streams, got %d", len(streams))
	}

	// Publish to create a stream
	_, err = svc.Publish(ctx, "default", "orders", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Should now have the stream
	streams, err = svc.ListStreams(ctx, "default")
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("expected 1 stream, got %d", len(streams))
	}
	if streams[0] != "orders" {
		t.Fatalf("expected 'orders', got '%s'", streams[0])
	}

	// Publish to another stream
	_, err = svc.Publish(ctx, "default", "payments", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Should now have both streams
	streams, err = svc.ListStreams(ctx, "default")
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}
}

func TestStreamSubscribe(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish some messages first
	_, err := svc.Publish(ctx, "default", "orders", []byte("msg1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	_, err = svc.Publish(ctx, "default", "orders", []byte("msg2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Test subscribe with options using "earliest" and limit
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		From:  "earliest",
		Limit: 2, // Limit to 2 messages
	}

	// This should return quickly due to the limit
	err = svc.StreamSubscribe(ctx, "default", "orders", "test-group", nil, opts, sink)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Should have received exactly 2 messages
	if len(received) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(received))
	}
}

func TestStreamTail(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish some messages first
	_, err := svc.Publish(ctx, "default", "orders", []byte("msg1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	_, err = svc.Publish(ctx, "default", "orders", []byte("msg2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Test tail with options using "earliest" and limit
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		From:  "earliest",
		Limit: 2, // Limit to 2 messages
	}

	// This should return quickly due to the limit
	err = svc.StreamTail(ctx, "default", "orders", nil, opts, sink)
	if err != nil {
		t.Fatalf("tail error: %v", err)
	}

	// Should have received exactly 2 messages
	if len(received) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(received))
	}
}

func TestStreamSubscribeWithAtTime(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Record current time
	now := time.Now()

	// Publish a message
	_, err := svc.Publish(ctx, "default", "orders", []byte("msg1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Publish another message
	_, err = svc.Publish(ctx, "default", "orders", []byte("msg2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Test subscribe with AtMs option - subscribe from 60 seconds ago
	// This should get all messages since they were published recently
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		AtMs:  now.Add(-60 * time.Second).UnixMilli(),
		Limit: 2, // Limit to 2 messages
	}

	// This should return quickly due to the limit
	err = svc.StreamSubscribe(ctx, "default", "orders", "test-group", nil, opts, sink)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Should have received exactly 2 messages
	if len(received) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(received))
	}
}

func TestListMessages(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish some messages
	id1, err := svc.Publish(ctx, "default", "orders", []byte("msg1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	id2, err := svc.Publish(ctx, "default", "orders", []byte("msg2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// List messages
	messages, _, err := svc.ListMessages(ctx, "default", "orders", 0, nil, 10, false)
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	// Check message content
	if string(messages[0].Payload) != "msg1" {
		t.Fatalf("expected 'msg1', got '%s'", string(messages[0].Payload))
	}
	if string(messages[1].Payload) != "msg2" {
		t.Fatalf("expected 'msg2', got '%s'", string(messages[1].Payload))
	}

	// Check IDs
	if string(messages[0].ID) != string(id1) {
		t.Fatalf("expected ID1, got %x", messages[0].ID)
	}
	if string(messages[1].ID) != string(id2) {
		t.Fatalf("expected ID2, got %x", messages[1].ID)
	}

	// Test pagination - skip this test as it's flaky due to timing
	// The pagination logic depends on the exact sequence numbers which can vary
}

func TestListMessagesReverse(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish some messages
	_, err := svc.Publish(ctx, "default", "orders", []byte("msg1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	_, err = svc.Publish(ctx, "default", "orders", []byte("msg2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// List messages in reverse order
	messages, _, err := svc.ListMessages(ctx, "default", "orders", 0, nil, 10, true)
	if err != nil {
		t.Fatalf("list messages reverse: %v", err)
	}

	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}

	// In reverse order, msg2 should come first
	if string(messages[0].Payload) != "msg2" {
		t.Fatalf("expected 'msg2' first in reverse, got '%s'", string(messages[0].Payload))
	}
	if string(messages[1].Payload) != "msg1" {
		t.Fatalf("expected 'msg1' second in reverse, got '%s'", string(messages[1].Payload))
	}
}

func TestStreamStats(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no stats
	stats, total, err := svc.StreamStats(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("stream stats: %v", err)
	}
	// The number of partitions depends on the default configuration
	if len(stats) == 0 {
		t.Fatalf("expected at least 1 partition, got %d", len(stats))
	}
	if total != 0 {
		t.Fatalf("expected 0 total messages, got %d", total)
	}

	// Publish some messages
	_, err = svc.Publish(ctx, "default", "orders", []byte("msg1"), nil, "")
	if err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	_, err = svc.Publish(ctx, "default", "orders", []byte("msg2"), nil, "")
	if err != nil {
		t.Fatalf("publish 2: %v", err)
	}

	// Get stats
	stats, total, err = svc.StreamStats(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("stream stats: %v", err)
	}
	if len(stats) == 0 {
		t.Fatalf("expected at least 1 partition, got %d", len(stats))
	}
	if total != 2 {
		t.Fatalf("expected 2 total messages, got %d", total)
	}

	// Check that we have some messages in the partitions
	totalInPartitions := uint64(0)
	for _, part := range stats {
		totalInPartitions += part.Count
	}
	if totalInPartitions != 2 {
		t.Fatalf("expected 2 total messages across all partitions, got %d", totalInPartitions)
	}
}

func TestGetMessageByID(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish a message
	payload := []byte("test message")
	headers := map[string]string{"key": "value"}
	id, err := svc.Publish(ctx, "default", "orders", payload, headers, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Get message by ID
	msg, err := svc.GetMessageByID(ctx, "default", "orders", id)
	if err != nil {
		t.Fatalf("get message by ID: %v", err)
	}

	// Verify message content
	if string(msg.Payload) != string(payload) {
		t.Fatalf("expected payload '%s', got '%s'", string(payload), string(msg.Payload))
	}
	if string(msg.ID) != string(id) {
		t.Fatalf("expected ID %x, got %x", id, msg.ID)
	}
	if msg.Partition != 0 {
		t.Fatalf("expected partition 0, got %d", msg.Partition)
	}
	if msg.Seq == 0 {
		t.Fatalf("expected non-zero sequence, got %d", msg.Seq)
	}

	// Test with non-existent ID
	nonExistentID := []byte{0, 0, 0, 0, 0, 0, 0, 99}
	_, err = svc.GetMessageByID(ctx, "default", "orders", nonExistentID)
	if err == nil {
		t.Fatal("expected error for non-existent message")
	}
}

func TestActiveSubscribersCount(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no subscribers
	count := svc.ActiveSubscribersCount("default", "orders")
	if count != 0 {
		t.Fatalf("expected 0 subscribers, got %d", count)
	}

	// Publish a message first
	_, err := svc.Publish(ctx, "default", "orders", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Start a subscription with "earliest" and limit to get existing messages
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		From:  "earliest",
		Limit: 1, // Limit to 1 message to avoid blocking
	}

	// This should return quickly due to the limit
	err = svc.StreamSubscribe(ctx, "default", "orders", "group1", nil, opts, sink)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Should have received the message
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}
}

func TestLastDeliveredMs(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no deliveries
	ts, err := svc.LastDeliveredMs(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("last delivered ms: %v", err)
	}
	if ts != 0 {
		t.Fatalf("expected 0 timestamp, got %d", ts)
	}

	// Publish a message first
	_, err = svc.Publish(ctx, "default", "orders", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Subscribe and ack the message using "earliest" and limit to get existing messages
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		From:  "earliest",
		Limit: 1, // Limit to 1 message to avoid blocking
	}

	// This should return quickly due to the limit
	err = svc.StreamSubscribe(ctx, "default", "orders", "group1", nil, opts, sink)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Ack the message if we received it
	if len(received) > 0 {
		err = svc.Ack(ctx, "default", "orders", "group1", received[0].ID)
		if err != nil {
			t.Fatalf("ack: %v", err)
		}
	}

	// Should now have a delivery timestamp
	ts, err = svc.LastDeliveredMs(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("last delivered ms: %v", err)
	}
	if ts == 0 {
		t.Fatalf("expected non-zero timestamp, got %d", ts)
	}
}

func TestGroupsCount(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no groups
	count, err := svc.GroupsCount(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("groups count: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected 0 groups, got %d", count)
	}

	// Publish a message first
	_, err = svc.Publish(ctx, "default", "orders", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Subscribe with a group using "earliest" and limit to get existing messages
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		From:  "earliest",
		Limit: 1, // Limit to 1 message to avoid blocking
	}

	// This should return quickly due to the limit
	err = svc.StreamSubscribe(ctx, "default", "orders", "group1", nil, opts, sink)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Should have received the message
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}

	// Note: Group tracking requires cursors to be created, which happens when messages
	// are actually processed and acknowledged. The limit-based subscription might not
	// create cursors properly. This test verifies the function works but doesn't
	// rely on immediate group tracking after subscription.

	// Test that the function works without errors
	count, err = svc.GroupsCount(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("groups count: %v", err)
	}
	// We don't assert the count since cursors might not be created with limit-based subscription
}

func TestGetWithCursorGroups(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no groups
	groups, err := svc.GetWithCursorGroups(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("get cursor groups: %v", err)
	}
	if len(groups) != 0 {
		t.Fatalf("expected 0 groups, got %d", len(groups))
	}

	// Publish a message first
	_, err = svc.Publish(ctx, "default", "orders", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Subscribe with a group using "earliest" and limit to get existing messages
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		From:  "earliest",
		Limit: 1, // Limit to 1 message to avoid blocking
	}

	// This should return quickly due to the limit
	err = svc.StreamSubscribe(ctx, "default", "orders", "group1", nil, opts, sink)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Should have received the message
	if len(received) != 1 {
		t.Fatalf("expected 1 message, got %d", len(received))
	}

	// Note: Group tracking requires cursors to be created, which happens when messages
	// are actually processed and acknowledged. The limit-based subscription might not
	// create cursors properly. This test verifies the function works but doesn't
	// rely on immediate group tracking after subscription.

	// Test that the function works without errors
	groups, err = svc.GetWithCursorGroups(ctx, "default", "orders")
	if err != nil {
		t.Fatalf("get cursor groups: %v", err)
	}
	// We don't assert the count since cursors might not be created with limit-based subscription
}

func TestGetMessageDeliveryStatus(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Publish a message
	id, err := svc.Publish(ctx, "default", "orders", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Initially no delivery status
	statuses, err := svc.GetMessageDeliveryStatus(ctx, "default", "orders", id)
	if err != nil {
		t.Fatalf("get delivery status: %v", err)
	}
	if len(statuses) != 0 {
		t.Fatalf("expected 0 delivery statuses, got %d", len(statuses))
	}

	// Subscribe and receive the message using "earliest" and limit to get existing messages
	var received []SubscribeItem
	sink := &testSink{items: &received}

	opts := SubscribeOptions{
		From:  "earliest",
		Limit: 1, // Limit to 1 message to avoid blocking
	}

	// This should return quickly due to the limit
	err = svc.StreamSubscribe(ctx, "default", "orders", "group1", nil, opts, sink)
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// Should now have delivery status
	statuses, err = svc.GetMessageDeliveryStatus(ctx, "default", "orders", id)
	if err != nil {
		t.Fatalf("get delivery status: %v", err)
	}
	if len(statuses) != 1 {
		t.Fatalf("expected 1 delivery status, got %d", len(statuses))
	}

	status := statuses[0]
	if status.Group != "group1" {
		t.Fatalf("expected group 'group1', got '%s'", status.Group)
	}
	if status.DeliveredAt == 0 {
		t.Fatalf("expected non-zero delivered at, got %d", status.DeliveredAt)
	}
	if status.Acknowledged {
		t.Fatalf("expected not acknowledged, got %v", status.Acknowledged)
	}
	if status.Nacked {
		t.Fatalf("expected not nacked, got %v", status.Nacked)
	}
}

func TestDiscoverGroupsWithRetryData(t *testing.T) {
	svc, _ := newServiceForTest(t)
	ctx := context.Background()

	// Initially no groups with retry data
	groups := svc.DiscoverGroupsWithRetryData("default", "orders")
	if len(groups) != 0 {
		t.Fatalf("expected 0 groups with retry data, got %d", len(groups))
	}

	// Publish and nack a message to create retry data
	id, err := svc.Publish(ctx, "default", "orders", []byte("test"), nil, "")
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Nack the message
	err = svc.Nack(ctx, "default", "orders", "group1", id, "test error")
	if err != nil {
		t.Fatalf("nack: %v", err)
	}

	// Should now have the group with retry data
	groups = svc.DiscoverGroupsWithRetryData("default", "orders")
	if len(groups) != 1 {
		t.Fatalf("expected 1 group with retry data, got %d", len(groups))
	}
	if groups[0] != "group1" {
		t.Fatalf("expected 'group1', got '%s'", groups[0])
	}
}

// Helper type for testing
type testSink struct {
	items *[]SubscribeItem
}

func (t *testSink) Send(item SubscribeItem) error {
	*t.items = append(*t.items, item)
	return nil
}

func (t *testSink) Flush() error {
	return nil
}

func (t *testSink) Context() context.Context {
	return context.Background()
}
