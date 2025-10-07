package transports

import "context"

// Message represents a stream message item returned by ListMessages.
type Message struct {
	ID        []byte
	Payload   []byte
	Header    []byte
	Seq       uint64
	Partition uint32
}

// SubscribeRequest describes a subscription request for a stream.
type SubscribeRequest struct {
	Namespace  string
	Stream     string
	Group      string
	StartToken []byte
	From       string
	AtMs       int64
	Limit      int
	// Optional retry policy and pacing for this subscription (persisted server-side)
	Policy      *RetryPolicy
	RetryPaceMs int64
}

// PartitionStats contains per-partition counters.
type PartitionStats struct {
	Partition uint32
	FirstSeq  uint64
	LastSeq   uint64
	Count     uint64
	Bytes     uint64
}

// StreamStats aggregates stream statistics.
type StreamStats struct {
	TotalCount        uint64
	TotalBytes        uint64
	LastPublishMs     uint64
	LastDeliveredMs   uint64
	ActiveSubscribers int
	GroupsCount       int
	Partitions        []PartitionStats
}

// SrStreamsTransport abstracts the transport used by the CLI (gRPC/HTTP).
type StreamsTransport interface {
	Create(ctx context.Context, ns, name string, partitions int) error
	Publish(ctx context.Context, ns, st string, payload []byte, headers map[string]string) error
	Subscribe(ctx context.Context, req SubscribeRequest, onMessage func(id, payload []byte) error) error
	Tail(ctx context.Context, req TailRequest, onMessage func(id, payload []byte) error) error
	Ack(ctx context.Context, ns, st, group string, id []byte) error
	Nack(ctx context.Context, ns, st, group string, id []byte, errMsg string) error
	ListMessages(ctx context.Context, ns, st string, partition int, startToken []byte, limit int, reverse bool) (items []Message, nextToken []byte, err error)
	GetStats(ctx context.Context, ns, ch string) (StreamStats, error)
	Flush(ctx context.Context, ns, st string, partition int) (deleted uint64, err error)
	DeleteMessage(ctx context.Context, ns, st string, id []byte) error
}

// TailRequest describes a tail (no-group) streaming request
type TailRequest struct {
	Namespace  string
	Stream     string
	StartToken []byte
	From       string
	AtMs       int64
	Limit      int
	Filter     string
}

// RetryPolicy mirrors gRPC request policy for CLI
type RetryPolicy struct {
	Type        string
	BaseMs      int64
	CapMs       int64
	Factor      float64
	MaxAttempts uint32
}
