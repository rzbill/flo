package streamsvc

import (
	"context"
	"time"
)

type BackoffType string

const (
	BackoffExp       BackoffType = "exp"
	BackoffExpJitter BackoffType = "exp-jitter"
	BackoffFixed     BackoffType = "fixed"
	BackoffNone      BackoffType = "none"
)

type RetryPolicy struct {
	Type        BackoffType
	Base        time.Duration
	Cap         time.Duration
	Factor      float64
	MaxAttempts uint32
}

type groupConfig struct {
	Policy      *RetryPolicy   `json:"policy,omitempty"`
	RetryPace   *time.Duration `json:"retry_pace_ms,omitempty"`
	UpdatedAtMs int64          `json:"updated_at_ms,omitempty"`
}

// SubscribeItem represents a delivered event for streaming.
type SubscribeItem struct {
	ID        []byte
	Payload   []byte
	Headers   map[string]string
	Partition uint32
	Seq       uint64
	// WriteTsMs carries the original publish timestamp when available for e2e metrics.
	WriteTsMs int64
}

// MessageListItem is a single message returned by ListMessages.
type MessageListItem struct {
	ID        []byte
	Payload   []byte
	Header    []byte
	Partition uint32
	Seq       uint64
}

// PartitionStat summarizes a single partition.
type PartitionStat struct {
	Partition uint32
	FirstSeq  uint64
	LastSeq   uint64
	Count     uint64
	Bytes     uint64
	// LastPublishMs is the header timestamp (ms) of the latest record in the partition.
	LastPublishMs uint64
}

// SubscribeSink is implemented by transports to receive streamed items.
type SubscribeSink interface {
	Send(SubscribeItem) error
	Context() context.Context
	Flush() error
}

// SubscribeOptions controls starting position for subscribe.
// From: "latest" (default) or "earliest". AtMs: start at first event with header ts >= AtMs.
type SubscribeOptions struct {
	From string
	AtMs int64
	// Filter is an optional CEL expression evaluated per message.
	// When empty, all messages are delivered.
	Filter string
	// Limit is the maximum number of messages to deliver before stopping.
	// When 0, no limit is applied and the function runs indefinitely.
	Limit int
	// Optional retry policy override for this subscribe. When provided with a group,
	// it is persisted as the group's policy before streaming starts.
	Policy *RetryPolicy
	// Optional pacing override between multiple due retry deliveries. Persisted when provided.
	RetryPace *time.Duration
}

// SearchOptions control historical search over a stream.
type SearchOptions struct {
	From    string
	AtMs    int64
	Reverse bool
	Limit   int
	Filter  string
}

// MessageDeliveryStatus represents the delivery status of a message to a specific group
type MessageDeliveryStatus struct {
	Group        string `json:"group"`
	DeliveredAt  int64  `json:"delivered_at_ms"`
	Acknowledged bool   `json:"acknowledged"`
	Nacked       bool   `json:"nacked"`
	RetryCount   uint32 `json:"retry_count"`
	LastError    string `json:"last_error,omitempty"`
}

// RetryMessageItem represents a message in the retry queue
type RetryMessageItem struct {
	ID            []byte            `json:"id"`
	Payload       []byte            `json:"payload"`
	Headers       map[string]string `json:"headers"`
	Partition     uint32            `json:"partition"`
	Seq           uint64            `json:"seq"`
	RetryCount    uint32            `json:"retry_count"`
	MaxRetries    uint32            `json:"max_retries"`
	NextRetryAtMs int64             `json:"next_retry_at_ms"`
	CreatedAtMs   int64             `json:"created_at_ms"`
	LastError     string            `json:"last_error"`
	Group         string            `json:"group"`
}

// DLQMessageItem represents a message in the DLQ
type DLQMessageItem struct {
	ID         []byte            `json:"id"`
	Payload    []byte            `json:"payload"`
	Headers    map[string]string `json:"headers"`
	Partition  uint32            `json:"partition"`
	Seq        uint64            `json:"seq"`
	RetryCount uint32            `json:"retry_count"`
	MaxRetries uint32            `json:"max_retries"`
	FailedAtMs int64             `json:"failed_at_ms"`
	LastError  string            `json:"last_error"`
	Group      string            `json:"group"`
}

// RetryDLQStats represents statistics for retry/DLQ data
type RetryDLQStats struct {
	Namespace    string   `json:"namespace"`
	Stream       string   `json:"stream"`
	Group        string   `json:"group"`
	RetryCount   uint32   `json:"retry_count"`
	DLQCount     uint32   `json:"dlq_count"`
	TotalRetries uint32   `json:"total_retries"`
	SuccessRate  float64  `json:"success_rate"`
	Groups       []string `json:"groups,omitempty"`
}

// StreamMeta stores per-stream overrides and labels (future use).
type StreamMeta struct {
	Partitions int               `json:"partitions"`
	Labels     map[string]string `json:"labels,omitempty"`
	// RetentionAgeMs trims entries older than this age when >0.
	RetentionAgeMs int64 `json:"retention_age_ms,omitempty"`
	// MaxLenBytes approximates total bytes cap per partition when >0.
	MaxLenBytes int64 `json:"max_len_bytes,omitempty"`
}

// CreateStreamOptions encapsulates options used when creating/updating a stream's metadata.
type CreateStreamOptions struct {
	Namespace      string
	Name           string
	Partitions     int
	RetentionAgeMs int64
	MaxLenBytes    int64
}

// SubscriptionInfo summarizes consumer group position and lag.
type SubscriptionInfo struct {
	Group             string `json:"group"`
	ActiveSubscribers int    `json:"active_subscribers"`
	LastDeliveredMs   uint64 `json:"last_delivered_ms"`
	TotalLag          uint64 `json:"total_lag"`
	Partitions        []struct {
		Partition uint32 `json:"partition"`
		CursorSeq uint64 `json:"cursor_seq"`
		EndSeq    uint64 `json:"end_seq"`
		Lag       uint64 `json:"lag"`
	} `json:"partitions"`
}

type Metric string

const (
	MetricPublishCount Metric = "publish_count"
	MetricAckCount     Metric = "ack_count"
	MetricPublishRate  Metric = "publish_rate"
	MetricBytesRate    Metric = "bytes_rate"
)

type Series struct {
	Label  string       `json:"label"`
	Points [][2]float64 `json:"points"` // [t_ms, value]
}

// Metric resolutions we maintain for counters.
const (
	Res1m = "1m"
	Res5m = "5m"
	Res1h = "1h"
)
