package workqueues

// Metric represents a time-series metric type for WorkQueues
type Metric string

const (
	MetricEnqueueCount  Metric = "enqueue_count"
	MetricDequeueCount  Metric = "dequeue_count"
	MetricCompleteCount Metric = "complete_count"
	MetricFailCount     Metric = "fail_count"
	MetricEnqueueRate   Metric = "enqueue_rate"
	MetricDequeueRate   Metric = "dequeue_rate"
	MetricCompleteRate  Metric = "complete_rate"
	MetricQueueDepth    Metric = "queue_depth" // pending count over time
)

// Series represents a time-series data set
type Series struct {
	Label  string       `json:"label"`
	Points [][2]float64 `json:"points"` // [timestamp_ms, value]
}

