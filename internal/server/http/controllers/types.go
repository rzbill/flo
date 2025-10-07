package controllers

// Common request/response types for HTTP controllers

// nsCreateReq represents a request to create a new namespace.
type nsCreateReq struct {
	Namespace string `json:"namespace"`
}

// publishReq represents a request to publish a message to a stream.
type publishReq struct {
	Namespace string            `json:"namespace"`
	Stream    string            `json:"stream"`
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers"`
	Key       string            `json:"key"`
}

// createReq represents a request to create a new stream.
type createReq struct {
	Namespace  string `json:"namespace"`
	Stream     string `json:"stream"`
	Partitions int    `json:"partitions"`
}

// ackReq represents a request to acknowledge a message.
type ackReq struct {
	Namespace string `json:"namespace"`
	Stream    string `json:"stream"`
	Group     string `json:"group"`
	ID        any    `json:"id"`
}

// nackReq represents a request to negatively acknowledge a message.
type nackReq struct {
	Namespace string `json:"namespace"`
	Stream    string `json:"stream"`
	Group     string `json:"group"`
	ID        any    `json:"id"`
	Error     string `json:"error"`
}

// listMessagesRespItem represents a message in a list response.
type listMessagesRespItem struct {
	ID        []byte `json:"id"`
	Payload   []byte `json:"payload"`
	Header    []byte `json:"header"`
	Seq       uint64 `json:"seq"`
	Partition uint32 `json:"partition"`
}

// partitionStatsJSON represents statistics for a single partition.
type partitionStatsJSON struct {
	Partition     uint32 `json:"partition"`
	FirstSeq      uint64 `json:"first_seq"`
	LastSeq       uint64 `json:"last_seq"`
	Count         uint64 `json:"count"`
	Bytes         uint64 `json:"bytes"`
	LastPublishMs uint64 `json:"last_publish_ms"`
}

// singleStreamStatsJSON represents statistics for a single stream.
type singleStreamStatsJSON struct {
	Namespace         string               `json:"namespace"`
	Stream            string               `json:"stream"`
	Partitions        []partitionStatsJSON `json:"partitions"`
	TotalCount        uint64               `json:"total_count"`
	TotalBytes        uint64               `json:"total_bytes"`
	ActiveSubscribers int                  `json:"active_subscribers"`
	LastPublishMs     uint64               `json:"last_publish_ms"`
	LastDeliveredMs   uint64               `json:"last_delivered_ms"`
	GroupsCount       int                  `json:"groups_count"`
}
