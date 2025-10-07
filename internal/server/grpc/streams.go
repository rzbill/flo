package grpcserver

import (
	"context"
	"time"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	streamsvc "github.com/rzbill/flo/internal/services/streams"
)

type streamsSvc struct {
	flov1.UnimplementedStreamsServiceServer
	svc *streamsvc.Service
}

func (s *streamsSvc) Create(ctx context.Context, req *flov1.CreateStreamRequest) (*flov1.CreateStreamResponse, error) {
	// Always use options-based API (compat retained by service)
	opts := streamsvc.CreateStreamOptions{
		Namespace:      req.GetNamespace(),
		Name:           req.GetName(),
		Partitions:     int(req.GetPartitions()),
		RetentionAgeMs: req.GetRetentionAgeMs(),
		MaxLenBytes:    req.GetMaxLenBytes(),
	}
	if err := s.svc.CreateStreamWithOptions(ctx, opts); err != nil {
		return nil, err
	}
	return &flov1.CreateStreamResponse{}, nil
}

func (s *streamsSvc) Publish(ctx context.Context, req *flov1.PublishRequest) (*flov1.PublishResponse, error) {
	id, err := s.svc.Publish(ctx, req.GetNamespace(), req.GetStream(), req.GetPayload(), req.GetHeaders(), req.GetKey())
	if err != nil {
		return nil, err
	}
	return &flov1.PublishResponse{Id: id}, nil
}

type grpcSink struct {
	stream flov1.StreamsService_SubscribeServer
}

func (g grpcSink) Send(it streamsvc.SubscribeItem) error {
	return g.stream.Send(&flov1.SubscribeResponse{Id: it.ID, Payload: it.Payload, Headers: it.Headers})
}
func (g grpcSink) Context() context.Context { return g.stream.Context() }
func (g grpcSink) Flush() error             { return nil }

func (s *streamsSvc) Subscribe(req *flov1.SubscribeRequest, stream flov1.StreamsService_SubscribeServer) error {
	opts := streamsvc.SubscribeOptions{}
	if req.GetFrom() == "earliest" {
		opts.From = "earliest"
	}
	if req.GetAtMs() > 0 {
		opts.AtMs = req.GetAtMs()
	}
	if req.GetLimit() > 0 {
		opts.Limit = int(req.GetLimit())
	}
	if f := req.GetFilter(); f != "" {
		opts.Filter = f
	}
	if p := req.GetPolicy(); p != nil {
		pol := streamsvc.RetryPolicy{Type: streamsvc.BackoffType(p.GetType()), Base: time.Duration(p.GetBaseMs()) * time.Millisecond, Cap: time.Duration(p.GetCapMs()) * time.Millisecond, Factor: p.GetFactor(), MaxAttempts: p.GetMaxAttempts()}
		opts.Policy = &pol
	}
	if rp := req.GetRetryPaceMs(); rp > 0 {
		d := time.Duration(rp) * time.Millisecond
		opts.RetryPace = &d
	}
	return s.svc.StreamSubscribe(stream.Context(), req.GetNamespace(), req.GetStream(), req.GetGroup(), req.GetStartToken(), opts, grpcSink{stream: stream})
}

func (s *streamsSvc) Tail(req *flov1.TailRequest, stream flov1.StreamsService_TailServer) error {
	opts := streamsvc.SubscribeOptions{}
	if req.GetFrom() == "earliest" {
		opts.From = "earliest"
	}
	if req.GetAtMs() > 0 {
		opts.AtMs = req.GetAtMs()
	}
	if req.GetLimit() > 0 {
		opts.Limit = int(req.GetLimit())
	}
	if f := req.GetFilter(); f != "" {
		opts.Filter = f
	}
	return s.svc.StreamTail(stream.Context(), req.GetNamespace(), req.GetStream(), req.GetStartToken(), opts, grpcSink{stream: stream})
}

func (s *streamsSvc) Ack(ctx context.Context, req *flov1.AckRequest) (*flov1.AckResponse, error) {
	if err := s.svc.Ack(ctx, req.GetNamespace(), req.GetStream(), req.GetGroup(), req.GetId()); err != nil {
		return nil, err
	}
	return &flov1.AckResponse{}, nil
}

func (s *streamsSvc) Nack(ctx context.Context, req *flov1.NackRequest) (*flov1.NackResponse, error) {
	if err := s.svc.Nack(ctx, req.GetNamespace(), req.GetStream(), req.GetGroup(), req.GetId(), req.GetError()); err != nil {
		return nil, err
	}
	return &flov1.NackResponse{}, nil
}

func (s *streamsSvc) ListMessages(ctx context.Context, req *flov1.ListMessagesRequest) (*flov1.ListMessagesResponse, error) {
	part := req.GetPartition()
	if part < 0 {
		part = 0
	}
	items, next, err := s.svc.ListMessages(ctx, req.GetNamespace(), req.GetStream(), uint32(part), req.GetStartToken(), int(req.GetLimit()), req.GetReverse())
	if err != nil {
		return nil, err
	}
	out := &flov1.ListMessagesResponse{NextToken: next}
	out.Items = make([]*flov1.MessageItem, 0, len(items))
	for _, it := range items {
		out.Items = append(out.Items, &flov1.MessageItem{Id: it.ID, Payload: it.Payload, Header: it.Header, Seq: it.Seq, Partition: it.Partition})
	}
	return out, nil
}

func (s *streamsSvc) GetStats(ctx context.Context, req *flov1.GetStreamStatsRequest) (*flov1.GetStreamStatsResponse, error) {
	parts, total, err := s.svc.StreamStats(ctx, req.GetNamespace(), req.GetStream())
	if err != nil {
		return nil, err
	}
	out := &flov1.GetStreamStatsResponse{TotalCount: total}
	out.Partitions = make([]*flov1.PartitionStats, 0, len(parts))
	var lastPubMax uint64
	var totalBytes uint64
	for _, ps := range parts {
		out.Partitions = append(out.Partitions, &flov1.PartitionStats{Partition: ps.Partition, FirstSeq: ps.FirstSeq, LastSeq: ps.LastSeq, Count: ps.Count, Bytes: ps.Bytes, LastPublishMs: ps.LastPublishMs})
		totalBytes += ps.Bytes
		if ps.LastPublishMs > lastPubMax {
			lastPubMax = ps.LastPublishMs
		}
	}
	out.TotalBytes = totalBytes
	out.LastPublishMs = lastPubMax
	out.ActiveSubscribers = int32(s.svc.ActiveSubscribersCount(req.GetNamespace(), req.GetStream()))
	if ts, _ := s.svc.LastDeliveredMs(ctx, req.GetNamespace(), req.GetStream()); ts > 0 {
		out.LastDeliveredMs = ts
	}
	if gc, err := s.svc.GroupsCount(ctx, req.GetNamespace(), req.GetStream()); err == nil {
		out.GroupsCount = int32(gc)
	}
	return out, nil
}

func (s *streamsSvc) Flush(ctx context.Context, req *flov1.FlushStreamRequest) (*flov1.FlushStreamResponse, error) {
	var partition *uint32
	if req.GetPartition() >= 0 {
		p := uint32(req.GetPartition())
		partition = &p
	}

	deletedCount, err := s.svc.FlushStream(ctx, req.GetNamespace(), req.GetStream(), partition)
	if err != nil {
		return nil, err
	}

	return &flov1.FlushStreamResponse{DeletedCount: deletedCount}, nil
}

func (s *streamsSvc) DeleteMessage(ctx context.Context, req *flov1.DeleteMessageRequest) (*flov1.DeleteMessageResponse, error) {
	if err := s.svc.DeleteMessage(ctx, req.GetNamespace(), req.GetStream(), req.GetMessageId()); err != nil {
		return nil, err
	}

	return &flov1.DeleteMessageResponse{}, nil
}

func (s *streamsSvc) ListRetryMessages(ctx context.Context, req *flov1.ListRetryMessagesRequest) (*flov1.ListRetryMessagesResponse, error) {
	items, next, err := s.svc.ListRetryMessages(ctx, req.GetNamespace(), req.GetStream(), req.GetGroup(), req.GetStartToken(), int(req.GetLimit()), req.GetReverse())
	if err != nil {
		return nil, err
	}

	out := &flov1.ListRetryMessagesResponse{NextToken: next}
	out.Items = make([]*flov1.RetryMessageItem, 0, len(items))
	for _, it := range items {
		out.Items = append(out.Items, &flov1.RetryMessageItem{
			Id:            it.ID,
			Payload:       it.Payload,
			Headers:       it.Headers,
			Partition:     it.Partition,
			Seq:           it.Seq,
			RetryCount:    it.RetryCount,
			MaxRetries:    it.MaxRetries,
			NextRetryAtMs: it.NextRetryAtMs,
			CreatedAtMs:   it.CreatedAtMs,
			LastError:     it.LastError,
			Group:         it.Group,
		})
	}
	return out, nil
}

func (s *streamsSvc) ListDLQMessages(ctx context.Context, req *flov1.ListDLQMessagesRequest) (*flov1.ListDLQMessagesResponse, error) {
	items, next, err := s.svc.ListDLQMessages(ctx, req.GetNamespace(), req.GetStream(), req.GetGroup(), req.GetStartToken(), int(req.GetLimit()), req.GetReverse())
	if err != nil {
		return nil, err
	}

	out := &flov1.ListDLQMessagesResponse{NextToken: next}
	out.Items = make([]*flov1.DLQMessageItem, 0, len(items))
	for _, it := range items {
		out.Items = append(out.Items, &flov1.DLQMessageItem{
			Id:         it.ID,
			Payload:    it.Payload,
			Headers:    it.Headers,
			Partition:  it.Partition,
			Seq:        it.Seq,
			RetryCount: it.RetryCount,
			MaxRetries: it.MaxRetries,
			FailedAtMs: it.FailedAtMs,
			LastError:  it.LastError,
			Group:      it.Group,
		})
	}
	return out, nil
}

func (s *streamsSvc) GetRetryDLQStats(ctx context.Context, req *flov1.GetRetryDLQStatsRequest) (*flov1.GetRetryDLQStatsResponse, error) {
	stats, err := s.svc.GetRetryDLQStats(ctx, req.GetNamespace(), req.GetStream(), req.GetGroup())
	if err != nil {
		return nil, err
	}

	out := &flov1.GetRetryDLQStatsResponse{}
	out.Stats = make([]*flov1.RetryDLQStats, 0, len(stats))
	for _, stat := range stats {
		out.Stats = append(out.Stats, &flov1.RetryDLQStats{
			Namespace:    stat.Namespace,
			Stream:       stat.Stream,
			Group:        stat.Group,
			RetryCount:   stat.RetryCount,
			DlqCount:     stat.DLQCount,
			TotalRetries: stat.TotalRetries,
			SuccessRate:  stat.SuccessRate,
			Groups:       stat.Groups,
		})
	}
	return out, nil
}
