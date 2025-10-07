// Package transports provides pluggable transport implementations for the CLI.
package transports

import (
	"context"
	"io"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	"google.golang.org/grpc"
)

// GrpcTransport implements StreamsTransport over gRPC.
type GrpcTransport struct {
	dial func(ctx context.Context) (*grpc.ClientConn, error)
}

// NewGrpcTransport constructs a new GrpcTransport using the provided dialer.
func NewGrpcTransport(dial func(ctx context.Context) (*grpc.ClientConn, error)) *GrpcTransport {
	return &GrpcTransport{dial: dial}
}

func (t *GrpcTransport) withClient(ctx context.Context, fn func(cli flov1.StreamsServiceClient) error) error {
	conn, err := t.dial(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	cli := flov1.NewStreamsServiceClient(conn)
	return fn(cli)
}

// Create creates a stream via gRPC.
func (t *GrpcTransport) Create(ctx context.Context, ns, name string, partitions int) error {
	return t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		_, err := cli.Create(ctx, &flov1.CreateStreamRequest{Namespace: ns, Name: name, Partitions: int32(partitions)})
		return err
	})
}

// Publish sends a message via gRPC.
func (t *GrpcTransport) Publish(ctx context.Context, ns, st string, payload []byte, headers map[string]string) error {
	return t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		_, err := cli.Publish(ctx, &flov1.PublishRequest{Namespace: ns, Stream: st, Payload: payload, Headers: headers})
		return err
	})
}

// Subscribe streams messages and invokes onMessage for each item.
func (t *GrpcTransport) Subscribe(ctx context.Context, req SubscribeRequest, onMessage func(id, payload []byte) error) error {
	return t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		greq := &flov1.SubscribeRequest{Namespace: req.Namespace, Stream: req.Stream, Group: req.Group, StartToken: req.StartToken, From: req.From, AtMs: req.AtMs, Limit: int32(req.Limit)}
		if req.Policy != nil {
			greq.Policy = &flov1.RetryPolicy{Type: req.Policy.Type, BaseMs: req.Policy.BaseMs, CapMs: req.Policy.CapMs, Factor: req.Policy.Factor, MaxAttempts: req.Policy.MaxAttempts}
		}
		if req.RetryPaceMs > 0 {
			greq.RetryPaceMs = req.RetryPaceMs
		}
		stream, err := cli.Subscribe(ctx, greq)
		if err != nil {
			return err
		}
		for {
			m, err := stream.Recv()
			if err != nil {
				if err == io.EOF || err == context.Canceled {
					return nil
				}
				return err
			}
			if cbErr := onMessage(m.GetId(), m.GetPayload()); cbErr != nil {
				return cbErr
			}
		}
	})
}

// Tail streams messages without a group.
func (t *GrpcTransport) Tail(ctx context.Context, req TailRequest, onMessage func(id, payload []byte) error) error {
	return t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		greq := &flov1.TailRequest{Namespace: req.Namespace, Stream: req.Stream, StartToken: req.StartToken, From: req.From, AtMs: req.AtMs, Limit: int32(req.Limit), Filter: req.Filter}
		stream, err := cli.Tail(ctx, greq)
		if err != nil {
			return err
		}
		for {
			m, err := stream.Recv()
			if err != nil {
				if err == io.EOF || err == context.Canceled {
					return nil
				}
				return err
			}
			if cbErr := onMessage(m.GetId(), m.GetPayload()); cbErr != nil {
				return cbErr
			}
		}
	})
}

// Ack acknowledges a message via gRPC.
func (t *GrpcTransport) Ack(ctx context.Context, ns, st, group string, id []byte) error {
	return t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		_, err := cli.Ack(ctx, &flov1.AckRequest{Namespace: ns, Stream: st, Group: group, Id: id})
		return err
	})
}

// Nack negatively acknowledges a message via gRPC with an optional error message.
func (t *GrpcTransport) Nack(ctx context.Context, ns, st, group string, id []byte, errMsg string) error {
	return t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		_, err := cli.Nack(ctx, &flov1.NackRequest{Namespace: ns, Stream: st, Group: group, Id: id, Error: errMsg})
		return err
	})
}

// ListMessages lists messages from a partition.
func (t *GrpcTransport) ListMessages(ctx context.Context, ns, st string, partition int, startToken []byte, limit int, reverse bool) ([]Message, []byte, error) {
	var outItems []Message
	var next []byte
	err := t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		resp, err := cli.ListMessages(ctx, &flov1.ListMessagesRequest{Namespace: ns, Stream: st, Partition: int32(partition), StartToken: startToken, Limit: int32(limit), Reverse: reverse})
		if err != nil {
			return err
		}
		outItems = make([]Message, 0, len(resp.GetItems()))
		for _, it := range resp.GetItems() {
			outItems = append(outItems, Message{ID: it.GetId(), Payload: it.GetPayload(), Header: it.GetHeader(), Seq: it.GetSeq(), Partition: it.GetPartition()})
		}
		next = resp.GetNextToken()
		return nil
	})
	return outItems, next, err
}

// GetStats returns stream stats for a namespace/stream.
func (t *GrpcTransport) GetStats(ctx context.Context, ns, st string) (StreamStats, error) {
	var stats StreamStats
	err := t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		resp, err := cli.GetStats(ctx, &flov1.GetStreamStatsRequest{Namespace: ns, Stream: st})
		if err != nil {
			return err
		}
		stats.TotalCount = resp.GetTotalCount()
		stats.TotalBytes = resp.GetTotalBytes()
		stats.LastPublishMs = resp.GetLastPublishMs()
		stats.LastDeliveredMs = resp.GetLastDeliveredMs()
		stats.ActiveSubscribers = int(resp.GetActiveSubscribers())
		stats.GroupsCount = int(resp.GetGroupsCount())
		stats.Partitions = make([]PartitionStats, 0, len(resp.GetPartitions()))
		for _, ps := range resp.GetPartitions() {
			stats.Partitions = append(stats.Partitions, PartitionStats{Partition: ps.GetPartition(), FirstSeq: ps.GetFirstSeq(), LastSeq: ps.GetLastSeq(), Count: ps.GetCount(), Bytes: ps.GetBytes()})
		}
		return nil
	})
	return stats, err
}

// Flush deletes messages (optionally limited to a partition) and returns deleted count.
func (t *GrpcTransport) Flush(ctx context.Context, ns, st string, partition int) (uint64, error) {
	var deleted uint64
	err := t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		resp, err := cli.Flush(ctx, &flov1.FlushStreamRequest{Namespace: ns, Stream: st, Partition: int32(partition)})
		if err != nil {
			return err
		}
		deleted = resp.GetDeletedCount()
		return nil
	})
	return deleted, err
}

// DeleteMessage deletes a message by ID.
func (t *GrpcTransport) DeleteMessage(ctx context.Context, ns, st string, id []byte) error {
	return t.withClient(ctx, func(cli flov1.StreamsServiceClient) error {
		_, err := cli.DeleteMessage(ctx, &flov1.DeleteMessageRequest{Namespace: ns, Stream: st, MessageId: id})
		return err
	})
}
