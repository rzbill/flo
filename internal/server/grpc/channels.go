package grpcserver

import (
	"context"

	"github.com/rzbill/flo/internal/eventlog"
	channelsvc "github.com/rzbill/flo/internal/services/channels"
	flov1 "github.com/rzbill/flo/proto/gen/go/flo/v1"
)

type channelsSvc struct {
	flov1.UnimplementedChannelsServiceServer
	svc *channelsvc.Service
}

func (s *channelsSvc) CreateChannel(ctx context.Context, req *flov1.CreateChannelRequest) (*flov1.CreateChannelResponse, error) {
	if err := s.svc.CreateChannel(ctx, req.GetNamespace(), req.GetName(), int(req.GetPartitions())); err != nil {
		return nil, err
	}
	return &flov1.CreateChannelResponse{}, nil
}

func (s *channelsSvc) Publish(ctx context.Context, req *flov1.PublishRequest) (*flov1.PublishResponse, error) {
	id, err := s.svc.Publish(ctx, req.GetNamespace(), req.GetChannel(), req.GetPayload(), req.GetHeaders(), req.GetKey())
	if err != nil {
		return nil, err
	}
	return &flov1.PublishResponse{Id: id}, nil
}

type grpcSink struct {
	stream flov1.ChannelsService_SubscribeServer
}

func (g grpcSink) Send(it channelsvc.SubscribeItem) error {
	return g.stream.Send(&flov1.SubscribeResponse{Id: it.ID, Payload: it.Payload})
}
func (g grpcSink) Context() context.Context { return g.stream.Context() }
func (g grpcSink) Flush() error             { return nil }

func (s *channelsSvc) Subscribe(req *flov1.SubscribeRequest, stream flov1.ChannelsService_SubscribeServer) error {
	return s.svc.StreamSubscribe(stream.Context(), req.GetNamespace(), req.GetChannel(), req.GetGroup(), req.GetStartToken(), grpcSink{stream: stream})
}

func (s *channelsSvc) Ack(ctx context.Context, req *flov1.AckRequest) (*flov1.AckResponse, error) {
	if err := s.svc.Ack(ctx, req.GetNamespace(), req.GetChannel(), req.GetGroup(), req.GetId()); err != nil {
		return nil, err
	}
	return &flov1.AckResponse{}, nil
}

func (s *channelsSvc) Nack(ctx context.Context, req *flov1.NackRequest) (*flov1.NackResponse, error) {
	if err := s.svc.Nack(ctx, req.GetNamespace(), req.GetChannel(), req.GetGroup(), req.GetId()); err != nil {
		return nil, err
	}
	return &flov1.NackResponse{}, nil
}

func tokenFromSeq(seq uint64) (t eventlog.Token) {
	t = eventlog.Token{}
	t[0] = byte(seq >> 56)
	t[1] = byte(seq >> 48)
	t[2] = byte(seq >> 40)
	t[3] = byte(seq >> 32)
	t[4] = byte(seq >> 24)
	t[5] = byte(seq >> 16)
	t[6] = byte(seq >> 8)
	t[7] = byte(seq)
	return
}
