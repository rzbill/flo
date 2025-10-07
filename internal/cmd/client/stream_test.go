package client

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	"google.golang.org/grpc"
)

// --- HTTP CLI tests ---

func TestPublishGRPC_PrintsStatus(t *testing.T) {
	stub := &streamsStub{toSend: 0}
	addr, stop := startGRPCStub(t, stub)
	defer stop()
	// Use env for grpc endpoint
	t.Setenv("FLO_GRPC", addr)

	cmd := newStreamPublishCommand()
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--namespace", "default", "--stream", "orders", "--data", "hi"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !strings.Contains(buf.String(), "status:") {
		t.Fatalf("expected status in output, got: %s", buf.String())
	}
}

// --- gRPC subscribe CLI tests ---

type streamsStub struct {
	flov1.UnimplementedStreamsServiceServer
	toSend    int
	ackCount  int32
	nackCount int32
}

func (s *streamsStub) Publish(ctx context.Context, req *flov1.PublishRequest) (*flov1.PublishResponse, error) {
	// Return a deterministic id to satisfy CLI output expectations
	return &flov1.PublishResponse{Id: []byte("id-0")}, nil
}

func (s *streamsStub) Subscribe(req *flov1.SubscribeRequest, stream grpc.ServerStreamingServer[flov1.SubscribeResponse]) error {
	count := s.toSend
	if req.GetLimit() > 0 && int(req.GetLimit()) < count {
		count = int(req.GetLimit())
	}
	for i := 0; i < count; i++ {
		msg := &flov1.SubscribeResponse{
			Id:      []byte(fmt.Sprintf("id-%d", i)),
			Payload: []byte(fmt.Sprintf("payload-%d", i)),
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
}

func (s *streamsStub) Ack(ctx context.Context, req *flov1.AckRequest) (*flov1.AckResponse, error) {
	s.ackCount++
	return &flov1.AckResponse{}, nil
}

func (s *streamsStub) Nack(ctx context.Context, req *flov1.NackRequest) (*flov1.NackResponse, error) {
	s.nackCount++
	return &flov1.NackResponse{}, nil
}

func startGRPCStub(t *testing.T, svc flov1.StreamsServiceServer) (addr string, stop func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	flov1.RegisterStreamsServiceServer(gs, svc)
	done := make(chan struct{})
	go func() {
		_ = gs.Serve(l)
		close(done)
	}()
	stop = func() {
		gs.GracefulStop()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			ts := gs
			_ = ts.Stop
		}
	}
	return l.Addr().String(), stop
}

// no-op

func TestSubscribeGRPC_AckBehavior(t *testing.T) {
	stub := &streamsStub{toSend: 3}
	addr, stop := startGRPCStub(t, stub)
	defer stop()
	t.Setenv("FLO_GRPC", addr)

	cmd := newStreamSubscribeCommand()
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--namespace", "default", "--stream", "orders", "--group", "workers", "--limit", "3", "--ack"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if stub.ackCount != 3 {
		t.Fatalf("expected 3 acks, got %d", stub.ackCount)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 output lines, got %d", len(lines))
	}
}

func TestSubscribeGRPC_NackBehavior(t *testing.T) {
	stub := &streamsStub{toSend: 2}
	addr, stop := startGRPCStub(t, stub)
	defer stop()
	t.Setenv("FLO_GRPC", addr)

	cmd := newStreamSubscribeCommand()
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--namespace", "default", "--stream", "orders", "--group", "workers", "--limit", "2", "--nack", "--nack-error", "oops"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if stub.nackCount != 2 {
		t.Fatalf("expected 2 nacks, got %d", stub.nackCount)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 output lines, got %d", len(lines))
	}
}

func TestSubscribeGRPC_NoAckBehavior(t *testing.T) {
	stub := &streamsStub{toSend: 2}
	addr, stop := startGRPCStub(t, stub)
	defer stop()
	t.Setenv("FLO_GRPC", addr)

	cmd := newStreamSubscribeCommand()
	buf := &bytes.Buffer{}
	cmd.SetOut(buf)
	cmd.SetErr(buf)
	cmd.SetArgs([]string{"--namespace", "default", "--stream", "orders", "--group", "workers", "--limit", "2", "--no-ack"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}

	if stub.ackCount != 0 || stub.nackCount != 0 {
		t.Fatalf("expected no ack/nack, got ack=%d nack=%d", stub.ackCount, stub.nackCount)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 output lines, got %d", len(lines))
	}
}

func TestSubscribeGRPC_FlagValidation(t *testing.T) {
	stub := &streamsStub{toSend: 1}
	addr, stop := startGRPCStub(t, stub)
	defer stop()
	t.Setenv("FLO_GRPC", addr)

	cmd := newStreamSubscribeCommand()
	cmd.SetArgs([]string{"--namespace", "default", "--stream", "orders", "--group", "workers", "--limit", "1", "--ack", "--nack"})
	if err := cmd.Execute(); err == nil {
		t.Fatalf("expected error for conflicting flags, got nil")
	}
}
