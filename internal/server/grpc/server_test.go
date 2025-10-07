package grpcserver

import (
	"context"
	"net"
	"testing"
	"time"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/runtime"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
	logpkg "github.com/rzbill/flo/pkg/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

func dialer(s *grpc.Server) func(context.Context, string) (net.Conn, error) {
	lis := bufconn.Listen(bufSize)
	go func() { _ = s.Serve(lis) }()
	return func(ctx context.Context, s string) (net.Conn, error) { return lis.Dial() }
}

func TestHealthOverGRPC(t *testing.T) {
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("rt open: %v", err)
	}
	defer rt.Close()
	logger, _ := logpkg.ApplyConfig(&logpkg.Config{Level: "error", Format: "text"})
	srv := New(rt, logger)
	d := dialer(srv.grpc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(d), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := flov1.NewHealthServiceClient(conn)
	res, err := c.Check(ctx, &flov1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("check: %v", err)
	}
	if res.GetStatus() == "" {
		t.Fatalf("empty status")
	}
}

func TestStreamsPublishAckOverGRPC(t *testing.T) {
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("rt open: %v", err)
	}
	defer rt.Close()
	logger, _ := logpkg.ApplyConfig(&logpkg.Config{Level: "error", Format: "text"})
	srv := New(rt, logger)
	d := dialer(srv.grpc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(d), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := flov1.NewStreamsServiceClient(conn)

	// Create stream
	if _, err := c.Create(ctx, &flov1.CreateStreamRequest{Namespace: "default", Name: "orders"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Publish
	pub, err := c.Publish(ctx, &flov1.PublishRequest{Namespace: "default", Stream: "orders", Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(pub.GetId()) == 0 {
		t.Fatalf("missing id")
	}
	// Ack
	if _, err := c.Ack(ctx, &flov1.AckRequest{Namespace: "default", Stream: "orders", Group: "workers", Id: pub.GetId()}); err != nil {
		t.Fatalf("ack: %v", err)
	}
}
