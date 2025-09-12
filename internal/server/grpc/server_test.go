package grpcserver

import (
	"context"
	"net"
	"testing"
	"time"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/runtime"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
	flov1 "github.com/rzbill/flo/proto/gen/go/flo/v1"
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
	srv := New(rt)
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

func TestChannelsPublishAckOverGRPC(t *testing.T) {
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("rt open: %v", err)
	}
	defer rt.Close()
	srv := New(rt)
	d := dialer(srv.grpc)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(d), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	c := flov1.NewChannelsServiceClient(conn)

	// Create channel
	if _, err := c.CreateChannel(ctx, &flov1.CreateChannelRequest{Namespace: "default", Name: "orders"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Publish
	pub, err := c.Publish(ctx, &flov1.PublishRequest{Namespace: "default", Channel: "orders", Payload: []byte("hi")})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if len(pub.GetId()) == 0 {
		t.Fatalf("missing id")
	}
	// Ack
	if _, err := c.Ack(ctx, &flov1.AckRequest{Namespace: "default", Channel: "orders", Group: "workers", Id: pub.GetId()}); err != nil {
		t.Fatalf("ack: %v", err)
	}
}
