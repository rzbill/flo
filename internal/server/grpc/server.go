package grpcserver

import (
	"context"
	"net"

	"github.com/rzbill/flo/internal/runtime"
	channelsvc "github.com/rzbill/flo/internal/services/channels"
	flov1 "github.com/rzbill/flo/proto/gen/go/flo/v1"
	"google.golang.org/grpc"
)

// Server owns the gRPC server instance and runtime.
type Server struct {
	rt    *runtime.Runtime
	chsvc *channelsvc.Service
	grpc  *grpc.Server
	lis   net.Listener
}

// New constructs a gRPC server and registers services.
func New(rt *runtime.Runtime, opts ...grpc.ServerOption) *Server {
	s := &Server{rt: rt, chsvc: channelsvc.New(rt), grpc: grpc.NewServer(opts...)}
	flov1.RegisterHealthServiceServer(s.grpc, &healthSvc{rt: rt})
	flov1.RegisterChannelsServiceServer(s.grpc, &channelsSvc{svc: s.chsvc})
	return s
}

// ListenAndServe binds to addr and serves until ctx is done.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.lis = l
	errCh := make(chan error, 1)
	go func() { errCh <- s.grpc.Serve(l) }()
	select {
	case <-ctx.Done():
		s.grpc.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// Close stops the server and closes the listener.
func (s *Server) Close() {
	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	if s.lis != nil {
		_ = s.lis.Close()
	}
}
