package grpcserver

import (
	"context"
	"net"
	"time"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	"github.com/rzbill/flo/internal/runtime"
	streamsvc "github.com/rzbill/flo/internal/services/streams"
	workqueuesvc "github.com/rzbill/flo/internal/services/workqueues"
	logpkg "github.com/rzbill/flo/pkg/log"
	"google.golang.org/grpc"
)

// Server owns the gRPC server instance and runtime.
type Server struct {
	rt    *runtime.Runtime
	chsvc *streamsvc.Service
	wqsvc *workqueuesvc.Service
	grpc  *grpc.Server
	lis   net.Listener
}

// New constructs a gRPC server and registers services.
func New(rt *runtime.Runtime, logger logpkg.Logger, opts ...grpc.ServerOption) *Server {
	s := &Server{
		rt:    rt,
		chsvc: streamsvc.NewWithLogger(rt, logger.With(logpkg.Component("streams"))),
		wqsvc: workqueuesvc.NewWithLogger(rt, logger.With(logpkg.Component("workqueues"))),
		grpc:  grpc.NewServer(opts...),
	}
	flov1.RegisterHealthServiceServer(s.grpc, &healthSvc{rt: rt})
	flov1.RegisterStreamsServiceServer(s.grpc, &streamsSvc{svc: s.chsvc})
	flov1.RegisterWorkQueuesServiceServer(s.grpc, &workQueuesSvc{svc: s.wqsvc})
	return s
}

// NewWithService constructs a gRPC server using shared service instances.
func NewWithService(rt *runtime.Runtime, streams *streamsvc.Service, workQueues *workqueuesvc.Service, logger logpkg.Logger, opts ...grpc.ServerOption) *Server {
	s := &Server{
		rt:    rt,
		chsvc: streams,
		wqsvc: workQueues,
		grpc:  grpc.NewServer(opts...),
	}
	flov1.RegisterHealthServiceServer(s.grpc, &healthSvc{rt: rt})
	flov1.RegisterStreamsServiceServer(s.grpc, &streamsSvc{svc: s.chsvc})
	flov1.RegisterWorkQueuesServiceServer(s.grpc, &workQueuesSvc{svc: s.wqsvc})
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
		done := make(chan struct{})
		go func() {
			s.grpc.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			// Force close long-lived streams (e.g., Subscribe) so shutdown doesn't hang
			s.grpc.Stop()
		}
	}
	if s.lis != nil {
		_ = s.lis.Close()
	}
}
