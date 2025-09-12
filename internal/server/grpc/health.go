package grpcserver

import (
	"context"

	"github.com/rzbill/flo/internal/runtime"
	flov1 "github.com/rzbill/flo/proto/gen/go/flo/v1"
)

type healthSvc struct {
	flov1.UnimplementedHealthServiceServer
	rt *runtime.Runtime
}

func (h *healthSvc) Check(ctx context.Context, _ *flov1.HealthCheckRequest) (*flov1.HealthCheckResponse, error) {
	if err := h.rt.CheckHealth(ctx); err != nil {
		return &flov1.HealthCheckResponse{Status: "not_serving"}, nil
	}
	return &flov1.HealthCheckResponse{Status: "ok"}, nil
}
