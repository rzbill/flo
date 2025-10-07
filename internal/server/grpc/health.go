package grpcserver

import (
	"context"

	flov1 "github.com/rzbill/flo/api/flo/v1"
	"github.com/rzbill/flo/internal/runtime"
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
