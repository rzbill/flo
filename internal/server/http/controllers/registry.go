package controllers

import (
	"net/http"

	"github.com/rzbill/flo/internal/runtime"
	streamsvc "github.com/rzbill/flo/internal/services/streams"
	workqueuesvc "github.com/rzbill/flo/internal/services/workqueues"
)

// ControllerRegistry manages all HTTP controllers.
//
// It provides a centralized way to register all controller routes
// and manages the lifecycle of individual controllers.
type ControllerRegistry struct {
	general    *GeneralController
	streams    *StreamsController
	workqueues *WorkQueuesController
}

// NewControllerRegistry creates a new controller registry.
//
// It initializes all controllers with the provided runtime and services.
func NewControllerRegistry(rt *runtime.Runtime, streamsSvc *streamsvc.Service, workQueuesSvc *workqueuesvc.Service) *ControllerRegistry {
	return &ControllerRegistry{
		general:    NewGeneralController(rt, streamsSvc),
		streams:    NewStreamsController(rt, streamsSvc),
		workqueues: NewWorkQueuesController(rt, workQueuesSvc),
	}
}

// RegisterAllRoutes registers all controller routes with the given mux.
//
// This method sets up all HTTP endpoints for the Flo service,
// including general endpoints (health, namespaces), stream-specific
// endpoints, and work queue endpoints.
func (r *ControllerRegistry) RegisterAllRoutes(mux *http.ServeMux) {
	r.general.RegisterRoutes(mux)
	r.streams.RegisterRoutes(mux)
	r.workqueues.RegisterRoutes(mux)
}
