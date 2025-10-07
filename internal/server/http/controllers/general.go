package controllers

import (
	"encoding/json"
	"net/http"

	"github.com/rzbill/flo/internal/runtime"
	streamsvc "github.com/rzbill/flo/internal/services/streams"
)

// GeneralController handles general HTTP endpoints like health and namespaces.
//
// It provides endpoints for service health monitoring and namespace management
// operations that are not specific to streams.
type GeneralController struct {
	rt *runtime.Runtime
	ch *streamsvc.Service
}

// NewGeneralController creates a new general controller.
//
// The controller requires both a runtime instance for configuration and
// a streams service for business logic operations.
func NewGeneralController(rt *runtime.Runtime, svc *streamsvc.Service) *GeneralController {
	return &GeneralController{
		rt: rt,
		ch: svc,
	}
}

// RegisterRoutes registers general routes with the given mux.
//
// This method sets up HTTP endpoints for:
// - Health checks (/v1/healthz)
// - Namespace management (/v1/namespaces, /v1/ns/create)
func (c *GeneralController) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/v1/namespaces", c.handleListNamespaces)
	mux.HandleFunc("/v1/healthz", c.handleHealth)
	mux.HandleFunc("/v1/ns/create", c.handleNSCreate)
}

// handleListNamespaces lists all namespaces.
//
// Returns a JSON response with an array of namespace names.
func (c *GeneralController) handleListNamespaces(w http.ResponseWriter, r *http.Request) {
	list, err := c.ch.ListNamespaces(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to list namespaces")
		return
	}
	writeJSON(w, map[string]any{"namespaces": list})
}

// handleHealth returns the health status of the service.
//
// Returns 200 OK with {"status": "ok"} if healthy, 503 Service Unavailable otherwise.
func (c *GeneralController) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := c.rt.CheckHealth(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_serving")
		return
	}
	writeJSON(w, map[string]string{"status": "ok"})
}

// handleNSCreate creates a new namespace.
//
// Expects a JSON body with a "namespace" field. Returns 201 Created on success.
func (c *GeneralController) handleNSCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	var req nsCreateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "Invalid request body")
		return
	}
	if _, err := c.ch.EnsureNamespace(r.Context(), req.Namespace); err != nil {
		writeError(w, http.StatusInternalServerError, "Failed to create namespace")
		return
	}
	writeCreated(w)
}
