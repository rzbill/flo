package httpserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	cfgpkg "github.com/rzbill/flo/internal/config"
	"github.com/rzbill/flo/internal/runtime"
	pebblestore "github.com/rzbill/flo/internal/storage/pebble"
)

func TestHealthHandler(t *testing.T) {
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("rt open: %v", err)
	}
	defer rt.Close()
	s := New(rt)
	req := httptest.NewRequest(http.MethodGet, "/v1/healthz", nil)
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestPublishHandler(t *testing.T) {
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("rt open: %v", err)
	}
	defer rt.Close()
	s := New(rt)
	body := `{"namespace":"default","channel":"orders","payload":"aGVsbG8="}`
	req := httptest.NewRequest(http.MethodPost, "/v1/channels/publish", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestNackHandlerCreatesRetry(t *testing.T) {
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("rt open: %v", err)
	}
	defer rt.Close()
	s := New(rt)
	// Ensure ns and channel
	_, _ = rt.EnsureNamespace("default")
	body := `{"namespace":"default","channel":"orders","group":"workers","id":"AAAAAAAAAAI="}`
	req := httptest.NewRequest(http.MethodPost, "/v1/channels/nack", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("status: %d", w.Code)
	}
}

func TestCreateChannelHandler(t *testing.T) {
	dir := t.TempDir()
	rt, err := runtime.Open(runtime.Options{DataDir: dir, Fsync: pebblestore.FsyncModeAlways, Config: cfgpkg.Default()})
	if err != nil {
		t.Fatalf("rt open: %v", err)
	}
	defer rt.Close()
	s := New(rt)
	body := `{"namespace":"default","channel":"orders","partitions":2}`
	req := httptest.NewRequest(http.MethodPost, "/v1/channels/create", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.srv.Handler.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status: %d", w.Code)
	}
}
