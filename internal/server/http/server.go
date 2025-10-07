package httpserver

import (
	"context"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/rzbill/flo/internal/runtime"
	"github.com/rzbill/flo/internal/server/http/controllers"
	streamsvc "github.com/rzbill/flo/internal/services/streams"
	workqueuesvc "github.com/rzbill/flo/internal/services/workqueues"
	ui "github.com/rzbill/flo/internal/ui"
	logpkg "github.com/rzbill/flo/pkg/log"
)

// uiFS provided by internal/ui package

// Server owns the HTTP server and routes.
type Server struct {
	rt          *runtime.Runtime
	srv         *http.Server
	lis         net.Listener
	ch          *streamsvc.Service
	wq          *workqueuesvc.Service
	uiBase      string
	controllers *controllers.ControllerRegistry
}

// New constructs a new HTTP Server.
func New(rt *runtime.Runtime, logger logpkg.Logger) *Server {
	mux := http.NewServeMux()
	ch := streamsvc.NewWithLogger(rt, logger.With(logpkg.Component("streams")))
	wq := workqueuesvc.NewWithLogger(rt, logger.With(logpkg.Component("workqueues")))
	controllers := controllers.NewControllerRegistry(rt, ch, wq)
	s := &Server{
		rt:          rt,
		ch:          ch,
		wq:          wq,
		srv:         &http.Server{Handler: cors(mux)},
		uiBase:      "/",
		controllers: controllers,
	}
	s.mountRoutes(mux, false)
	return s
}

// NewUIOnly constructs an HTTP server that serves only the UI.
func NewUIOnly(rt *runtime.Runtime) *Server {
	mux := http.NewServeMux()
	s := &Server{
		rt:     rt,
		ch:     streamsvc.New(rt),
		wq:     workqueuesvc.New(rt),
		srv:    &http.Server{Handler: cors(mux)},
		uiBase: "/",
	}
	s.mountRoutes(mux, true)
	return s
}

// NewWithService constructs an HTTP server using shared service instances.
func NewWithService(rt *runtime.Runtime, streamsSvc *streamsvc.Service, workQueuesSvc *workqueuesvc.Service, logger logpkg.Logger) *Server {
	mux := http.NewServeMux()
	controllers := controllers.NewControllerRegistry(rt, streamsSvc, workQueuesSvc)
	s := &Server{
		rt:          rt,
		ch:          streamsSvc,
		wq:          workQueuesSvc,
		srv:         &http.Server{Handler: cors(mux)},
		uiBase:      "/",
		controllers: controllers,
	}
	s.mountRoutes(mux, false)
	return s
}

// SetUIBase overrides the base path for the UI (e.g., "/" or "/ui").
func (s *Server) SetUIBase(base string) {
	if base != "" {
		s.uiBase = base
	}
}

func (s *Server) mountRoutes(mux *http.ServeMux, uiOnly bool) {
	base := strings.TrimRight(s.uiBase, "/")
	if base == "" {
		base = "/"
	}
	assets := strings.TrimRight(base, "/") + "/assets/"
	// UI: assets and SPA fallback
	if base == "/" {
		mux.Handle("/assets/", http.StripPrefix("/", http.FileServer(ui.FS())))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, "/assets/") || path.Ext(r.URL.Path) != "" {
				http.StripPrefix("/", http.FileServer(ui.FS())).ServeHTTP(w, r)
				return
			}
			f, err := ui.FS().Open("index.html")
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			info, _ := f.Stat()
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			http.ServeContent(w, r, "index.html", info.ModTime(), f)
			_ = f.Close()
		})
	} else {
		mux.Handle(assets, http.StripPrefix(base+"/", http.FileServer(ui.FS())))
		mux.HandleFunc(base+"/", func(w http.ResponseWriter, r *http.Request) {
			if strings.HasPrefix(r.URL.Path, assets) || path.Ext(r.URL.Path) != "" {
				http.StripPrefix(base+"/", http.FileServer(ui.FS())).ServeHTTP(w, r)
				return
			}
			f, err := ui.FS().Open("index.html")
			if err != nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			info, _ := f.Stat()
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			http.ServeContent(w, r, "index.html", info.ModTime(), f)
			_ = f.Close()
		})
	}
	if uiOnly {
		return
	}
	// Register all controller routes
	s.controllers.RegisterAllRoutes(mux)
}

// ListenAndServe starts serving HTTP until ctx is done or an error occurs.
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.lis = l
	errCh := make(chan error, 1)
	go func() { errCh <- s.srv.Serve(l) }()
	select {
	case <-ctx.Done():
		cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(cctx)
		return nil
	case err := <-errCh:
		return err
	}
}

// Close stops the HTTP server listener.
func (s *Server) Close() {
	if s.lis != nil {
		_ = s.lis.Close()
	}
}

func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
