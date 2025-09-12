package httpserver

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/rzbill/flo/internal/eventlog"
	"github.com/rzbill/flo/internal/runtime"
	channelsvc "github.com/rzbill/flo/internal/services/channels"
)

type Server struct {
	rt  *runtime.Runtime
	srv *http.Server
	lis net.Listener
	ch  *channelsvc.Service
}

func New(rt *runtime.Runtime) *Server {
	mux := http.NewServeMux()
	s := &Server{rt: rt, ch: channelsvc.New(rt), srv: &http.Server{Handler: cors(mux)}}
	mux.HandleFunc("/v1/healthz", s.handleHealth)
	mux.HandleFunc("/v1/ns/create", s.handleNSCreate)
	mux.HandleFunc("/v1/channels/publish", s.handlePublish)
	mux.HandleFunc("/v1/channels/create", s.handleCreate)
	mux.HandleFunc("/v1/channels/subscribe", s.handleSubscribeSSE)
	mux.HandleFunc("/v1/channels/ack", s.handleAck)
	mux.HandleFunc("/v1/channels/nack", s.handleNack)
	return s
}

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

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if err := s.rt.CheckHealth(r.Context()); err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "not_serving"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

type nsCreateReq struct {
	Namespace string `json:"namespace"`
}

func (s *Server) handleNSCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req nsCreateReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if _, err := s.ch.EnsureNamespace(r.Context(), req.Namespace); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

type publishReq struct {
	Namespace string            `json:"namespace"`
	Channel   string            `json:"channel"`
	Payload   []byte            `json:"payload"`
	Headers   map[string]string `json:"headers"`
	Key       string            `json:"key"`
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req publishReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if _, err := s.ch.EnsureNamespace(r.Context(), req.Namespace); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if _, err := s.ch.Publish(r.Context(), req.Namespace, req.Channel, req.Payload, req.Headers, req.Key); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

type createReq struct {
	Namespace  string `json:"namespace"`
	Channel    string `json:"channel"`
	Partitions int    `json:"partitions"`
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req createReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.ch.CreateChannel(r.Context(), req.Namespace, req.Channel, req.Partitions); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

type sseSink struct {
	w http.ResponseWriter
	r *http.Request
}

func (s sseSink) Send(it channelsvc.SubscribeItem) error {
	_ = json.NewEncoder(s.w).Encode(map[string]any{"id": it.ID, "payload": it.Payload})
	s.w.Write([]byte("\n\n"))
	return nil
}
func (s sseSink) Context() context.Context { return s.r.Context() }
func (s sseSink) Flush() error {
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

func (s *Server) handleSubscribeSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ns := r.URL.Query().Get("namespace")
	ch := r.URL.Query().Get("channel")
	group := r.URL.Query().Get("group")
	if ns == "" {
		ns = s.rt.Config().DefaultNamespaceName
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	if err := s.ch.StreamSubscribe(r.Context(), ns, ch, group, nil, sseSink{w: w, r: r}); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

type ackReq struct {
	Namespace string `json:"namespace"`
	Channel   string `json:"channel"`
	Group     string `json:"group"`
	Id        []byte `json:"id"`
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req ackReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.ch.Ack(r.Context(), req.Namespace, req.Channel, req.Group, req.Id); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var req ackReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.ch.Nack(r.Context(), req.Namespace, req.Channel, req.Group, req.Id); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func tokenFromSeq(seq uint64) (t eventlog.Token) {
	t = eventlog.Token{}
	t[0] = byte(seq >> 56)
	t[1] = byte(seq >> 48)
	t[2] = byte(seq >> 40)
	t[3] = byte(seq >> 32)
	t[4] = byte(seq >> 24)
	t[5] = byte(seq >> 16)
	t[6] = byte(seq >> 8)
	t[7] = byte(seq)
	return
}
