// Package admin serves the pipeline's own operational endpoints:
//
//	/metrics      self-monitoring (Prometheus format)
//	/healthz      liveness: the process is serving
//	/readyz       readiness: the pipeline is running (503 while starting/draining)
//	/debug/pprof  profiling (opt-in)
package admin

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server is the admin HTTP server.
type Server struct {
	srv *http.Server
	lis net.Listener
	log *slog.Logger
}

// New creates (but does not start) the admin server.
func New(listen string, pprofOn bool, gatherer prometheus.Gatherer, ready func() bool, log *slog.Logger) (*Server, error) {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ready\n"))
	})
	if pprofOn {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return &Server{srv: &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}, lis: lis, log: log}, nil
}

// Addr is the bound address.
func (s *Server) Addr() net.Addr { return s.lis.Addr() }

// Start serves in the background.
func (s *Server) Start() {
	s.log.Info("admin endpoint listening", "addr", s.lis.Addr().String())
	go func() {
		if err := s.srv.Serve(s.lis); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("admin server failed", "err", err)
		}
	}()
}

// Shutdown stops the server.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }
