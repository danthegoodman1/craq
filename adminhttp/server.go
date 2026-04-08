package adminhttp

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"sync/atomic"

	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/gologger"
	"github.com/danthegoodman1/craq/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
)

type Config struct {
	Address  string
	Logger   *zerolog.Logger
	Gatherer prometheus.Gatherer
}

type Server struct {
	component string
	logger    zerolog.Logger
	gatherer  prometheus.Gatherer
	http      *http.Server
	lis       net.Listener
	address   string
	ready     atomic.Bool
}

type healthResponse struct {
	Component string `json:"component"`
	Live      bool   `json:"live,omitempty"`
	Ready     bool   `json:"ready,omitempty"`
}

func NewCoordinator(server *coordserver.Server, cfg Config) *Server {
	return newServer("coordserver", cfg, func(mux *http.ServeMux) {
		mux.HandleFunc("/admin/v1/routing", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, server.RoutingStatus(r.Context()))
		})
		mux.HandleFunc("/admin/v1/state", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, server.AdminState(r.Context()))
		})
	})
}

func NewStorage(node *storage.Node, cfg Config) *Server {
	return newServer("storage", cfg, func(mux *http.ServeMux) {
		mux.HandleFunc("/admin/v1/state", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, node.AdminState())
		})
	})
}

func newServer(component string, cfg Config, registerState func(*http.ServeMux)) *Server {
	logger := gologger.NewLogger()
	if cfg.Logger != nil {
		logger = cfg.Logger.With().Logger()
	}
	gatherer := cfg.Gatherer
	if gatherer == nil {
		gatherer = prometheus.NewRegistry()
	}
	mux := http.NewServeMux()
	s := &Server{
		component: component,
		logger:    logger,
		gatherer:  gatherer,
		address:   cfg.Address,
	}
	s.ready.Store(true)
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, healthResponse{Component: component, Live: true})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !s.ready.Load() {
			writeJSON(w, http.StatusServiceUnavailable, healthResponse{Component: component, Ready: false})
			return
		}
		writeJSON(w, http.StatusOK, healthResponse{Component: component, Ready: true})
	})
	mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	registerState(mux)
	s.http = &http.Server{Handler: mux}
	return s
}

func (s *Server) ListenAndServe() error {
	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	return s.Serve(lis)
}

func (s *Server) Serve(lis net.Listener) error {
	s.lis = lis
	s.http.Addr = lis.Addr().String()
	s.logger.Info().Str("component", "adminhttp").Str("admin_component", s.component).Str("address", lis.Addr().String()).Msg("admin http listening")
	return s.http.Serve(lis)
}

func (s *Server) Close(ctx context.Context) error {
	s.ready.Store(false)
	s.logger.Info().Str("component", "adminhttp").Str("admin_component", s.component).Msg("admin http closing")
	return s.http.Shutdown(ctx)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
