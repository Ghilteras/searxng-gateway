// Gateway — HTTP search gateway with SearXNG primary + Brave fallback.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"sx/internal/brave"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/metrics"
	"sx/internal/proxy"
	"sx/internal/searxng"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	metrics.Init()

	c, err := cache.New(cfg.CacheSize)
	if err != nil {
		log.Fatalf("cache: %v", err)
	}

	sx := searxng.New(cfg.SearxngBackendURL, cfg.SearxngTimeout)
	bv := brave.New(cfg.BraveAPIKey, cfg.BraveTimeout)
	p := proxy.New(cfg, sx, bv, c)

	mux := newRouter(p, cfg)
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		log.Printf("searxng-gateway listening on %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}

// newRouter builds the HTTP handler routing for the gateway:
//   - /healthz  → 200 OK
//   - /search   → proxy.Search with JSON serialisation
//   - cfg.MetricsPath → Prometheus metrics
func newRouter(p *proxy.Proxy, cfg *config.Config) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/search", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		if q == "" {
			http.Error(w, "missing q", http.StatusBadRequest)
			return
		}
		resp, err := p.Search(r.Context(), q)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	})

	mux.Handle(cfg.MetricsPath, promhttp.Handler())
	return mux
}
