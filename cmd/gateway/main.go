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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/metrics"
	"sx/internal/proxy"
	"sx/internal/quota"
	"sx/internal/searxng"
	"sx/internal/tracing"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	metrics.Init()
	traceShutdown, err := tracing.Init(context.Background())
	if err != nil {
		log.Fatalf("tracing config: %v", err)
	}

	c, err := cache.New(cfg.CacheSize, cfg.CacheTTL)
	if err != nil {
		log.Fatalf("cache: %v", err)
	}

	sx := searxng.New(cfg.SearxngBackendURL, cfg.SearxngTimeout)
	breakerMgr := breaker.New()

	// Initialize fallback backends from FALLBACK_PROVIDERS env var
	fallbackMgr := backends.NewManager()
	for _, name := range cfg.FallbackProviders {
		timeout := cfg.BraveTimeout // default, overridden per-provider in factory
		backend, err := backends.NewFromEnv(name, timeout)
		if err != nil {
			log.Printf("warning: skipping fallback provider %q: %v", name, err)
			continue
		}
		fallbackMgr.Register(backend)
	}
	if len(cfg.FallbackProviders) > 0 {
		if err := fallbackMgr.SetFallbacks(cfg.FallbackProviders); err != nil {
			log.Printf("warning: fallback chain setup: %v", err)
		}
	}

	p := proxy.New(cfg, sx, c, breakerMgr, fallbackMgr)

	// Start quota scraper (Brave + Serper API usage, every 5min).
	scraperCtx, stopScraper := context.WithCancel(context.Background())
	defer stopScraper()
	go quota.StartScraper(scraperCtx, cfg.BraveAPIKey, os.Getenv("SERPER_API_KEY"), 5*time.Minute)

	mux := newRouter(p, cfg)
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("searxng-gateway listening on %s", cfg.ListenAddr)
		serveErr <- srv.ListenAndServe()
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-stop:
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("listen: %v", err)
		}
	}
	log.Println("shutdown")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	traceCtx, cancelTrace := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelTrace()
	if err := traceShutdown(traceCtx); err != nil {
		log.Printf("trace shutdown: %v", err)
	}
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
		started := time.Now()
		defer func() { metrics.SearchRequestDuration.Observe(time.Since(started).Seconds()) }()
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := otel.Tracer("sx/cmd/gateway").Start(ctx, "http.server /search")
		span.SetAttributes(attribute.String("http.route", "/search"))
		defer span.End()
		q := r.URL.Query().Get("q")
		if q == "" {
			span.SetAttributes(attribute.Int("http.status_code", http.StatusBadRequest), attribute.String("outcome", "error"))
			span.SetStatus(codes.Error, "")
			http.Error(w, "missing q", http.StatusBadRequest)
			return
		}
		resp, err := p.Search(ctx, q)
		if err != nil {
			span.SetAttributes(attribute.Int("http.status_code", http.StatusBadGateway), attribute.String("outcome", "error"))
			span.SetStatus(codes.Error, "")
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
		span.SetAttributes(attribute.Int("http.status_code", http.StatusOK), attribute.String("outcome", "success"), attribute.Int("result_count", len(resp.Results)))
		span.SetStatus(codes.Ok, "")
	})

	mux.Handle(cfg.MetricsPath, promhttp.Handler())
	return mux
}
