package proxy

import (
	"bytes"
	"context"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"sx/backends"
	"sx/internal/breaker"
	"sx/internal/cache"
	"sx/internal/metrics"
	"sx/internal/searxng"
)

// syncLogSink is a goroutine-safe log destination. An abandoned premium worker can
// still log after the request returns, so a bare bytes.Buffer installed with
// log.SetOutput is a data race; every test that captures log output must use this.
type syncLogSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncLogSink) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncLogSink) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func histogramSnapshot(t *testing.T, name string) (uint64, float64) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather failed: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, m := range mf.GetMetric() {
				h := m.GetHistogram()
				return h.GetSampleCount(), h.GetSampleSum()
			}
		}
	}
	t.Fatalf("histogram %q not found", name)
	return 0, 0
}

func TestFirstPremiumJoinStopsAtParentDeadline(t *testing.T) {
	metrics.Init()
	for _, withSearxngResult := range []bool{false, true} {
		name := "empty"
		if withSearxngResult {
			name = "one_searxng_result"
		}
		t.Run(name, func(t *testing.T) {
			beforeTimeout := outcomeCounter(t, "timeout")
			beforeSearxngOK := outcomeCounter(t, "searxng_ok")
			beforeCalls, beforeCallSum := histogramSnapshot(t, "searxng_gateway_premium_calls_per_request")
			beforeStageCalls, beforeStageSum := histogramSnapshot(t, "searxng_gateway_premium_stage_duration_seconds")
			beforeTerminal := providerAttemptCounter(t, "brave", "primary", "cancelled")
			beforeEmpty := providerAttemptCounter(t, "brave", "primary", "empty")
			c, _ := cache.New(100, 0)
			cfg := newCfg()
			cfg.FallbackTimeout = 50 * time.Millisecond
			cfg.SearxngTimeout = time.Second
			cfg.SufficientMinResults = 10

			started := make(chan struct{})
			canceled := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			var firstCalls, secondCalls atomic.Int64
			first := &contextBackend{name: "brave", fn: func(ctx context.Context) ([]backends.SearchResult, error) {
				firstCalls.Add(1)
				close(started)
				<-ctx.Done()
				close(canceled)
				<-release
				return nil, ctx.Err()
			}}
			second := &contextBackend{name: "exa", fn: func(context.Context) ([]backends.SearchResult, error) {
				secondCalls.Add(1)
				return nil, nil
			}}
			sx := &contextSearxng{fn: func(context.Context) (*searxng.Response, error) {
				<-started
				resp := &searxng.Response{}
				if withSearxngResult {
					resp.Results = []searxng.Result{{Title: "kept", URL: "https://kept.test", Engine: "wikipedia"}}
				}
				return resp, nil
			}}
			p := newTestProxy(cfg, sx, c, breaker.New(), first, second)
			var logs syncLogSink
			previous := log.Writer()
			log.SetOutput(&logs)
			t.Cleanup(func() {
				log.SetOutput(previous)
				releaseOnce.Do(func() { close(release) })
			})
			type searchResult struct {
				response *searxng.Response
				err      error
			}
			searchDone := make(chan searchResult, 1)
			go func() {
				response, err := p.Search(context.Background(), "held-premium")
				searchDone <- searchResult{response: response, err: err}
			}()
			<-started
			<-canceled
			select {
			case got := <-searchDone:
				if withSearxngResult {
					if got.err != nil || got.response == nil || len(got.response.Results) != 1 || got.response.Results[0].URL != "https://kept.test" {
						t.Fatalf("response=%v error=%v; want only the SearXNG result", got.response, got.err)
					}
				} else if got.response != nil || got.err == nil {
					t.Fatalf("response=%v error=%v; want nil response and timeout error", got.response, got.err)
				}
			case <-time.After(time.Second):
				t.Fatal("Search waited for premium completion after its 50ms parent budget")
			}

			if firstCalls.Load() != 1 || secondCalls.Load() != 0 {
				t.Fatalf("provider calls brave=%d exa=%d; want one brave call and no second provider call", firstCalls.Load(), secondCalls.Load())
			}
			if !withSearxngResult && c.Len() != 0 {
				t.Fatalf("cache length=%d, want 0", c.Len())
			}
			if withSearxngResult {
				if got := outcomeCounter(t, "searxng_ok") - beforeSearxngOK; got != 1 {
					t.Fatalf("searxng_ok delta=%v, want 1", got)
				}
			} else {
				if got := outcomeCounter(t, "timeout") - beforeTimeout; got != 1 {
					t.Fatalf("timeout delta=%v, want 1", got)
				}
				output := logs.String()
				if !strings.Contains(output, `"in_flight":true`) || strings.Contains(output, `"provider":"brave","skip":"not_attempted"`) {
					t.Fatalf("diagnostic does not preserve the in-flight provider state: %s", output)
				}
			}
			calls, callSum := histogramSnapshot(t, "searxng_gateway_premium_calls_per_request")
			if calls != beforeCalls+1 || callSum != beforeCallSum+1 {
				t.Fatalf("premium_calls_per_request count/sum=%d/%.0f; want %d/%.0f", calls, callSum, beforeCalls+1, beforeCallSum+1)
			}
			stageCalls, stageSum := histogramSnapshot(t, "searxng_gateway_premium_stage_duration_seconds")
			if stageCalls != beforeStageCalls+1 {
				t.Fatalf("premium_stage_duration count delta=%d, want 1 (one observation per request)", stageCalls-beforeStageCalls)
			}
			if delta := stageSum - beforeStageSum; delta > 0.5 {
				t.Fatalf("premium_stage_duration sum delta=%.3fs, want at most the parent budget plus scheduling slack; the censoring evidence is the pre-release COUNT delta, not this bound", delta)
			}
			outcomeBeforeRelease := outcomeCounter(t, map[bool]string{true: "searxng_ok", false: "timeout"}[withSearxngResult])
			cacheBeforeRelease := c.Len()
			releaseOnce.Do(func() { close(release) })

			deadline := time.Now().Add(time.Second)
			for providerAttemptCounter(t, "brave", "primary", "cancelled")+providerAttemptCounter(t, "brave", "primary", "empty") == beforeTerminal+beforeEmpty && time.Now().Before(deadline) {
				// Yield until the explicitly released worker completes its terminal accounting.
				runtime.Gosched()
			}
			terminal := providerAttemptCounter(t, "brave", "primary", "cancelled") + providerAttemptCounter(t, "brave", "primary", "empty")
			if terminal != beforeTerminal+beforeEmpty+1 {
				t.Fatalf("terminal provider attempts delta=%v, want exactly 1", terminal-beforeTerminal-beforeEmpty)
			}
			if got := outcomeCounter(t, map[bool]string{true: "searxng_ok", false: "timeout"}[withSearxngResult]); got != outcomeBeforeRelease {
				t.Fatalf("request outcome changed after worker release: before=%v after=%v", outcomeBeforeRelease, got)
			}
			if c.Len() != cacheBeforeRelease {
				t.Fatalf("cache length changed after worker release: before=%d after=%d", cacheBeforeRelease, c.Len())
			}
		})
	}
}
