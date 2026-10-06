// Package breaker: per-engine circuit breaker for provider faults.
//
// Premium providers are admitted through Execute, which runs the actual
// provider call inside gobreaker.Execute so MaxRequests constrains real
// half-open probes. Provider faults (auth, quota, rate limit, network, 5xx,
// degraded, invalid response, provider timeout, panic) trip the breaker;
// non-provider failures (caller cancellation, overall request deadline,
// query-validation errors, empty results, missing config) are marked with
// ErrNonProviderFault and never trip.
//
// SearXNG engines keep the synthetic RecordClientError/RecordSuccess path:
// their outcome is reported by SearXNG in one response, not call-by-call.
package breaker

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sony/gobreaker"
)

// ErrNonProviderFault marks a call outcome that must not count as a provider
// failure for the circuit breaker: caller cancellation, an overall request
// deadline, a query-validation error, an unconfigured provider, or an empty
// result set.
var ErrNonProviderFault = errors.New("breaker: non-provider fault")

// isSuccessful is the gobreaker IsSuccessful policy shared by every breaker.
// Only nil errors and explicit non-provider faults count as successes; any
// other error is a provider failure that can trip the breaker.
func isSuccessful(err error) bool {
	return err == nil || errors.Is(err, ErrNonProviderFault)
}

var (
	breakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_circuit_breaker_state",
			Help: "0=closed, 1=half-open, 2=open. Premium providers trip on genuine provider faults; SearXNG engines on client errors.",
		},
		[]string{"engine"},
	)

	breakerTriggeredAt = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_circuit_breaker_triggered_at",
			Help: "Unix timestamp when circuit breaker went open (0 if closed)",
		},
		[]string{"engine", "reason"},
	)

	breakerTripsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_circuit_breaker_trips_total",
			Help: "Total number of times circuit breaker tripped (provider faults or client errors)",
		},
		[]string{"engine", "reason"},
	)

	breakerRecoveryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_circuit_breaker_recovery_total",
			Help: "Total auto-recoveries (closed state after open)",
		},
		[]string{"engine"},
	)
)

// EngineSettings holds per-engine circuit breaker tuning.
type EngineSettings struct {
	MaxRequests uint32
	Interval    time.Duration
	Timeout     time.Duration
	Name        string
}

// DefaultSettings returns the default circuit breaker settings.
func DefaultSettings() EngineSettings {
	return EngineSettings{
		MaxRequests: 1,
		Interval:    60 * time.Second,
		Timeout:     5 * time.Minute,
	}
}

// PerEngineSettings returns settings for a specific engine (alias for DefaultSettings).
func PerEngineSettings(engine string) EngineSettings {
	s := DefaultSettings()
	s.Name = engine
	return s
}

// Manager holds per-engine circuit breakers.
type Manager struct {
	mu       sync.RWMutex
	breakers map[string]*gobreaker.CircuitBreaker
	reasons  map[string]string
	reasonMu sync.RWMutex // guards reasons only; never held across cb.Execute
	// settings resolves per-engine tuning; overridable in tests.
	settings func(engine string) EngineSettings
}

// New creates a new Manager.
func New() *Manager {
	return &Manager{
		breakers: make(map[string]*gobreaker.CircuitBreaker),
		reasons:  make(map[string]string),
		settings: PerEngineSettings,
	}
}

// NewWithSettings creates a Manager whose breakers use settings(engine).
// It is intended for embedding and deterministic tests; callers must configure
// it before any breaker is created.
func NewWithSettings(settings func(engine string) EngineSettings) *Manager {
	m := New()
	if settings != nil {
		m.settings = settings
	}
	return m
}

// Execute admits fn through the circuit breaker for engine. This is the real
// admission point: the provider call itself runs inside fn, so the breaker's
// MaxRequests constrains actual half-open probes. fn must return
// ErrNonProviderFault (directly or wrapped) for outcomes that must not trip the
// breaker. When the breaker is open, fn is not called and gobreaker.ErrOpenState
// (or ErrTooManyRequests in half-open) is returned.
func (m *Manager) Execute(engine string, fn func() (interface{}, error)) (interface{}, error) {
	if engine == "" {
		return fn()
	}
	return m.getBreaker(engine).Execute(fn)
}

// RecordReason stores the bounded reason attached to the next trip of engine.
// It must be called before fn returns the outcome error, so OnStateChange can
// label the trip.
func (m *Manager) RecordReason(engine, reason string) {
	if engine == "" {
		return
	}
	m.reasonMu.Lock()
	m.reasons[engine] = reason
	m.reasonMu.Unlock()
}

// IsOpen returns true if the circuit breaker for engine is open.
func (m *Manager) IsOpen(engine string) bool {
	m.mu.RLock()
	cb, ok := m.breakers[engine]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	return cb.State() == gobreaker.StateOpen
}

// State returns the current state of the breaker for engine.
func (m *Manager) State(engine string) gobreaker.State {
	m.mu.RLock()
	cb, ok := m.breakers[engine]
	m.mu.RUnlock()
	if !ok {
		return gobreaker.StateClosed
	}
	return cb.State()
}

// LastReason returns the last reason recorded for a given engine.
func (m *Manager) LastReason(engine string) string {
	m.reasonMu.RLock()
	defer m.reasonMu.RUnlock()
	return m.reasons[engine]
}

// RecordClientError trips the breaker for a SearXNG engine reporting a client
// error (blocked/rate-limited). Premium providers do not use this path; they
// admit real calls through Execute.
func (m *Manager) RecordClientError(engine, reason string) {
	if engine == "" {
		return
	}
	m.reasonMu.Lock()
	m.reasons[engine] = reason
	m.reasonMu.Unlock()
	cb := m.getBreaker(engine)
	// Force 1 failure to trip the breaker (threshold=1)
	_, _ = cb.Execute(func() (interface{}, error) {
		return nil, &clientError{reason: reason}
	})
}

// RecordEngineSeen ensures the circuit breaker state gauge series exists for an
// engine without triggering a state transition. The gauge is set from the
// breaker's actual state, so initialisation can never reset an already-open
// breaker to zero.
//
// Lesson MEMORY 2026-07-24 L1: promauto gauges don't emit a series until
// .Set() is called. Engines like 'serper' that are 4xx-blocked but with
// reasons not matching isClientError() would otherwise be invisible in
// the CB panel.
func (m *Manager) RecordEngineSeen(engine string) {
	if engine == "" {
		return
	}
	// Set the gauge from the breaker's actual state so initialisation can never
	// reset an already-open breaker to zero.
	breakerState.WithLabelValues(engine).Set(stateFloat(m.State(engine)))
}

// RecordSuccess feeds a success (closes the breaker if half-open).
func (m *Manager) RecordSuccess(engine string) {
	if engine == "" {
		return
	}
	m.reasonMu.Lock()
	delete(m.reasons, engine)
	m.reasonMu.Unlock()
	cb := m.getBreaker(engine)
	_, _ = cb.Execute(func() (interface{}, error) {
		return nil, nil
	})
}

func (m *Manager) getBreaker(engine string) *gobreaker.CircuitBreaker {
	m.mu.RLock()
	cb, ok := m.breakers[engine]
	m.mu.RUnlock()
	if ok {
		return cb
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if cb, ok = m.breakers[engine]; ok {
		return cb
	}
	settings := PerEngineSettings(engine)
	if m.settings != nil {
		settings = m.settings(engine)
	}
	name := engine
	if name == "" {
		name = "default"
	}
	cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:         name,
		MaxRequests:  settings.MaxRequests,
		Interval:     settings.Interval,
		Timeout:      settings.Timeout,
		IsSuccessful: isSuccessful,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Trip on the FIRST genuine provider failure.
			return counts.ConsecutiveFailures >= 1
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			breakerState.WithLabelValues(name).Set(stateFloat(to))
			if to == gobreaker.StateOpen {
				m.reasonMu.RLock()
				reason := m.reasons[name]
				m.reasonMu.RUnlock()
				if reason == "" {
					reason = "unknown"
				}
				timestamp := float64(time.Now().Unix())
				breakerTriggeredAt.WithLabelValues(name, reason).Set(timestamp)
				breakerTripsTotal.WithLabelValues(name, reason).Inc()
			}
			if to == gobreaker.StateClosed && from != gobreaker.StateClosed {
				breakerRecoveryTotal.WithLabelValues(name).Inc()
				log.Printf("[CB] %s recovered: %s -> %s", name, from, to)
			}
		},
	})
	m.breakers[engine] = cb
	return cb
}

// clientError is a private error type used to trigger the breaker.
type clientError struct {
	reason string
}

func (e *clientError) Error() string {
	return e.reason
}

func stateFloat(s gobreaker.State) float64 {
	switch s {
	case gobreaker.StateClosed:
		return 0
	case gobreaker.StateHalfOpen:
		return 1
	case gobreaker.StateOpen:
		return 2
	}
	return -1
}
