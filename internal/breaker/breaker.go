// Package breaker: per-engine circuit breaker for 4xx client errors.
// 4xx = server is blocking us (403, 429) → skip engine.
// 5xx/timeout = use retry with backoff (in internal/retry).
package breaker

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sony/gobreaker"
)

var (
	breakerState = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "searxng_gateway_circuit_breaker_state",
			Help: "0=closed, 1=half-open, 2=open. Triggered by 4xx client errors.",
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
			Help: "Total number of times circuit breaker tripped (4xx errors)",
		},
		[]string{"engine", "reason"},
	)

	breakerRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_circuit_breaker_requests_total",
			Help: "Total requests through circuit breaker",
		},
		[]string{"engine", "state"},
	)

	breakerRejectionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "searxng_gateway_circuit_breaker_rejections_total",
			Help: "Total requests rejected by circuit breaker (open state)",
		},
		[]string{"engine"},
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
}

// New creates a new Manager.
func New() *Manager {
	return &Manager{
		breakers: make(map[string]*gobreaker.CircuitBreaker),
		reasons:  make(map[string]string),
	}
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
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.reasons[engine]
}

// RecordClientError triggers the breaker on 4xx (client error = blocked/rate-limited).
func (m *Manager) RecordClientError(engine, reason string) {
	if engine == "" {
		return
	}
	m.reasons[engine] = reason
	cb := m.getBreaker(engine)
	// Force 1 failure to trip the breaker (threshold=1)
	_, _ = cb.Execute(func() (interface{}, error) {
		return nil, &clientError{reason: reason}
	})
}

// RecordSuccess feeds a success (closes the breaker if half-open).
func (m *Manager) RecordSuccess(engine string) {
	if engine == "" {
		return
	}
	delete(m.reasons, engine)
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
	name := engine
	if name == "" {
		name = "default"
	}
	cb = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        name,
		MaxRequests: settings.MaxRequests,
		Interval:    settings.Interval,
		Timeout:     settings.Timeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Trip on FIRST 4xx client error
			return counts.ConsecutiveFailures >= 1
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			breakerState.WithLabelValues(name).Set(stateFloat(to))
			if to == gobreaker.StateOpen {
				m.mu.RLock()
				reason := m.reasons[name]
				m.mu.RUnlock()
				if reason == "" {
					reason = "unknown"
				}
				timestamp := float64(time.Now().Unix())
				breakerTriggeredAt.WithLabelValues(name, reason).Set(timestamp)
				breakerTripsTotal.WithLabelValues(name, reason).Inc()
			}
			if from == gobreaker.StateOpen && to == gobreaker.StateClosed {
				breakerRecoveryTotal.WithLabelValues(name).Inc()
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
