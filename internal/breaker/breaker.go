// Package breaker: per-engine circuit breaker for provider faults.
//
// Premium providers are admitted through gobreaker's TwoStepCircuitBreaker.
// Execute adapts its native Allow/done API while leaving MaxRequests to
// constrain actual half-open probes. RecordReason + Execute retains a
// per-engine compatibility slot: concurrent calls can overwrite or consume
// another call's reason, so it cannot provide exact per-call attribution.
// Decision-path consumers requiring exact attribution must use Manager.Allow
// and pass that call's own reason to done; they must not route the decision
// through RecordReason. Provider faults (auth, quota, rate limit, network, 5xx,
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
	breakers map[string]*breakerEntry
	// settings resolves per-engine tuning; overridable in tests.
	settings func(engine string) EngineSettings
}

// breakerEntry serializes only admission, state snapshots, and terminal
// accounting/provenance. Its lock is released before the provider call and is
// never held across provider work.
type breakerEntry struct {
	mu               sync.Mutex
	cb               *gobreaker.TwoStepCircuitBreaker
	tripReason       string
	pendingReason    string // compatibility only: per-engine slot; concurrent calls can misattribute a trip
	accountingReason string // visible only during serialized native terminal accounting
}

// AdmissionSnapshot is the state and frozen trip provenance observed when an
// admission is rejected. An empty/unknown Reason means UNKNOWN, never “not
// structural”; consumers must treat unknown as a veto rather than evidence that
// the provider is structurally unavailable.
type AdmissionSnapshot struct {
	State  gobreaker.State
	Reason string
}

// ReasonContention identifies a rejected half-open admission because its probe
// slot is already occupied; it is not the reason that opened the breaker.
const ReasonContention = "contention"

// New creates a new Manager.
func New() *Manager {
	return &Manager{
		breakers: make(map[string]*breakerEntry),
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
	entry := m.getBreaker(engine)
	done, _, err := m.allow(entry)
	if err != nil {
		return nil, err
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			entry.mu.Lock()
			reason := entry.pendingReason
			entry.pendingReason = ""
			entry.mu.Unlock()
			done(false, reason)
			panic(recovered)
		}
	}()
	result, callErr := fn()
	entry.mu.Lock()
	reason := entry.pendingReason
	entry.pendingReason = ""
	entry.mu.Unlock()
	done(isSuccessful(callErr), reason)
	return result, callErr
}

// Allow admits a request using gobreaker's native two-step admission. The
// returned completion callback must be invoked once when the request ends,
// with the success classification and the reason for a provider failure. The
// snapshot is meaningful on rejection: open-state rejection carries that
// epoch's frozen trip reason; half-open probe contention carries
// ReasonContention instead. A missing reason is unknown, not proof of a
// non-structural outage.
func (m *Manager) Allow(engine string) (done func(success bool, reason string), snapshot AdmissionSnapshot, err error) {
	if engine == "" {
		return func(bool, string) {}, AdmissionSnapshot{State: gobreaker.StateClosed}, nil
	}
	return m.allow(m.getBreaker(engine))
}

func (m *Manager) allow(entry *breakerEntry) (func(bool, string), AdmissionSnapshot, error) {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	done, err := entry.cb.Allow()
	state := entry.cb.State()
	snapshot := AdmissionSnapshot{State: state}
	if err != nil {
		if errors.Is(err, gobreaker.ErrTooManyRequests) {
			snapshot.Reason = ReasonContention
		} else if state == gobreaker.StateOpen {
			snapshot.Reason = entry.tripReason
		}
		return nil, snapshot, err
	}
	return func(success bool, reason string) {
		entry.mu.Lock()
		defer entry.mu.Unlock()
		before := entry.cb.State()
		entry.accountingReason = reason
		done(success)
		entry.accountingReason = ""
		after := entry.cb.State()
		if before != gobreaker.StateOpen && after == gobreaker.StateOpen {
			entry.tripReason = reason
		}
	}, snapshot, nil
}

// RecordReason stores the bounded reason attached to the next trip of engine.
// It must be called before fn returns the outcome error, so OnStateChange can
// label the trip.
func (m *Manager) RecordReason(engine, reason string) {
	if engine == "" {
		return
	}
	entry := m.getBreaker(engine)
	entry.mu.Lock()
	entry.pendingReason = reason
	entry.mu.Unlock()
}

// IsOpen returns true if the circuit breaker for engine is open.
func (m *Manager) IsOpen(engine string) bool {
	m.mu.RLock()
	entry, ok := m.breakers[engine]
	m.mu.RUnlock()
	if !ok {
		return false
	}
	return entry.cb.State() == gobreaker.StateOpen
}

// State returns the current state of the breaker for engine.
func (m *Manager) State(engine string) gobreaker.State {
	m.mu.RLock()
	entry, ok := m.breakers[engine]
	m.mu.RUnlock()
	if !ok {
		return gobreaker.StateClosed
	}
	return entry.cb.State()
}

// LastReason returns the frozen trip reason of the last open epoch. It is only
// meaningful while State(engine) == StateOpen and is not cleared on recovery.
func (m *Manager) LastReason(engine string) string {
	m.mu.RLock()
	entry := m.breakers[engine]
	m.mu.RUnlock()
	if entry == nil {
		return ""
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.tripReason
}

// RecordClientError trips the breaker for a SearXNG engine reporting a client
// error (blocked/rate-limited). Premium providers do not use this path; they
// admit real calls through Execute.
func (m *Manager) RecordClientError(engine, reason string) {
	if engine == "" {
		return
	}
	entry := m.getBreaker(engine)
	// Force 1 failure to trip the breaker (threshold=1)
	done, _, err := m.allow(entry)
	if err == nil {
		done(false, reason)
	}
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
	entry := m.getBreaker(engine)
	done, _, err := m.allow(entry)
	if err == nil {
		done(true, "")
	}
}

func (m *Manager) getBreaker(engine string) *breakerEntry {
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
	entry := &breakerEntry{}
	entry.cb = gobreaker.NewTwoStepCircuitBreaker(gobreaker.Settings{
		Name:         name,
		MaxRequests:  settings.MaxRequests,
		Interval:     settings.Interval,
		Timeout:      settings.Timeout,
		IsSuccessful: isSuccessful,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Require two consecutive genuine provider failures before tripping.
			return counts.ConsecutiveFailures >= 2
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			breakerState.WithLabelValues(name).Set(stateFloat(to))
			if to == gobreaker.StateOpen {
				reason := entry.accountingReason
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
	m.breakers[engine] = entry
	return entry
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
