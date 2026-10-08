package backends

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

// Manager coordinates search across multiple backends with fallback support
type Manager struct {
	primary   SearchBackend
	fallbacks []SearchBackend
	registry  map[string]SearchBackend
	// Round-robin index for NextAvailable — advances atomically across calls.
	rrIdx atomic.Uint64
}

// NewManager creates a new backend manager
func NewManager() *Manager {
	return &Manager{
		registry: make(map[string]SearchBackend),
	}
}

// Register adds a backend to the registry
func (m *Manager) Register(backend SearchBackend) {
	m.registry[backend.Name()] = backend
}

// SetFallbacks sets the fallback backends in order
func (m *Manager) SetFallbacks(names []string) error {
	m.fallbacks = nil
	for _, name := range names {
		backend, ok := m.registry[name]
		if !ok {
			return fmt.Errorf("unknown fallback backend: %s (available: %s)", name, m.availableNames())
		}
		m.fallbacks = append(m.fallbacks, backend)
	}
	return nil
}

// GetBackend returns a backend by name
func (m *Manager) GetBackend(name string) (SearchBackend, bool) {
	b, ok := m.registry[name]
	return b, ok
}

// AvailableBackends returns names of all registered backends
func (m *Manager) AvailableBackends() []string {
	names := make([]string, 0, len(m.registry))
	for name := range m.registry {
		names = append(names, name)
	}
	return names
}

// ConfiguredBackends returns names of backends that are available (configured)
func (m *Manager) ConfiguredBackends() []string {
	names := make([]string, 0, len(m.registry))
	for name, backend := range m.registry {
		if backend.IsAvailable() {
			names = append(names, name)
		}
	}
	return names
}

// GetAvailable returns all registered backends that report IsAvailable() == true.
// It does NOT check the circuit breaker — only configuration-level availability.
func (m *Manager) GetAvailable() []SearchBackend {
	result := make([]SearchBackend, 0, len(m.registry))
	for _, b := range m.registry {
		if b.IsAvailable() {
			result = append(result, b)
		}
	}
	return result
}

// NextAvailable returns the next available backend selected via round-robin.
// Backends in the exclude set are skipped. Backends where IsAvailable()
// returns false are also skipped. Returns nil when no candidate remains.
// The atomic round-robin index advances on every call, distributing load
// evenly across backends over multiple requests.
func (m *Manager) NextAvailable(exclude map[string]bool) SearchBackend {
	available := make([]SearchBackend, 0, len(m.registry))
	for _, b := range m.registry {
		if b.IsAvailable() && !exclude[b.Name()] {
			available = append(available, b)
		}
	}
	if len(available) == 0 {
		return nil
	}
	sort.SliceStable(available, func(i, j int) bool {
		return poolOrderIndex(available[i].Name()) < poolOrderIndex(available[j].Name())
	})
	idx := int(m.rrIdx.Add(1)-1) % len(available)
	return available[idx]
}

func poolOrderIndex(name string) int {
	for i, candidate := range PremiumPoolOrder {
		if candidate == name {
			return i
		}
	}
	return len(PremiumPoolOrder)
}

func (m *Manager) availableNames() string {
	return strings.Join(m.AvailableBackends(), ", ")
}
