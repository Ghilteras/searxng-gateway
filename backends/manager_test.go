package backends

import (
	"testing"
)

// mockBackend is a configurable mock for testing
type mockBackend struct {
	name      string
	available bool
	results   []SearchResult
	err       error
}

func (m *mockBackend) Name() string      { return m.name }
func (m *mockBackend) IsAvailable() bool { return m.available }
func (m *mockBackend) Search(opts SearchOptions) ([]SearchResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.results, nil
}

func TestManager_Register(t *testing.T) {
	mgr := NewManager()
	b := &mockBackend{name: "mock1", available: true}
	mgr.Register(b)

	backends := mgr.AvailableBackends()
	if len(backends) != 1 || backends[0] != "mock1" {
		t.Errorf("expected [mock1], got %v", backends)
	}
}

func TestManager_SetFallbacks(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "primary", available: true})
	mgr.Register(&mockBackend{name: "fb1", available: true})
	mgr.Register(&mockBackend{name: "fb2", available: true})

	if err := mgr.SetFallbacks([]string{"fb1", "fb2"}); err != nil {
		t.Errorf("SetFallbacks failed: %v", err)
	}

	if err := mgr.SetFallbacks([]string{"fb1", "nonexistent"}); err == nil {
		t.Error("SetFallbacks should fail for unknown backend")
	}
}

func TestManager_GetBackend(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "test", available: true})

	b, ok := mgr.GetBackend("test")
	if !ok || b == nil {
		t.Error("expected to find registered backend")
	}

	_, ok = mgr.GetBackend("nonexistent")
	if ok {
		t.Error("expected false for unregistered backend")
	}
}

func TestManager_ConfiguredBackends(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "available", available: true})
	mgr.Register(&mockBackend{name: "unavailable", available: false})

	configured := mgr.ConfiguredBackends()
	if len(configured) != 1 || configured[0] != "available" {
		t.Errorf("expected [available], got %v", configured)
	}
}

func TestManager_GetAvailable(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "available1", available: true})
	mgr.Register(&mockBackend{name: "unavailable", available: false})
	mgr.Register(&mockBackend{name: "available2", available: true})

	got := mgr.GetAvailable()
	if len(got) != 2 {
		t.Errorf("GetAvailable len = %d, want 2", len(got))
	}
	names := make([]string, len(got))
	for i, b := range got {
		names[i] = b.Name()
	}
	// Should contain both available backends
	hasAvailable1 := false
	hasAvailable2 := false
	for _, n := range names {
		if n == "available1" {
			hasAvailable1 = true
		}
		if n == "available2" {
			hasAvailable2 = true
		}
	}
	if !hasAvailable1 || !hasAvailable2 {
		t.Errorf("GetAvailable names = %v, want both 'available1' and 'available2'", names)
	}
}

func TestManager_GetAvailable_None(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "unavailable", available: false})

	got := mgr.GetAvailable()
	if len(got) != 0 {
		t.Errorf("GetAvailable len = %d, want 0", len(got))
	}
}

func TestManager_NextAvailable_RoundRobin(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "brave", available: true})
	mgr.Register(&mockBackend{name: "exa", available: true})
	mgr.Register(&mockBackend{name: "jina", available: true})
	mgr.Register(&mockBackend{name: "tavily", available: true})

	// Call NextAvailable multiple times and verify cycling
	counts := make(map[string]int)
	for i := 0; i < 24; i++ {
		b := mgr.NextAvailable(map[string]bool{})
		if b == nil {
			t.Fatalf("NextAvailable returned nil on call %d", i)
		}
		counts[b.Name()]++
	}

	// Each of 4 backends should have been selected 6 times (24/4)
	for _, name := range []string{"brave", "exa", "jina", "tavily"} {
		if counts[name] != 6 {
			t.Errorf("counts[%s] = %d, want 6", name, counts[name])
		}
	}
}

func TestManager_NextAvailable_Exclude(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "brave", available: true})
	mgr.Register(&mockBackend{name: "exa", available: true})

	// Exclude brave → should get exa every time
	for i := 0; i < 5; i++ {
		b := mgr.NextAvailable(map[string]bool{"brave": true})
		if b == nil {
			t.Fatalf("NextAvailable returned nil when exa should be available")
		}
		if b.Name() != "exa" {
			t.Errorf("got %q, want exa (brave excluded)", b.Name())
		}
	}
}

func TestManager_NextAvailable_AllExcluded(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "brave", available: true})

	b := mgr.NextAvailable(map[string]bool{"brave": true})
	if b != nil {
		t.Errorf("NextAvailable with all excluded should return nil, got %q", b.Name())
	}
}

func TestManager_NextAvailable_UnavailableSkipped(t *testing.T) {
	mgr := NewManager()
	mgr.Register(&mockBackend{name: "unavailable", available: false})

	b := mgr.NextAvailable(map[string]bool{})
	if b != nil {
		t.Errorf("NextAvailable with only unavailable backends should return nil, got %q", b.Name())
	}
}
