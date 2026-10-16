package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker"
)

// TestManager_ConcurrentReasonAccess exercises concurrent breaker operations.
// Each entry's mutex protects admission and reason/accounting metadata, and is
// never held across provider work; the manager mutex protects only the entry map.
func TestManager_ConcurrentReasonAccess(t *testing.T) {
	m := New()

	engines := []string{"brave", "exa", "jina", "tavily"}
	const goroutines = 32
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// Barrier to ensure all goroutines start simultaneously.
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				engine := engines[(id+j)%len(engines)]
				switch j % 3 {
				case 0:
					m.RecordClientError(engine, "403 forbidden")
				case 1:
					m.RecordSuccess(engine)
				case 2:
					_ = m.LastReason(engine)
				}
			}
		}(i)
	}

	close(start) // release all goroutines at once
	wg.Wait()
}

func TestExecuteRealAdmissionCooldownProbeRecoveryAndReopen(t *testing.T) {
	m := New()
	m.settings = func(engine string) EngineSettings {
		return EngineSettings{Name: engine, MaxRequests: 1, Timeout: 20 * time.Millisecond}
	}
	var calls atomic.Int64
	providerFailure := errors.New("provider failure")
	_, err := m.Execute("premium:tavily", func() (interface{}, error) {
		calls.Add(1)
		return nil, providerFailure
	})
	if !errors.Is(err, providerFailure) || m.State("premium:tavily") != gobreaker.StateClosed {
		t.Fatalf("first provider fault: err=%v state=%v, want closed", err, m.State("premium:tavily"))
	}
	_, err = m.Execute("premium:tavily", func() (interface{}, error) {
		calls.Add(1)
		return nil, providerFailure
	})
	if !errors.Is(err, providerFailure) || m.State("premium:tavily") != gobreaker.StateOpen {
		t.Fatalf("second consecutive provider fault: err=%v state=%v, want open", err, m.State("premium:tavily"))
	}
	_, err = m.Execute("premium:tavily", func() (interface{}, error) {
		calls.Add(1)
		return "unexpected", nil
	})
	if !errors.Is(err, gobreaker.ErrOpenState) || calls.Load() != 2 {
		t.Fatalf("open breaker admission: err=%v calls=%d, want ErrOpenState and 2 calls", err, calls.Load())
	}

	// Wait for the configured cooldown, then race several callers. Exactly one
	// actual request may pass through the half-open breaker.
	deadline := time.Now().Add(time.Second)
	for m.State("premium:tavily") != gobreaker.StateHalfOpen && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.State("premium:tavily") != gobreaker.StateHalfOpen {
		t.Fatal("breaker did not enter half-open after cooldown")
	}
	var admitted atomic.Int64
	start := make(chan struct{})
	probeDone := make(chan struct{})
	deniedDone := make(chan struct{}, 16)
	probeRelease := make(chan struct{})
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, executeErr := m.Execute("premium:tavily", func() (interface{}, error) {
				admitted.Add(1)
				close(probeDone)
				<-probeRelease
				return "healthy", nil
			})
			if executeErr != nil {
				deniedDone <- struct{}{}
			}
		}()
	}
	close(start)
	select {
	case <-probeDone:
	case <-time.After(time.Second):
		t.Fatal("no half-open probe was admitted")
	}
	// The admitted probe stays in-flight until all other calls have been
	// rejected, preventing the first success from closing the breaker early.
	for range 15 {
		select {
		case <-deniedDone:
		case <-time.After(time.Second):
			close(probeRelease)
			t.Fatal("half-open caller was neither admitted nor rejected")
		}
	}
	close(probeRelease)
	wg.Wait()
	if got := admitted.Load(); got != 1 {
		t.Fatalf("half-open actual provider calls = %d, want exactly 1", got)
	}
	if got := m.State("premium:tavily"); got != gobreaker.StateClosed {
		t.Fatalf("successful probe state = %v, want closed", got)
	}

	_, err = m.Execute("premium:tavily", func() (interface{}, error) {
		calls.Add(1)
		return nil, providerFailure
	})
	if !errors.Is(err, providerFailure) || m.State("premium:tavily") != gobreaker.StateClosed {
		t.Fatalf("first genuine fault after recovery: err=%v state=%v, want closed", err, m.State("premium:tavily"))
	}
	_, err = m.Execute("premium:tavily", func() (interface{}, error) {
		calls.Add(1)
		return nil, providerFailure
	})
	if !errors.Is(err, providerFailure) || m.State("premium:tavily") != gobreaker.StateOpen {
		t.Fatalf("second consecutive fault after recovery: err=%v state=%v, want reopened", err, m.State("premium:tavily"))
	}
	deadline = time.Now().Add(time.Second)
	for m.State("premium:tavily") != gobreaker.StateHalfOpen && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.State("premium:tavily") != gobreaker.StateHalfOpen {
		t.Fatal("breaker did not re-enter half-open after second cooldown")
	}
	_, err = m.Execute("premium:tavily", func() (interface{}, error) {
		calls.Add(1)
		return nil, providerFailure
	})
	if !errors.Is(err, providerFailure) || m.State("premium:tavily") != gobreaker.StateOpen {
		t.Fatalf("failed half-open probe: err=%v state=%v, want reopened", err, m.State("premium:tavily"))
	}
}

func TestNonProviderFaultDoesNotTrip(t *testing.T) {
	m := New()
	_, err := m.Execute("premium:brave", func() (interface{}, error) {
		return nil, ErrNonProviderFault
	})
	if !errors.Is(err, ErrNonProviderFault) {
		t.Fatalf("Execute error = %v, want non-provider marker", err)
	}
	if got := m.State("premium:brave"); got != gobreaker.StateClosed {
		t.Fatalf("non-provider fault state = %v, want closed", got)
	}
}

func TestAllowFreezesTripReasonAcrossOverlappingGeneration(t *testing.T) {
	m := New()
	firstDone, _, err := m.Allow("premium:overlap")
	if err != nil {
		t.Fatal(err)
	}
	tripDone, _, err := m.Allow("premium:overlap")
	if err != nil {
		t.Fatal(err)
	}
	lateDone, _, err := m.Allow("premium:overlap")
	if err != nil {
		t.Fatal(err)
	}

	// The first completion establishes one consecutive failure. The second
	// completion trips and freezes the structural provenance for this epoch.
	firstDone(false, "transient-network")
	tripDone(false, "structural-quota")
	if got := m.State("premium:overlap"); got != gobreaker.StateOpen {
		t.Fatalf("state after structural trip = %v, want open", got)
	}
	_, snapshot, err := m.Allow("premium:overlap")
	if !errors.Is(err, gobreaker.ErrOpenState) || snapshot.Reason != "structural-quota" {
		t.Fatalf("open rejection = snapshot %#v err %v, want frozen structural reason", snapshot, err)
	}

	// This call was admitted in the pre-trip generation. gobreaker ignores its
	// late result, and the manager must not let it rewrite the open-epoch reason.
	lateDone(false, "late-transient")
	_, snapshot, err = m.Allow("premium:overlap")
	if !errors.Is(err, gobreaker.ErrOpenState) || snapshot.Reason != "structural-quota" {
		t.Fatalf("open rejection after stale completion = snapshot %#v err %v, want unchanged reason", snapshot, err)
	}
}

func TestAllowUnknownTripAndHalfOpenContentionSnapshot(t *testing.T) {
	unknownManager := NewWithSettings(func(engine string) EngineSettings {
		return EngineSettings{Name: engine, MaxRequests: 1, Timeout: time.Minute}
	})
	first, _, err := unknownManager.Allow("premium:unknown")
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := unknownManager.Allow("premium:unknown")
	if err != nil {
		t.Fatal(err)
	}
	first(false, "transient-network")
	second(false, "")
	_, snapshot, err := unknownManager.Allow("premium:unknown")
	if !errors.Is(err, gobreaker.ErrOpenState) || snapshot.Reason != "" {
		t.Fatalf("unknown open rejection = snapshot %#v err %v, want unknown reason", snapshot, err)
	}

	// A separate breaker proves that probe-slot contention masks, rather than
	// inherits, the structural reason of the open epoch.
	m := NewWithSettings(func(engine string) EngineSettings {
		return EngineSettings{Name: engine, MaxRequests: 1, Timeout: 20 * time.Millisecond}
	})
	trip1, _, err := m.Allow("premium:structural")
	if err != nil {
		t.Fatal(err)
	}
	trip2, _, err := m.Allow("premium:structural")
	if err != nil {
		t.Fatal(err)
	}
	trip1(false, "structural-credentials")
	trip2(false, "structural-credentials")
	deadline := time.Now().Add(time.Second)
	for m.State("premium:structural") != gobreaker.StateHalfOpen && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if m.State("premium:structural") != gobreaker.StateHalfOpen {
		t.Fatal("breaker did not enter half-open")
	}
	probeDone, _, err := m.Allow("premium:structural")
	if err != nil {
		t.Fatalf("half-open probe admission: %v", err)
	}
	_, snapshot, err = m.Allow("premium:structural")
	if !errors.Is(err, gobreaker.ErrTooManyRequests) || snapshot.State != gobreaker.StateHalfOpen || snapshot.Reason != ReasonContention {
		t.Fatalf("probe contention = snapshot %#v err %v, want contention, not structural provenance", snapshot, err)
	}
	probeDone(true, "")
}

func TestExecutePanicCountsFailureAndReleasesHalfOpenProbe(t *testing.T) {
	m := NewWithSettings(func(engine string) EngineSettings {
		return EngineSettings{Name: engine, MaxRequests: 1, Timeout: 20 * time.Millisecond}
	})
	const engine = "premium:panic"
	failure := errors.New("provider failure")
	for range 2 {
		_, err := m.Execute(engine, func() (interface{}, error) { return nil, failure })
		if !errors.Is(err, failure) {
			t.Fatalf("initial failure = %v, want %v", err, failure)
		}
	}
	if got := m.State(engine); got != gobreaker.StateOpen {
		t.Fatalf("state after two failures = %v, want open", got)
	}
	waitForState(t, m, engine, gobreaker.StateHalfOpen)

	panicValue := "provider panic"
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Fatalf("recovered panic = %#v, want %#v", got, panicValue)
			}
		}()
		_, _ = m.Execute(engine, func() (interface{}, error) { panic(panicValue) })
	}()
	if got := m.State(engine); got != gobreaker.StateOpen {
		t.Fatalf("state after panicking half-open probe = %v, want open (failure accounted)", got)
	}

	waitForState(t, m, engine, gobreaker.StateHalfOpen)
	if _, err := m.Execute(engine, func() (interface{}, error) { return "recovered", nil }); err != nil {
		t.Fatalf("probe after panic cooldown = %v, want admitted recovery probe", err)
	}
	if got := m.State(engine); got != gobreaker.StateClosed {
		t.Fatalf("state after recovery probe = %v, want closed", got)
	}
}

func waitForState(t *testing.T, m *Manager, engine string, want gobreaker.State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for m.State(engine) != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := m.State(engine); got != want {
		t.Fatalf("state = %v, want %v", got, want)
	}
}
