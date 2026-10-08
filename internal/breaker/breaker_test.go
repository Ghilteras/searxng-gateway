package breaker

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sony/gobreaker"
)

// TestManager_ConcurrentReasonAccess is a regression test for unsynchronized
// map access on Manager.reasons. Before the fix, RecordClientError writes,
// RecordSuccess deletes, and LastReason reads m.reasons without any lock
// (m.mu guards breakers, not reasons). Under concurrent goroutines this
// triggers "fatal error: concurrent map writes" or a race detector report.
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
