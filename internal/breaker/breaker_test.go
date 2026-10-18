package breaker

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sony/gobreaker"
)

func failAdmission(t *testing.T, m *Manager, engine, reason string) {
	t.Helper()
	done, _, err := m.Allow(engine)
	if err != nil {
		t.Fatalf("admission for %s: %v", engine, err)
	}
	done(false, reason)
}

func TestManagerSnapshotStatesAndUnknownDoesNotCreateEntry(t *testing.T) {
	m := NewWithSettings(func(engine string) EngineSettings {
		return EngineSettings{Name: engine, MaxRequests: 1, Timeout: 20 * time.Millisecond}
	})
	if got := m.Snapshot("unknown"); got != (AdmissionSnapshot{State: gobreaker.StateClosed}) {
		t.Fatalf("unknown snapshot = %#v", got)
	}
	if len(m.breakers) != 0 {
		t.Fatal("unknown snapshot created an entry")
	}
	closed := m.Snapshot("premium:snapshot")
	if closed != (AdmissionSnapshot{State: gobreaker.StateClosed}) {
		t.Fatalf("new breaker snapshot = %#v", closed)
	}
	failAdmission(t, m, "premium:snapshot", "transient")
	failAdmission(t, m, "premium:snapshot", "structural-quota")
	open := m.Snapshot("premium:snapshot")
	if open != (AdmissionSnapshot{State: gobreaker.StateOpen, Reason: "structural-quota"}) {
		t.Fatalf("open snapshot = %#v", open)
	}
	waitForState(t, m, "premium:snapshot", gobreaker.StateHalfOpen)
	halfOpen := m.Snapshot("premium:snapshot")
	if halfOpen != (AdmissionSnapshot{State: gobreaker.StateHalfOpen}) {
		t.Fatalf("half-open snapshot = %#v, want no structural reason", halfOpen)
	}
}

func TestAllowBooleanCompletionControlsFailureAccounting(t *testing.T) {
	m := New()
	done, _, err := m.Allow("premium:boolean")
	if err != nil {
		t.Fatal(err)
	}
	done(true, "ignored") // cancellation/validation/empty outcomes are successes.
	failAdmission(t, m, "premium:boolean", "first-provider-fault")
	if got := m.Snapshot("premium:boolean"); got.State != gobreaker.StateClosed || got.Reason != "" {
		t.Fatalf("one failure after success = %#v, want closed", got)
	}
	failAdmission(t, m, "premium:boolean", "provider-fault")
	if got := m.Snapshot("premium:boolean"); got != (AdmissionSnapshot{State: gobreaker.StateOpen, Reason: "provider-fault"}) {
		t.Fatalf("two provider failures = %#v", got)
	}
}

func TestOpenSnapshotWithEmptyTripReasonStaysUnknown(t *testing.T) {
	m := New()
	failAdmission(t, m, "premium:unknown-provenance", "")
	failAdmission(t, m, "premium:unknown-provenance", "")
	_, snapshot, err := m.Allow("premium:unknown-provenance")
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("open breaker admission error = %v, want ErrOpenState", err)
	}
	want := AdmissionSnapshot{State: gobreaker.StateOpen, Reason: ""}
	if snapshot != want {
		t.Fatalf("open snapshot with empty trip reason = %#v, want %#v", snapshot, want)
	}
	if got := m.Snapshot("premium:unknown-provenance"); got != want {
		t.Fatalf("snapshot with empty trip reason = %#v, want %#v", got, want)
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
	firstDone(false, "transient-network")
	tripDone(false, "structural-quota")
	if got := m.Snapshot("premium:overlap"); got != (AdmissionSnapshot{State: gobreaker.StateOpen, Reason: "structural-quota"}) {
		t.Fatalf("snapshot after trip = %#v", got)
	}
	lateDone(false, "late-transient")
	if got := m.Snapshot("premium:overlap"); got != (AdmissionSnapshot{State: gobreaker.StateOpen, Reason: "structural-quota"}) {
		t.Fatalf("late completion rewrote trip provenance: %#v", got)
	}
}

func TestAllowHalfOpenContentionKeepsBlockedProbeSlot(t *testing.T) {
	m := NewWithSettings(func(engine string) EngineSettings {
		return EngineSettings{Name: engine, MaxRequests: 1, Timeout: 20 * time.Millisecond}
	})
	failAdmission(t, m, "premium:contention", "transient")
	failAdmission(t, m, "premium:contention", "structural-credentials")
	waitForState(t, m, "premium:contention", gobreaker.StateHalfOpen)
	entered, release := make(chan struct{}), make(chan struct{})
	probeResult := make(chan error, 1)
	go func() {
		probeDone, _, admissionErr := m.Allow("premium:contention")
		if admissionErr != nil {
			probeResult <- admissionErr
			return
		}
		close(entered)
		<-release // provider probe remains in flight until the blocked call returns.
		probeDone(true, "")
		probeResult <- nil
	}()
	select {
	case <-entered:
	case err := <-probeResult:
		t.Fatalf("probe admission: %v", err)
	case <-time.After(time.Second):
		t.Fatal("probe did not enter")
	}
	var wg sync.WaitGroup
	const callers = 15
	denied := make(chan struct{}, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, snapshot, admissionErr := m.Allow("premium:contention")
			if errors.Is(admissionErr, gobreaker.ErrTooManyRequests) && snapshot.State == gobreaker.StateHalfOpen && snapshot.Reason == ReasonContention {
				denied <- struct{}{}
			}
		}()
	}
	for range callers {
		select {
		case <-denied:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("half-open admission was not rejected")
		}
	}
	wg.Wait()
	close(release)
	if err := <-probeResult; err != nil {
		t.Fatalf("probe completion: %v", err)
	}
	if got := m.Snapshot("premium:contention"); got.State != gobreaker.StateClosed {
		t.Fatalf("successful probe state = %#v", got)
	}
}

func TestManagerConcurrentTransitionsAndSnapshots(t *testing.T) {
	m := NewWithSettings(func(engine string) EngineSettings {
		return EngineSettings{Name: engine, MaxRequests: 1, Timeout: time.Millisecond}
	})
	const goroutines, iterations = 32, 200
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				engine := []string{"premium:a", "premium:b"}[(id+j)%2]
				switch j % 3 {
				case 0:
					m.RecordClientError(engine, "403")
				case 1:
					m.RecordSuccess(engine)
				default:
					_ = m.Snapshot(engine)
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
}

func waitForState(t *testing.T, m *Manager, engine string, want gobreaker.State) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for m.Snapshot(engine).State != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := m.Snapshot(engine).State; got != want {
		t.Fatalf("state = %v, want %v", got, want)
	}
}
