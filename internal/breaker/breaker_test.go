package breaker

import (
	"sync"
	"testing"
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
