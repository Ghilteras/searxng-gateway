package metrics

import "testing"

func TestInitIdempotent(t *testing.T) {
	Init() // first
	// Second call must not panic, even if collectors are already registered.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Init() panicked on second call: %v", r)
		}
	}()
	Init() // must not panic on re-register
}

func TestRequestsTotalNotNil(t *testing.T) {
	Init()
	if RequestsTotal == nil {
		t.Fatal("RequestsTotal is nil after Init")
	}
}
