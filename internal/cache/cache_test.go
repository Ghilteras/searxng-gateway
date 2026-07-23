package cache

import "testing"

func TestSetGet(t *testing.T) {
	c, err := New(10)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, ok := c.Get("missing"); ok {
		t.Error("Get on empty cache returned ok=true")
	}
	c.Set("k", "v")
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("Get after Set returned ok=false")
	}
	if got != "v" {
		t.Errorf("Get = %v, want %v", got, "v")
	}
}

func TestLen(t *testing.T) {
	c, _ := New(10)
	if c.Len() != 0 {
		t.Errorf("Len on empty = %d, want 0", c.Len())
	}
	c.Set("a", 1)
	c.Set("b", 2)
	if c.Len() != 2 {
		t.Errorf("Len after 2 sets = %d, want 2", c.Len())
	}
}
