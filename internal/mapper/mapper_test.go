package mapper

import (
	"testing"

	"sx/internal/brave"
)

func TestToSearxngResultsEmpty(t *testing.T) {
	got := ToSearxngResults(nil)
	if len(got) != 0 {
		t.Errorf("len = %d, want 0", len(got))
	}
}

func TestToSearxngResultsMapping(t *testing.T) {
	in := []brave.Result{
		{Title: "T1", URL: "https://x", Description: "D1", Age: "2 days ago"},
		{Title: "T2", URL: "https://y", Description: "D2", Age: ""},
	}
	got := ToSearxngResults(in)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Title != "T1" || got[0].URL != "https://x" || got[0].Content != "D1" {
		t.Errorf("got[0] = %+v, want T1/URL/D1", got[0])
	}
	if got[0].Engine != "brave-api" {
		t.Errorf("got[0].Engine = %q, want brave-api", got[0].Engine)
	}
	if got[0].Score != 1.0 {
		t.Errorf("got[0].Score = %f, want 1.0", got[0].Score)
	}
	if got[0].PublishedDate != "2 days ago" {
		t.Errorf("got[0].PublishedDate = %q, want %q", got[0].PublishedDate, "2 days ago")
	}
	if got[1].PublishedDate != "" {
		t.Errorf("got[1].PublishedDate = %q, want empty (Age was empty)", got[1].PublishedDate)
	}
}
