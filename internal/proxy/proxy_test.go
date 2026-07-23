package proxy

import (
	"context"
	"errors"
	"testing"
	"time"

	"sx/internal/brave"
	"sx/internal/cache"
	"sx/internal/config"
	"sx/internal/searxng"
)

type fakeSearxng struct {
	resp *searxng.Response
	err  error
}

func (f *fakeSearxng) Search(_ context.Context, _ string) (*searxng.Response, error) {
	return f.resp, f.err
}

type fakeBrave struct {
	resp *brave.Response
	err  error
}

func (f *fakeBrave) Search(_ context.Context, _ string) (*brave.Response, error) {
	return f.resp, f.err
}

func newCfg() *config.Config {
	return &config.Config{
		FallbackMinResults: 5,
		FallbackMinEngines: 2,
		FallbackTimeout:    30 * time.Second,
		CacheTTL:           time.Hour,
	}
}

func TestSearchSearxngOK(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "a", Engine: "brave"},
		{Title: "b", Engine: "ddg"},
		{Title: "c", Engine: "startpage"},
		{Title: "d", Engine: "wikipedia"},
		{Title: "e", Engine: "github"},
	}}}
	c, _ := cache.New(10)
	p := New(newCfg(), sx, &fakeBrave{}, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 5 {
		t.Errorf("len = %d, want 5 (SearXNG, no fallback)", len(out.Results))
	}
}

func TestSearchFallbackBrave(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{
		{Title: "a", Engine: "wikipedia"},
	}}}
	bv := &fakeBrave{resp: &brave.Response{}}
	bv.resp.Web.Results = append(bv.resp.Web.Results, brave.Result{Title: "T1", URL: "u1", Description: "d1", Age: "1d"})
	c, _ := cache.New(10)
	p := New(newCfg(), sx, bv, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 1 {
		t.Errorf("len = %d, want 1 (Brave fallback)", len(out.Results))
	}
	if out.Results[0].Engine != "brave-api" {
		t.Errorf("Engine = %q, want brave-api", out.Results[0].Engine)
	}
}

func TestSearchFallbackTimeout(t *testing.T) {
	sx := &fakeSearxng{err: context.DeadlineExceeded}
	bv := &fakeBrave{resp: &brave.Response{}}
	bv.resp.Web.Results = append(bv.resp.Web.Results, brave.Result{Title: "T", URL: "u", Description: "d"})
	c, _ := cache.New(10)
	p := New(newCfg(), sx, bv, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if len(out.Results) != 1 {
		t.Errorf("len = %d, want 1 (Brave on SearXNG timeout)", len(out.Results))
	}
}

func TestSearchCacheHit(t *testing.T) {
	c, _ := cache.New(10)
	c.Set("x", &searxng.Response{Results: []searxng.Result{{Title: "cached"}}})
	sx := &fakeSearxng{} // must NOT be called
	p := New(newCfg(), sx, &fakeBrave{}, c)
	out, err := p.Search(context.Background(), "x")
	if err != nil {
		t.Fatalf("Search error = %v", err)
	}
	if out.Results[0].Title != "cached" {
		t.Errorf("Title = %q, want cached", out.Results[0].Title)
	}
}

func TestSearchFallbackBraveFails(t *testing.T) {
	sx := &fakeSearxng{resp: &searxng.Response{Results: []searxng.Result{{Engine: "wikipedia"}}}}
	bv := &fakeBrave{err: errors.New("upstream 500")}
	c, _ := cache.New(10)
	p := New(newCfg(), sx, bv, c)
	if _, err := p.Search(context.Background(), "x"); err == nil {
		t.Error("Search expected error when both SearXNG and Brave fail")
	}
}
