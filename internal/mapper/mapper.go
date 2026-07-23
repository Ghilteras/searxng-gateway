package mapper

import (
	"sx/internal/brave"
	"sx/internal/searxng"
)

// ToSearxngResults converts a slice of Brave search results into SearXNG results.
func ToSearxngResults(in []brave.Result) []searxng.Result {
	out := make([]searxng.Result, 0, len(in))
	for _, b := range in {
		r := searxng.Result{
			Title:         b.Title,
			URL:           b.URL,
			Content:       b.Description,
			Engine:        "brave-api",
			Score:         1.0,
			PublishedDate: b.Age,
		}
		out = append(out, r)
	}
	return out
}
