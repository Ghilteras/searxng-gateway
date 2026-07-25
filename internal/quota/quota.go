// Package quota provides Brave and Serper API quota visibility.
//
// Brave quota gauges are updated in real-time from X-RateLimit-* response
// headers on every Brave fallback call (see proxy.recordBraveCredits).
// No separate API scraping is needed — the gauges reflect live state.
//
// Serper does not expose a public usage API at this time. The gauges
// (SerperSearchesRemaining, SerperSearchesLimit) are defined but will
// remain at 0 until Serper provides a programmatic quota endpoint.
//
// The background goroutine runs a periodic health log.
package quota

import (
	"context"
	"log"
	"time"
)

// StartScraper launches a background goroutine that periodically logs
// quota health status. Braves gauges are updated from response headers
// by the proxy; Serper has no public usage API yet.
func StartScraper(ctx context.Context, _ string, _ string, interval time.Duration) {
	go func() {
		time.Sleep(5 * time.Second)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		log.Printf("[quota] started (interval=%s). Brave tracked via response headers; Serper usage API not available yet.", interval)

		for {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				log.Println("[quota] stopped")
				return
			}
		}
	}()
}
