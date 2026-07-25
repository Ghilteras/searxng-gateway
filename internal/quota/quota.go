// Package quota provides a background scraper that periodically fetches
// API usage/quota from Brave Search and Serper and updates Prometheus gauges.
package quota

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"sx/internal/metrics"
)

// StartScraper launches a background goroutine that scrapes Brave and Serper
// API usage every `interval`. The goroutine exits when ctx is cancelled.
// If either apiKey is empty, that scraper is silently skipped.
func StartScraper(ctx context.Context, braveAPIKey, serperAPIKey string, interval time.Duration) {
	go func() {
		time.Sleep(5 * time.Second)

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			if braveAPIKey != "" {
				scrapeBrave(ctx, braveAPIKey)
			}
			if serperAPIKey != "" {
				scrapeSerper(ctx, serperAPIKey)
			}

			select {
			case <-ticker.C:
			case <-ctx.Done():
				log.Println("[quota] scraper stopped")
				return
			}
		}
	}()
}

func scrapeBrave(ctx context.Context, apiKey string) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.search.brave.com/res/v1/usage/month", nil)
	if err != nil {
		log.Printf("[quota] Brave: request error: %v", err)
		return
	}
	req.Header.Set("X-Subscription-Token", apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[quota] Brave: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[quota] Brave: status %d", resp.StatusCode)
		return
	}

	var usage struct {
		MonthlyUsage struct {
			CurrentMonth struct {
				Total     int `json:"total"`
				Remaining int `json:"remaining"`
			} `json:"current_month"`
		} `json:"monthly_usage"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&usage); err != nil {
		log.Printf("[quota] Brave: parse error: %v", err)
		return
	}

	metrics.BraveCreditsRemaining.WithLabelValues("month").Set(float64(usage.MonthlyUsage.CurrentMonth.Remaining))
	metrics.BraveCreditsLimit.WithLabelValues("month").Set(float64(usage.MonthlyUsage.CurrentMonth.Total))
	log.Printf("[quota] Brave: %d/%d remaining", usage.MonthlyUsage.CurrentMonth.Remaining, usage.MonthlyUsage.CurrentMonth.Total)
}

func scrapeSerper(ctx context.Context, apiKey string) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.serper.dev/account", nil)
	if err != nil {
		log.Printf("[quota] Serper: request error: %v", err)
		return
	}
	req.Header.Set("X-API-KEY", apiKey)
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[quota] Serper: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[quota] Serper: status %d", resp.StatusCode)
		return
	}

	var account struct {
		Plan              string `json:"plan"`
		SearchesRemaining int    `json:"searchesRemaining"`
		SearchesLimit     int    `json:"searchesLimit"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&account); err != nil {
		log.Printf("[quota] Serper: parse error: %v", err)
		return
	}

	metrics.SerperSearchesRemaining.WithLabelValues("month").Set(float64(account.SearchesRemaining))
	metrics.SerperSearchesLimit.WithLabelValues("month").Set(float64(account.SearchesLimit))
	log.Printf("[quota] Serper: %d/%d remaining (plan: %s)", account.SearchesRemaining, account.SearchesLimit, account.Plan)
}
