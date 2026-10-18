package backends

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
)

func TestProviderContractAccountUnavailabilityClasses(t *testing.T) {
	data, err := os.ReadFile("../testdata/monitoring/provider_contract.json")
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Eligibility struct {
			Reasons []struct {
				Reason string `json:"reason"`
				Class  string `json:"class"`
			} `json:"reasons"`
		} `json:"eligibility"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}

	var fixtureRuntimeFailures []string
	for _, reason := range contract.Eligibility.Reasons {
		if reason.Class == "runtime_failure" {
			fixtureRuntimeFailures = append(fixtureRuntimeFailures, reason.Reason)
		}
	}
	accountUnavailableReasons := []string{
		UnavailabilityCredentialsRejected,
		UnavailabilityQuotaExhausted,
		UnavailabilityDisabled,
	}
	sort.Strings(fixtureRuntimeFailures)
	sort.Strings(accountUnavailableReasons)
	fixtureRuntimeFailureSet := make(map[string]bool, len(fixtureRuntimeFailures))
	for _, reason := range fixtureRuntimeFailures {
		fixtureRuntimeFailureSet[reason] = true
	}

	// Producer account-unavailability reasons must be contained in fixture runtime_failure because that class also covers transient states such as breaker_open, while configuration could let a consumer's mask silence the alert during the account outage.
	for _, reason := range accountUnavailableReasons {
		if !fixtureRuntimeFailureSet[reason] {
			t.Errorf("backend account-unavailability reasons = %v; fixture runtime_failure reasons = %v; missing %q", accountUnavailableReasons, fixtureRuntimeFailures, reason)
		}
	}
}

func TestBackendError_Error(t *testing.T) {
	err := &BackendError{
		Backend: "test",
		Err:     fmt.Errorf("something went wrong"),
		Code:    ErrCodeNetwork,
	}

	want := "test backend: something went wrong"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

func TestBackendError_Unwrap(t *testing.T) {
	inner := fmt.Errorf("inner error")
	err := &BackendError{
		Backend: "test",
		Err:     inner,
		Code:    ErrCodeAuth,
	}

	if err.Unwrap() != inner {
		t.Errorf("Unwrap() returned different error")
	}
}

func TestBackendError_Codes(t *testing.T) {
	// Verify error code constants are distinct
	codes := []int{ErrCodeUnavailable, ErrCodeNetwork, ErrCodeAuth, ErrCodeRateLimit, ErrCodeInvalidResponse}
	seen := make(map[int]bool)
	for _, code := range codes {
		if seen[code] {
			t.Errorf("duplicate error code: %d", code)
		}
		seen[code] = true
	}
}
