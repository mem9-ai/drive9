package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

// A heartbeat rejected with 401/403 must not end the reporter. Before this the
// loop returned, so the lease silently disappeared: nothing surfaced it
// (`mount health` reads local state only) and the migration controller could
// move a region out from under a live mount. The reporter now treats it as
// "lease lost" and re-registers with backoff.
func TestMountLeaseReporterReRegistersAfterRejectedHeartbeat(t *testing.T) {
	var registers atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/mounts":
			registers.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id":                  "t1",
				"mount_id":                   "m1",
				"mount_point":                "/mnt/drive9",
				"mode":                       "read_write",
				"heartbeat_interval_seconds": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/mounts/m1/heartbeat":
			// The credential is rejected; the lease row is gone with it.
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"token expired"}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/mounts/m1":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	reporter := startMountLease(context.Background(), client.New(ts.URL, "key"), "m1", "/mnt/drive9", false)
	if reporter == nil {
		t.Fatal("reporter is nil")
	}
	defer reporter.Close()

	// One registration, one rejected heartbeat, then a second registration: the
	// reporter must have gone back through registration instead of exiting.
	deadline := time.After(6 * time.Second)
	for registers.Load() < 2 {
		select {
		case <-deadline:
			t.Fatalf("reporter did not re-register after a rejected heartbeat (registers=%d)", registers.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}
	select {
	case <-reporter.done:
		t.Fatal("reporter exited after a rejected heartbeat instead of re-registering")
	default:
	}
}

func TestNextMountLeaseRetryDoublesAndCaps(t *testing.T) {
	if got := nextMountLeaseRetry(mountLeaseRetry); got != 2*mountLeaseRetry {
		t.Fatalf("first backoff = %v, want %v", got, 2*mountLeaseRetry)
	}
	if got := nextMountLeaseRetry(mountLeaseRetryMax); got != mountLeaseRetryMax {
		t.Fatalf("capped backoff = %v, want %v", got, mountLeaseRetryMax)
	}
	if got := nextMountLeaseRetry(3 * time.Minute); got != mountLeaseRetryMax {
		t.Fatalf("backoff above cap = %v, want %v", got, mountLeaseRetryMax)
	}
}
