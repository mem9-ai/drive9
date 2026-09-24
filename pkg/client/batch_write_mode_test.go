package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestCachedBatchWriteModeSupported(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		want       bool
	}{
		{"supported", `{"storage_capabilities":{"batch_write_mode_v1":true}}`, 200, true},
		{"legacy", `{}`, 200, false},
		{"missing", `{"storage_capabilities":{}}`, 200, false},
		{"disabled", `{"storage_capabilities":{"batch_write_mode_v1":false}}`, 200, false},
		{"malformed", `{"storage_capabilities":{"batch_write_mode_v1":"true"}}`, 200, false},
		{"trailing", `{"storage_capabilities":{"batch_write_mode_v1":true}} {}`, 200, false},
		{"unavailable", `{"storage_capabilities":{"batch_write_mode_v1":true}}`, 503, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != http.MethodGet || r.URL.Path != "/v1/status" {
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(ts.Close)
			c := New(ts.URL, "")
			if c.CachedBatchWriteModeSupported() || calls.Load() != 0 {
				t.Fatal("unnegotiated capability must be false without I/O")
			}
			c.Warm(context.Background())
			if got := c.CachedBatchWriteModeSupported(); got != tc.want {
				t.Fatalf("capability=%t, want %t", got, tc.want)
			}
			if calls.Load() != 1 {
				t.Fatal("cached capability lookup triggered another status request")
			}
		})
	}
}
