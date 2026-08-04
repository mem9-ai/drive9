//go:build !integration

package client

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestRemoveAllRetriesOn503 verifies the bounded retry loop around recursive
// deletes: two 503 responses (honoring Retry-After) followed by a 200 must
// surface as success, with exactly three requests issued.
func TestRemoveAllRetriesOn503(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || !strings.Contains(r.URL.RawQuery, "recursive=1") {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		switch n := calls.Add(1); n {
		case 1:
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2:
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			_, _ = fmt.Fprint(w, `{"status":"ok"}`)
		}
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	if err := c.RemoveAll("/data/"); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("requests = %d, want 3", got)
	}
}

// TestRemoveAllRetryLimitExceeded verifies the retry loop is bounded: a server
// that keeps answering 503 must surface the last 503 as an error after
// 1 + removeAllMaxRetries requests.
func TestRemoveAllRetryLimitExceeded(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "0")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprint(w, `{"error":"recursive delete in progress, retry to resume"}`)
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	err := c.RemoveAll("/data/")
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("error = %v, want 503 *StatusError", err)
	}
	if got, want := calls.Load(), int32(1+removeAllMaxRetries); got != want {
		t.Fatalf("requests = %d, want %d", got, want)
	}
}
