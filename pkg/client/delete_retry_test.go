//go:build !integration

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type closeTrackingDeleteBody struct {
	io.ReadCloser
	closed chan struct{}
}

func (b *closeTrackingDeleteBody) Close() error {
	close(b.closed)
	return b.ReadCloser.Close()
}

func TestDeleteFileReusesConnectionAfterSuccessBody(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Query().Get("kind") != "file" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
		}
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	gotConn := make(chan bool, 2)
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) { gotConn <- info.Reused },
	})
	for _, path := range []string{"/first", "/second"} {
		if err := c.DeleteFileCtx(ctx, path); err != nil {
			t.Fatalf("DeleteFileCtx(%q): %v", path, err)
		}
	}
	if <-gotConn {
		t.Fatal("first request unexpectedly reused a connection")
	}
	if !<-gotConn {
		t.Fatal("second request did not reuse the first connection")
	}
}

func TestDeleteFileSuccessBodyReadFailureDoesNotRetry(t *testing.T) {
	c := New("http://example.test", "")
	var calls int
	c.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: http.StatusOK, Body: failingReadCloser{}}, nil
	})
	if err := c.DeleteFileCtx(context.Background(), "/file"); err != nil {
		t.Fatalf("acknowledged delete returned body read error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("requests = %d, want 1", calls)
	}
}

func TestDeleteFileStalledSuccessBodyReturnsWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	releaseBody := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-releaseBody:
			_, _ = fmt.Fprint(w, `{"status":"ok"}`)
		}
	}))
	defer ts.Close()
	defer close(releaseBody)

	c := New(ts.URL, "")
	closed := make(chan struct{})
	transport := c.httpClient.Transport
	c.httpClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		resp, err := transport.RoundTrip(req)
		if err == nil {
			resp.Body = &closeTrackingDeleteBody{ReadCloser: resp.Body, closed: closed}
		}
		return resp, err
	})
	done := make(chan error, 1)
	go func() { done <- c.DeleteFileCtx(context.Background(), "/file") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acknowledged delete returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DeleteFileCtx blocked on a stalled success body")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
	select {
	case <-closed:
	default:
		t.Fatal("success response body was not closed")
	}
}

func TestDeleteFileDrainBudgetStartsAfterHeaders(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(maxDeleteSuccessDrainTime + 100*time.Millisecond)
		_, _ = fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer ts.Close()

	if err := New(ts.URL, "").DeleteFileCtx(context.Background(), "/file"); err != nil {
		t.Fatalf("DELETE with slow response headers: %v", err)
	}
}

type countingDeleteBody struct {
	reader *strings.Reader
	read   int
	closed bool
}

func (b *countingDeleteBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}

func (b *countingDeleteBody) Close() error {
	b.closed = true
	return nil
}

func TestDeleteFileSuccessBodyDrainIsBounded(t *testing.T) {
	body := &countingDeleteBody{reader: strings.NewReader(strings.Repeat("x", 1<<20))}
	c := New("http://example.test", "")
	c.httpClient.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	})
	if err := c.DeleteFileCtx(context.Background(), "/file"); err != nil {
		t.Fatalf("DeleteFileCtx: %v", err)
	}
	if body.read > int(maxDeleteSuccessBodyBytes+1) {
		t.Fatalf("read %d bytes from oversized success body", body.read)
	}
	if !body.closed {
		t.Fatal("success body was not closed")
	}
}

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
	if !strings.Contains(se.Message, "recursive delete in progress") {
		t.Fatalf("error message = %q, want server response", se.Message)
	}
	if got, want := calls.Load(), int32(1+removeAllMaxRetries); got != want {
		t.Fatalf("requests = %d, want %d", got, want)
	}
}

// TestDeleteDoesNotRetryOn503 verifies the 503 retry loop only applies to
// recursive deletes: a plain (non-recursive) delete must surface the first
// 503 immediately, without re-issuing the request.
func TestDeleteDoesNotRetryOn503(t *testing.T) {
	var calls atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.Contains(r.URL.RawQuery, "recursive=1") {
			t.Errorf("unexpected recursive query: %s", r.URL)
		}
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer ts.Close()

	c := New(ts.URL, "")
	err := c.Delete("/data/file.txt")
	var se *StatusError
	if !errors.As(err, &se) || se.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("error = %v, want 503 *StatusError", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1 (no retry for non-recursive delete)", got)
	}
}

// TestRemoveAllRetryDelayClampsRetryAfter verifies that a huge Retry-After
// value is clamped to removeAllMaxRetryDelay instead of parking the client
// for days, while normal and missing values keep their existing behavior.
func TestRemoveAllRetryDelayClampsRetryAfter(t *testing.T) {
	tests := []struct {
		name       string
		retryAfter string
		backoff    time.Duration
		want       time.Duration
	}{
		{name: "huge value is clamped", retryAfter: "999999", backoff: time.Second, want: removeAllMaxRetryDelay},
		{name: "overflow-scale value is clamped before conversion", retryAfter: "10000000000", backoff: time.Second, want: removeAllMaxRetryDelay},
		{name: "zero means immediate retry", retryAfter: "0", backoff: time.Second, want: 0},
		{name: "small value is honored", retryAfter: "2", backoff: time.Second, want: 2 * time.Second},
		{name: "missing header uses backoff", retryAfter: "", backoff: 2 * time.Second, want: 2 * time.Second},
		{name: "invalid header uses backoff", retryAfter: "soon", backoff: 4 * time.Second, want: 4 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := removeAllRetryDelay(tt.retryAfter, tt.backoff); got != tt.want {
				t.Fatalf("removeAllRetryDelay(%q, %v) = %v, want %v", tt.retryAfter, tt.backoff, got, tt.want)
			}
		})
	}
}

// TestRemoveAllCtxCancelDuringBackoff verifies that cancelling the context
// while the retry loop is waiting out a Retry-After delay aborts the wait
// immediately instead of sleeping through it.
func TestRemoveAllCtxCancelDuringBackoff(t *testing.T) {
	var calls atomic.Int32
	responded := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusServiceUnavailable)
		close(responded)
	}))
	defer ts.Close()

	ctx, cancel := context.WithCancel(context.Background())
	c := New(ts.URL, "")
	done := make(chan error, 1)
	go func() { done <- c.RemoveAllCtx(ctx, "/data/") }()

	// Wait until the first 503 response is sent (the retry loop is now in its
	// backoff wait), then cancel.
	<-responded
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RemoveAllCtx did not return promptly after cancel")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("requests = %d, want 1", got)
	}
}
