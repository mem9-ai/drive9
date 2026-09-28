package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testProxy(t *testing.T, mode string) (string, *atomic.Int32, string) {
	t.Helper()
	var upstreamCount atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("saved"))
	}))
	t.Cleanup(upstream.Close)
	target, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(t.TempDir(), "events.jsonl")
	logFile, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = logFile.Close() })
	method := "PUT"
	if mode == "x4" {
		method = ""
	}
	proxy := newFaultProxy(target, mode, method, "/case", 0, 5*time.Millisecond, &eventLog{out: logFile})
	proxy.proxy.Transport = upstream.Client().Transport
	front := httptest.NewServer(proxy)
	t.Cleanup(front.Close)
	return front.URL, &upstreamCount, logPath
}

func put(t *testing.T, endpoint string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, endpoint+"/case/file", io.NopCloser(strings.NewReader("data")))
	if err != nil {
		t.Fatal(err)
	}
	return (&http.Client{Timeout: 3 * time.Second}).Do(req)
}

func Test429ThenPassThrough(t *testing.T) {
	endpoint, calls, logPath := testProxy(t, "x3")
	first, err := put(t, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()
	if first.StatusCode != http.StatusTooManyRequests || first.Header.Get("Retry-After") != "1" {
		t.Fatalf("first response = %d, Retry-After=%q", first.StatusCode, first.Header.Get("Retry-After"))
	}
	second, err := put(t, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Body.Close()
	if second.StatusCode != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("second response = %d, upstream calls = %d", second.StatusCode, calls.Load())
	}
	evidence, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(evidence), "injected_429") {
		t.Fatalf("missing 429 evidence: %s, %v", evidence, err)
	}
}

func TestDropOnlyFirstSuccessfulResponse(t *testing.T) {
	endpoint, calls, logPath := testProxy(t, "x5")
	first, err := put(t, endpoint)
	if err == nil {
		_ = first.Body.Close()
		t.Fatalf("first successful upstream response reached client: %d", first.StatusCode)
	}
	second, err := put(t, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Body.Close()
	if second.StatusCode != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("second response = %d, upstream calls = %d", second.StatusCode, calls.Load())
	}
	evidence, err := os.ReadFile(logPath)
	if err != nil || !strings.Contains(string(evidence), "upstream_success_response_dropped") {
		t.Fatalf("missing response-drop evidence: %s, %v", evidence, err)
	}
}

func TestHangThenRecover(t *testing.T) {
	endpoint, calls, logPath := testProxy(t, "x1")
	first, err := put(t, endpoint)
	if err == nil {
		_ = first.Body.Close()
		t.Fatal("first request did not lose its response")
	}
	second, err := put(t, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Body.Close()
	if second.StatusCode != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("second response = %d, upstream calls = %d", second.StatusCode, calls.Load())
	}
	evidence, _ := os.ReadFile(logPath)
	if !strings.Contains(string(evidence), "hang_end_reset") {
		t.Fatalf("missing hang evidence: %s", evidence)
	}
}

func TestDelayAndResetThenRecover(t *testing.T) {
	endpoint, calls, logPath := testProxy(t, "x2")
	first, err := put(t, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()
	second, err := put(t, endpoint)
	if err == nil {
		_ = second.Body.Close()
		t.Fatal("second request did not lose its response")
	}
	third, err := put(t, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	_ = third.Body.Close()
	if third.StatusCode != http.StatusOK || calls.Load() != 2 {
		t.Fatalf("third response = %d, upstream calls = %d", third.StatusCode, calls.Load())
	}
	evidence, _ := os.ReadFile(logPath)
	if !strings.Contains(string(evidence), "reset_after_delay") {
		t.Fatalf("missing reset evidence: %s", evidence)
	}
}

func TestResetDuringMountStyleRequest(t *testing.T) {
	endpoint, calls, logPath := testProxy(t, "x4")
	first, err := (&http.Client{Timeout: 3 * time.Second}).Get(endpoint + "/case/status")
	if err == nil {
		_ = first.Body.Close()
		t.Fatal("first mount-style request did not reset")
	}
	second, err := (&http.Client{Timeout: 3 * time.Second}).Get(endpoint + "/case/status")
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Body.Close()
	if second.StatusCode != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("second response = %d, upstream calls = %d", second.StatusCode, calls.Load())
	}
	evidence, _ := os.ReadFile(logPath)
	if !strings.Contains(string(evidence), "reset_during_mount") {
		t.Fatalf("missing mount reset evidence: %s", evidence)
	}
}
