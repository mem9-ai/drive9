package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalEndpoint(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1:1234", "http://[::1]:1234"} {
		if !localEndpoint(raw) {
			t.Fatal(raw)
		}
	}
	for _, raw := range []string{"", "https://127.0.0.1:1234", "http://example.com:1234", "http://127.0.0.1", "http://user@127.0.0.1:1234", "http://127.0.0.1:1234/path", "http://127.0.0.1:1234?x", "http://127.0.0.1:1234#x"} {
		if localEndpoint(raw) {
			t.Fatal(raw)
		}
	}
}
func TestProbeFailureDoesNotPrintServerBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = w.Write([]byte("private server detail"))
	}))
	defer server.Close()
	if err := run(context.Background(), server.URL); err == nil || err.Error() != "main seed" {
		t.Fatal(err)
	}
	if err := run(context.Background(), "invalid"); err == nil || err.Error() != "invalid loopback endpoint" {
		t.Fatal(err)
	}
}
