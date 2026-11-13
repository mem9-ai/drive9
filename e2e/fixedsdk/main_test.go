package main

import (
	"context"
	"github.com/mem9-ai/drive9/pkg/client"
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

func TestQueuePhaseRejectsInvalidConfigurationBeforeIO(t *testing.T) {
	for _, phase := range []string{"", "unknown", "stage", "recover"} {
		if err := queuePhase(context.Background(), "http://example.invalid:80", phase, t.TempDir(), client.ViewBinding{}); err == nil || err.Error() != "invalid queue probe configuration" {
			t.Fatal(phase, err)
		}
	}
	binding := client.ViewBinding{LayerID: "a", WorkID: "work", Fence: 1}
	for _, dir := range []string{"", "relative"} {
		if err := queuePhase(context.Background(), "http://127.0.0.1:1", "stage", dir, binding); err == nil {
			t.Fatal("accepted relative staging")
		}
	}
	binding.Validation = true
	if err := queuePhase(context.Background(), "http://127.0.0.1:1", "recover", t.TempDir(), binding); err == nil {
		t.Fatal("accepted validation binding")
	}
}
