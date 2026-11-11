package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestMountLeaseReporterRegistersHeartbeatsAndDeletes(t *testing.T) {
	registered := make(chan client.MountLeaseRequest, 1)
	heartbeat := make(chan string, 1)
	deleted := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/mounts":
			var req client.MountLeaseRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Errorf("decode register: %v", err)
			}
			registered <- req
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tenant_id":"t1","mount_id":"m1","mount_point":"/mnt/drive9","mode":"read_write","heartbeat_interval_seconds":1}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/mounts/m1/heartbeat":
			heartbeat <- r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tenant_id":"t1","mount_id":"m1","mode":"read_write"}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/v1/mounts/m1":
			deleted <- r.URL.Path
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
	select {
	case req := <-registered:
		if req.MountID != "m1" || req.MountPoint != "/mnt/drive9" || req.Mode != "read_write" || req.PID <= 0 {
			t.Fatalf("register request = %+v", req)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("mount lease was not registered")
	}
	// The server's advisory interval is one second, so the reporter should
	// heartbeat on the production path before Close.
	select {
	case <-heartbeat:
	case <-time.After(3 * time.Second):
		t.Fatal("mount lease was not heartbeat")
	}
	reporter.Close()
	select {
	case path := <-deleted:
		if path != "/v1/mounts/m1" {
			t.Fatalf("delete path = %q", path)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("mount lease was not deleted")
	}
}

func TestMountLeaseReporterIgnoresUnsupportedEndpoint(t *testing.T) {
	seen := make(chan struct{}, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen <- struct{}{}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	reporter := startMountLease(context.Background(), client.New(ts.URL, "key"), "m1", "/mnt/drive9", true)
	if reporter == nil {
		t.Fatal("reporter is nil")
	}
	select {
	case <-seen:
	case <-time.After(3 * time.Second):
		t.Fatal("mount lease registration was not attempted")
	}
	reporter.Close()
}
