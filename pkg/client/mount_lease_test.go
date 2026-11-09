package client

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMountLeaseClientLifecycle(t *testing.T) {
	var methods []string
	var paths []string
	var bodies []string
	var auth []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		paths = append(paths, r.URL.Path)
		auth = append(auth, r.Header.Get("Authorization"))
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tenant_id":"t1","mount_id":"m1","mount_point":"/mnt/drive9","mode":"read_write","expires_at":"2026-11-09T12:00:00Z","heartbeat_interval_seconds":20}`))
	}))
	defer ts.Close()

	c := New(ts.URL, "tenant-key")
	lease, err := c.RegisterMountLease(context.Background(), MountLeaseRequest{
		MountID: "m1", MountPoint: "/mnt/drive9", Host: "host", PID: 42, Mode: "read_write", TTLSeconds: 60,
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if lease.HeartbeatIntervalSeconds != 20 {
		t.Fatalf("lease = %+v", lease)
	}
	if _, err := c.HeartbeatMountLease(context.Background(), "m1", 30); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if err := c.DeleteMountLease(context.Background(), "m1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	wantMethods := []string{http.MethodPost, http.MethodPost, http.MethodDelete}
	wantPaths := []string{"/v1/mounts", "/v1/mounts/m1/heartbeat", "/v1/mounts/m1"}
	for i := range wantMethods {
		if methods[i] != wantMethods[i] || paths[i] != wantPaths[i] {
			t.Fatalf("request %d = %s %s, want %s %s", i, methods[i], paths[i], wantMethods[i], wantPaths[i])
		}
		if auth[i] != "Bearer tenant-key" {
			t.Fatalf("request %d auth = %q", i, auth[i])
		}
	}
	if !strings.Contains(bodies[0], `"mount_id":"m1"`) || !strings.Contains(bodies[1], `"ttl_seconds":30`) {
		t.Fatalf("bodies = %#v", bodies)
	}
}

func TestMountLeaseClientExpiredHeartbeat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":"mount lease not found or expired"}`))
	}))
	defer ts.Close()

	_, err := New(ts.URL, "key").HeartbeatMountLease(context.Background(), "m1", 30)
	if err == nil || !IsNotFound(err) {
		t.Fatalf("heartbeat error = %v, want not found", err)
	}
}
