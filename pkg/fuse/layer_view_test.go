package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestResolveLayerMountViewBaseRootAndReadOnly(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1a"
	var bindCalled atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/task=auth":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(client.LayerInfo{
				LayerID:      layerID,
				BaseRootPath: "/workspace",
				State:        "active",
				Fence:        0,
			})
		case r.URL.Path == "/v1/layers/"+layerID+"/bind-mount":
			bindCalled.Store(true)
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodHead && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/workspace/a.txt":
			if got := r.Header.Get("X-FS-Layer-Fence"); got != "" {
				t.Errorf("read-only view unexpectedly sent fence %q", got)
			}
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	base := client.New(ts.URL, "sk-test")

	// 1. Defaulted remoteRoot adopts layer.BaseRootPath ("/workspace").
	b, err := resolveLayerMountView(context.Background(), base, "task=auth", "/", true, "mnt-1", true, defaultLayerLeaseTiming())
	if err != nil {
		t.Fatalf("resolveLayerMountView defaulted: %v", err)
	}
	defer b.Unbind(context.Background())
	if b.LayerID != layerID || b.RemoteRoot != "/workspace" || b.Fence != 0 || bindCalled.Load() {
		t.Fatalf("unexpected binding: %+v (bindCalled=%v)", b, bindCalled.Load())
	}
	if _, err := b.ViewClient.StatCtx(context.Background(), "/workspace/a.txt"); err != nil {
		t.Fatalf("StatCtx through read-only view: %v", err)
	}

	// 2. Explicit remoteRoot outside layer.BaseRootPath fails before mounting.
	if _, err := resolveLayerMountView(context.Background(), base, "task=auth", "/other", false, "mnt-2", true, defaultLayerLeaseTiming()); err == nil {
		t.Fatal("expected error for remoteRoot outside layer base_root_path")
	}

	// 3. Explicit remoteRoot "/" when base_root_path is "/workspace" also fails!
	if _, err := resolveLayerMountView(context.Background(), base, "task=auth", "/", false, "mnt-3", true, defaultLayerLeaseTiming()); err == nil {
		t.Fatal("expected error for explicit / outside /workspace base_root_path")
	}
}

func TestLayerMountViewExpireBindRenewSelfFenceAndUnbind(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1b"

	var mu sync.Mutex
	var events []string
	var allowRenew atomic.Bool
	allowRenew.Store(true)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/"+layerID:
			_ = json.NewEncoder(w).Encode(client.LayerInfo{
				LayerID:          layerID,
				BaseRootPath:     "/repo",
				State:            "active",
				Fence:            3,
				ExclusiveMountID: "dead-mnt",
				MountExpired:     true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+layerID+"/unbind-mount":
			var req client.LayerMountUnbindRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			events = append(events, "unbind:"+req.Kind)
			mu.Unlock()
			if req.Kind == "expire" {
				_ = json.NewEncoder(w).Encode(client.LayerMountUnbindResult{LayerID: layerID, Fence: 4, Unbound: true})
				return
			}
			_ = json.NewEncoder(w).Encode(client.LayerMountUnbindResult{LayerID: layerID, Fence: 6, Unbound: true})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+layerID+"/bind-mount":
			var req client.LayerMountBindRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.ExpectedFence != 4 {
				t.Errorf("BindLayerMount ExpectedFence = %d, want 4", req.ExpectedFence)
			}
			mu.Lock()
			events = append(events, "bind")
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(client.LayerMountLease{
				LayerID:         layerID,
				MountID:         req.MountID,
				OperationID:     req.OperationID,
				Fence:           5,
				MountLeaseTTLMS: 180,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+layerID+"/renew-mount":
			mu.Lock()
			events = append(events, "renew")
			mu.Unlock()
			if !allowRenew.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"error":"unavailable"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(client.LayerMountLease{
				LayerID:         layerID,
				MountID:         "mnt-live",
				Fence:           5,
				MountLeaseTTLMS: 180,
			})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/repo/file.txt":
			if got := r.Header.Get("X-FS-Layer-Fence"); got != "5" {
				t.Errorf("X-FS-Layer-Fence = %q, want 5", got)
			}
			_, _ = w.Write([]byte(`{"status":"ok","revision":1}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/repo/file.txt":
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	timing := layerLeaseTiming{
		renewInterval:   40 * time.Millisecond,
		selfFenceMargin: 60 * time.Millisecond,
		retryInterval:   25 * time.Millisecond,
		callTimeout:     100 * time.Millisecond,
	}
	base := client.New(ts.URL, "sk-test")
	b, err := resolveLayerMountView(context.Background(), base, layerID, "/repo/sub", false, "mnt-live", false, timing)
	if err != nil {
		t.Fatalf("resolveLayerMountView writable: %v", err)
	}
	var callbackFired atomic.Bool
	b.OnSelfFence(func() { callbackFired.Store(true) })

	// Write succeeds while lease is healthy.
	if err := b.ViewClient.WriteCtx(context.Background(), "/repo/file.txt", []byte("ok")); err != nil {
		t.Fatalf("WriteCtx before self-fence: %v", err)
	}
	time.Sleep(65 * time.Millisecond) // allow at least one renew

	// Block renewals and let monotonic clock cross into selfFenceMargin.
	allowRenew.Store(false)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) && !callbackFired.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if !callbackFired.Load() {
		t.Fatal("expected monotonic pre-expiry self-fence callback to fire")
	}

	// Mutating Set C requests are now blocked locally, while Set B reads still succeed!
	if err := b.ViewClient.WriteCtx(context.Background(), "/repo/file.txt", []byte("blocked")); !errors.Is(err, ErrLayerLeaseSelfFenced) {
		t.Fatalf("WriteCtx after self-fence err = %v, want ErrLayerLeaseSelfFenced", err)
	}
	if _, err := b.ViewClient.StatCtx(context.Background(), "/repo/file.txt"); err != nil {
		t.Fatalf("StatCtx after self-fence should still succeed: %v", err)
	}

	b.Unbind(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 4 || events[0] != "unbind:expire" || events[1] != "bind" || events[len(events)-1] != "unbind:fence" {
		t.Fatalf("unexpected lease event sequence: %v", events)
	}
}

func TestResolveLayerMountViewRejectsActiveOddFenceAndInvalidPostExpireFence(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1c"
	var bindCalls atomic.Int32
	var expiredMode atomic.Bool

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/layers/"+layerID:
			_ = json.NewEncoder(w).Encode(client.LayerInfo{
				LayerID:          layerID,
				BaseRootPath:     "/repo",
				State:            "active",
				Fence:            3,
				ExclusiveMountID: "other-mnt",
				MountExpired:     expiredMode.Load(),
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+layerID+"/unbind-mount":
			// Simulate broken/racing expire response returning an odd fence instead of non-negative even.
			_ = json.NewEncoder(w).Encode(client.LayerMountUnbindResult{LayerID: layerID, Fence: 3, Unbound: false})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/layers/"+layerID+"/bind-mount":
			bindCalls.Add(1)
			w.WriteHeader(http.StatusConflict)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	base := client.New(ts.URL, "sk-test")

	// 1. Active odd fence (!MountExpired) must fail immediately without calling bind-mount or resetting ExpectedFence=0.
	_, err := resolveLayerMountView(context.Background(), base, layerID, "/repo", true, "mnt-new", false, defaultLayerLeaseTiming())
	if err == nil {
		t.Fatal("expected conflict error when layer already has active odd fence")
	}
	if !errors.Is(err, client.ErrConflict) {
		t.Fatalf("expected ErrConflict cause, got %v", err)
	}
	if bindCalls.Load() != 0 {
		t.Fatalf("bind-mount should not be called on active odd fence, got %d calls", bindCalls.Load())
	}

	// 2. Post-expire unbind returning non-even fence must fail closed without resetting ExpectedFence=0.
	expiredMode.Store(true)
	_, err = resolveLayerMountView(context.Background(), base, layerID, "/repo", true, "mnt-new", false, defaultLayerLeaseTiming())
	if err == nil {
		t.Fatal("expected error when expire unbind returns odd fence")
	}
	if bindCalls.Load() != 0 {
		t.Fatalf("bind-mount should not be called when post-expire fence is odd, got %d calls", bindCalls.Load())
	}
}

func TestLayerRenewRetriesBeforeSafetyMargin(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1b"
	retried := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		if calls.Load() == 2 {
			close(retried)
		}
		_ = json.NewEncoder(w).Encode(client.LayerMountLease{LayerID: layerID, Fence: 1, MountLeaseTTLMS: 1000})
	}))
	defer server.Close()
	b := &layerMountBinding{LayerID: layerID, Fence: 1, MountID: "test-mount", BaseClient: client.New(server.URL, "")}
	b.startRenewLoop(time.Now(), 1000, layerLeaseTiming{
		renewInterval: 500 * time.Millisecond, retryInterval: 10 * time.Millisecond,
		selfFenceMargin: 200 * time.Millisecond, callTimeout: 100 * time.Millisecond,
	})
	defer b.StopRenew()
	select {
	case <-retried:
	case <-b.renewDone:
		t.Fatal("mount self-fenced after a single temporary error without retrying")
	case <-time.After(3 * time.Second):
		t.Fatal("renewal did not retry")
	}
}

func TestLayerStalledRenewStopsAtSafetyMargin(t *testing.T) {
	const layerID = "layer_01j8z6kh2m9q7w3r5t6y8v0p1b"
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	b := &layerMountBinding{LayerID: layerID, Fence: 1, MountID: "test-mount", BaseClient: client.New(server.URL, "")}
	b.startRenewLoop(time.Now(), 500, layerLeaseTiming{
		renewInterval: 50 * time.Millisecond, retryInterval: 10 * time.Millisecond,
		selfFenceMargin: 200 * time.Millisecond, callTimeout: 5 * time.Second,
	})
	defer b.StopRenew()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("renewal did not start")
	}
	select {
	case <-b.renewDone:
		if !errors.Is(b.checkWriteAllowed(), ErrLayerLeaseSelfFenced) {
			t.Fatal("old lease still permits writes after the safety margin")
		}
	case <-time.After(time.Second):
		t.Fatal("a stalled renewal delayed self-fencing beyond lease expiration")
	}
}

func TestRenewReturnAtOldCutoffUsesConfirmedLease(t *testing.T) {
	for _, tt := range []struct {
		name   string
		ttl    int64
		fenced bool
	}{
		{"extended", 60000, false},
		{"already inside new margin", 1000, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				b := &layerMountBinding{renewDone: make(chan struct{})}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				go b.runRenewLoop(ctx, time.Now().Add(5*time.Second), layerLeaseTiming{
					renewInterval: time.Second, selfFenceMargin: time.Second, callTimeout: 5 * time.Second,
				}, func(call context.Context, _ string, _ client.LayerMountRenewRequest) (*client.LayerMountLease, error) {
					calls++
					// The response was accepted, then the caller resumed at the old cutoff.
					<-call.Done()
					return &client.LayerMountLease{MountLeaseTTLMS: tt.ttl}, nil
				})
				time.Sleep(4 * time.Second)
				synctest.Wait()
				if calls != 1 || b.fenced.Load() != tt.fenced {
					t.Fatalf("renewals=%d fenced=%v, want 1 and %v", calls, b.fenced.Load(), tt.fenced)
				}
				cancel()
				<-b.renewDone
			})
		})
	}
}
