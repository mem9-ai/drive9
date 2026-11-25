//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func layerStartupTestConfig(t *testing.T, server, layer, root string) MountConfig {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("XDG_RUNTIME_DIR", dir)
	t.Setenv("HOME", dir)
	return MountConfig{
		Server:     server,
		APIKey:     "sk-test",
		MountPoint: dir,
		CacheDir:   filepath.Join(dir, "cache"),
		LayerRef:   layer,
		RemoteRoot: root,
		Extensions: []string{"core"},
		SyncMode:   "strict",
	}
}

func TestPrepareLayerMountViewBaseRootAndReadOnly(t *testing.T) {
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
		case r.Method == http.MethodHead && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/workspace":
			w.Header().Set("X-Dat9-IsDir", "true")
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
	cfg := layerStartupTestConfig(t, ts.URL, "task=auth", "/")
	cfg.ReadOnly, cfg.RemoteRootDefaulted = true, true
	mv, err := prepareMountView(t.Context(), cfg, base, "mnt-1")
	if err != nil {
		t.Fatalf("prepareMountView defaulted: %v", err)
	}
	defer func() {
		if err := mv.close(context.Background()); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	b := mv.switcher.current.layerLease
	if b.LayerID != layerID || b.RemoteRoot != "/workspace" || b.Fence != 0 || bindCalled.Load() {
		t.Fatalf("unexpected binding: %+v (bindCalled=%v)", b, bindCalled.Load())
	}
	if _, err := b.ViewClient.StatCtx(context.Background(), "/workspace/a.txt"); err != nil {
		t.Fatalf("StatCtx through read-only view: %v", err)
	}

	// 2. Explicit remoteRoot outside layer.BaseRootPath fails before mounting.
	cfg.RemoteRoot, cfg.RemoteRootDefaulted = "/other", false
	if _, err := prepareMountView(t.Context(), cfg, base, "mnt-2"); err == nil {
		t.Fatal("expected error for remoteRoot outside layer base_root_path")
	}

	// 3. Explicit remoteRoot "/" when base_root_path is "/workspace" also fails!
	cfg.RemoteRoot = "/"
	if _, err := prepareMountView(t.Context(), cfg, base, "mnt-3"); err == nil {
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
				MountLeaseTTLMS: 60_000,
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
		case r.Method == http.MethodHead && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/repo/sub":
			w.Header().Set("X-Dat9-IsDir", "true")
		case r.Method == http.MethodPut && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/repo/file.txt":
			if got := r.Header.Get("X-FS-Layer-Fence"); got != "5" {
				t.Errorf("X-FS-Layer-Fence = %q, want 5", got)
			}
			_, _ = w.Write([]byte(`{"status":"ok","revision":1}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/views/"+layerID+"/v1/fs/repo/file.txt":
			if !allowRenew.Load() && r.Header.Get("X-FS-Layer-Fence") != "" {
				w.WriteHeader(http.StatusConflict)
				return
			}
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
	cfg := layerStartupTestConfig(t, ts.URL, layerID, "/repo/sub")
	mv, err := prepareMountView(t.Context(), cfg, base, "mnt-live")
	if err != nil {
		t.Fatalf("prepareMountView writable: %v", err)
	}
	defer func() {
		if mv != nil {
			_ = mv.close(context.Background())
		}
	}()
	b := mv.switcher.current.layerLease
	// Exercise the bound lease with short renewal and fencing intervals.
	b.StopRenew()
	b.startRenewLoop(time.Now(), 180, timing)
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

	if err := mv.close(t.Context()); err != nil {
		t.Fatalf("close: %v", err)
	}
	mv = nil

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 4 || events[0] != "unbind:expire" || events[1] != "bind" || events[len(events)-1] != "unbind:fence" {
		t.Fatalf("unexpected lease event sequence: %v", events)
	}
}

func TestPrepareLayerMountViewRejectsActiveOddFenceAndInvalidPostExpireFence(t *testing.T) {
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
	cfg := layerStartupTestConfig(t, ts.URL, layerID, "/repo")
	_, err := prepareMountView(t.Context(), cfg, base, "mnt-new")
	if err == nil {
		t.Fatal("expected conflict error when layer already has active odd fence")
	}
	assertMountExit(t, err, ExitLayerLeaseBusy, ExitReasonLayerLeaseBusy)
	if !errors.Is(err, client.ErrConflict) {
		t.Fatalf("expected ErrConflict cause, got %v", err)
	}
	if bindCalls.Load() != 0 {
		t.Fatalf("bind-mount should not be called on active odd fence, got %d calls", bindCalls.Load())
	}

	// 2. Post-expire unbind returning non-even fence must fail closed without resetting ExpectedFence=0.
	expiredMode.Store(true)
	_, err = prepareMountView(t.Context(), cfg, base, "mnt-new")
	if err == nil {
		t.Fatal("expected error when expire unbind returns odd fence")
	}
	if bindCalls.Load() != 0 {
		t.Fatalf("bind-mount should not be called when post-expire fence is odd, got %d calls", bindCalls.Load())
	}
}

func TestPrepareMountViewClassifiesLeaseRequestConflict(t *testing.T) {
	const layer = "layer_01j8z6kh2m9q7w3r5t6y8v0p1c"
	for _, op := range []string{"expire", "bind"} {
		t.Run(op, func(t *testing.T) {
			var conflicted atomic.Bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/v1/layers/" + layer:
					info := client.LayerInfo{LayerID: layer, BaseRootPath: "/", State: "active"}
					if op == "expire" {
						info.Fence = 3
						info.MountExpired = true
					}
					_ = json.NewEncoder(w).Encode(info)
					return
				case "/v1/layers/" + layer + "/unbind-mount":
					var req client.LayerMountUnbindRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					if req.Kind == "op" && op == "bind" {
						_ = json.NewEncoder(w).Encode(client.LayerMountUnbindResult{LayerID: layer})
						return
					}
					if req.Kind != "expire" || op != "expire" {
						t.Errorf("unexpected unbind kind %q", req.Kind)
					}
				case "/v1/layers/" + layer + "/bind-mount":
					if op != "bind" {
						t.Error("bind after expire conflict")
					}
				default:
					t.Errorf("unexpected request: %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				conflicted.Store(true)
				http.Error(w, "busy", http.StatusConflict)
			}))
			defer ts.Close()
			cfg := layerStartupTestConfig(t, ts.URL, layer, "/")
			_, err := prepareMountView(t.Context(), cfg, client.New(ts.URL, cfg.APIKey), "new-worker")
			if !conflicted.Load() {
				t.Fatal("lease request did not return conflict")
			}
			assertMountExit(t, err, ExitLayerLeaseBusy, ExitReasonLayerLeaseBusy)
		})
	}
}
