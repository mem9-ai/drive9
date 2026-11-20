//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/appendlog"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func mountViewFixture(t *testing.T) (MountConfig, *client.Client, *atomic.Bool, *atomic.Int64) {
	t.Helper()
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")
	main, layer := testbackend.NewBackend(), testbackend.NewBackend()
	main.Seed("/a", []byte("main"))
	layer.Seed("/a", []byte("private"))
	denied, reads := &atomic.Bool{}, &atomic.Int64{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/status" {
			_ = json.NewEncoder(w).Encode(map[string]string{"tenant_id": "tenant-a"})
			return
		}
		reads.Add(1)
		if prefix := viewprotocol.RoutePrefix + "a"; strings.HasPrefix(r.URL.Path, prefix+"/") {
			binding, err := viewprotocol.Parse(r.Header)
			if denied.Load() || err != nil || binding != (viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1}) {
				http.Error(w, "qualification expired", http.StatusForbidden)
				return
			}
			clone := r.Clone(r.Context())
			clone.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
			layer.ServeHTTP(w, clone)
			return
		}
		main.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	cfg := MountConfig{Server: server.URL, APIKey: "test-key", MountPoint: t.TempDir(), CacheDir: t.TempDir(), Extensions: []string{"core"}, WritePolicy: "write-sync", SyncMode: "strict"}
	return cfg, client.New(server.URL, cfg.APIKey), denied, reads
}

func TestMountViewStartupSwitchAndReopen(t *testing.T) {
	cfg, cl, denied, _ := mountViewFixture(t)
	v, err := prepareMountView(t.Context(), cfg, cl, "actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.close(context.Background()) })
	initial := v.currentRuntime()
	snapshot, err := v.switcher.journal.Snapshot()
	if err != nil || snapshot.Binding.LayerID != "" || snapshot.Generation != 1 {
		t.Fatalf("initial: %+v %v", snapshot, err)
	}
	request := mountcontrol.SwitchRequest{MountID: snapshot.MountID, OperationID: "switch-a", ExpectedGeneration: 1, Target: viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1}}
	if got := v.switcher.apply(t.Context(), request); got.Receipt != nil || got.ErrorKind != "TARGET_UNAVAILABLE" {
		t.Fatalf("switch before serving: %+v", got)
	}
	// Exercise real HTTP qualification, runtime construction, journal and
	// dispatch. Only kernel notifications are substituted: no mount is made.
	v.server.Store(&gofuse.Server{})
	v.ready.Store(true)
	v.switcher.notifier = &viewCacheProbe{}
	v.switcher.fence = func(context.Context) error { return nil }
	denied.Store(true)
	if got := v.switcher.apply(t.Context(), request); got.Receipt != nil || got.ErrorKind != "TARGET_UNAVAILABLE" {
		t.Fatalf("expired qualification accepted: %+v", got)
	}
	denied.Store(false)
	if got := v.switcher.apply(t.Context(), request); got.Error != "" || got.Receipt.State != mountstate.SwitchDone {
		t.Fatalf("switch: %+v", got)
	}
	current := v.currentRuntime()
	if current.fs == initial.fs || current.fs.perf != initial.fs.perf || current.binding != request.Target || current.fs.server != v.server.Load() {
		t.Fatal("target runtime lost mount connection, shared counters or binding")
	}
	var out gofuse.EntryOut
	if status := v.switcher.adapter.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "a", &out); status != gofuse.OK || out.Size != 7 {
		t.Fatalf("dispatch did not read selected view: %v %+v", status, out)
	}
	v.switcher.adapter.Forget(out.NodeId, 1)
	if err := v.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	cfg.MountID, cfg.Supervised = snapshot.MountID, true
	v, err = prepareMountView(t.Context(), cfg, cl, "new-worker")
	if err != nil {
		t.Fatal(err)
	}
	if got := v.switcher.adapter.currentTarget(); got.generation != 2 || v.currentRuntime().binding != request.Target {
		t.Fatalf("worker reset selected view: %+v", got)
	}
	if v.ready.Load() || v.server.Load() != nil {
		t.Fatal("reopened journal implied live kernel connection")
	}
	if err := v.drainShutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := v.switcher.adapter.admission.enter(); !errors.Is(err, errViewAdmissionPaused) {
		t.Fatalf("shutdown resumed admission: %v", err)
	}
}

func TestMountViewPendingAndMissingIdentityNeverBuildMain(t *testing.T) {
	cfg, cl, _, reads := mountViewFixture(t)
	cfg.Supervised = true
	if v, err := prepareMountView(t.Context(), cfg, cl, "actor"); v != nil || err == nil || reads.Load() != 0 {
		t.Fatalf("missing supervised identity: %v %v", v, err)
	}
	cfg.Supervised = false
	j, err := createMountViewJournal(t.Context(), cfg, cl)
	if err != nil {
		t.Fatal(err)
	}
	s, err := j.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	decision, err := j.Decide("original-op", 1, viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.MountID = s.MountID
	v, err := prepareMountView(t.Context(), cfg, cl, "actor")
	if v != nil || !errors.Is(err, mountstate.ErrSwitchPending) || reads.Load() != 0 || !strings.Contains(err.Error(), "original-op") || !strings.Contains(err.Error(), `layer="a"`) {
		t.Fatalf("pending startup did not report original target: %v %v", v, err)
	}
	j, err = openMountViewJournal(t.Context(), cfg, cl)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = j.Close() }()
	s, err = j.Snapshot()
	if err != nil || s.Pending == nil || *s.Pending != decision {
		t.Fatalf("startup changed pending decision: %+v %v", s, err)
	}
}

func TestMountViewCloseRetainsOwnedRuntimeAfterFailure(t *testing.T) {
	s, _, req, _ := viewSwitchFixture(t)
	target := newXattrFS(t)
	stopFailure := errors.New("target background still running")
	failed := true
	target.chain, _ = fs.NewChain(append([]fs.Handler{&viewSwitchLifecycle{stop: func(context.Context) error {
		if failed {
			return stopFailure
		}
		return nil
	}}}, target.chain.Handlers()...)...)
	s.pending = &mountViewPending{target: &mountRuntime{fs: target}}
	v := &mountView{switcher: s}
	v.ready.Store(true)
	if err := v.close(t.Context()); !errors.Is(err, stopFailure) || v.ready.Load() {
		t.Fatalf("failed close: %v", err)
	}
	if _, _, err := s.journal.Lookup(req.OperationID); err != nil {
		t.Fatalf("journal released before cleanup: %v", err)
	}
	failed = false
	if err := v.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.journal.Snapshot(); err == nil {
		t.Fatal("successful close retained journal")
	}
}

func TestFixedMountRuntimeUsesNormalContentRoutes(t *testing.T) {
	cfg, cl, _, _ := mountViewFixture(t)
	cfg.Extensions = []string{"core", "extent", "appendlog"}
	cfg.AppendLogPatterns, cfg.ExtentPaths = []string{"**"}, []string{"**"}
	bound, err := cl.WithView(viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1})
	if err != nil {
		t.Fatal(err)
	}
	r, err := buildMountRuntime(cfg, bound, "/", "actor")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.stop(context.Background()) }()
	if r.fs.localDirAttrs {
		t.Fatal("fixed directories bypassed server qualification")
	}
	for _, handler := range r.fs.chain.Handlers() {
		switch handler.(type) {
		case *extent.Handler, *appendlog.AppendLog:
			t.Fatalf("fixed view installed a legacy content plane: %T", handler)
		}
	}
}

func TestPrepareMountRuntimeOrViewNonLayerFallbackAndFailClosedBoundaries(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")

	for _, tc := range []struct {
		name       string
		statusCode int
		statusBody string
		apiKey     string
		token      string
		supervised bool
		mountID    string
		wantView   bool
		wantErr    bool
	}{
		{name: "owner_200_foreground", statusCode: http.StatusOK, statusBody: `{"tenant_id":"tenant-a"}`, apiKey: "owner-key", wantView: true},
		{name: "fs_scoped_403_foreground", statusCode: http.StatusForbidden, statusBody: `{"error":"status is owner-only"}`, apiKey: "scoped-key"},
		{name: "fs_scoped_403_supervised_no_id", statusCode: http.StatusForbidden, statusBody: `{"error":"status is owner-only"}`, apiKey: "scoped-key", supervised: true},
		{name: "capability_token_401_foreground", statusCode: http.StatusUnauthorized, statusBody: `{"error":"unauthorized"}`, token: "cap-jwt"},
		{name: "capability_token_401_supervised_no_id", statusCode: http.StatusUnauthorized, statusBody: `{"error":"unauthorized"}`, token: "cap-jwt", supervised: true},
		{name: "standalone_404_foreground", statusCode: http.StatusNotFound, statusBody: "not found", apiKey: "owner-key"},
		{name: "standalone_501_supervised", statusCode: http.StatusNotImplemented, statusBody: "not implemented", apiKey: "owner-key", supervised: true},
		{name: "missing_tenant_foreground", statusCode: http.StatusOK, statusBody: `{"status":"active"}`, apiKey: "owner-key"},
		{name: "owner_supervised_missing_id_fails_closed", statusCode: http.StatusOK, statusBody: `{"tenant_id":"tenant-a"}`, apiKey: "owner-key", supervised: true, wantErr: true},
		{name: "explicit_mount_id_with_403_fails_closed", statusCode: http.StatusForbidden, statusBody: `{"error":"status is owner-only"}`, apiKey: "scoped-key", mountID: "0123456789abcdef0123456789abcdef", wantErr: true},
		{name: "invalid_api_key_401_fails_closed", statusCode: http.StatusUnauthorized, statusBody: `{"error":"invalid key"}`, apiKey: "bad-key", wantErr: true},
		{name: "transient_503_fails_closed", statusCode: http.StatusServiceUnavailable, statusBody: `{"error":"unavailable"}`, apiKey: "owner-key", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := testbackend.NewBackend()
			backend.Seed("/hello.txt", []byte("world"))
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte(tc.statusBody))
					return
				}
				backend.ServeHTTP(w, r)
			}))
			defer srv.Close()

			cfg := MountConfig{
				MountID:     tc.mountID,
				Server:      srv.URL,
				APIKey:      tc.apiKey,
				Token:       tc.token,
				MountPoint:  t.TempDir(),
				RemoteRoot:  "/",
				CacheDir:    t.TempDir(),
				Supervised:  tc.supervised,
				Extensions:  []string{"core"},
				WritePolicy: "write-sync",
				SyncMode:    "strict",
			}
			cl, err := mountClient(cfg)
			if err != nil {
				t.Fatal(err)
			}
			view, rt, err := prepareMountRuntimeOrView(t.Context(), cfg, cl, "/", "actor")
			if tc.wantErr {
				if err == nil || view != nil || rt != nil {
					t.Fatalf("expected error, got view=%v rt=%v err=%v", view, rt, err)
				}
				return
			}
			if err != nil || rt == nil {
				t.Fatalf("unexpected failure: view=%v rt=%v err=%v", view, rt, err)
			}
			if tc.wantView {
				if view == nil {
					t.Fatal("expected view-enabled runtime for owner key")
				}
				_ = view.close(context.Background())
			} else {
				if view != nil {
					t.Fatal("expected classic unswitched main runtime fallback")
				}
				_ = rt.stop(context.Background())
			}
		})
	}
}

func TestControlServerErrorPolicyPreservesMainResilience(t *testing.T) {
	cfg, cl, _, _ := mountViewFixture(t)
	v, err := prepareMountView(t.Context(), cfg, cl, "actor")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.close(context.Background()) }()

	sockErr := errors.New("listen unix: operation not permitted")
	if shouldFailClosedOnControlServerError(nil, sockErr) {
		t.Fatal("classic main mount must not fail closed on control socket error")
	}
	if shouldFailClosedOnControlServerError(v, sockErr) {
		t.Fatal("initial main mount view must not fail closed on control socket error")
	}

	v.server.Store(&gofuse.Server{})
	v.ready.Store(true)
	v.switcher.notifier = &viewCacheProbe{}
	v.switcher.fence = func(context.Context) error { return nil }
	req := mountcontrol.SwitchRequest{
		MountID:            v.currentRuntime().generation.MountID,
		OperationID:        "switch-layer-a",
		ExpectedGeneration: 1,
		Target:             viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1},
	}
	if resp := v.switcher.apply(t.Context(), req); resp.Error != "" {
		t.Fatalf("switch to layer: %+v", resp)
	}
	if !shouldFailClosedOnControlServerError(v, sockErr) {
		t.Fatal("fixed layer mount view must fail closed when control socket fails")
	}
}

func TestMountViewExtentEngineReusedAcrossRepeatedRoundTripSwitches(t *testing.T) {
	cfg, cl, _, _ := mountViewFixture(t)
	cfg.Extensions = []string{"core", "extent"}
	cfg.ExtentCacheSize = 32 << 20
	cfg.DiskReadCacheFreeRatio = -1

	v, err := prepareMountView(t.Context(), cfg, cl, "actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.close(context.Background()) })
	if v.sharedExtent == nil {
		t.Fatal("initial main generation did not initialize shared extent engine")
	}
	firstEngine := v.sharedExtent

	v.server.Store(&gofuse.Server{})
	v.ready.Store(true)
	v.switcher.notifier = &viewCacheProbe{}
	v.switcher.fence = func(context.Context) error { return nil }

	snap, err := v.switcher.journal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Warm up one main -> layer -> main cycle, then measure goroutine stability
	// across repeated cycles with ExtentCacheSize > 0.
	layerBinding := viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1}
	gen := uint64(1)
	stepSwitch := func(op string, target viewprotocol.Binding) {
		t.Helper()
		resp := v.switcher.apply(t.Context(), mountcontrol.SwitchRequest{
			MountID:            snap.MountID,
			OperationID:        op,
			ExpectedGeneration: gen,
			Target:             target,
		})
		if resp.Error != "" || resp.Receipt == nil || resp.Receipt.State != mountstate.SwitchDone {
			t.Fatalf("switch %s failed: %+v", op, resp)
		}
		gen = resp.Receipt.Generation
	}

	stepSwitch("warm-to-layer", layerBinding)
	stepSwitch("warm-to-main", viewprotocol.Binding{})
	time.Sleep(50 * time.Millisecond)
	baselineGoroutines := runtime.NumGoroutine()

	const rounds = 5
	for i := 0; i < rounds; i++ {
		stepSwitch("to-layer-"+string(rune('0'+i)), layerBinding)
		stepSwitch("to-main-"+string(rune('0'+i)), viewprotocol.Binding{})
	}
	if v.sharedExtent != firstEngine {
		t.Fatal("returning main generation allocated a second extent.Engine")
	}

	time.Sleep(50 * time.Millisecond)
	afterGoroutines := runtime.NumGoroutine()
	if delta := afterGoroutines - baselineGoroutines; delta > 4 {
		t.Fatalf("goroutine leak across %d main<->layer switches with ExtentCacheSize>0: before=%d after=%d (delta=%d)",
			rounds, baselineGoroutines, afterGoroutines, delta)
	}

	// Only one <baseMountHash>/extent directory may exist under CacheDir.
	var extentDirs []string
	err = filepath.WalkDir(cfg.CacheDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() && d.Name() == "extent" {
			extentDirs = append(extentDirs, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(extentDirs) != 1 {
		t.Fatalf("expected exactly 1 extent cache dir across switches, got %d: %v", len(extentDirs), extentDirs)
	}
}
