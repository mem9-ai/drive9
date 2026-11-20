//go:build !windows

package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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

func TestMountViewRealExtentReadWriteIsolationAcrossMainLayerMainSwitch(t *testing.T) {
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("XDG_RUNTIME_DIR", "")

	mainBackend, layerBackend := testbackend.NewBackend(), testbackend.NewBackend()
	layerBackend.Seed("/a", []byte("layer-private"))

	var mu sync.Mutex
	type extObj struct {
		data string
		rev  int64
		id   string
	}
	blocks := map[string][]byte{}
	extentFiles := map[string]extObj{}
	var srvURL string
	seedExtent := func(path, content, id string, rev int64) {
		sum := sha256.Sum256([]byte(content))
		hexSum := hex.EncodeToString(sum[:])
		mu.Lock()
		blocks[hexSum] = []byte(content)
		extentFiles[path] = extObj{data: content, rev: rev, id: id}
		mu.Unlock()
	}
	seedExtent("/ext.bin", "GEN1-MAIN-EXTENT", "res-ext-1", 1)

	var uploadTarget string
	var uploadSize int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenant_id": "tenant-a",
				"status":    "active",
				"storage_capabilities": map[string]any{
					"completion_identity_v1": true,
				},
			})
			return
		case strings.HasPrefix(r.URL.Path, "/obj/"):
			key := strings.TrimPrefix(r.URL.Path, "/obj/")
			if r.Method == http.MethodPut {
				body, _ := io.ReadAll(r.Body)
				sum := sha256.Sum256(body)
				hexSum := hex.EncodeToString(sum[:])
				mu.Lock()
				blocks[key] = body
				blocks[hexSum] = body
				mu.Unlock()
				w.Header().Set("ETag", hexSum)
				w.WriteHeader(http.StatusOK)
				return
			}
			mu.Lock()
			data, ok := blocks[key]
			mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			if rng := r.Header.Get("Range"); rng != "" {
				var start, end int64
				if _, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end); err == nil {
					if end >= int64(len(data)) {
						end = int64(len(data)) - 1
					}
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(data[start : end+1])
					return
				}
			}
			_, _ = w.Write(data)
			return
		case r.URL.Path == "/v2/uploads/initiate":
			var req struct {
				Path      string `json:"path"`
				TotalSize int64  `json:"total_size"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			uploadTarget = req.Path
			uploadSize = req.TotalSize
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id":         "up-1",
				"key":               "",
				"part_size":         int64(client.ExtentBlockSize),
				"total_parts":       1,
				"checksum_contract": map[string]any{"supported": []string{"SHA-256"}, "required": true},
			})
			return
		case r.URL.Path == "/v2/uploads/up-1/presign-batch":
			var req struct {
				Parts []struct {
					Number   int `json:"part_number"`
					Checksum struct {
						Value string `json:"value"`
					} `json:"checksum"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			sz := uploadSize
			mu.Unlock()
			out := make([]map[string]any, 0, len(req.Parts))
			for _, p := range req.Parts {
				out = append(out, map[string]any{
					"number": p.Number,
					"url":    fmt.Sprintf("%s/obj/%s", srvURL, p.Checksum.Value),
					"size":   sz,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": out})
			return
		case r.URL.Path == "/v2/uploads/up-1/complete":
			var req struct {
				Parts []struct {
					ETag string `json:"etag"`
				} `json:"parts"`
			}
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			var combined []byte
			for _, p := range req.Parts {
				combined = append(combined, blocks[strings.Trim(p.ETag, `"`)]...)
			}
			id := fmt.Sprintf("res-up-%d", len(extentFiles)+1)
			extentFiles[uploadTarget] = extObj{data: string(combined), rev: 2, id: id}
			mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "completed", "revision": 2, "resource_id": id})
			return
		}
		if prefix := viewprotocol.RoutePrefix + "a"; strings.HasPrefix(r.URL.Path, prefix+"/") {
			clone := r.Clone(r.Context())
			clone.URL.Path = strings.TrimPrefix(r.URL.Path, prefix)
			layerBackend.ServeHTTP(w, clone)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1/fs/") {
			p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
			mu.Lock()
			eo, ok := extentFiles[p]
			mu.Unlock()
			if ok {
				if r.Method == http.MethodHead {
					w.Header().Set("X-Dat9-Content-Layout", "extent")
					w.Header().Set("X-Dat9-Revision", fmt.Sprint(eo.rev))
					w.Header().Set("X-Dat9-Resource-ID", eo.id)
					w.Header().Set("X-Dat9-Mode", "420")
					w.Header().Set("Content-Length", fmt.Sprint(len(eo.data)))
					w.WriteHeader(http.StatusOK)
					return
				}
				if r.Method == http.MethodGet {
					sum := sha256.Sum256([]byte(eo.data))
					hexSum := hex.EncodeToString(sum[:])
					w.Header().Set("X-Dat9-Content-Layout", "extent")
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusPartialContent)
					_ = json.NewEncoder(w).Encode(client.ExtentReadPlan{
						Size:     int64(len(eo.data)),
						Revision: eo.rev,
						Blocks: []client.ExtentReadPlanBlock{{
							FileOffset: 0,
							Length:     int64(len(eo.data)),
							BlockKey:   hexSum,
							URL:        srvURL + "/obj/" + hexSum,
						}},
					})
					return
				}
			}
		}
		mainBackend.ServeHTTP(w, r)
	}))
	defer server.Close()
	srvURL = server.URL

	cfg := MountConfig{
		Server:      server.URL,
		APIKey:      "owner-key",
		MountPoint:  t.TempDir(),
		CacheDir:    t.TempDir(),
		Extensions:  []string{"core", "extent"},
		ExtentPaths: []string{"*.ext"},
		WritePolicy: "write-sync",
		SyncMode:    "strict",
	}
	cl := client.New(server.URL, cfg.APIKey)
	cl.Warm(t.Context())

	v, err := prepareMountView(t.Context(), cfg, cl, "actor")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.close(context.Background()) })
	v.currentRuntime().fs.Init2(t.Context())
	if err := v.currentRuntime().fs.StartErr(); err != nil {
		t.Fatal(err)
	}
	v.server.Store(&gofuse.Server{})
	v.ready.Store(true)
	v.switcher.notifier = &viewCacheProbe{}
	v.switcher.fence = func(context.Context) error { return nil }

	firstEngine := v.sharedExtent
	if firstEngine == nil {
		t.Fatal("expected shared extent engine on initial main")
	}

	readAdapterFile := func(name string, wantLen int) string {
		t.Helper()
		var entry gofuse.EntryOut
		if st := v.switcher.adapter.Lookup(nil, &gofuse.InHeader{NodeId: 1}, name, &entry); st != gofuse.OK {
			t.Fatalf("lookup %s: %v", name, st)
		}
		var opened gofuse.OpenOut
		if st := v.switcher.adapter.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Flags: syscall.O_RDONLY}, &opened); st != gofuse.OK {
			t.Fatalf("open %s: %v", name, st)
		}
		res, st := v.switcher.adapter.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: opened.Fh, Size: uint32(wantLen)}, make([]byte, wantLen))
		if st != gofuse.OK {
			t.Fatalf("read %s: %v", name, st)
		}
		data, status := res.Bytes(make([]byte, wantLen))
		res.Done()
		if status != gofuse.OK {
			t.Fatalf("read bytes %s: %v", name, status)
		}
		v.switcher.adapter.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: opened.Fh})
		v.switcher.adapter.Forget(entry.NodeId, 1)
		return string(data)
	}

	// Gen 1 (main): read extent file through the FUSE adapter.
	if got := readAdapterFile("ext.bin", len("GEN1-MAIN-EXTENT")); got != "GEN1-MAIN-EXTENT" {
		t.Fatalf("gen1 read = %q", got)
	}

	// Switch to Gen 2 (Layer), verify Layer content, and mutate main's extent file remotely.
	snap, err := v.switcher.journal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	layerTarget := viewprotocol.Binding{LayerID: "a", WorkID: "work-a", Fence: 1}
	if resp := v.switcher.apply(t.Context(), mountcontrol.SwitchRequest{MountID: snap.MountID, OperationID: "to-layer", ExpectedGeneration: 1, Target: layerTarget}); resp.Error != "" {
		t.Fatalf("switch to layer: %+v", resp)
	}
	if got := readAdapterFile("a", len("layer-private")); got != "layer-private" {
		t.Fatalf("gen2 layer read = %q", got)
	}
	seedExtent("/ext.bin", "GEN3-MAIN-EXTENT-UPDATED", "res-ext-3", 5)

	// Switch back to Gen 3 (main): reused extent engine must observe updated main content
	// without stale Gen 1 size/revision state, and new extent writes must land on main.
	if resp := v.switcher.apply(t.Context(), mountcontrol.SwitchRequest{MountID: snap.MountID, OperationID: "back-to-main", ExpectedGeneration: 2, Target: viewprotocol.Binding{}}); resp.Error != "" {
		t.Fatalf("switch back to main: %+v", resp)
	}
	if v.sharedExtent != firstEngine {
		t.Fatal("shared extent engine was not reused on returning main")
	}
	if got := readAdapterFile("ext.bin", len("GEN3-MAIN-EXTENT-UPDATED")); got != "GEN3-MAIN-EXTENT-UPDATED" {
		t.Fatalf("gen3 main read = %q, want GEN3-MAIN-EXTENT-UPDATED", got)
	}

	var createdEntry gofuse.CreateOut
	if st := v.switcher.adapter.Create(nil, &gofuse.CreateIn{InHeader: gofuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0o644}, "gen3.ext", &createdEntry); st != gofuse.OK {
		t.Fatalf("create gen3.ext: %v", st)
	}
	payload := []byte("gen3-extent-write-payload")
	if written, st := v.switcher.adapter.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: createdEntry.NodeId}, Fh: createdEntry.Fh, Size: uint32(len(payload))}, payload); st != gofuse.OK || int(written) != len(payload) {
		t.Fatalf("write gen3.ext: st=%v n=%d", st, written)
	}
	if st := v.switcher.adapter.Flush(nil, &gofuse.FlushIn{InHeader: gofuse.InHeader{NodeId: createdEntry.NodeId}, Fh: createdEntry.Fh}); st != gofuse.OK {
		t.Fatalf("flush gen3.ext: %v", st)
	}
	v.switcher.adapter.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: createdEntry.NodeId}, Fh: createdEntry.Fh})
	v.switcher.adapter.Forget(createdEntry.NodeId, 1)

	mu.Lock()
	landed := extentFiles["/gen3.ext"].data
	mu.Unlock()
	if landed != string(payload) {
		t.Fatalf("gen3 extent upload = %q, want %q", landed, payload)
	}
}
