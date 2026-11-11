//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
		case *extent.Extent, *appendlog.AppendLog:
			t.Fatalf("fixed view installed a legacy content plane: %T", handler)
		}
	}
}
