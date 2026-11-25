//go:build !windows

package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/fusecomp/perf"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

type fakeLayerState struct {
	Info          client.LayerInfo
	OwnerMountID  string
	LastBindOpID  string
	UnbindFailCnt int
	Files         map[string][]byte
}

type fakeSwitchBackend struct {
	mu         sync.Mutex
	mainFiles  map[string][]byte
	layers     map[string]*fakeLayerState
	bindCalls  int
	unbindOps  []client.LayerMountUnbindRequest
	renewCnt   int
	onBind     func(layerID string)
	onRenew    func(layerID string)
	onUnbind   func(layerID string, req client.LayerMountUnbindRequest)
	onViewList func(layerID string)
}

func newFakeSwitchBackend(t *testing.T) (*fakeSwitchBackend, *httptest.Server) {
	t.Helper()
	fb := &fakeSwitchBackend{
		mainFiles: map[string][]byte{"/seed.txt": []byte("main-v1")},
		layers: map[string]*fakeLayerState{
			"layer_01j00000000000000000000001": {
				Info:  client.LayerInfo{LayerID: "layer_01j00000000000000000000001", Name: "L1", State: "active", BaseRootPath: "/", Fence: 0},
				Files: map[string][]byte{"/seed.txt": []byte("l1-v1")},
			},
			"layer_01j00000000000000000000002": {
				Info:  client.LayerInfo{LayerID: "layer_01j00000000000000000000002", Name: "L2", State: "active", BaseRootPath: "/", Fence: 0},
				Files: map[string][]byte{"/seed.txt": []byte("l2-v1")},
			},
			"layer_01j00000000000000000000003": {
				Info:  client.LayerInfo{LayerID: "layer_01j00000000000000000000003", Name: "sub", State: "active", BaseRootPath: "/sub", Fence: 0},
				Files: map[string][]byte{"/sub/a.txt": []byte("sub-v1")},
			},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(fb.serveHTTP))
	t.Cleanup(srv.Close)
	return fb, srv
}

func (b *fakeSwitchBackend) serveHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.URL.Path == "/v1/status" {
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "active", "small_file_threshold": 50000})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/fs") {
		p := strings.TrimPrefix(r.URL.Path, "/v1/fs")
		if p == "" {
			p = "/"
		}
		files := b.mainFiles
		if r.URL.Query().Has("list") {
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []map[string]any{}})
			return
		}
		if p == "/" {
			w.Header().Set("X-Drive9-Is-Dir", "true")
			w.WriteHeader(http.StatusOK)
			return
		}
		data, ok := files[p]
		if !ok {
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}
		return
	}
	if r.URL.Path == "/v1/layers" && r.Method == http.MethodGet {
		var items []client.LayerInfo
		nameQ := r.URL.Query().Get("name")
		for _, st := range b.layers {
			if nameQ != "" && st.Info.Name != nameQ {
				continue
			}
			items = append(items, st.Info)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"layers": items})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/views/") {
		b.serveViewsSubpath(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/layers/") {
		b.serveLayersSubpath(w, r)
		return
	}
	http.NotFound(w, r)
}

type noopViewNotifier struct{}

func (noopViewNotifier) InodeNotify(uint64, int64, int64) gofuse.Status { return gofuse.OK }
func (noopViewNotifier) EntryNotify(uint64, string) gofuse.Status       { return gofuse.OK }

func viewSwitchFixture(t *testing.T) (*mountViewSwitch, *fakeSwitchBackend, string, *membudget.Budget) {
	t.Helper()
	shortDir, err := os.MkdirTemp("/tmp", "d9sw")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
	t.Setenv("XDG_RUNTIME_DIR", shortDir)
	t.Setenv("HOME", shortDir)

	fb, srv := newFakeSwitchBackend(t)
	mp := filepath.Join(shortDir, "mnt")
	require.NoError(t, os.MkdirAll(mp, 0o755))
	cacheDir := filepath.Join(shortDir, "cache")
	cfg := MountConfig{
		Server:           srv.URL,
		APIKey:           "test-key-placeholder",
		MountPoint:       mp,
		RemoteRoot:       "/",
		CacheDir:         cacheDir,
		WritePolicy:      "writeback",
		SyncMode:         "strict",
		MemoryBudget:     64 << 20,
		MemoryBudgetMode: "enforce",
		Extensions:       []string{"core", "writeback", "cache"},
	}
	baseClient := client.New(srv.URL, cfg.APIKey)
	scope, err := mountstate.SwitchScope(cfg.Server, "api_key:test", "/", mp)
	require.NoError(t, err)
	j, err := mountstate.OpenSwitchJournal(filepath.Join(shortDir, "switch.journal"), "mount-1", scope, mountstate.SwitchBinding{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = j.Close() })

	mb := membudget.New(64<<20, membudget.ModeEnforce, nil)
	counters := perf.NewCounters(false)
	var sharedExt *extent.Engine
	rt, err := buildViewRuntime(cfg, baseClient, "/", "mount-1", mountstate.SwitchBinding{}, nil, mb, counters, &sharedExt)
	require.NoError(t, err)
	rt.fs.Init2(context.Background())
	t.Cleanup(func() { _ = rt.stop(context.Background()) })

	adapter := &viewRequestFS{inner: rt.fs}
	adapter.selected.Store(&viewRequestTarget{fs: rt.fs, generation: 1})
	sw := &mountViewSwitch{
		goos:       "linux",
		cfg:        cfg,
		baseClient: baseClient,
		remoteRoot: "/",
		actorID:    "mount-1",
		journal:    j,
		adapter:    adapter,
		current:    rt,
		notifier:   noopViewNotifier{},
		fence:      func(context.Context) error { return nil },
		timing:     layerLeaseTiming{renewInterval: 5 * time.Second, callTimeout: 500 * time.Millisecond},
	}
	sw.buildRuntime = func(_ context.Context, target mountstate.SwitchBinding, lease *layerMountBinding) (*mountRuntime, error) {
		vc := baseClient
		if target.LayerID != "" && lease != nil && lease.ViewClient != nil {
			vc = lease.ViewClient
		}
		return buildViewRuntime(cfg, vc, "/", "mount-1", target, lease, mb, counters, &sharedExt)
	}
	t.Cleanup(func() {
		sw.stopAsyncUnbinds()
		if cur := sw.currentRuntime(); cur != nil {
			_ = cur.stop(context.Background())
		}
	})
	return sw, fb, mp, mb
}

func startViewControlFixture(t *testing.T, sw *mountViewSwitch) *mountControlServer {
	t.Helper()
	ctrl, err := startMountViewControlServer(sw.cfg.MountPoint, sw)
	require.NoError(t, err)
	t.Cleanup(ctrl.Close)
	return ctrl
}
