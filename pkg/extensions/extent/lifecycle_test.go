package extent

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/stretchr/testify/require"
)

func chainReexecRefusal(chain *fs.Chain) (string, int) {
	for _, h := range chain.Handlers() {
		if b, ok := h.(fs.ReexecGater); ok {
			if reason, count := b.ReexecRefusal(); count > 0 {
				return reason, count
			}
		}
	}
	return "", 0
}

func TestHandlerLifecycleDrainAndStop(t *testing.T) {
	var failFlush atomic.Bool
	failFlush.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failFlush.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"revision":2}`))
	}))
	defer srv.Close()

	cl := client.New(srv.URL, "token")
	eng := NewEngine(EngineConfig{
		Client:     cl,
		RemoteRoot: "/",
		CacheSize:  0,
	})
	h := New(Options{Engine: eng, Threshold: 1})

	if err := h.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if reason, n := h.ReexecRefusal(); reason != "" || n != 0 {
		t.Fatalf("idle ReexecRefusal = (%q, %d), want empty", reason, n)
	}
	if err := h.Drain(context.Background()); err != nil {
		t.Fatalf("idle Drain: %v", err)
	}

	ino := meta.Ino(42)
	cause := errors.New("transient commit error")
	eng.Bridge.mu.Lock()
	eng.Bridge.paths[ino] = "/extent.bin"
	eng.Bridge.commitErr[ino] = cause
	eng.Bridge.mu.Unlock()
	eng.Bridge.kickPendingFlush()

	if reason, n := h.ReexecRefusal(); reason != "extent_pending" || n != 1 {
		t.Fatalf("pending ReexecRefusal = (%q, %d), want (extent_pending, 1)", reason, n)
	}
	if err := h.Drain(context.Background()); err == nil {
		t.Fatal("Drain succeeded with unresolved commitErr")
	}

	if err := h.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	eng.Bridge.mu.Lock()
	running := eng.Bridge.retryRunning
	eng.Bridge.mu.Unlock()
	if running {
		t.Fatal("background retry loop still running after Stop")
	}
}

func TestHandlerLifecycleRealPathOpenEmptyFreshCreateAndSubsequentWrite(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	chain, err := fs.NewChain(h, stubTerminal{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = chain.Stop(context.Background()) })
	require.NoError(t, chain.Start(context.Background()))

	res, err := chain.Dispatch(context.Background(), &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/d/open-empty.bin",
		Flags:   syscall.O_RDWR | syscall.O_CREAT,
		Mode:    0o644,
		HasMode: true,
		Caller:  fs.Caller{Pid: 1},
	})
	require.NoError(t, err)
	eh := res.Handle.(*engineHandle)

	// Unmaterialized 0-byte local create must refuse view-switch / re-exec.
	reason, count := chainReexecRefusal(chain)
	require.Equal(t, "extent_pending", reason)
	require.Equal(t, 1, count)

	// Drain fails while the server is unavailable, preserving uncommitted state.
	f.failStatus.Store(http.StatusServiceUnavailable)
	require.Error(t, chain.Drain(context.Background()))
	reason, count = chainReexecRefusal(chain)
	require.Equal(t, "extent_pending", reason)
	require.Equal(t, 1, count)

	// Restoring the server lets Drain materialize the empty file and clear refusal,
	// while keeping the open handle usable for later writes.
	f.failStatus.Store(0)
	require.NoError(t, chain.Drain(context.Background()))
	reason, count = chainReexecRefusal(chain)
	require.Empty(t, reason)
	require.Zero(t, count)

	require.NoError(t, eh.Write(context.Background(), 0, []byte("after-drain-payload")))
	require.NoError(t, eh.Release(context.Background()))
	require.Equal(t, "after-drain-payload", f.contentOf("/d/open-empty.bin"))
	reason, count = chainReexecRefusal(chain)
	require.Empty(t, reason)
	require.Zero(t, count)
}

func TestHandlerLifecycleRealPathClosedEmptyReleaseFailureRetainsUntilDrain(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	chain, err := fs.NewChain(h, stubTerminal{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = chain.Stop(context.Background()) })
	require.NoError(t, chain.Start(context.Background()))

	res, err := chain.Dispatch(context.Background(), &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/d/closed-empty.bin",
		Flags:   syscall.O_RDWR | syscall.O_CREAT,
		Mode:    0o600,
		HasMode: true,
		Caller:  fs.Caller{Pid: 1},
	})
	require.NoError(t, err)

	// Close fails with 503 during final empty materialization: handle is closed,
	// but the bridge must retain uncommitted state rather than forgetting it.
	f.failStatus.Store(http.StatusServiceUnavailable)
	require.Error(t, res.Handle.Release(context.Background()))

	h.mu.Lock()
	openCount := len(h.handles)
	h.mu.Unlock()
	require.Zero(t, openCount)

	reason, count := chainReexecRefusal(chain)
	require.Equal(t, "extent_pending", reason)
	require.Equal(t, 1, count)
	require.Error(t, chain.Drain(context.Background()))

	// Once the server recovers, Drain completes the final empty create and retires
	// the closed inode so subsequent opens do not inherit stale freshCreate state.
	f.failStatus.Store(0)
	require.NoError(t, chain.Drain(context.Background()))
	reason, count = chainReexecRefusal(chain)
	require.Empty(t, reason)
	require.Zero(t, count)

	f.mu.Lock()
	_, created := f.classic["/d/closed-empty.bin"]
	chmodMode := f.chmods["/d/closed-empty.bin"]
	f.mu.Unlock()
	require.True(t, created)
	require.Equal(t, uint32(0o600), chmodMode)
	require.Equal(t, 0, h.opts.Engine.Bridge.PendingCommitCount())
}

func TestHandlerLifecycleRealPathClosedBatchedWriteFailureRetainsAndUnpins(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	cl := client.New(f.URL, "")
	require.True(t, cl.WarmCompletionIdentity(context.Background()))
	eng := NewEngine(EngineConfig{Client: cl, RemoteRoot: "/", CacheDir: engineCacheDir(t)})
	h := New(Options{Engine: eng, Threshold: 0, Events: fs.NewEventBus()})
	chain, err := fs.NewChain(h, stubTerminal{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = chain.Stop(context.Background()) })
	require.NoError(t, chain.Start(context.Background()))

	res, err := chain.Dispatch(context.Background(), &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/d/closed-write.bin",
		Flags:   syscall.O_RDWR | syscall.O_CREAT,
		Mode:    0o644,
		HasMode: true,
		Caller:  fs.Caller{Pid: 1},
	})
	require.NoError(t, err)
	require.NoError(t, res.Handle.Write(context.Background(), 0, []byte("small-batched-write")))

	f.failStatus.Store(http.StatusServiceUnavailable)
	require.Error(t, res.Handle.Release(context.Background()))

	reason, count := chainReexecRefusal(chain)
	require.Equal(t, "extent_pending", reason)
	require.Equal(t, 1, count)
	require.Greater(t, eng.Bridge.blocks.pinnedBudget(), int64(0))
	require.Error(t, chain.Drain(context.Background()))

	f.failStatus.Store(0)
	require.NoError(t, chain.Drain(context.Background()))
	reason, count = chainReexecRefusal(chain)
	require.Empty(t, reason)
	require.Zero(t, count)
	require.Zero(t, eng.Bridge.blocks.pinnedBudget())
	require.Equal(t, "small-batched-write", f.contentOf("/d/closed-write.bin"))
}

func TestHandlerLifecycleRealPathTruncateAndLegacyEmptyCloseNoFalsePositive(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/trunc.bin", "0123456789")
	h := newTestHandler(t, f, 0)
	chain, err := fs.NewChain(h, stubTerminal{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = chain.Stop(context.Background()) })
	require.NoError(t, chain.Start(context.Background()))

	res, err := chain.Dispatch(context.Background(), &fs.Op{
		Kind:   fs.OpCreate,
		Path:   "/d/trunc.bin",
		Flags:  syscall.O_RDWR,
		Caller: fs.Caller{Pid: 1},
	})
	require.NoError(t, err)
	_, err = chain.Dispatch(context.Background(), &fs.Op{
		Kind:    fs.OpSetAttr,
		Path:    "/d/trunc.bin",
		HasSize: true,
		Size:    4,
		Caller:  fs.Caller{Pid: 1},
	})
	require.NoError(t, err)

	reason, count := chainReexecRefusal(chain)
	require.Equal(t, "extent_pending", reason)
	require.Equal(t, 1, count)

	require.NoError(t, chain.Drain(context.Background()))
	reason, count = chainReexecRefusal(chain)
	require.Empty(t, reason)
	require.Zero(t, count)
	require.Equal(t, "0123", f.contentOf("/d/trunc.bin"))
	require.NoError(t, res.Handle.Release(context.Background()))

	// Legacy fallback empty placeholder (!nativeExtent, even without createEcho)
	// must NOT leave freshCreate blocking view switches after final close.
	f.legacy = true
	f.createEcho = false
	emptyRes, err := chain.Dispatch(context.Background(), &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/d/legacy-empty.bin",
		Flags:   syscall.O_RDWR | syscall.O_CREAT,
		Mode:    0o644,
		HasMode: true,
		Caller:  fs.Caller{Pid: 1},
	})
	require.NoError(t, err)
	require.NoError(t, emptyRes.Handle.Release(context.Background()))
	reason, count = chainReexecRefusal(chain)
	require.Empty(t, reason)
	require.Zero(t, count)
	require.NoError(t, chain.Drain(context.Background()))
}

type stubTerminal struct{}

func (stubTerminal) Do(context.Context, *fs.Op, fs.Next) (fs.Result, error) {
	return fs.Result{Handle: &stubCore{}}, nil
}
