package extent

import (
	"context"
	"net/http"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestClosedRetainedRenamePreservesRecreatedOldPathAndInvalidation(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	bus := fs.NewEventBus()
	eng := NewEngine(EngineConfig{
		Client:         client.New(f.URL, ""),
		RemoteRoot:     "/",
		CacheDir:       engineCacheDir(t),
		FreeSpaceRatio: -1,
	})
	h := New(Options{Engine: eng, Threshold: 0, Events: bus})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	ctx := context.Background()
	resA, err, _ := dispatchCreate(ctx, h, "/d/a.bin", syscall.O_RDWR|syscall.O_CREAT)
	require.NoError(t, err)
	ehA := resA.Handle.(*engineHandle)
	require.NoError(t, ehA.Write(ctx, 0, []byte("FIRST")))

	f.mu.Lock()
	f.statFail["/d/a.bin"] = true
	f.statFail["/d/b.bin"] = true
	f.mu.Unlock()
	require.Error(t, ehA.Release(ctx))
	require.Equal(t, 1, eng.Bridge.PendingCommitCount())

	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/d/a.bin", RenamedTo: "/d/b.bin"})
	bus.Emit(fs.Event{Kind: fs.EventDelete, Path: "/d/a.bin"})

	f.mu.Lock()
	delete(f.statFail, "/d/a.bin")
	f.mu.Unlock()
	resA2, err, _ := dispatchCreate(ctx, h, "/d/a.bin", syscall.O_RDWR|syscall.O_CREAT)
	require.NoError(t, err)
	ehA2 := resA2.Handle.(*engineHandle)
	require.NotEqual(t, ehA.ino, ehA2.ino)
	require.NoError(t, ehA2.Write(ctx, 0, []byte("SECOND")))
	f.mu.Lock()
	f.statFail["/d/a.bin"] = true
	f.mu.Unlock()
	require.Error(t, ehA2.Release(ctx))
	require.Equal(t, 2, eng.Bridge.PendingCommitCount())

	bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/d/b.bin"})
	bus.Emit(fs.Event{Kind: fs.EventInvalidated, Path: "/"})
	require.Equal(t, 2, eng.Bridge.PendingCommitCount())

	f.mu.Lock()
	delete(f.statFail, "/d/b.bin")
	f.mu.Unlock()
	require.NoError(t, eng.FlushFileFinal(ctx, uint64(ehA.ino)))
	eng.Bridge.retireClosed(ehA.ino)
	require.Equal(t, 1, eng.Bridge.PendingCommitCount())
	require.Equal(t, "FIRST", f.contentOf("/d/b.bin"))

	f.mu.Lock()
	delete(f.statFail, "/d/a.bin")
	f.mu.Unlock()
	require.NoError(t, h.Drain(ctx))
	require.Zero(t, eng.Bridge.PendingCommitCount())
	require.Equal(t, "FIRST", f.contentOf("/d/b.bin"))
	require.Equal(t, "SECOND", f.contentOf("/d/a.bin"))
}

func TestClosedRetainedDisplacedByRenameOverIsForgotten(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	f.seedExtent(t, "/d/src.bin", "FRESH")
	bus := fs.NewEventBus()
	eng := NewEngine(EngineConfig{
		Client:         client.New(f.URL, ""),
		RemoteRoot:     "/",
		CacheDir:       engineCacheDir(t),
		FreeSpaceRatio: -1,
	})
	h := New(Options{Engine: eng, Threshold: 0, Events: bus})
	t.Cleanup(func() { _ = h.Stop(context.Background()) })

	ctx := context.Background()
	resDst, err, _ := dispatchCreate(ctx, h, "/d/dst.bin", syscall.O_RDWR|syscall.O_CREAT)
	require.NoError(t, err)
	ehDst := resDst.Handle.(*engineHandle)
	require.NoError(t, ehDst.Write(ctx, 0, []byte("STALE")))

	f.failStatus.Store(http.StatusServiceUnavailable)
	require.Error(t, ehDst.Release(ctx))
	require.Equal(t, 1, eng.Bridge.PendingCommitCount())

	f.failStatus.Store(0)
	f.rename(t, "/d/src.bin", "/d/dst.bin")
	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/d/src.bin", RenamedTo: "/d/dst.bin"})
	bus.Emit(fs.Event{Kind: fs.EventDelete, Path: "/d/src.bin"})

	require.NoError(t, h.Drain(ctx))
	require.Zero(t, eng.Bridge.PendingCommitCount())
	require.Equal(t, "FRESH", f.contentOf("/d/dst.bin"))
}
