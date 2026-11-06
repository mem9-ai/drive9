package writeback

import (
	"context"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// A write must never mutate the classic buffer after the descriptor migrated:
// Sync/Flush/Release would then publish through the engine and silently drop
// the acknowledged bytes. With the plane choice and the buffer mutation in one
// critical section, a racing write either forwards to the engine or completes
// first — and then adoption refuses it and the next landing retries.
func TestMigrationNeverStrandsAConcurrentWrite(t *testing.T) {
	srv := newMigrationServer(t)
	ch, wb, _ := newMigratingStack(t, srv)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/race.bin", Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h := res.Handle.(*bufferingHandle)
	if err := h.Write(ctx, 0, make([]byte, 1<<20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Race writers against the landing that triggers adoption.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = h.Write(ctx, int64(i)*512, []byte("racing-writer-bytes"))
		}(i)
	}
	if wb.queue != nil {
		_ = wb.queue.WaitPathOutcome("/race.bin", 10*time.Second)
	}
	wg.Wait()

	// Whatever plane the handle ended on, its bytes must be publishable and
	// must survive the close.
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("sync after the race: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, tracked := wb.live.Load("/race.bin"); tracked {
		t.Fatal("released handle is still tracked in the live registry")
	}
}

// Migration must not hold the extent plane's path lock while it runs under the
// write-back path lock: rename holds the extent lock across a chain call that
// flushes the source writer and waits for the queue. Exercising both
// concurrently catches the ABBA ordering.
func TestMigrationAndRenameDoNotDeadlock(t *testing.T) {
	srv := newMigrationServer(t)
	ch, wb, _ := newMigratingStack(t, srv)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/lock.bin", Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h := res.Handle.(*bufferingHandle)
	if err := h.Write(ctx, 0, make([]byte, 1<<20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = ch.Dispatch(ctx, &fs.Op{
			Kind: fs.OpRename, Path: "/lock.bin", Path2: "/lock.ren", Caller: fs.Caller{Pid: 1},
		})
	}()
	if wb.queue != nil {
		_ = wb.queue.WaitPathOutcome("/lock.bin", 10*time.Second)
	}
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("rename and promotion completion deadlocked")
	}
}

// A chmod deferred past a dirty buffer must still reach the remote when the
// descriptor migrates: this plane will never see another publication boundary
// for it, so the engine handle would otherwise keep the pre-chmod permissions.
func TestMigrationPublishesDeferredMode(t *testing.T) {
	srv := newMigrationServer(t)
	ch, wb, _ := newMigratingStack(t, srv)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/mode.bin", Flags: syscall.O_RDWR | syscall.O_CREAT, Mode: 0o644, HasMode: true, Caller: fs.Caller{Pid: 1}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h := res.Handle.(*bufferingHandle)

	// A dirty buffer defers the chmod (applying it mid-buffer would race the
	// upload), which is the window the migration has to cover.
	if err := h.Write(ctx, 0, make([]byte, 1<<20)); err != nil {
		t.Fatalf("write: %v", err)
	}
	h.mu.Lock()
	h.pendingMode, h.hasPendingMode = 0o600, true
	h.mu.Unlock()
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if wb.queue != nil {
		if err := wb.queue.WaitPathOutcome("/mode.bin", 10*time.Second); err != nil {
			t.Fatalf("wait for landing: %v", err)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		srv.mu.Lock()
		mode := srv.lastChmod
		srv.mu.Unlock()
		if mode == 0o600 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("deferred chmod never reached the remote (last=%o); the migrated descriptor dropped it", mode)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
