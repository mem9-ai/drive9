package writeback

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestSparseGrowthDrainUsesShadowSpill(t *testing.T) {
	for _, offset := range []int64{(16 << 20) + 1, (64 << 20) + 1} {
		t.Run(fmt.Sprintf("offset_%d", offset), func(t *testing.T) {
			testSparseGrowthDrainUsesShadowSpill(t, offset)
		})
	}
}

func testSparseGrowthDrainUsesShadowSpill(t *testing.T, offset int64) {
	t.Helper()
	// Keep the old implementation's accidental whole-image allocation bounded.
	// Large logical-file coverage must not use an unguarded multi-GiB probe.
	if offset <= commitInlineStreamThreshold || offset > (64<<20)+1 {
		t.Fatalf("sparse-growth probe offset %d exceeds its allocation safety bound", offset)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := testbackend.NewBackend()
	entered := make(chan struct{})
	reject := make(chan struct{})
	var enterOnce, rejectOnce sync.Once
	release := func() { rejectOnce.Do(func() { close(reject) }) }
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			// Reject before reading any upload body: testbackend's whole-file
			// storage must not turn a streaming upload into a fixture allocation.
			enterOnce.Do(func() { close(entered) })
			select {
			case <-reject:
			case <-r.Context().Done():
				return
			}
			http.Error(w, "fixture rejects upload", http.StatusForbidden)
			return
		}
		backend.ServeHTTP(w, r)
	})
	chain, wb := newUploadStack(t, handler, Options{
		Policy: PolicyWriteBack, Staging: ledger, WriteCache: cache,
		UploadConcurrency: 1,
	})
	t.Cleanup(func() {
		wb.queue.AbandonLayer()
		release()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := wb.Stop(stopCtx); err != nil {
			t.Error(err)
		}
	})
	h, first := stageReviewDrainFile(t, ctx, chain)
	var largeCache atomic.Bool
	wb.SetPendingSeeder(func(path string, data []byte) {
		if path == "/f" && int64(len(data)) >= commitInlineStreamThreshold {
			largeCache.Store(true)
			cancel()
		}
	})
	if err := h.Write(ctx, offset, []byte("z")); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.DrainFlush(ctx) }()
	select {
	case <-entered:
	case err := <-done:
		if largeCache.Load() {
			t.Fatalf("large image passed cache seeder instead of shadow spill: %v", err)
		}
		t.Fatalf("drain returned before its streamed upload: %v", err)
	case <-ctx.Done():
		if largeCache.Load() {
			t.Fatal("large image passed cache seeder instead of shadow spill")
		}
		t.Fatal("drain did not reach its streamed upload")
	}
	if largeCache.Load() {
		t.Fatal("large image passed cache seeder instead of shadow spill")
	}
	meta, ok := ledger.Pending.GetMeta("/f")
	if !ok || !meta.ShadowSpill || meta.Size != offset+1 {
		t.Fatalf("pending metadata=%+v exists=%v; want sparse shadow spill of %d bytes", meta, ok, offset+1)
	}
	if meta, ok := cache.GetMeta("/f"); ok {
		t.Errorf("obsolete small cache image still owns the path: %+v", meta)
	}
	got := make([]byte, len(first))
	if _, err := ledger.Shadow.ReadAt("/f", 0, got); err != nil || !bytes.Equal(got, first) {
		t.Fatalf("shadow prefix differs: %v", err)
	}
	tail := make([]byte, 1)
	if _, err := ledger.Shadow.ReadAt("/f", offset, tail); err != nil || tail[0] != 'z' {
		t.Fatalf("shadow tail=%q error=%v", tail, err)
	}
	if err := ledger.Shadow.CheckWriteBackQuota(0); err != nil {
		t.Fatalf("sparse spill exhausted its written-byte quota: %v", err)
	}
	// Publication was inspected while blocked. Cancel only the fixture's
	// queue now, preserving its local payload and avoiding upload retries.
	wb.queue.AbandonLayer()
	release()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("drain did not stop after the fixture rejected its upload")
	}
}

func TestSparseGrowthStageFailureKeepsAcknowledgedBytes(t *testing.T) {
	const offset = int64(16<<20) + 1
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	ledger, err := staging.NewStagingWithQuota(root, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	backend := testbackend.NewBackend()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut || r.Method == http.MethodPost {
			http.Error(w, "fixture rejects upload before reading its body", http.StatusForbidden)
			return
		}
		backend.ServeHTTP(w, r)
	})
	chain, wb := newUploadStack(t, handler, Options{
		Policy: PolicyWriteBack, Staging: ledger, WriteCache: cache,
		UploadConcurrency: 1,
	})
	stopSSETestWriteback(t, wb)
	t.Cleanup(wb.queue.AbandonLayer)
	h, first := stageReviewDrainFile(t, ctx, chain)
	initialMeta, ok := cache.GetMeta("/f")
	if !ok {
		t.Fatal("initial small snapshot was not staged")
	}
	if err := h.Write(ctx, offset, []byte("z")); err != nil {
		t.Fatal(err)
	}
	h.mu.Lock()
	writeGen := h.wgen
	h.mu.Unlock()
	shadowDir := filepath.Join(root, "shadow")
	savedDir := filepath.Join(root, "saved-shadow")
	restore := func() {
		t.Helper()
		if _, err := os.Stat(savedDir); os.IsNotExist(err) {
			return
		}
		if _, err := os.Stat(shadowDir); err == nil {
			if err := os.Rename(shadowDir, filepath.Join(root, "used-blocker")); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Rename(savedDir, shadowDir); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(restore)
	if err := os.Rename(shadowDir, savedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(shadowDir, []byte("block shadow staging"), 0o600); err != nil {
		t.Fatal(err)
	}
	var largeCache atomic.Bool
	wb.SetPendingSeeder(func(path string, data []byte) {
		if path == "/f" && int64(len(data)) >= commitInlineStreamThreshold {
			largeCache.Store(true)
			cancel()
		}
	})
	if err := h.DrainFlush(ctx); err == nil {
		t.Error("drain succeeded despite the unavailable shadow store")
	}
	restore()
	if largeCache.Load() {
		t.Error("failed sparse restage materialized a large write-cache image")
	}
	h.mu.Lock()
	dirty, afterGen := h.dirty, h.wgen
	h.mu.Unlock()
	if !dirty || afterGen != writeGen {
		t.Errorf("failed restage left dirty=%v generation=%d; want acknowledged generation %d still dirty", dirty, afterGen, writeGen)
	}
	if meta, ok := cache.GetMeta("/f"); ok && (meta.Generation != initialMeta.Generation || meta.Size != int64(len(first))) {
		t.Errorf("failed sparse staging replaced the small cache snapshot: %+v", meta)
	}
	got := make([]byte, len(first))
	if n, err := h.Read(ctx, 0, got); err != nil || n != len(first) || !bytes.Equal(got, first) {
		t.Fatalf("failed restage lost the acknowledged prefix: n=%d error=%v", n, err)
	}
	tail := make([]byte, 1)
	if n, err := h.Read(ctx, offset, tail); err != nil || n != 1 || tail[0] != 'z' {
		t.Fatalf("failed restage lost the acknowledged tail: n=%d data=%q error=%v", n, tail, err)
	}
}

func newSparseGrowthSuccessStack(t *testing.T, events *fs.EventBus) (*fs.Chain, *Writeback, *testbackend.Backend) {
	t.Helper()
	ledger, err := staging.NewStagingWithQuota(t.TempDir(), 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	backend := testbackend.NewBackend()
	chain, wb, _ := newWriteCacheClientStack(t, PolicyWriteBack, backend, Options{Staging: ledger, Events: events})
	stopSSETestWriteback(t, wb)
	return chain, wb, backend
}

func TestSparseGrowthDrainLandingKeepsOpenWriterUsable(t *testing.T) {
	const offset = int64(16<<20) + 1
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	chain, wb, backend := newSparseGrowthSuccessStack(t, nil)
	h, first := stageReviewDrainFile(t, ctx, chain)
	if err := h.Write(ctx, offset, []byte("z")); err != nil {
		t.Fatal(err)
	}
	if err := h.DrainFlush(ctx); err != nil {
		t.Fatal(err)
	}
	want := make([]byte, offset+1)
	copy(want, first)
	want[offset] = 'z'
	assertReviewDrainRemote(t, backend, want)
	if wb.opts.Staging.Pending.HasPending("/f") || wb.opts.Staging.Shadow.Has("/f") {
		t.Fatal("landing did not retire its pending shadow image")
	}
	got := make([]byte, len(first))
	if n, err := h.Read(ctx, 0, got); err != nil || n != len(got) || !bytes.Equal(got, first) {
		t.Fatalf("open writer lost its prefix after shadow cleanup: n=%d error=%v", n, err)
	}
	tail := make([]byte, 1)
	if n, err := h.Read(ctx, offset, tail); err != nil || n != 1 || tail[0] != 'z' {
		t.Fatalf("open writer lost its tail after shadow cleanup: n=%d data=%q error=%v", n, tail, err)
	}
	patchOffset := int64(len(first) + 17)
	patch := []byte("next drain")
	if err := h.Write(ctx, patchOffset, patch); err != nil {
		t.Fatal(err)
	}
	copy(want[patchOffset:], patch)
	if err := h.DrainFlush(ctx); err != nil {
		t.Fatal(err)
	}
	assertReviewDrainRemote(t, backend, want)
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	assertReviewDrainRemote(t, backend, want)
}

func TestSparseGrowthStageEventPreservesNewWrite(t *testing.T) {
	const offset = int64(16<<20) + 1
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bus := fs.NewEventBus()
	chain, wb, backend := newSparseGrowthSuccessStack(t, bus)
	h, first := stageReviewDrainFile(t, ctx, chain)
	if err := h.Write(ctx, offset, []byte("z")); err != nil {
		t.Fatal(err)
	}
	patchOffset := int64(len(first) + 9)
	patch := []byte("write after stage")
	var called atomic.Bool
	var writeErr error
	off := bus.On(fs.EventStaged, func(event fs.Event) {
		if event.Path == "/f" && called.CompareAndSwap(false, true) {
			writeErr = h.Write(ctx, patchOffset, patch)
		}
	})
	defer off()
	if err := h.DrainFlush(ctx); err != nil {
		t.Fatal(err)
	}
	off()
	if !called.Load() || writeErr != nil {
		t.Fatalf("stage event did not acknowledge a later write: called=%v error=%v", called.Load(), writeErr)
	}
	h.mu.Lock()
	dirty := h.dirty
	h.mu.Unlock()
	if !dirty {
		t.Error("staging retired the write acknowledged by EventStaged")
	}
	want := make([]byte, offset+1)
	copy(want, first)
	want[offset] = 'z'
	assertReviewDrainRemote(t, backend, want)
	copy(want[patchOffset:], patch)
	if err := h.DrainFlush(ctx); err != nil {
		t.Fatal(err)
	}
	assertReviewDrainRemote(t, backend, want)
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	assertReviewDrainRemote(t, backend, want)
}

func TestSparseGrowthStageEventPreservesTruncateFlush(t *testing.T) {
	const offset = int64(16<<20) + 1
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	bus := fs.NewEventBus()
	chain, wb, backend := newSparseGrowthSuccessStack(t, bus)
	h, first := stageReviewDrainFile(t, ctx, chain)
	if err := h.Write(ctx, offset, []byte("z")); err != nil {
		t.Fatal(err)
	}
	const size = int64(4096)
	truncated := make(chan error, 1)
	flushed := make(chan error, 1)
	var called atomic.Bool
	var truncateErr error
	off := bus.On(fs.EventStaged, func(event fs.Event) {
		if event.Path != "/f" || !called.CompareAndSwap(false, true) {
			return
		}
		go func() {
			err := h.truncateLocal(ctx, size)
			truncated <- err
			if err == nil {
				err = h.Flush(ctx)
			}
			flushed <- err
		}()
		// The newer mutation must precede retirement of the old stage, but
		// its Flush may wait for that stage's path lock after this returns.
		select {
		case truncateErr = <-truncated:
		case <-ctx.Done():
			truncateErr = ctx.Err()
		}
	})
	defer off()
	if err := h.DrainFlush(ctx); err != nil {
		t.Fatal(err)
	}
	off()
	if !called.Load() || truncateErr != nil {
		t.Fatalf("stage event did not acknowledge the truncate: called=%v error=%v", called.Load(), truncateErr)
	}
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatalf("newer truncate Flush: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("newer truncate Flush did not finish")
	}
	if err := wb.queue.WaitIdle(ctx); err != nil {
		t.Fatal(err)
	}
	if meta, ok := wb.wbCache.GetMeta("/f"); ok {
		h.mu.Lock()
		staged := h.stagedToCache
		h.mu.Unlock()
		if meta.Size != size || !staged {
			t.Fatalf("newer cached truncate lost ownership: metadata=%+v staged=%v", meta, staged)
		}
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	assertReviewDrainRemote(t, backend, first[:size])
}

func TestSparseGrowthWriteCacheHelperRejectsLargeImage(t *testing.T) {
	const offset = int64(16<<20) + 1
	ctx := context.Background()
	chain, wb, _ := newSparseGrowthSuccessStack(t, nil)
	h, first := stageReviewDrainFile(t, ctx, chain)
	before, ok := wb.wbCache.GetMeta("/f")
	if !ok {
		t.Fatal("initial small image is not staged")
	}
	if err := h.Write(ctx, offset, []byte("z")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := h.stageToWriteCache(ctx, stageFullDurable)
	if err == nil || snapshot != nil {
		t.Fatalf("large image was accepted by the whole-image helper: snapshot=%+v error=%v", snapshot, err)
	}
	after, ok := wb.wbCache.GetMeta("/f")
	if !ok || after.Generation != before.Generation || after.Size != int64(len(first)) {
		t.Fatalf("helper rejection changed the initial snapshot: %+v exists=%v", after, ok)
	}
	h.mu.Lock()
	dirty := h.dirty
	h.mu.Unlock()
	if !dirty {
		t.Fatal("helper rejection retired the large acknowledged image")
	}
}
