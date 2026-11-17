package writeback

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestWriteBackUploader_AutoBatchWriteWithAtomicMode(t *testing.T) {
	var (
		batchCalls atomic.Int32
		chmodCalls atomic.Int32
		mu         sync.Mutex
		committed  = make(map[string]client.BatchWriteItem)
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"inline_threshold":51200,"storage_capabilities":{"batch_write_mode_v1":true}}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/fs:batch-write":
			batchCalls.Add(1)
			var req struct {
				Items []client.BatchWriteItem `json:"items"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			results := make([]client.BatchWriteResult, len(req.Items))
			mu.Lock()
			for i, it := range req.Items {
				committed[it.Path] = it
				results[i] = client.BatchWriteResult{
					Path:     it.Path,
					Status:   http.StatusCreated,
					Revision: int64(i + 1),
				}
			}
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
		case r.Method == http.MethodPost && r.URL.RawQuery == "chmod":
			chmodCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPut:
			mu.Lock()
			committed[r.URL.Path[len("/v1/fs"):]] = client.BatchWriteItem{Path: r.URL.Path[len("/v1/fs"):]}
			mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	dir := t.TempDir()
	cache, err := staging.NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	cl := client.New(ts.URL, "")
	if th := cl.SmallFileThreshold(context.Background()); th != 51200 {
		t.Fatalf("SmallFileThreshold = %d, want 51200", th)
	}
	if !cl.CachedBatchWriteModeSupported() {
		t.Fatal("expected CachedBatchWriteModeSupported() == true")
	}

	const total = 24
	for i := 0; i < total; i++ {
		p := fmt.Sprintf("/uz/f%03d.txt", i)
		payload := []byte(fmt.Sprintf("payload-%d", i))
		if _, _, err := cache.PutWithBaseRevAndModeAndLineageTimingsNoSync(p, payload, int64(len(payload)), staging.PendingNew, 0, 0o755, true, "", "", false); err != nil {
			t.Fatal(err)
		}
	}

	uploader := NewWriteBackUploader(cl, cache, 1)
	uploader.ConfigureBatchWrite(25*time.Millisecond, 64, client.MaxBatchWriteBytes)
	for i := 0; i < total; i++ {
		uploader.Submit(fmt.Sprintf("/uz/f%03d.txt", i))
	}
	uploader.DrainAll()

	if batchCalls.Load() == 0 {
		t.Fatal("expected at least 1 /v1/fs:batch-write request")
	}
	if chmodCalls.Load() != 0 {
		t.Fatalf("chmodCalls = %d, want 0 (mode committed atomically in batch-write)", chmodCalls.Load())
	}
	mu.Lock()
	if len(committed) != total {
		t.Fatalf("committed %d files, want %d", len(committed), total)
	}
	for p, it := range committed {
		if !it.HasMode || it.Mode&0o777 != 0o755 {
			t.Fatalf("%s committed with HasMode=%v Mode=%o, want true/0755", p, it.HasMode, it.Mode)
		}
	}
	mu.Unlock()
	if rem, _, _ := cache.PendingSummary(); rem != 0 {
		t.Fatalf("remaining cached items = %d, want 0", rem)
	}
}

func TestStrictSyncAwaitsExtentMigrationBeforeReturning(t *testing.T) {
	srv := newMigrationServer(t)
	eng := extent.NewEngine(extent.EngineConfig{
		Client:     client.New(srv.URL, ""),
		RemoteRoot: "/",
	})
	plane := extent.New(extent.Options{Engine: eng, Threshold: 1})
	ch, wb := newUploadStack(t, srv.handler, Options{
		Policy:         PolicyWriteBack,
		SyncMode:       "strict",
		ExtentTakeover: true,
		ExtentMinSize:  1,
		ExtentOpener:   plane,
	})
	ctx := context.Background()
	defer func() {
		if wb.queue != nil {
			wb.queue.DrainAll()
		}
	}()

	res, err := ch.Dispatch(ctx, &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/strict-mig.bin",
		Flags:   syscall.O_RDWR | syscall.O_CREAT,
		Mode:    0o644,
		HasMode: true,
		Caller:  fs.Caller{Pid: 1},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h := res.Handle.(*bufferingHandle)
	if err := h.Write(ctx, 0, make([]byte, 64<<10)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("first strict Sync: %v", err)
	}
	// Strict Sync must not return until extent handle migration completes, so
	// an immediate subsequent pwrite+fsync never dirties the retired classic
	// buffer or triggers a second whole-file session.
	if h.extentHandle() == nil {
		t.Fatal("expected h.extentHandle() != nil immediately upon strict Sync return")
	}
	_, _, _, sessionsBefore, _, _ := srv.snapshot()
	if err := h.Write(ctx, 0, []byte("delta")); err != nil {
		t.Fatalf("post-sync write: %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("second Sync: %v", err)
	}
	_, _, _, sessionsAfter, patchesAfter, _ := srv.snapshot()
	if sessionsAfter != sessionsBefore {
		t.Fatalf("whole-file sessions grew from %d to %d after strict Sync migration", sessionsBefore, sessionsAfter)
	}
	if patchesAfter == 0 {
		t.Fatal("expected incremental extent PATCH session on second Sync")
	}
	_ = h.Release(ctx)
}

func TestLargeSequentialWriteEvictsLivePartsBeforeFlush(t *testing.T) {
	srv := newMigrationServer(t)
	ch, wb, _ := newMigratingStack(t, srv)
	ctx := context.Background()
	defer func() {
		if wb.queue != nil {
			wb.queue.DrainAll()
		}
	}()

	res, err := ch.Dispatch(ctx, &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/huge-spill.bin",
		Flags:   syscall.O_RDWR | syscall.O_CREAT,
		Mode:    0o644,
		HasMode: true,
		Caller:  fs.Caller{Pid: 1},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	h := res.Handle.(*bufferingHandle)

	// Write 80 MiB (> 64 MiB commitInlineStreamThreshold) OUT OF ORDER in row
	// pairs (chunk 1 before chunk 0, modeling concurrent Linux FUSE worker
	// threads that disable WriteBuffer.sequential) BEFORE calling Flush/Release.
	chunk := make([]byte, 1<<20)
	for i := range chunk {
		chunk[i] = byte(i % 251)
	}
	for i := 0; i < 80; i += 2 {
		if err := h.Write(ctx, int64(i+1)<<20, chunk); err != nil {
			t.Fatalf("write %d MiB: %v", i+1, err)
		}
		if err := h.Write(ctx, int64(i)<<20, chunk); err != nil {
			t.Fatalf("write %d MiB: %v", i, err)
		}
	}

	h.mu.Lock()
	evictedBeforeFlush := h.liveSpillFile != nil && h.buf.MemoryUsage() < commitInlineStreamThreshold
	h.mu.Unlock()
	if !evictedBeforeFlush {
		t.Fatal("expected resident parts to spill to liveSpillFile and keep Go heap below threshold even on out-of-order writes")
	}

	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	if wb.queue != nil {
		if err := wb.queue.WaitPathOutcome("/huge-spill.bin", 30*time.Second); err != nil {
			t.Fatalf("wait upload: %v", err)
		}
	}
	layout, _, size, _, _, _ := srv.snapshot()
	if layout != "extent" || size != 80<<20 {
		t.Fatalf("remote landed layout=%q size=%d, want extent / %d", layout, size, 80<<20)
	}
}
