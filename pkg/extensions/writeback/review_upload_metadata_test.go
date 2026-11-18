package writeback

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestSyncUsesUploadedMetadataAfterRestage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("remote"))
	chain, wb, _ := newWriteCacheClientStack(t, PolicyWriteBack, b, Options{})
	stopSSETestWriteback(t, wb)
	h1 := dispatchOpen(t, chain, "/f", uint32(syscall.O_RDWR))
	first := bytes.Repeat([]byte("a"), 65536)
	if err := h1.Write(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	if err := h1.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	h2 := dispatchOpen(t, chain, "/f", uint32(syscall.O_RDWR))
	t.Cleanup(func() {
		for _, h := range []fs.Handle{h1, h2} {
			if err := h.Release(context.Background()); err != nil {
				t.Error(err)
			}
		}
	})
	unlock := wb.uploader.acquirePath("/f")
	var once sync.Once
	release := func() { once.Do(unlock) }
	t.Cleanup(release)
	done := make(chan error, 1)
	go func() { done <- h1.Sync(ctx) }()
	waitReviewSyncUploadGate(t, ctx)
	// A separate handle may replace the path's staged image while this
	// descriptor waits for an upload, without racing operations on one FH.
	want := append(append([]byte(nil), first...), []byte("0123456789abcdef")...)
	if err := h2.Write(ctx, 0, want); err != nil {
		t.Fatal(err)
	}
	if err := h2.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Sync did not finish after the upload gate opened")
	}
	node, ok := b.Node("/f")
	if !ok || !bytes.Equal(node.Data, want) {
		t.Fatalf("remote bytes=%d exists=%v; want the complete %d-byte second image", len(node.Data), ok, len(want))
	}
	attr, landed := wb.landedAttr("/f")
	if !landed || attr.Size != int64(len(want)) || attr.Rev != node.Rev {
		t.Fatalf("landed attr=%+v exists=%v; want uploaded size=%d revision=%d", attr, landed, len(want), node.Rev)
	}
}

func waitReviewSyncUploadGate(t *testing.T, ctx context.Context) {
	t.Helper()
	// Grow the dump buffer until it holds every goroutine: later tests in
	// the package leak workers (retry backoffs, parked dispatches), and a
	// truncated dump can simply not contain the frame this waits for —
	// which reads as "Sync never reached the gate".
	stack := make([]byte, 1<<20)
	if n := runtime.Stack(stack, true); n == len(stack) {
		stack = make([]byte, len(stack)*4)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		n := runtime.Stack(stack, true)
		if n == len(stack) { // still saturated: regrow and rescrape
			stack = make([]byte, len(stack)*4)
			continue
		}
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "TestSyncUsesUploadedMetadataAfterRestage.func") &&
				strings.Contains(goroutine, "(*bufferingHandle).Sync") &&
				strings.Contains(goroutine, "(*WriteBackUploader).acquirePath") {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("Sync did not reach the held upload gate")
		}
	}
}

func TestSyncWithoutDataUploadDoesNotInventLanding(t *testing.T) {
	for _, kind := range []string{"no-cache", "chmod-only"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			b := testbackend.NewBackend()
			b.Seed("/f", []byte("remote"))
			before, _ := b.Node("/f")
			chain, wb, _ := newWriteCacheClientStack(t, PolicyWriteBack, b, Options{})
			stopSSETestWriteback(t, wb)
			h := writebackHandleOf(dispatchOpen(t, chain, "/f", uint32(syscall.O_RDWR)))
			if kind == "chmod-only" {
				if err := wb.wbCache.PutWithBaseRevAndMode("/f", before.Data, int64(len(before.Data)), staging.PendingOverwrite, before.Rev, 0o600, true); err != nil {
					t.Fatal(err)
				}
				gen := wb.wbCache.MetaGeneration("/f")
				if updated, err := wb.wbCache.MarkChmodPending("/f", gen); err != nil || !updated {
					t.Fatalf("mark chmod pending: updated=%v err=%v", updated, err)
				}
			}
			// A background landing may have consumed the cache while the
			// descriptor still remembers its flush-time staged flag.
			h.mu.Lock()
			h.stagedToCache = true
			h.mu.Unlock()
			if err := h.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if _, exists := wb.landedAttr("/f"); exists {
				t.Fatal("Sync invented landed size/revision without a data upload")
			}
			after, ok := b.Node("/f")
			if !ok || !bytes.Equal(after.Data, before.Data) || after.Rev != before.Rev {
				t.Fatalf("no-data Sync changed remote content/revision: %+v exists=%v", after, ok)
			}
			if kind == "chmod-only" && after.Mode&0o777 != 0o600 {
				t.Fatalf("chmod-only Sync mode=%#o, want 0600", after.Mode&0o777)
			}
			if puts := b.Requests()["PUT /v1/fs/f"]; puts != 0 {
				t.Fatalf("no-data Sync issued %d data uploads", puts)
			}
			if _, exists := wb.wbCache.GetMeta("/f"); exists {
				t.Fatal("no-data Sync left consumed metadata in the cache")
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A chmod denied with 403 means the token may never change permission bits (an fs_scoped
// mount), so retrying cannot succeed: the upload is acknowledged and only the mode is
// dropped. Any other failure stays retryable and keeps the mode pending.
func TestUploadSyncChmodForbiddenDropsMode(t *testing.T) {
	b := testbackend.NewBackend()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("chmod") {
			http.Error(w, `{"error":"chmod is owner-only"}`, http.StatusForbidden)
			return
		}
		b.ServeHTTP(w, r)
	})
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	_, wb := newUploadStack(t, handler, Options{Policy: PolicyWriteBack, WriteCache: cache})
	stopSSETestWriteback(t, wb)
	ctx := context.Background()
	if err := cache.PutWithBaseRevAndMode("/f", []byte("data"), 4, staging.PendingNew, 0, 0o600, true); err != nil {
		t.Fatal(err)
	}
	if _, err := wb.uploader.UploadSyncWithRevision(ctx, "/f"); err != nil {
		t.Fatalf("a forbidden chmod must not fail the upload: %v", err)
	}
	node, exists := b.Node("/f")
	if !exists || string(node.Data) != "data" {
		t.Fatalf("content did not commit: %+v exists=%v", node, exists)
	}
	if node.Mode&0o777 == 0o600 {
		t.Fatalf("denied mode was applied: %o", node.Mode&0o777)
	}
	if marker, exists := cache.GetMeta("/f"); exists && marker.Kind == staging.PendingChmod {
		t.Fatalf("a forbidden chmod left a retry marker: %+v", marker)
	}
}

func TestUploadSyncNoDataOutcomeMetadata(t *testing.T) {
	for _, kind := range []string{"no-cache", "chmod-only"} {
		t.Run(kind, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.Seed("/f", []byte("remote"))
			before, _ := b.Node("/f")
			var rejectMode atomic.Bool
			rejectMode.Store(true)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("chmod") && rejectMode.Load() {
					http.Error(w, "chmod denied", http.StatusInternalServerError)
					return
				}
				b.ServeHTTP(w, r)
			})
			cache, err := staging.NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			_, wb := newUploadStack(t, handler, Options{Policy: PolicyWriteBack, WriteCache: cache})
			stopSSETestWriteback(t, wb)
			if kind == "chmod-only" {
				if err := cache.PutWithBaseRevAndMode("/f", before.Data, int64(len(before.Data)), staging.PendingOverwrite, before.Rev, 0o600, true); err != nil {
					t.Fatal(err)
				}
				if _, err := wb.uploader.UploadSyncWithRevision(context.Background(), "/f"); err == nil {
					t.Fatal("data upload with a rejected chmod returned success")
				}
				if puts := b.Requests()["PUT /v1/fs/f"]; puts != 1 {
					t.Fatalf("chmod marker has no successful data upload: PUTs=%d", puts)
				}
				if marker, exists := cache.GetMeta("/f"); !exists || marker.Kind != staging.PendingChmod {
					t.Fatalf("data PUT plus chmod failure left no retry marker: %+v exists=%v", marker, exists)
				}
			}
			putsBefore := b.Requests()["PUT /v1/fs/f"]
			landingBefore, hadLanding := wb.landedAttr("/f")
			if hadLanding {
				remote, exists := b.Node("/f")
				if !exists || landingBefore.Rev != remote.Rev || landingBefore.Size != int64(len(remote.Data)) {
					t.Fatal("prior data landing does not match the actual remote image")
				}
			}
			rejectMode.Store(false)
			rev, meta, err := wb.uploader.uploadSyncWithMetadata(context.Background(), "/f")
			if err != nil || rev != 0 {
				t.Fatalf("no-data upload returned revision=%d metadata=%+v error=%v", rev, meta, err)
			}
			if kind == "no-cache" && meta != nil {
				t.Fatalf("absent cache returned metadata %+v", meta)
			}
			if kind == "chmod-only" && (meta == nil || meta.Kind != staging.PendingChmod) {
				t.Fatalf("successful chmod retry lost its existing-data marker: %+v", meta)
			}
			if puts := b.Requests()["PUT /v1/fs/f"]; puts != putsBefore {
				t.Fatalf("no-data retry issued another PUT: before=%d after=%d", putsBefore, puts)
			}
			if after, exists := wb.landedAttr("/f"); exists != hadLanding || (exists && (after.Rev != landingBefore.Rev || after.Size != landingBefore.Size)) {
				t.Fatal("no-data retry changed the prior size/revision watermark")
			}
		})
	}
}

func TestCreateChmodRetrySyncDoesNotRepublishContent(t *testing.T) {
	for _, policy := range []Policy{PolicyCloseSync, PolicyWriteBack} {
		t.Run(policy.String(), func(t *testing.T) {
			ctx := context.Background()
			b := testbackend.NewBackend()
			recorder := &methodRecorder{}
			var rejectMode atomic.Bool
			rejectMode.Store(true)
			handler := recorder.wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("chmod") && rejectMode.Load() {
					http.Error(w, "chmod denied", http.StatusInternalServerError)
					return
				}
				b.ServeHTTP(w, r)
			}))
			cache, err := staging.NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			chain, wb := newUploadStack(t, handler, Options{Policy: policy, WriteCache: cache})
			stopSSETestWriteback(t, wb)
			h := createModeFile(t, chain, "/f", 0o600)
			want := []byte("content survives chmod retry")
			if err := h.Write(ctx, 0, want); err != nil {
				t.Fatal(err)
			}
			firstErr := h.Flush(ctx)
			if policy == PolicyWriteBack {
				if firstErr != nil {
					t.Fatal(firstErr)
				}
				firstErr = h.Sync(ctx)
			}
			if firstErr == nil {
				t.Fatal("rejected post-upload chmod returned success")
			}
			landed, exists := b.Node("/f")
			if !exists || !bytes.Equal(landed.Data, want) {
				t.Fatal("failed chmod discarded the successfully uploaded content")
			}
			if meta, exists := cache.GetMeta("/f"); !exists || meta.Kind != staging.PendingChmod {
				t.Fatalf("failed chmod left no retry marker: %+v exists=%v", meta, exists)
			}
			rejectMode.Store(false)
			if err := h.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			after, exists := b.Node("/f")
			if !exists || !bytes.Equal(after.Data, want) || after.Rev != landed.Rev || after.Mode&0o777 != 0o600 {
				t.Fatalf("chmod retry/release changed the committed image: %+v exists=%v", after, exists)
			}
			if puts := recorder.count("PUT"); puts != 1 {
				t.Fatalf("chmod retry/release published content %d times, want 1", puts)
			}
		})
	}
}

func TestDrainWithoutDataUploadPreservesUnlandedBuffer(t *testing.T) {
	for _, boundary := range []string{"drain", "sync"} {
		t.Run(boundary, func(t *testing.T) {
			ctx := context.Background()
			b := testbackend.NewBackend()
			chain, wb, _ := newWriteCacheClientStack(t, PolicyWriteBack, b, Options{})
			stopSSETestWriteback(t, wb)
			h := writebackHandleOf(dispatchOpen(t, chain, "/f", uint32(syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR)))
			want := bytes.Repeat([]byte("a"), 65536)
			if err := h.Write(ctx, 0, want); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			// Drop only the redundant staged payload; the open handle still owns
			// the full acknowledged image and must publish it at Release.
			if err := os.Remove(wb.wbCache.DatFile("/f")); err != nil {
				t.Fatal(err)
			}
			wb.wbCache.DropInMemoryPayload("/f")
			if _, exists := wb.wbCache.GetMeta("/f"); !exists {
				t.Fatal("missing payload must leave metadata for the upload's no-op path")
			}
			barrier := h.DrainFlush
			if boundary == "sync" {
				barrier = h.Sync
			}
			// The injected cache fault checks retained bytes, not whether
			// strict fsync should return an error or recover the upload.
			if err := barrier(ctx); err != nil {
				t.Logf("%s with a missing cache payload returned: %v", boundary, err)
			}
			h.mu.Lock()
			landed := h.landed
			h.mu.Unlock()
			if landed {
				if node, exists := b.Node("/f"); !exists || !bytes.Equal(node.Data, want) {
					t.Error("barrier marked the file landed without its complete remote image")
				}
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			node, exists := b.Node("/f")
			if !exists || !bytes.Equal(node.Data, want) {
				t.Fatalf("Release lost the unlanded image: remote bytes=%d exists=%v; want %d", len(node.Data), exists, len(want))
			}
		})
	}
}

func TestFlushWithoutSnapshotCallers(t *testing.T) {
	t.Run("close-sync-direct-truncate", func(t *testing.T) {
		ctx := context.Background()
		b := testbackend.NewBackend()
		b.Seed("/f", []byte("remote"))
		chain, wb, _ := newWriteCacheClientStack(t, PolicyCloseSync, b, Options{})
		stopSSETestWriteback(t, wb)
		// This component-only shape omits the kernel's separate SETATTR
		// for O_TRUNC, deliberately exercising Flush's defensive no-op.
		h := writebackHandleOf(dispatchOpen(t, chain, "/f", uint32(syscall.O_TRUNC|syscall.O_WRONLY)))
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if _, exists := wb.wbCache.GetMeta("/f"); exists {
			t.Fatal("nil-snapshot Flush created cache metadata")
		}
		if puts := b.Requests()["PUT /v1/fs/f"]; puts != 0 {
			t.Fatalf("nil-snapshot Flush issued %d uploads", puts)
		}
		h.mu.Lock()
		staged := h.stagedToCache
		h.mu.Unlock()
		if staged {
			t.Fatal("nil-snapshot Flush marked the handle staged")
		}
		if node, exists := b.Node("/f"); !exists || string(node.Data) != "remote" {
			t.Fatal("defensive Flush changed the remote image before Release")
		}
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
		node, exists := b.Node("/f")
		if !exists || len(node.Data) != 0 || b.Requests()["PUT /v1/fs/f"] != 1 {
			t.Fatalf("Release did not publish the deferred truncation exactly once: bytes=%d exists=%v", len(node.Data), exists)
		}
	})
	t.Run("writeback-synthetic-dirty-without-buffer", func(t *testing.T) {
		ctx := context.Background()
		b := testbackend.NewBackend()
		b.Seed("/f", []byte("remote"))
		chain, wb, _ := newWriteCacheClientStack(t, PolicyWriteBack, b, Options{})
		stopSSETestWriteback(t, wb)
		h := writebackHandleOf(dispatchOpen(t, chain, "/f", uint32(syscall.O_WRONLY)))
		// Normal invalidation preserves dirty buffers; force the defensive
		// caller state without claiming an SSE transition creates it.
		h.mu.Lock()
		h.dirty = true
		h.mu.Unlock()
		if err := h.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		h.mu.Lock()
		staged := h.stagedToCache
		h.mu.Unlock()
		if staged {
			t.Fatal("nil-snapshot Flush marked the handle staged")
		}
		if _, exists := wb.wbCache.GetMeta("/f"); exists || b.Requests()["PUT /v1/fs/f"] != 0 {
			t.Fatal("nil-snapshot Flush published a synthetic image")
		}
		if err := h.Release(ctx); err != nil {
			t.Fatal(err)
		}
		node, exists := b.Node("/f")
		if !exists || string(node.Data) != "remote" || b.Requests()["PUT /v1/fs/f"] != 0 {
			t.Fatal("defensive no-buffer state changed the remote image")
		}
	})
}
