package writeback

import (
	"bytes"
	"context"
	"errors"
	"hash/crc32"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestUploadAcknowledgesNewCreateAcrossHandles(t *testing.T) {
	for _, background := range []bool{false, true} {
		name := "sync"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			backend := testbackend.NewBackend()
			entered, allow := make(chan struct{}), make(chan struct{})
			var once, enteredOnce sync.Once
			unblock := func() { once.Do(func() { close(allow) }) }
			defer unblock()
			chain, wb := newReviewDrainStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					enteredOnce.Do(func() { close(entered) })
					select {
					case <-allow:
					case <-ctx.Done():
						return
					}
				}
				backend.ServeHTTP(w, r)
			}), nil)
			h1, want := stageReviewDrainFile(t, ctx, chain)
			var h2 fs.Handle
			type opened struct {
				handle fs.Handle
				err    error
			}
			openDone := make(chan opened, 1)
			if background {
				h2 = dispatchOpen(t, chain, "/f", uint32(syscall.O_RDONLY))
				wb.uploader.Submit("/f")
			} else {
				go func() {
					result, err := chain.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
					openDone <- opened{handle: result.Handle, err: err}
				}()
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("winning consumer did not enter its PUT")
			}
			synced := make(chan error, 1)
			go func() { synced <- h1.Sync(ctx) }()
			waitP2SyncUploadGate(t, ctx)
			unblock()
			select {
			case err := <-synced:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("losing Sync did not finish")
			}
			if !background {
				select {
				case result := <-openDone:
					if result.err != nil || result.handle == nil {
						t.Fatalf("second open: handle=%v err=%v", result.handle, result.err)
					}
					h2 = result.handle
				case <-ctx.Done():
					t.Fatal("second open did not finish")
				}
			}
			if err := h2.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h1.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, want)
			if puts := backend.Requests()["PUT /v1/fs/f"]; puts != 1 {
				t.Fatalf("content PUTs = %d after two closes, want one successful publication", puts)
			}
		})
	}
}

func waitP2SyncUploadGate(t *testing.T, ctx context.Context) {
	t.Helper()
	stack := make([]byte, 1<<20)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "TestUploadAcknowledgesNewCreateAcrossHandles.func") &&
				strings.Contains(goroutine, "(*bufferingHandle).Sync") &&
				strings.Contains(goroutine, "(*WriteBackUploader).acquirePath") {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("losing Sync did not reach the winning consumer's gate")
		}
	}
}

func TestUploadAcknowledgementPreservesNewerMutation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		truncate bool
		flush    bool
	}{
		{name: "write"},
		{name: "truncate", truncate: true},
		{name: "flushed-write", flush: true},
		{name: "flushed-truncate", truncate: true, flush: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			backend := testbackend.NewBackend()
			entered, allow := make(chan struct{}), make(chan struct{})
			var enteredOnce, allowOnce sync.Once
			unblock := func() { allowOnce.Do(func() { close(allow) }) }
			defer unblock()
			chain, wb := newReviewDrainStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut {
					enteredOnce.Do(func() { close(entered) })
					select {
					case <-allow:
					case <-ctx.Done():
						return
					}
				}
				backend.ServeHTTP(w, r)
			}), nil)
			h, first := stageReviewDrainFile(t, ctx, chain)
			done := make(chan error, 1)
			go func() { done <- wb.uploader.UploadSync(ctx, "/f") }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("upload did not start")
			}
			want := append(append([]byte(nil), first...), []byte("later")...)
			if tc.truncate {
				want = first[:len(first)/2]
				if err := h.truncateLocal(ctx, int64(len(want))); err != nil {
					t.Fatal(err)
				}
			} else if err := h.Write(ctx, int64(len(first)), []byte("later")); err != nil {
				t.Fatal(err)
			}
			if tc.flush {
				if err := h.Flush(ctx); err != nil {
					t.Fatal(err)
				}
			}
			h.mu.Lock()
			buf, gen, visTrunc := h.buf, h.wgen, h.visTrunc
			dirty := h.dirty
			h.mu.Unlock()
			unblock()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("upload did not complete")
			}
			h.mu.Lock()
			preserved := h.dirty == dirty && h.buf == buf && h.wgen == gen && h.visTrunc == visTrunc
			acknowledged := h.landed && !h.isNew
			h.mu.Unlock()
			if !preserved || !acknowledged {
				t.Fatalf("new mutation preserved=%v, published name acknowledged=%v", preserved, acknowledged)
			}
			if tc.flush {
				committed, _ := backend.Node("/f")
				meta, exists := wb.wbCache.GetMeta("/f")
				if !exists || meta.Kind != staging.PendingOverwrite || meta.BaseRev != committed.Rev {
					t.Fatalf("newer staged image = %+v exists=%v; want overwrite of actual revision %d", meta, exists, committed.Rev)
				}
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, want)
			if puts := backend.Requests()["PUT /v1/fs/f"]; puts != 2 {
				t.Fatalf("content PUTs = %d, want one per acknowledged image", puts)
			}
		})
	}
}

func TestMissingUploadDoesNotAcknowledgeFromOldLanding(t *testing.T) {
	for _, missingPayload := range []bool{false, true} {
		name := "no-cache"
		if missingPayload {
			name = "missing-payload"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			backend := testbackend.NewBackend()
			backend.Seed("/f", []byte("former object"))
			old, _ := backend.Node("/f")
			chain, wb := newReviewDrainStack(t, backend, nil)
			wb.landedAt("/f", old.Rev, int64(len(old.Data)), crc32.ChecksumIEEE(old.Data), old.Data)
			wb.landedMu.Lock()
			info := wb.landed["/f"]
			info.at = time.Now().Add(-time.Minute)
			wb.landed["/f"] = info
			wb.landedMu.Unlock()
			if err := wb.opts.Client.DeleteCtx(ctx, "/f"); err != nil {
				t.Fatal(err)
			}
			h, want := stageReviewDrainFile(t, ctx, chain)
			if missingPayload {
				if err := os.Remove(wb.wbCache.DatFile("/f")); err != nil {
					t.Fatal(err)
				}
				wb.wbCache.DropInMemoryPayload("/f")
			} else {
				wb.wbCache.Remove("/f")
			}
			if err := h.Sync(ctx); err != nil {
				t.Logf("missing redundant cache: %v", err)
			}
			h.mu.Lock()
			retained := h.isNew && !h.landed && h.buf != nil && bytes.Equal(h.buf.Bytes(), want)
			h.mu.Unlock()
			if !retained {
				t.Fatal("a historical landing acknowledged a new object's missing upload")
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, want)
			if puts := backend.Requests()["PUT /v1/fs/f"]; puts != 1 {
				t.Fatalf("new-object PUTs = %d, want 1", puts)
			}
		})
	}
}

func TestChmodOnlyConsumerAcknowledgesNewCreate(t *testing.T) {
	ctx := context.Background()
	backend := testbackend.NewBackend()
	var rejectMode atomic.Bool
	rejectMode.Store(true)
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bus := fs.NewEventBus()
	var contentEvents atomic.Int32
	bus.On(fs.EventLanded, func(fs.Event) { contentEvents.Add(1) })
	chain, wb := newUploadStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("chmod") && rejectMode.Load() {
			http.Error(w, "chmod denied", http.StatusInternalServerError)
			return
		}
		backend.ServeHTTP(w, r)
	}), Options{Policy: PolicyWriteBack, WriteCache: cache, Events: bus})
	stopSSETestWriteback(t, wb)
	h := createModeFile(t, chain, "/f", 0o600)
	want := []byte("content already committed before chmod retry")
	if err := h.Write(ctx, 0, want); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.uploader.UploadSync(ctx, "/f"); err == nil {
		t.Fatal("denied chmod did not fail the first upload")
	}
	first, exists := backend.Node("/f")
	if !exists || !bytes.Equal(first.Data, want) {
		t.Fatal("data PUT did not commit before the failed chmod")
	}
	rejectMode.Store(false)
	if err := wb.uploader.UploadSync(ctx, "/f"); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	after, exists := backend.Node("/f")
	if !exists || !bytes.Equal(after.Data, want) || after.Rev != first.Rev || after.Mode&0o777 != 0o600 {
		t.Fatalf("chmod-only completion changed content: %+v exists=%v", after, exists)
	}
	if puts := backend.Requests()["PUT /v1/fs/f"]; puts != 1 {
		t.Fatalf("content PUTs = %d after chmod-only completion, want 1", puts)
	}
	if got := contentEvents.Load(); got != 1 {
		t.Fatalf("content events = %d, want only the actual data PUT", got)
	}
	if attr, exists := wb.landedAttr("/f"); !exists || attr.Rev != first.Rev || attr.Size != int64(len(want)) {
		t.Fatalf("content watermark=%+v exists=%v; want the original data PUT", attr, exists)
	}
}

func TestFailedChmodAcknowledgesActualRevisionBeforeForeignWrite(t *testing.T) {
	for _, background := range []bool{false, true} {
		name := "sync"
		if background {
			name = "background"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			backend := testbackend.NewBackend()
			var rejectMode atomic.Bool
			rejectMode.Store(true)
			cache, err := staging.NewWriteBackCache(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			chain, wb := newUploadStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Has("chmod") && rejectMode.Load() {
					http.Error(w, "chmod denied", http.StatusInternalServerError)
					return
				}
				backend.ServeHTTP(w, r)
			}), Options{Policy: PolicyWriteBack, WriteCache: cache})
			stopSSETestWriteback(t, wb)
			h := createModeFile(t, chain, "/f", 0o600)
			if err := h.Write(ctx, 0, []byte("local")); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if background {
				wb.uploader.Submit("/f")
				if err := wb.uploader.WaitIdle(ctx); err != nil {
					t.Fatal(err)
				}
			} else if err := wb.uploader.UploadSync(ctx, "/f"); err == nil {
				t.Fatal("the failed chmod must still be reported")
			}
			committed, exists := backend.Node("/f")
			if !exists || string(committed.Data) != "local" {
				t.Fatal("the data PUT did not commit")
			}
			if meta, exists := cache.GetMeta("/f"); !exists || meta.Kind != staging.PendingChmod {
				t.Fatalf("chmod retry source = %+v exists=%v", meta, exists)
			}
			foreign := []byte("foreign replacement")
			if err := wb.opts.Client.WriteCtx(ctx, "/f", foreign); err != nil {
				t.Fatal(err)
			}
			rejectMode.Store(false)
			if err := wb.uploader.UploadSync(ctx, "/f"); err != nil {
				t.Fatal(err)
			}
			if err := h.Sync(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h.Write(ctx, 0, []byte("LOCAL")); err != nil {
				t.Fatal(err)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			meta, exists := cache.GetMeta("/f")
			if !exists || meta.BaseRev != committed.Rev {
				t.Fatalf("next write base=%+v exists=%v; want own committed revision %d", meta, exists, committed.Rev)
			}
			if err := h.Sync(ctx); !errors.Is(err, client.ErrConflict) {
				t.Fatalf("stale write returned %v, want a revision conflict", err)
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.uploader.WaitIdle(ctx); err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, foreign)
		})
	}
}

func TestUploadCompletionDoesNotSeedNewerBytesAtOldRevision(t *testing.T) {
	backend := testbackend.NewBackend()
	oldImage, newerImage := []byte("old"), []byte("NEW")
	backend.Seed("/f", oldImage)
	committed, _ := backend.Node("/f")
	_, wb, _ := newWriteCacheClientStack(t, PolicyWriteBack, backend, Options{})
	stopSSETestWriteback(t, wb)
	if err := wb.wbCache.PutWithBaseRev("/f", oldImage, int64(len(oldImage)), staging.PendingNew, 0); err != nil {
		t.Fatal(err)
	}
	uploaded, _ := wb.wbCache.GetMeta("/f")
	if err := wb.wbCache.PutWithBaseRev("/f", newerImage, int64(len(newerImage)), staging.PendingOverwrite, committed.Rev); err != nil {
		t.Fatal(err)
	}
	wb.SetCommitSeedLimit(int64(len(oldImage)))
	wb.SetCommitSeeder(func(path string, data []byte, rev int64) {
		if path == "/f" && rev == committed.Rev && !bytes.Equal(data, oldImage) {
			t.Errorf("seeded %q under old revision %d; that revision contains %q", data, rev, oldImage)
		}
	})
	wb.uploader.OnSuccess(*uploaded, committed.Rev, StagingGens{WriteBackGen: uploaded.Generation})
}

func TestReleaseStagesAfterWinningUploadConsumesCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend := testbackend.NewBackend()
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var conflicts atomic.Int32
	chain, wb := newUploadStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		backend.ServeHTTP(rec, r)
		if rec.Code == http.StatusConflict {
			conflicts.Add(1)
		}
		for name, values := range rec.Header() {
			w.Header()[name] = values
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())
	}), Options{Policy: PolicyWriteBack, WriteCache: cache})
	stopSSETestWriteback(t, wb)
	h, first := stageReviewDrainFile(t, ctx, chain)
	consumed, allowAck := make(chan struct{}), make(chan struct{})
	var ackOnce sync.Once
	unblockAck := func() { ackOnce.Do(func() { close(allowAck) }) }
	defer unblockAck()
	onSuccess := wb.uploader.OnSuccess
	wb.uploader.OnSuccess = func(meta staging.WriteBackMeta, rev int64, gens StagingGens) {
		close(consumed)
		<-allowAck
		onSuccess(meta, rev, gens)
	}
	uploaded := make(chan error, 1)
	go func() { uploaded <- wb.uploader.UploadSync(ctx, "/f") }()
	select {
	case <-consumed:
	case <-ctx.Done():
		t.Fatal("winner did not consume its cache image")
	}
	want := append(append([]byte(nil), first...), []byte("later close")...)
	if err := h.Write(ctx, int64(len(first)), []byte("later close")); err != nil {
		t.Fatal(err)
	}
	unlock := wb.pathLocks.lock("/f")
	var pathOnce sync.Once
	releasePath := func() { pathOnce.Do(unlock) }
	defer releasePath()
	released := make(chan error, 1)
	go func() { released <- h.Release(ctx) }()
	waitP2ReleaseUploadGate(t, ctx)
	if _, registered := wb.liveSet.Load(h); !registered {
		t.Error("closing handle disappeared before its durability boundary")
	}
	releasePath()
	unblockAck()
	select {
	case err := <-released:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Release did not stage its newer image")
	}
	select {
	case err := <-uploaded:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("winner did not finish its notification")
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	node, exists := backend.Node("/f")
	queued, _ := wb.QueuePendingStats()
	snapshot := wb.uploader.Snapshot()
	puts := backend.Requests()["PUT /v1/fs/f"]
	if !exists || !bytes.Equal(node.Data, want) || puts != 2 || conflicts.Load() != 0 || queued != 0 || snapshot.Cached != 0 || wb.opts.Staging.Pending.HasPending("/f") {
		t.Fatalf("Release overlap: remote=%d bytes exists=%v PUTs=%d conflicts=%d queued=%d cached=%d; want complete %d-byte image, two PUTs and no pending", len(node.Data), exists, puts, conflicts.Load(), queued, snapshot.Cached, len(want))
	}
}

func waitP2ReleaseUploadGate(t *testing.T, ctx context.Context) {
	t.Helper()
	stack := make([]byte, 1<<20)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		n := runtime.Stack(stack, true)
		for _, goroutine := range strings.Split(string(stack[:n]), "\n\n") {
			if strings.Contains(goroutine, "TestReleaseStagesAfterWinningUploadConsumesCache.func") &&
				strings.Contains(goroutine, "(*bufferingHandle).Release") &&
				strings.Contains(goroutine, "(*WriteBackUploader).acquirePath") {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("Release did not reach the winning consumer's upload gate")
		}
	}
}
