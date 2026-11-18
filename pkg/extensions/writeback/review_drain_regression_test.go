package writeback

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func newReviewDrainStack(t *testing.T, handler http.Handler, cache *staging.WriteBackCache) (*fs.Chain, *Writeback) {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	if cache == nil {
		var err error
		cache, err = staging.NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: cache})
	if err != nil {
		t.Fatal(err)
	}
	stopSSETestWriteback(t, wb)
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	chain, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return chain, wb
}

func stageReviewDrainFile(t *testing.T, ctx context.Context, chain *fs.Chain) (*bufferingHandle, []byte) {
	t.Helper()
	h := writebackHandleOf(dispatchOpen(t, chain, "/f", uint32(syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR)))
	first := bytes.Repeat([]byte("a"), 65536)
	if err := h.Write(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	return h, first
}

func assertReviewDrainRemote(t *testing.T, backend *testbackend.Backend, want []byte) {
	t.Helper()
	node, ok := backend.Node("/f")
	if !ok || !bytes.Equal(node.Data, want) {
		t.Fatalf("remote exists=%v size=%d, want complete %d-byte image", ok, len(node.Data), len(want))
	}
}

func TestDrainRestagesWritesAfterFlush(t *testing.T) {
	for _, planeDrain := range []bool{false, true} {
		name := "handle"
		if planeDrain {
			name = "plane"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			backend := testbackend.NewBackend()
			chain, wb := newReviewDrainStack(t, backend, nil)
			h, first := stageReviewDrainFile(t, ctx, chain)
			tail := []byte("0123456789abcdef")
			if err := h.Write(ctx, int64(len(first)), tail); err != nil {
				t.Fatal(err)
			}
			var err error
			if planeDrain {
				err = wb.Drain(ctx)
			} else {
				err = h.DrainFlush(ctx)
			}
			if err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, append(first, tail...))
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, append(first, tail...))
		})
	}
}

func TestDrainUploadPreservesInFlightWrite(t *testing.T) {
	for _, tc := range []struct {
		name             string
		dirtyBeforeDrain bool
		truncate         bool
	}{
		{name: "clean_staged"},
		{name: "dirty_restage", dirtyBeforeDrain: true},
		{name: "truncate", truncate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			backend := testbackend.NewBackend()
			entered := make(chan []byte, 1)
			allow := make(chan struct{})
			var release sync.Once
			unblock := func() { release.Do(func() { close(allow) }) }
			defer unblock()
			var firstPut atomic.Bool
			chain, wb := newReviewDrainStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && firstPut.CompareAndSwap(false, true) {
					data, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}
					r.Body = io.NopCloser(bytes.NewReader(data))
					entered <- data
					select {
					case <-allow:
					case <-r.Context().Done():
						return
					}
				}
				backend.ServeHTTP(w, r)
			}), nil)
			h, before := stageReviewDrainFile(t, ctx, chain)
			if tc.dirtyBeforeDrain {
				tail := []byte("0123456789abcdef")
				if err := h.Write(ctx, int64(len(before)), tail); err != nil {
					t.Fatal(err)
				}
				before = append(before, tail...)
			}
			done := make(chan error, 1)
			go func() { done <- h.DrainFlush(ctx) }()
			select {
			case uploaded := <-entered:
				if !bytes.Equal(uploaded, before) {
					t.Errorf("drain captured %d bytes, want the complete %d-byte pre-drain image", len(uploaded), len(before))
				}
			case err := <-done:
				t.Fatalf("drain returned before its upload gate: %v", err)
			case <-ctx.Done():
				t.Fatal("drain did not reach its upload gate")
			}
			tail := []byte("write during upload")
			want := append(append([]byte(nil), before...), tail...)
			if tc.truncate {
				want = before[:len(before)/2]
				if err := h.truncateLocal(ctx, int64(len(want))); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := h.Write(ctx, int64(len(before)), tail); err != nil {
					t.Fatal(err)
				}
			}
			unblock()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("drain did not complete")
			}
			h.mu.Lock()
			dirty, visTrunc := h.dirty, h.visTrunc
			h.mu.Unlock()
			if !dirty {
				t.Error("upload completion retired the mutation acknowledged during upload")
			}
			if tc.truncate && !visTrunc {
				t.Error("upload completion retired the newer visible truncate")
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, want)
		})
	}
}

func TestDrainRestagePreservesWriteDuringStaging(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend := testbackend.NewBackend()
	chain, wb := newReviewDrainStack(t, backend, nil)
	h, first := stageReviewDrainFile(t, ctx, chain)
	tail, later := []byte("0123456789abcdef"), []byte("write during restage")
	if err := h.Write(ctx, int64(len(first)), tail); err != nil {
		t.Fatal(err)
	}
	before := append(first, tail...)
	var stages int
	var writeErr error
	wb.SetPendingSeeder(func(path string, _ []byte) {
		if path == "/f" {
			stages++
			if stages == 1 {
				writeErr = h.Write(ctx, int64(len(before)), later)
			}
		}
	})
	if err := h.DrainFlush(ctx); err != nil {
		t.Fatal(err)
	}
	wb.SetPendingSeeder(nil)
	if stages != 1 || writeErr != nil {
		t.Fatalf("restaging callback count=%d write error=%v; want one successful concurrent write", stages, writeErr)
	}
	h.mu.Lock()
	dirty := h.dirty
	h.mu.Unlock()
	if !dirty {
		t.Error("drain retired a write acknowledged after its staging snapshot")
	}
	assertReviewDrainRemote(t, backend, before)
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	assertReviewDrainRemote(t, backend, append(before, later...))
}

func TestDrainFailuresRemainRetryable(t *testing.T) {
	for _, failure := range []string{"stage", "upload"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			backend := testbackend.NewBackend()
			var rejectUpload atomic.Bool
			root := t.TempDir()
			cacheDir := filepath.Join(root, "pending")
			cache, err := staging.NewWriteBackCache(cacheDir)
			if err != nil {
				t.Fatal(err)
			}
			chain, wb := newReviewDrainStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPut && rejectUpload.Load() {
					http.Error(w, "injected upload failure", http.StatusServiceUnavailable)
					return
				}
				backend.ServeHTTP(w, r)
			}), cache)
			h, first := stageReviewDrainFile(t, ctx, chain)
			tail := []byte("0123456789abcdef")
			if err := h.Write(ctx, int64(len(first)), tail); err != nil {
				t.Fatal(err)
			}
			want := append(first, tail...)
			savedDir := filepath.Join(root, "saved-pending")
			restoreCache := func() {
				t.Helper()
				if _, err := os.Stat(savedDir); os.IsNotExist(err) {
					return
				}
				if _, err := os.Stat(cacheDir); err == nil {
					if err := os.Rename(cacheDir, filepath.Join(root, "used-blocker")); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Rename(savedDir, cacheDir); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(restoreCache)
			if failure == "stage" {
				if err := os.Rename(cacheDir, savedDir); err != nil {
					t.Fatal(err)
				}
				// A regular file at the cache root rejects Put without losing
				// the previous snapshot, which is restored before the retry.
				if err := os.WriteFile(cacheDir, []byte("block staging"), 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				rejectUpload.Store(true)
			}
			if err := h.DrainFlush(ctx); err == nil {
				t.Error("drain reported success despite the injected failure")
			}
			rejectUpload.Store(false)
			restoreCache()
			h.mu.Lock()
			dirty := h.dirty
			h.mu.Unlock()
			cached, cachedOK := cache.Get("/f")
			if !dirty && (!cachedOK || !bytes.Equal(cached, want)) {
				t.Error("failed drain left neither a dirty handle nor the complete cached image")
			}
			if err := h.DrainFlush(ctx); err != nil {
				t.Fatalf("retry drain: %v", err)
			}
			assertReviewDrainRemote(t, backend, want)
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			assertReviewDrainRemote(t, backend, want)
		})
	}
}

func TestDrainCacheLookupPreservesLaterWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend := testbackend.NewBackend()
	chain, wb := newReviewDrainStack(t, backend, nil)
	h, first := stageReviewDrainFile(t, ctx, chain)
	h.mu.Lock()
	staged := h.stagedToCache
	h.mu.Unlock()
	if !staged {
		t.Fatal("Flush did not publish the staged snapshot")
	}
	gate := wb.wbCache.AcquirePathLock("/f")
	held := true
	defer func() {
		if held {
			wb.wbCache.ReleasePathLock("/f", gate)
		}
	}()
	done := make(chan error, 1)
	go func() { done <- h.DrainFlush(ctx) }()
	// The cache lock parks DrainFlush after its initial handle snapshot.
	// Observe that handoff before acknowledging a newer write generation.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		h.mu.Lock()
		consumed := !h.stagedToCache
		h.mu.Unlock()
		if consumed {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("drain returned before acquiring the cache snapshot: %v", err)
		case <-ctx.Done():
			t.Fatal("drain did not consume the staged snapshot")
		case <-ticker.C:
		}
	}
	tail := []byte("0123456789abcdef")
	if err := h.Write(ctx, int64(len(first)), tail); err != nil {
		t.Fatal(err)
	}
	wb.wbCache.ReleasePathLock("/f", gate)
	held = false
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("drain did not complete after releasing the cache snapshot")
	}
	assertReviewDrainRemote(t, backend, first)
	h.mu.Lock()
	dirty := h.dirty
	h.mu.Unlock()
	if !dirty {
		t.Error("drain retired a write acknowledged after its initial generation snapshot")
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	assertReviewDrainRemote(t, backend, append(first, tail...))
}
