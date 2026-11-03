package writeback

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
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

func TestSSECloseSyncEmptyCreateDuringStaging(t *testing.T) {
	for _, kind := range []fs.EventKind{fs.EventCheckout, fs.EventUnverified} {
		t.Run(kind.String(), func(t *testing.T) {
			b := testbackend.NewBackend()
			bus := fs.NewEventBus()
			chain, wb, _ := newWriteCacheClientStack(t, PolicyCloseSync, b, Options{Events: bus})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stopSSETestWriteback(t, wb)
			h := dispatchOpen(t, chain, "/empty", uint32(syscall.O_CREAT|syscall.O_EXCL|syscall.O_WRONLY))
			var invalidations int
			wb.SetPendingSeeder(func(path string, _ []byte) {
				if path == "/empty" {
					invalidations++
					bus.Emit(fs.Event{Kind: kind, Path: "/"})
				}
			})
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if invalidations != 1 {
				t.Fatalf("staging invalidations = %d, want 1", invalidations)
			}
			committed, ok := b.Node("/empty")
			if !ok || len(committed.Data) != 0 || committed.Rev <= 0 {
				t.Fatalf("empty creation = %+v, exists=%v", committed, ok)
			}
			if err := h.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := h.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if err := wb.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			if node, ok := b.Node("/empty"); !ok || len(node.Data) != 0 || node.Rev != committed.Rev {
				t.Fatalf("empty file = %+v, exists=%v; want original revision %d", node, ok, committed.Rev)
			}
			if attr, ok := wb.landedAttr("/empty"); !ok || attr.Size != 0 || attr.Rev != committed.Rev {
				t.Fatalf("landed attr = %+v, exists=%v; want size 0 revision %d", attr, ok, committed.Rev)
			}
			if puts := b.Requests()["PUT /v1/fs/empty"]; puts != 1 {
				t.Fatalf("empty publications = %d, want 1", puts)
			}
			probe := open(t, chain, "/probe")
			if err := probe.Write(ctx, 0, []byte("healthy")); err != nil {
				t.Fatal(err)
			}
			if err := probe.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := probe.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if node, ok := b.Node("/probe"); !ok || string(node.Data) != "healthy" {
				t.Fatalf("subsequent file = %q, exists=%v", node.Data, ok)
			}
		})
	}
}

func TestSSEInvalidationDuringSynchronousCacheUpload(t *testing.T) {
	for _, policy := range []Policy{PolicyCloseSync, PolicyWriteBack} {
		for _, kind := range []fs.EventKind{fs.EventCheckout, fs.EventUnverified} {
			t.Run(policy.String()+"/"+kind.String(), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				b := testbackend.NewBackend()
				b.Seed("/file", []byte("old"))
				bus := fs.NewEventBus()
				var puts atomic.Int32
				ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/file" {
						puts.Add(1)
						bus.Emit(fs.Event{Kind: kind, Path: "/"})
					}
					b.ServeHTTP(w, r)
				}))
				t.Cleanup(ts.Close)
				cl := client.New(ts.URL, "")
				cache, err := staging.NewWriteBackCache(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				wb, err := New(Options{Policy: policy, Client: cl, Events: bus, WriteCache: cache})
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
				h := dispatchOpen(t, chain, "/file", uint32(syscall.O_RDWR))
				want := bytes.Repeat([]byte("staged"), 4096)
				if err := h.Write(ctx, 0, want); err != nil {
					t.Fatal(err)
				}
				if err := h.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if policy == PolicyWriteBack {
					if got := puts.Load(); got != 0 {
						t.Fatalf("Flush published %d times before Sync", got)
					}
					if err := h.Sync(ctx); err != nil {
						t.Fatal(err)
					}
				}
				committed, ok := b.Node("/file")
				if !ok || !bytes.Equal(committed.Data, want) || committed.Rev <= 1 {
					t.Fatalf("uploaded size=%d revision=%d exists=%v; want complete %d-byte overwrite", len(committed.Data), committed.Rev, ok, len(want))
				}
				if attr, ok := wb.landedAttr("/file"); !ok || attr.Size != int64(len(want)) || attr.Rev != committed.Rev {
					t.Fatalf("landed attr = %+v, exists=%v; want size %d revision %d", attr, ok, len(want), committed.Rev)
				}
				if err := h.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if err := h.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if err := wb.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				if node, ok := b.Node("/file"); !ok || !bytes.Equal(node.Data, want) || node.Rev != committed.Rev {
					t.Fatalf("remote size=%d revision=%d exists=%v; want complete %d-byte revision %d", len(node.Data), node.Rev, ok, len(want), committed.Rev)
				}
				if got := puts.Load(); got != 1 {
					t.Fatalf("publications = %d, want 1", got)
				}
			})
		}
	}
}

func TestSSEStageWithoutBufferDoesNotPublish(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file", []byte("remote"))
	chain, wb, _ := newWriteCacheClientStack(t, PolicyCloseSync, b, Options{})
	stopSSETestWriteback(t, wb)
	ctx := context.Background()
	h := dispatchOpen(t, chain, "/file", uint32(syscall.O_RDWR))
	buffered := writebackHandleOf(h)
	// The helper permits an existing handle with no buffer; success must
	// remain a no-op, never a synthetic zero-byte staging publication.
	snapshot, err := buffered.stageToWriteCache(ctx, false)
	if err != nil || snapshot != nil {
		t.Fatalf("no-op stage returned snapshot=%+v err=%v", snapshot, err)
	}
	if meta, ok := wb.wbCache.GetMeta("/file"); ok {
		t.Fatalf("no-op created cache entry %+v", meta)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if node, ok := b.Node("/file"); !ok || string(node.Data) != "remote" || node.Rev != 1 {
		t.Fatalf("no-op changed remote: %+v exists=%v", node, ok)
	}
}

func TestSSECloseSyncStagePreservesNewerWrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file", []byte("old"))
	chain, wb, _ := newWriteCacheClientStack(t, PolicyCloseSync, b, Options{})
	stopSSETestWriteback(t, wb)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := dispatchOpen(t, chain, "/file", uint32(syscall.O_RDWR))
	first, tail := []byte("first snapshot"), []byte(" plus later write")
	if err := h.Write(ctx, 0, first); err != nil {
		t.Fatal(err)
	}
	var stages int
	wb.SetPendingSeeder(func(path string, _ []byte) {
		if path == "/file" {
			stages++
			if stages == 1 {
				if err := h.Write(ctx, int64(len(first)), tail); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	firstCommit, ok := b.Node("/file")
	if !ok || !bytes.Equal(firstCommit.Data, first) || firstCommit.Rev <= 1 {
		t.Fatalf("first upload = %q revision=%d exists=%v; want the captured snapshot", firstCommit.Data, firstCommit.Rev, ok)
	}
	if attr, ok := wb.landedAttr("/file"); !ok || attr.Size != int64(len(first)) || attr.Rev != firstCommit.Rev {
		t.Fatalf("first landed attr = %+v, exists=%v; want snapshot size %d revision %d", attr, ok, len(first), firstCommit.Rev)
	}
	buffered := writebackHandleOf(h)
	buffered.mu.Lock()
	dirty := buffered.dirty
	buffered.mu.Unlock()
	if !dirty {
		t.Fatal("first snapshot retired a write acknowledged during staging")
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), first...), tail...)
	secondCommit, ok := b.Node("/file")
	if !ok || !bytes.Equal(secondCommit.Data, want) || secondCommit.Rev <= firstCommit.Rev {
		t.Fatalf("second upload = %q revision=%d exists=%v; want %q at a newer revision", secondCommit.Data, secondCommit.Rev, ok, want)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if node, ok := b.Node("/file"); !ok || !bytes.Equal(node.Data, want) || node.Rev != secondCommit.Rev {
		t.Fatalf("final upload = %q revision=%d exists=%v; want %q revision %d", node.Data, node.Rev, ok, want, secondCommit.Rev)
	}
	if attr, ok := wb.landedAttr("/file"); !ok || attr.Size != int64(len(want)) || attr.Rev != secondCommit.Rev {
		t.Fatalf("landed attr = %+v, exists=%v; want size %d revision %d", attr, ok, len(want), secondCommit.Rev)
	}
	if puts := b.Requests()["PUT /v1/fs/file"]; puts != 2 {
		t.Fatalf("publications = %d, want two snapshots", puts)
	}
}

func stopSSETestWriteback(t *testing.T, wb *Writeback) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := wb.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
}
