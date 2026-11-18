package writeback

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
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

func newSSEDrainStack(t *testing.T, handler http.Handler) (*fs.Chain, *Writeback, *fs.EventBus) {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bus := fs.NewEventBus()
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Events: bus, WriteCache: cache, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := wb.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	chain, err := fs.NewChain(wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return chain, wb, bus
}

func TestSSEDrainReleasePreservesNewFile(t *testing.T) {
	for _, event := range []fs.EventKind{fs.EventCheckout, fs.EventUnverified} {
		t.Run(event.String(), func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				invalidate bool
				drainRace  bool
			}{
				{name: "no_event", drainRace: true},
				{name: "no_drain_overlap", invalidate: true},
				{name: "event_and_drain_overlap", invalidate: true, drainRace: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					backend := testbackend.NewBackend()
					entered, allow, landed := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var release sync.Once
					unblock := func() { release.Do(func() { close(allow) }) }
					defer unblock()
					var puts atomic.Int32
					chain, wb, bus := newSSEDrainStack(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method != http.MethodPut || !tc.drainRace {
							backend.ServeHTTP(w, r)
							return
						}
						n := puts.Add(1)
						gate := landed
						if n == 1 {
							close(entered)
							gate = allow
						}
						select {
						case <-gate:
						case <-ctx.Done():
							return
						}
						backend.ServeHTTP(w, r)
						if n == 1 {
							close(landed)
						}
					}))
					h := writebackHandleOf(dispatchOpen(t, chain, "/one.bin", uint32(syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR)))
					want := bytes.Repeat([]byte("x"), 65536)
					if err := h.Write(ctx, 0, want); err != nil {
						t.Fatal(err)
					}
					if err := h.Flush(ctx); err != nil {
						t.Fatal(err)
					}
					h.mu.Lock()
					ready := h.buf != nil && h.buf.Size() == int64(len(want)) && !h.dirty && h.stagedToCache && h.isNew && !h.landed
					h.mu.Unlock()
					if !ready {
						t.Fatal("new file did not reach the locally staged, unlanded state")
					}
					drainDone := make(chan error, 1)
					if tc.drainRace {
						go func() { drainDone <- h.DrainFlush(ctx) }()
						select {
						case <-entered:
						case <-ctx.Done():
							t.Fatal("drain did not enter its upload")
						}
					}
					if tc.invalidate {
						bus.Emit(fs.Event{Kind: event, Path: "/"})
					}
					if err := h.Release(ctx); err != nil {
						t.Fatal(err)
					}
					if tc.drainRace {
						unblock()
						select {
						case err := <-drainDone:
							if err != nil {
								t.Fatal(err)
							}
						case <-ctx.Done():
							t.Fatal("drain did not complete")
						}
					}
					if err := wb.Drain(ctx); err != nil {
						t.Fatal(err)
					}
					node, ok := backend.Node("/one.bin")
					if !ok || !bytes.Equal(node.Data, want) {
						t.Fatalf("remote exists=%v, bytes=%d after successful Flush/Release/Drain; want the complete %d-byte image", ok, len(node.Data), len(want))
					}
				})
			}
		})
	}
}

func TestSSEAfterDrainPreservesOpenNewFile(t *testing.T) {
	for _, event := range []fs.EventKind{fs.EventCheckout, fs.EventUnverified} {
		for _, probe := range []string{"read", "write"} {
			t.Run(event.String()+"/"+probe, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				backend := testbackend.NewBackend()
				chain, wb, bus := newSSEDrainStack(t, backend)
				h := writebackHandleOf(dispatchOpen(t, chain, "/open.bin", uint32(syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR)))
				want := bytes.Repeat([]byte("x"), 65536)
				if err := h.Write(ctx, 0, want); err != nil {
					t.Fatal(err)
				}
				if err := h.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				if err := wb.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				bus.Emit(fs.Event{Kind: event, Path: "/"})
				if probe == "read" {
					got := make([]byte, len(want))
					n, err := h.Read(ctx, 0, got)
					if err != nil || n != len(want) || !bytes.Equal(got, want) {
						t.Errorf("open handle read after drain/invalidation: bytes=%d err=%v; want the complete %d-byte image", n, err, len(want))
					}
				} else {
					want[len(want)/2] = 'y'
					if err := h.Write(ctx, int64(len(want)/2), []byte("y")); err != nil {
						t.Fatal(err)
					}
				}
				if err := h.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if err := wb.Drain(ctx); err != nil {
					t.Fatal(err)
				}
				node, ok := backend.Node("/open.bin")
				if !ok || !bytes.Equal(node.Data, want) {
					t.Fatalf("remote exists=%v, bytes=%d after invalidation/%s/release; want the complete %d-byte image", ok, len(node.Data), probe, len(want))
				}
			})
		}
	}
}
