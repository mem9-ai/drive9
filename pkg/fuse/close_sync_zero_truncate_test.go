package fuse

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func zeroTruncateFS(t *testing.T, backend http.Handler, cached bool) *FS {
	t.Helper()
	server := httptest.NewServer(backend)
	t.Cleanup(server.Close)
	cl := client.New(server.URL, "")
	bus := fs.NewEventBus()
	opts := writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl, Events: bus}
	if cached {
		opts.WriteCacheDir = t.TempDir()
	}
	wb, err := writeback.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := wb.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{Extensions: []fs.Handler{wb, terminal}, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func zeroTruncateSeed(t *testing.T, f *FS) (uint64, uint64) {
	t.Helper()
	in := &gofuse.CreateIn{Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644}
	in.NodeId = 1
	var out gofuse.CreateOut
	if code := f.Create(nil, in, "file", &out); code != gofuse.OK {
		t.Fatal(code)
	}
	writeAndFlushZeroTruncate(t, f, out.NodeId, out.Fh, "seed")
	return out.NodeId, out.Fh
}

func writeAndFlushZeroTruncate(t *testing.T, f *FS, ino, fh uint64, body string) {
	t.Helper()
	in := &gofuse.WriteIn{Size: uint32(len(body))}
	in.NodeId, in.Fh = ino, fh
	if n, code := f.Write(nil, in, []byte(body)); code != gofuse.OK || n != uint32(len(body)) {
		t.Fatalf("write: %d %v", n, code)
	}
	flush := &gofuse.FlushIn{Fh: fh}
	flush.NodeId = ino
	if code := f.Flush(nil, flush); code != gofuse.OK {
		t.Fatalf("flush: %v", code)
	}
}

func zeroTruncate(t *testing.T, f *FS, ino, fh uint64) gofuse.Status {
	t.Helper()
	var in gofuse.SetAttrIn
	in.NodeId, in.Fh = ino, fh
	in.Valid = gofuse.FATTR_SIZE
	if fh != 0 {
		in.Valid |= gofuse.FATTR_FH
	}
	var out gofuse.AttrOut
	return f.SetAttr(nil, &in, &out)
}

func releaseZeroTruncate(f *FS, ino, fh uint64) {
	in := &gofuse.ReleaseIn{Fh: fh}
	in.NodeId = ino
	f.Release(nil, in)
}

// close has already completed FLUSH. RELEASE can arrive before the next
// open, before its flush, or afterwards; none may republish the old image.
func TestCloseSyncZeroTruncateBeforeDelayedRelease(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for _, order := range []string{"before-open", "before-flush", "after-flush"} {
			t.Run(fmt.Sprintf("cache-%t/%s", cached, order), func(t *testing.T) {
				b := testbackend.NewBackend()
				f := zeroTruncateFS(t, b, cached)
				ino, old := zeroTruncateSeed(t, f)
				if code := zeroTruncate(t, f, ino, 0); code != gofuse.OK {
					t.Fatalf("path truncate: %v", code)
				}
				if n, _ := b.Node("/file"); len(n.Data) != 0 {
					t.Errorf("path truncate not committed: %q", n.Data)
				}
				if order == "before-open" {
					releaseZeroTruncate(f, ino, old)
				}
				in := &gofuse.OpenIn{Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC)}
				in.NodeId = ino
				var out gofuse.OpenOut
				if code := f.Open(nil, in, &out); code != gofuse.OK {
					t.Fatalf("reopen: %v", code)
				}
				// Linux follows OPEN(O_TRUNC) with SETATTR(size=0,FH).
				if code := zeroTruncate(t, f, ino, out.Fh); code != gofuse.OK {
					t.Fatalf("open truncate: %v", code)
				}
				if order == "before-flush" {
					releaseZeroTruncate(f, ino, old)
				}
				writeAndFlushZeroTruncate(t, f, ino, out.Fh, "final")
				committed, _ := b.Node("/file")
				if order == "after-flush" {
					releaseZeroTruncate(f, ino, old)
				}
				releaseZeroTruncate(f, ino, out.Fh)
				n, _ := b.Node("/file")
				if string(n.Data) != "final" || n.Rev != committed.Rev {
					t.Fatalf("late release changed final image: %+v", n)
				}
			})
		}
	}
}

func TestCloseSyncZeroTruncateFailureDoesNotBecomeReleaseWrite(t *testing.T) {
	for _, committed := range []bool{false, true} {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("committed-%t/retry-%t", committed, retry), func(t *testing.T) {
				b := testbackend.NewBackend()
				var failing atomic.Bool
				var attempts atomic.Int32
				f := zeroTruncateFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPut {
						attempts.Add(1)
					}
					if failing.Load() && r.Method == http.MethodPut {
						if !committed {
							http.Error(w, "denied", http.StatusForbidden)
							return
						}
						rr := httptest.NewRecorder()
						b.ServeHTTP(rr, r)
						if rr.Code != http.StatusOK {
							t.Errorf("injected commit: %d", rr.Code)
						}
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						_ = conn.Close()
						return
					}
					b.ServeHTTP(w, r)
				}), true)
				ino, old := zeroTruncateSeed(t, f)
				baseline := attempts.Load()
				failing.Store(true)
				if code := zeroTruncate(t, f, ino, 0); code == gofuse.OK {
					t.Fatalf("failed/unknown truncate reported success; attempts=%d", attempts.Load())
				}
				failing.Store(false)
				before, _ := b.Node("/file")
				want := "seed"
				if committed {
					want = ""
				}
				if string(before.Data) != want {
					t.Fatalf("remote=%q want %q", before.Data, want)
				}
				wantAttempts := baseline + 1
				if retry {
					if code := zeroTruncate(t, f, ino, 0); code != gofuse.OK {
						t.Fatalf("retry: %v", code)
					}
					if !committed {
						wantAttempts++
					}
					if n, _ := b.Node("/file"); len(n.Data) != 0 || attempts.Load() != wantAttempts {
						t.Fatalf("retry body=%q attempts=%d want=%d", n.Data, attempts.Load(), wantAttempts)
					}
				}
				// A different writer can publish before the asynchronous old RELEASE.
				b.Seed("/file", []byte("later"))
				releaseZeroTruncate(f, ino, old)
				after, _ := b.Node("/file")
				if string(after.Data) != "later" || attempts.Load() != wantAttempts {
					t.Fatalf("release replayed truncate: body=%q attempts=%d", after.Data, attempts.Load())
				}
			})
		}
	}
}

// A descriptor truncate and a pathname truncate with an actual dirty owner
// still wait for that owner's flush. The clean-handle fix must not publish it.
func TestCloseSyncZeroTruncateKeepsLocalOwner(t *testing.T) {
	for _, named := range []bool{false, true} {
		t.Run(fmt.Sprintf("named-%t", named), func(t *testing.T) {
			b := testbackend.NewBackend()
			f := zeroTruncateFS(t, b, true)
			ino, fh := zeroTruncateSeed(t, f)
			truncateFH := fh
			if !named {
				in := &gofuse.WriteIn{Size: 7}
				in.NodeId, in.Fh = ino, fh
				if _, code := f.Write(nil, in, []byte("pending")); code != gofuse.OK {
					t.Fatal(code)
				}
				truncateFH = 0
			}
			if code := zeroTruncate(t, f, ino, truncateFH); code != gofuse.OK {
				t.Fatal(code)
			}
			if n, _ := b.Node("/file"); string(n.Data) != "seed" {
				t.Fatalf("published local truncate: %q", n.Data)
			}
			writeAndFlushZeroTruncate(t, f, ino, fh, "final")
			releaseZeroTruncate(f, ino, fh)
			if n, _ := b.Node("/file"); string(n.Data) != "final" {
				t.Fatalf("final body: %q", n.Data)
			}
		})
	}
}

func TestCloseSyncZeroTruncatePreservesFollowingWrite(t *testing.T) {
	b := testbackend.NewBackend()
	entered, proceed := make(chan struct{}), make(chan struct{})
	var block atomic.Bool
	f := zeroTruncateFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && block.CompareAndSwap(true, false) {
			close(entered)
			select {
			case <-proceed:
			case <-r.Context().Done():
				return
			}
		}
		b.ServeHTTP(w, r)
	}), true)
	ino, fh := zeroTruncateSeed(t, f)
	block.Store(true)
	truncated := make(chan gofuse.Status, 1)
	go func() { truncated <- zeroTruncate(t, f, ino, 0) }()
	<-entered
	written := make(chan gofuse.Status, 1)
	go func() {
		in := &gofuse.WriteIn{Size: 5}
		in.NodeId, in.Fh = ino, fh
		_, code := f.Write(nil, in, []byte("later"))
		written <- code
	}()
	close(proceed)
	if code := <-truncated; code != gofuse.OK {
		t.Fatal(code)
	}
	if code := <-written; code != gofuse.OK {
		t.Fatal(code)
	}
	flush := &gofuse.FlushIn{Fh: fh}
	flush.NodeId = ino
	if code := f.Flush(nil, flush); code != gofuse.OK {
		t.Fatal(code)
	}
	releaseZeroTruncate(f, ino, fh)
	if n, _ := b.Node("/file"); string(n.Data) != "later" {
		t.Fatalf("truncate retired the following write: %q", n.Data)
	}
}

func TestCloseSyncZeroTruncateRejectsConcurrentRemoteWrite(t *testing.T) {
	b := testbackend.NewBackend()
	var race atomic.Bool
	var attempts atomic.Int32
	f := zeroTruncateFS(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && race.Load() {
			attempts.Add(1)
			if r.Header.Get("X-Dat9-Expected-Revision") == "" {
				t.Error("truncate omitted revision condition")
			}
			foreign := httptest.NewRequest(http.MethodPut, "/v1/fs/file", strings.NewReader("foreign"))
			rr := httptest.NewRecorder()
			b.ServeHTTP(rr, foreign)
			if rr.Code != http.StatusOK {
				t.Errorf("foreign write: %d", rr.Code)
			}
		}
		b.ServeHTTP(w, r)
	}), true)
	ino, fh := zeroTruncateSeed(t, f)
	race.Store(true)
	if code := zeroTruncate(t, f, ino, 0); code == gofuse.OK {
		t.Fatal("conflicting truncate succeeded")
	}
	releaseZeroTruncate(f, ino, fh)
	if n, _ := b.Node("/file"); string(n.Data) != "foreign" || attempts.Load() != 1 {
		t.Fatalf("conflict retried or overwrote remote: body=%q attempts=%d", n.Data, attempts.Load())
	}
}
