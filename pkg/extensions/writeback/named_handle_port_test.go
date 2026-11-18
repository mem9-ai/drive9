package writeback

// Fifth review round: FD-named truncates resolve by the supplied handle
// (not the recycled pathname), a flush's completion cannot drop a write
// acknowledged during its upload, and a combined SETATTR applies both
// fields.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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

// FATTR_FH truncates the naming descriptor's own buffer: after
// unlink+recreate, the old FD must not truncate the replacement object.
func TestPortNamedHandleTruncateTargetsOwnBuffer(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	oldH := open(t, ch, "/f").(*bufferingHandle)
	if err := oldH.Write(ctx, 0, []byte("OLD")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatal(err)
	}
	newH := open(t, ch, "/f").(*bufferingHandle)
	if err := newH.Write(ctx, 0, []byte("NEW")); err != nil {
		t.Fatal(err)
	}

	// SETATTR(size=1) naming the OLD descriptor.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 1, Handle: oldH}); err != nil {
		t.Fatal(err)
	}
	if err := newH.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "NEW" {
		t.Fatalf("replacement persisted %q, want NEW (the old descriptor truncated the new object)", string(n.Data))
	}
	// The old descriptor serves its own truncated buffer.
	buf := make([]byte, 4)
	nn, _ := oldH.Read(ctx, 0, buf)
	if nn != 1 || buf[0] != 'O' {
		t.Fatalf("old descriptor read %d bytes %q, want its own truncated O", nn, buf[:nn])
	}
}

// A write acknowledged while an older flush's upload is in flight keeps
// its dirty flag: the stale completion must not drop it, and the next
// durability barrier commits the newer bytes.
func TestPortFlushCompletionKeepsInFlightWrite(t *testing.T) {
	b := testbackend.NewBackend()
	release := make(chan struct{})
	var gateHit atomic.Bool
	var releaseOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/") && gateHit.CompareAndSwap(false, true) {
			<-release
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); ts.Close() })
	cl := client.New(ts.URL, "")
	wbcache, _ := staging.NewWriteBackCache(t.TempDir())
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := open(t, ch, "/f").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- h.Flush(ctx) }()
	time.Sleep(150 * time.Millisecond)
	if err := h.Write(ctx, 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	// The later barrier must commit the newer bytes.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "AAAABBBB" {
		t.Fatalf("persisted %q, want AAAABBBB (the in-flight write was dropped)", string(n.Data))
	}
}

// A combined SETATTR (mode + size) applies both fields and the final
// attrs carry both.
func TestPortCombinedSetAttrAppliesModeAndSize(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := open(t, ch, "/f").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Mode: 0o600, Size: 2, HasMode: true, HasSize: true, Handle: h})
	if err != nil {
		t.Fatal(err)
	}
	if res.Attr == nil || res.Attr.Size != 2 || res.Attr.Mode&0o777 != 0o600 {
		t.Fatalf("combined setattr reply = %+v, want size 2 mode 0600", res.Attr)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "ab" {
		t.Fatalf("persisted %q, want ab (the size half was dropped)", string(n.Data))
	}
	if n.Mode&0o777 != 0o600 {
		t.Fatalf("persisted mode = %o, want 0600", n.Mode&0o777)
	}
}

// FD-named chmod resolves by its own handle: after unlink+recreate, the
// old descriptor must not chmod the replacement.
func TestPortNamedHandleChmodTargetsOwnObject(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, _ := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	oldH := open(t, ch, "/f").(*bufferingHandle)
	if err := oldH.Write(ctx, 0, []byte("OLD")); err != nil {
		t.Fatal(err)
	}
	if err := oldH.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatal(err)
	}
	newH := open(t, ch, "/f").(*bufferingHandle)
	if err := newH.Write(ctx, 0, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Mode: 0o600, HasMode: true, Handle: oldH}); err != nil {
		t.Fatal(err)
	}
	if err := newH.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if n.Mode&0o777 == 0o600 {
		t.Fatal("the old descriptor's chmod reached the replacement object")
	}
}

// Releasing an older descriptor after a recreate must not hide the newer
// writer from the path index.
func TestPortReleaseKeepsSurvivingWriterIndexed(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, _ := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	oldH := open(t, ch, "/f").(*bufferingHandle)
	if err := oldH.Write(ctx, 0, []byte("OLD")); err != nil {
		t.Fatal(err)
	}
	if err := oldH.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/f"}); err != nil {
		t.Fatal(err)
	}
	newH := open(t, ch, "/f").(*bufferingHandle)
	if err := newH.Write(ctx, 0, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	// The old descriptor goes away; the newer writer must stay reachable.
	if err := oldH.Release(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 3)
	if _, err := res.Handle.Read(ctx, 0, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "NEW" {
		t.Fatalf("read after old release = %q, want NEW (the surviving writer was unindexed)", got)
	}
}

// An explicit chmod(000) on a dirty open handle survives the persistence
// boundary (the presence bit alone gates publication).
func TestPortZeroModeChmodPublishes(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, _ := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := open(t, ch, "/f").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("secret")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Mode: 0, HasMode: true, Handle: h}); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if n.Mode&0o777 != 0 {
		t.Fatalf("remote mode = %o, want 0 (chmod 000 dropped at the persistence boundary)", n.Mode&0o777)
	}
}

// The three remaining unfenced completions (review): staging that clears
// dirty before the flush captures its generation, the write-back
// DrainFlush upload, and the inline commit without a write cache — each
// must keep a write acknowledged during its upload.
func TestPortStagingRaceKeepsInFlightWrite(t *testing.T) {
	// Variant 3 first (deterministic, no write cache): the inline
	// commit's PUT parks after capturing its body.
	b := testbackend.NewBackend()
	release := make(chan struct{})
	var gateHit atomic.Bool
	var releaseOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/") && gateHit.CompareAndSwap(false, true) {
			<-release
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); ts.Close() })
	cl := client.New(ts.URL, "")
	wb, _ := New(Options{Policy: PolicyCloseSync, Client: cl})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := open(t, ch, "/f").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- h.Flush(ctx) }()
	time.Sleep(150 * time.Millisecond)
	if err := h.Write(ctx, 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "AAAABBBB" {
		t.Fatalf("inline-commit variant persisted %q, want AAAABBBB", string(n.Data))
	}
}

// Variant 2: the write-back DrainFlush upload parks; the acknowledged
// write survives it.
func TestPortDrainFlushRaceKeepsInFlightWrite(t *testing.T) {
	b := testbackend.NewBackend()
	release := make(chan struct{})
	var gateHit atomic.Bool
	var releaseOnce sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/fs/") && gateHit.CompareAndSwap(false, true) {
			<-release
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); ts.Close() })
	cl := client.New(ts.URL, "")
	wbcache, _ := staging.NewWriteBackCache(t.TempDir())
	wb, _ := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := open(t, ch, "/f").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	// Stage the snapshot locally (write-back small-file contract), then
	// let the drain walk upload it while the new write is acknowledged.
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	drained := make(chan error, 1)
	go func() { drained <- h.DrainFlush(ctx) }()
	time.Sleep(150 * time.Millisecond)
	if err := h.Write(ctx, 4, []byte("BBBB")); err != nil {
		t.Fatal(err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	// The boundary commits the surviving write.
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, _ := b.Node("/f")
	if string(n.Data) != "AAAABBBB" {
		t.Fatalf("drain-flush variant persisted %q, want AAAABBBB", string(n.Data))
	}
}
