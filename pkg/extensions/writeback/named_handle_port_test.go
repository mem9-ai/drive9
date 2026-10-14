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
