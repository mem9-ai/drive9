package writeback

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// v1 close-to-open for the pending tracks: a read-only re-open while the
// async landing lags (here a 2GiB spill — one byte at 2GiB+1) serves the
// pending bytes instead of waiting out the stream (open/25 row 3).
func TestPortReadOnlyReopenServesPendingSpill(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ledger, lerr := staging.NewStaging(t.TempDir())
	if lerr != nil {
		t.Fatal(lerr)
	}
	t.Cleanup(func() { ledger.Close() })
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache, Staging: ledger})
	if err != nil {
		t.Fatal(err)
	}
	rc, cerr := cache.New(cache.Options{PrefetchClient: cl, SequentialPrefetch: true})
	if cerr != nil {
		t.Fatal(cerr)
	}
	wb.SetCommitSeeder(rc.SeedCommit)
	terminal, _ := core.New(cl, core.Options{})
	ch, cerr2 := fs.NewChain(rc, wb, terminal)
	if cerr2 != nil {
		t.Fatal(cerr2)
	}
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 2147483649, []byte("a")); err != nil {
		t.Fatalf("pwrite at 2GiB+1: %v", err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	ro, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	rh := ro.Handle
	got := make([]byte, 1)
	if _, err := rh.Read(ctx, 2147483649, got); err != nil {
		t.Fatalf("pread: %v", err)
	}
	if string(got) != "a" {
		t.Fatalf("pread = %q, want a", got)
	}
	if err := rh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/f")
	if !ok {
		t.Fatal("remote missing after drain")
	}
	if int64(len(n.Data)) != 2147483650 || n.Data[2147483649] != 'a' {
		t.Fatalf("remote image = %d bytes, tail = %q", len(n.Data), n.Data[2147483649:])
	}
}

// v1's shadow spill stayed sparse: one byte at 2GiB+1 must not write
// 2GiB of zeros into the shadow (the byte quota rejected the port's
// full-image spill — open/25 row 3 on the mounted stack).
func TestPortSpillStaysSparseUnderQuota(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	// A 64MiB quota: the pre-fix spill of the 2GiB image was rejected
	// outright; the sparse spill writes only the touched part.
	ledger, err := staging.NewStagingWithQuota(t.TempDir(), -1, 64)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ledger.Close)
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: ledger, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/sparse.bin", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 2147483649, []byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatalf("flush (spill): %v", err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := wb.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node("/sparse.bin")
	if !ok || int64(len(n.Data)) != 2147483650 || n.Data[2147483649] != 'a' {
		t.Fatalf("remote image = %d bytes ok=%v", len(n.Data), ok)
	}
}

// v1's re-open of a not-yet-landed OVERWRITE pinned the pending image: a
// checksum spanning the async landing must not mix the old and new
// images (the tier-transition 8MiB smoke row read a mixed hash while the
// remote held the correct new image).
func TestPortReadOnlyReopenServesPendingOverwrite(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/tier.bin", bytes.Repeat([]byte("A"), 8<<20))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/tier.bin", Flags: uint32(syscall.O_WRONLY | syscall.O_TRUNC)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	if err := h.Write(ctx, 0, bytes.Repeat([]byte("B"), 8<<20)); err != nil {
		t.Fatal(err)
	}
	// Flush stages the .dat image; the upload stays pending (no Release
	// Submit yet, remote still the A image).
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	ro, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/tier.bin", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatalf("re-open over pending overwrite: %v", err)
	}
	got := make([]byte, 4096)
	if _, err := ro.Handle.Read(ctx, 3<<20, got); err != nil {
		t.Fatal(err)
	}
	if string(got[:64]) != strings.Repeat("B", 64) {
		t.Fatalf("read over pending overwrite = %q, want the staged B image", got[:16])
	}
}

// The kernel delivers open(O_TRUNC) of an existing file as SETATTR(size=0)
// on the live writer: the reply must carry permission bits — a Mode-less
// settle writes 0000 into the kernel's attr cache and every later open
// EACCESes under default_permissions (the mounted overwrite sequence).
func TestPortTruncateSetattrReplyKeepsPermissions(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("seed-data"))
	ch, _ := newStack(t, PolicyWriteBack, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_WRONLY)})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("x")); err != nil {
		t.Fatal(err)
	}
	set, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 0})
	if err != nil {
		t.Fatal(err)
	}
	if set.Attr == nil || set.Attr.Mode&0o777 == 0 {
		t.Fatalf("truncate reply attr = %+v, want permission bits", set.Attr)
	}
}

// An unlink of a pending-overlay handle (read-only re-open of a
// not-yet-landed image) must not crash the snapshot path — the handle has
// no terminal to fetch a clean image from (mount panic: unlink/14 row 3).
func TestPortUnlinkOfPendingOverlayHandle(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wbcache, err := staging.NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, WriteCache: wbcache})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/p.bin", Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("pending")); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	ro, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/p.bin", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatalf("pending re-open: %v", err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/p.bin"}); err != nil {
		t.Fatalf("unlink of pending-overlay handle: %v", err)
	}
	got := make([]byte, 7)
	if _, err := ro.Handle.Read(ctx, 0, got); err != nil {
		t.Fatalf("post-unlink read: %v", err)
	}
	if string(got) != "pending" {
		t.Fatalf("post-unlink read = %q", got)
	}
}
