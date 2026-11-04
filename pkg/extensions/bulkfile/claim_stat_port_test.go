package bulkfile

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// stubBelow pins the claim's own contract: it records every op the claim
// forwards below (kind + flags) and answers opens with a fixed size, so the
// tests assert exactly what the bulkfile layer resolved by itself. Any
// DIRECT client use by the claim shows up in the backing testbackend's
// request log — the alignment rows demand an empty one.
type stubBelow struct {
	size     int64
	rev      int64
	missing  bool // OpCreate without O_CREAT answers ENOENT (stale dentry)
	creates  []uint32
	getattrs int
}

type stubHandle struct{}

func (stubHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	return 0, nil
}
func (stubHandle) Write(ctx context.Context, off int64, data []byte) error { return nil }
func (stubHandle) Sync(ctx context.Context) error                          { return nil }
func (stubHandle) Flush(ctx context.Context) error                         { return nil }
func (stubHandle) Release(ctx context.Context) error                       { return nil }

func (s *stubBelow) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpGetAttr:
		s.getattrs++
		if s.missing {
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Size: s.size, Rev: s.rev}}, nil
	case fs.OpCreate:
		s.creates = append(s.creates, op.Flags)
		if s.missing && op.Flags&uint32(syscall.O_CREAT) == 0 {
			return fs.Result{}, fs.ErrNotFound
		}
		return fs.Result{
			Attr:   &fs.Attr{Kind: fs.KindFile, Size: s.size, Rev: s.rev},
			Handle: stubHandle{},
		}, nil
	}
	return next(ctx, op)
}

func claimStack(t *testing.T, below *stubBelow) (*fs.Chain, *testbackend.Backend) {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bf, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(bf, below)
	if err != nil {
		t.Fatal(err)
	}
	return ch, b
}

func assertNoDirectRemote(t *testing.T, b *testbackend.Backend) {
	t.Helper()
	if n := len(b.Requests()); n > 0 {
		t.Fatalf("claim used the client directly: %v", b.Requests())
	}
}

// TestPortKernelCreateClaimSkipsExistenceProbe: a kernel CREATE follows a
// negative lookup — v1's Create made no remote call at all, and the direct
// HEAD the claim added (#1026) cost every truncate-create one synchronous
// round trip (fsmark's create rate). The claim must resolve fresh from the
// request shape alone.
func TestPortKernelCreateClaimSkipsExistenceProbe(t *testing.T) {
	below := &stubBelow{size: 4096, rev: 9}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/fresh.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_TRUNC), Mode: 0o644,
		Kernel: &gofuse.CreateIn{}})
	if err != nil {
		t.Fatal(err)
	}
	sh, ok := res.Handle.(*streamHandle)
	if !ok {
		t.Fatalf("handle = %T, want *streamHandle", res.Handle)
	}
	if below.getattrs != 0 {
		t.Fatalf("kernel CREATE probed existence: %d stat ops below", below.getattrs)
	}
	assertNoDirectRemote(t, b)
	if len(below.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(below.creates))
	}
	if got := below.creates[0]; got&uint32(syscall.O_CREAT) == 0 || got&uint32(syscall.O_TRUNC) != 0 {
		t.Fatalf("inner open flags = %#x, want O_CREAT kept and O_TRUNC cleared", got)
	}
	if sh.pushed != 0 || sh.dirty {
		t.Fatalf("fresh claim pushed=%d dirty=%t, want 0/false", sh.pushed, sh.dirty)
	}
}

// TestPortKernelOpenExistingClaimsWithoutProbe: a kernel OPEN resolved an
// existing dentry (v1 read the inode entry; #1026's claim paid a remote
// HEAD instead). The existing size — the first boundary's truncation base —
// comes from the open reply's Attr, never the wire.
func TestPortKernelOpenExistingClaimWithoutProbe(t *testing.T) {
	below := &stubBelow{size: 4096, rev: 9}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/live.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_TRUNC), Mode: 0o644,
		Kernel: &gofuse.OpenIn{}})
	if err != nil {
		t.Fatal(err)
	}
	sh, ok := res.Handle.(*streamHandle)
	if !ok {
		t.Fatalf("handle = %T, want *streamHandle", res.Handle)
	}
	if below.getattrs != 0 {
		t.Fatalf("kernel OPEN probed existence: %d stat ops below", below.getattrs)
	}
	assertNoDirectRemote(t, b)
	if len(below.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(below.creates))
	}
	if got := below.creates[0]; got&uint32(syscall.O_CREAT) != 0 || got&uint32(syscall.O_TRUNC) != 0 {
		t.Fatalf("inner open flags = %#x, want O_CREAT and O_TRUNC cleared", got)
	}
	if sh.pushed != 4096 || !sh.dirty {
		t.Fatalf("existing claim pushed=%d dirty=%t, want 4096/true (truncation pending)", sh.pushed, sh.dirty)
	}
}

// TestPortSynthesizedClaimProbesThroughChain: an op with no kernel request
// behind it still resolves existence — through the chain (namespace cache
// first), matching the write plane's own probe, never a direct remote HEAD.
func TestPortSynthesizedClaimProbesThroughChain(t *testing.T) {
	below := &stubBelow{size: 100, rev: 3}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/live.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_TRUNC), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	assertNoDirectRemote(t, b)
	if below.getattrs != 1 {
		t.Fatalf("chain probes = %d, want 1", below.getattrs)
	}
	if len(below.creates) != 1 || below.creates[0]&uint32(syscall.O_CREAT) != 0 {
		t.Fatalf("existing synthesized claim must open without O_CREAT, creates=%v", below.creates)
	}
}

// TestPortStaleDentryOpenFallsBackToCreate: a kernel OPEN whose dentry
// outlived the remote row takes the fresh-create branch on ENOENT instead
// of failing an open the kernel had already resolved.
func TestPortStaleDentryOpenFallsBackToCreate(t *testing.T) {
	below := &stubBelow{missing: true}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/gone.bin",
		Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_TRUNC), Mode: 0o644,
		Kernel: &gofuse.OpenIn{}})
	if err != nil {
		t.Fatal(err)
	}
	sh, ok := res.Handle.(*streamHandle)
	if !ok {
		t.Fatalf("handle = %T, want *streamHandle", res.Handle)
	}
	assertNoDirectRemote(t, b)
	if len(below.creates) != 2 {
		t.Fatalf("creates = %d, want the failed open plus the create retry", len(below.creates))
	}
	if got := below.creates[1]; got&uint32(syscall.O_CREAT) == 0 {
		t.Fatalf("retry flags = %#x, want O_CREAT", got)
	}
	if sh.pushed != 0 || sh.dirty {
		t.Fatalf("fallback claim pushed=%d dirty=%t, want 0/false", sh.pushed, sh.dirty)
	}
}
