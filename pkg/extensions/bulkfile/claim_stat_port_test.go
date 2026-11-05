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
	mode     uint32 // mode the open/getattr replies carry (0 = none)
	missing  bool   // OpCreate without O_CREAT answers ENOENT (stale dentry)
	noAttr   bool   // OpCreate answers a handle with no attributes
	creates  []uint32
	getattrs int
	setattrs []int64 // sizes expressed through OpSetAttr
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
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Size: s.size, Rev: s.rev, Mode: s.mode}}, nil
	case fs.OpSetAttr:
		if op.Size >= 0 {
			s.setattrs = append(s.setattrs, op.Size)
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Size: op.Size, Rev: s.rev}}, nil
	case fs.OpCreate:
		s.creates = append(s.creates, op.Flags)
		if s.missing && op.Flags&uint32(syscall.O_CREAT) == 0 {
			return fs.Result{}, fs.ErrNotFound
		}
		if s.noAttr {
			return fs.Result{Handle: stubHandle{}}, nil
		}
		return fs.Result{
			Attr:   &fs.Attr{Kind: fs.KindFile, Size: s.size, Rev: s.rev, Mode: s.mode},
			Handle: stubHandle{},
		}, nil
	}
	return next(ctx, op)
}

// claimStack builds [bulkfile → stubBelow] over a counting backend: any
// direct client use by the claim lands in the backend's request log.
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

// assertNoDirectRemote fails when the claim touched the backing backend.
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
// request shape alone (the lazy write plane below answers a fresh create
// with a zero-size Attr).
func TestPortKernelCreateClaimSkipsExistenceProbe(t *testing.T) {
	below := &stubBelow{rev: 9}
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
	if sh.pushed != 0 || sh.dirty || sh.sizeUnknown {
		t.Fatalf("fresh claim pushed=%d dirty=%t sizeUnknown=%t, want 0/false/false", sh.pushed, sh.dirty, sh.sizeUnknown)
	}
}

// TestPortKernelCreateStaleNegativeReconciles: the kernel's negative lookup
// is not atomic with the CREATE — another client can create the name in
// between. The write plane below discovers the existing file (its own
// probe) and answers the open with the row's real size; the claim must
// adopt that as its truncation base, or a truncate-only close over the
// stale negative would preserve the old content forever (pushed=0,
// dirty=false, nothing to express).
func TestPortKernelCreateStaleNegativeReconciles(t *testing.T) {
	below := &stubBelow{size: 4096, rev: 9}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/raced.bin",
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
	if sh.pushed != 4096 || !sh.dirty {
		t.Fatalf("stale-negative claim pushed=%d dirty=%t, want 4096/true (truncation pending)", sh.pushed, sh.dirty)
	}
	// The boundary must express the truncation: a close with no writes
	// publishes size 0 below instead of leaving the raced file intact.
	if err := sh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if len(below.setattrs) != 1 || below.setattrs[0] != 0 {
		t.Fatalf("boundary setattr sizes = %v, want one truncation to 0", below.setattrs)
	}
}

// TestPortKernelOpenNoAttrTruncatesOnClose: a kernel OPEN of an existing
// dentry whose open below returns a handle with NO attributes (a localize
// overlay whose Stat failed): the existing size is unknown, and the claim
// must not assume empty — the first boundary truncates the file to the
// image explicitly, so an unwritten close leaves size 0.
func TestPortKernelOpenNoAttrTruncatesOnClose(t *testing.T) {
	below := &stubBelow{noAttr: true}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/opaque.bin",
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
	if !sh.sizeUnknown || !sh.dirty {
		t.Fatalf("no-attr claim sizeUnknown=%t dirty=%t, want true/true", sh.sizeUnknown, sh.dirty)
	}
	if err := sh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if len(below.setattrs) != 1 || below.setattrs[0] != 0 {
		t.Fatalf("boundary setattr sizes = %v, want one truncation to 0", below.setattrs)
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

// TestPortKernelOpenTruncWithoutCreateClaims: Linux strips O_CREAT from
// FUSE_OPEN for existing files, so `: > f` arrives as OPEN|O_TRUNC with
// no O_CREAT. The claim must take that shape too — its truncation
// ownership cannot depend on which kernel forwarded the flags.
func TestPortKernelOpenTruncWithoutCreateClaims(t *testing.T) {
	below := &stubBelow{size: 4096, rev: 9}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/live.bin",
		Flags:  uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Kernel: &gofuse.OpenIn{}})
	if err != nil {
		t.Fatal(err)
	}
	sh, ok := res.Handle.(*streamHandle)
	if !ok {
		t.Fatalf("handle = %T, want *streamHandle (the claim must own the kernel OPEN|O_TRUNC shape)", res.Handle)
	}
	if below.getattrs != 0 {
		t.Fatalf("kernel OPEN probed existence: %d stat ops below", below.getattrs)
	}
	assertNoDirectRemote(t, b)
	if len(below.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(below.creates))
	}
	if got := below.creates[0]; got&uint32(syscall.O_TRUNC) != 0 {
		t.Fatalf("inner open flags = %#x, want O_TRUNC cleared (the claim owns the truncation)", got)
	}
	if sh.pushed != 4096 || !sh.dirty {
		t.Fatalf("claim pushed=%d dirty=%t, want 4096/true (truncation pending)", sh.pushed, sh.dirty)
	}
	if err := sh.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if len(below.setattrs) != 1 || below.setattrs[0] != 0 {
		t.Fatalf("boundary setattr sizes = %v, want one truncation to 0", below.setattrs)
	}
}

// TestPortKernelOpenClaimKeepsExistingMode pins the kernel-OPEN claim's mode
// fidelity: FUSE OPEN dispatches OpCreate without a mode, so the claim must
// adopt the existing file's REAL mode from the open reply — a 0600 file
// truncated through OPEN|O_TRUNC stays 0600 through lookups and re-opens,
// not the 0644 default.
func TestPortKernelOpenClaimKeepsExistingMode(t *testing.T) {
	below := &stubBelow{size: 4096, rev: 9, mode: 0o600}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/mode.bin",
		Flags:  uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Kernel: &gofuse.OpenIn{}})
	if err != nil {
		t.Fatal(err)
	}
	sh, ok := res.Handle.(*streamHandle)
	if !ok {
		t.Fatalf("handle = %T, want *streamHandle", res.Handle)
	}
	assertNoDirectRemote(t, b)
	have, ok := sh.overlayAttr()
	if !ok {
		t.Fatal("claim has no overlay attributes")
	}
	if have.Mode != 0o600 {
		t.Fatalf("claimed mode = %o, want the existing file's 0600 (no chmod happened)", have.Mode)
	}
	// An explicit chmod(000) through the claim stays explicit and
	// distinguishable from an unknown mode.
	if _, err := sh.applyAttr(ctx, &fs.Op{Kind: fs.OpSetAttr, HasMode: true, Mode: 0}); err != nil {
		t.Fatal(err)
	}
	have2, _ := sh.overlayAttr()
	if have2.Mode != 0 {
		t.Fatalf("explicit chmod(000) reported as %o", have2.Mode)
	}
	_ = sh.Release(ctx)
}

// TestPortKernelOpenClaimKeepsExplicitZeroMode pins the harder half of the
// mode finding: a file already at chmod(000) BEFORE the open (root may open
// it writable — the FUSE gate permits uid 0) answers Mode 0 with HasMode
// semantics; the claim must adopt the explicit zero, not skip adoption and
// report the 0644 default.
func TestPortKernelOpenClaimKeepsExplicitZeroMode(t *testing.T) {
	below := &stubBelow{size: 512, rev: 4, mode: 0}
	ch, b := claimStack(t, below)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/zero.bin",
		Flags:  uint32(syscall.O_WRONLY | syscall.O_TRUNC),
		Kernel: &gofuse.OpenIn{}})
	if err != nil {
		t.Fatal(err)
	}
	sh, ok := res.Handle.(*streamHandle)
	if !ok {
		t.Fatalf("handle = %T, want *streamHandle", res.Handle)
	}
	assertNoDirectRemote(t, b)
	have, ok := sh.overlayAttr()
	if !ok {
		t.Fatal("claim has no overlay attributes")
	}
	if have.Mode != 0 {
		t.Fatalf("claimed mode = %o, want the file's explicit 000 (no chmod happened)", have.Mode)
	}
	_ = sh.Release(ctx)
}
