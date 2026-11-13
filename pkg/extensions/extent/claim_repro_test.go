package extent

import (
	"context"
	"crypto/rand"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// TestReclaimOfMaterializedEmptyPlaceholder runs the fio two-open lifecycle
// against the fake v2 server: a pattern-claimed kernel create is closed
// with zero writes (materializing the name as a 0-byte classic file), then
// the file is re-opened the way fio re-opens for O_DIRECT IO. The reopen
// must re-claim the name through the engine, and the first commit must
// replace the placeholder with an extent create — the name must never fall
// back to the classic plane.
func TestReclaimOfMaterializedEmptyPlaceholder(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/fio", true)
	h := newTestHandler(t, f, 1) // hybrid, exactly as the mount wires it
	h.opts.Patterns = []string{"*"}
	ctx := context.Background()

	next := fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{Handle: &stubCore{}}, nil
	})

	// Phase 1: fio's create open (O_CREAT|O_WRONLY|O_LARGEFILE, plus the
	// host's O_CREAT bit so the guard holds on every test platform).
	res, err := h.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/fio/two.bin",
		Flags: uint32(0x8041 | syscall.O_CREAT), Mode: 0o644, HasMode: true,
		Kernel: &gofuse.CreateIn{}, Caller: fs.Caller{Pid: 1}}, next)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("phase1 handle = %T, want claimed *engineHandle", res.Handle)
	}

	// Phase 2: zero-write close materializes the name as a classic file.
	if err := eh.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	f.mu.Lock()
	classicEmpty := f.hasClassic("/fio/two.bin") && f.extent["/fio/two.bin"] == ""
	f.mu.Unlock()
	if !classicEmpty {
		t.Fatal("zero-write close must materialize the name as a 0-byte classic file")
	}
	if !h.opts.Engine.Bridge.EmptyPlaceholderFor("/fio/two.bin") {
		t.Fatal("the materialized name must carry the bridge's empty-placeholder attribution")
	}

	// Phase 3: fio's IO open (O_RDWR|O_DIRECT, no O_CREAT) re-claims.
	res2, err2 := h.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/fio/two.bin",
		Flags: 0xc002, Mode: 0o644, HasMode: true,
		Kernel: &gofuse.OpenIn{}, Caller: fs.Caller{Pid: 1}}, next)
	if err2 != nil {
		t.Fatalf("reopen: %v", err2)
	}
	eh2, ok := res2.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("phase3 handle = %T, want re-claimed *engineHandle", res2.Handle)
	}

	// Phase 4: the image publishes as extent, replacing the placeholder.
	payload := make([]byte, 256<<10)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	if err := eh2.Write(ctx, 0, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := eh2.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	f.mu.Lock()
	gotExtent := f.extent["/fio/two.bin"]
	stillClassic := f.hasClassic("/fio/two.bin")
	f.mu.Unlock()
	if gotExtent != string(payload) {
		t.Fatalf("extent image = %d bytes (want %d); classic residue = %v", len(gotExtent), len(payload), stillClassic)
	}
	if h.opts.Engine.Bridge.EmptyPlaceholderFor("/fio/two.bin") {
		t.Fatal("a completed publication must consume the placeholder attribution")
	}
}

// TestKernelCreateClaimedByStarPattern reproduces the production shape of an
// ordinary kernel O_CREAT (fio/dd style) against a plane wired exactly like
// mount.go wires it for --profile coding-agent-extent: Threshold=1 (hybrid)
// and Patterns=["*"]. The kernel resolves the name as missing and sends
// FUSE_CREATE, so op.Kernel is a *gofuse.CreateIn. The pattern-matched
// create must be claimed by the engine plane from byte zero.
func TestKernelCreateClaimedByStarPattern(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/fio", true)
	h := newTestHandler(t, f, 1)
	h.opts.Patterns = []string{"*"}

	op := &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/fio/f6g-163911.bin",
		Flags:   uint32(syscall.O_CREAT | syscall.O_WRONLY),
		Mode:    0o644,
		HasMode: true,
		Kernel:  &gofuse.CreateIn{},
		Caller:  fs.Caller{Pid: 1},
	}
	var nextCalled bool
	res, err := h.Do(context.Background(), op, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		nextCalled = true
		return fs.Result{Handle: &stubCore{}}, nil
	})
	if err != nil {
		t.Fatalf("kernel create claim: %v", err)
	}
	if nextCalled {
		t.Fatal("kernel O_CREAT matching \"*\" must be claimed by the engine plane, not passed to the classic plane")
	}
	if _, ok := res.Handle.(*engineHandle); !ok {
		t.Fatalf("handle type = %T, want *engineHandle", res.Handle)
	}
}

// TestKernelCreateClaimWthTrunc mirrors fio's actual open flags, which
// include O_TRUNC on a fresh name.
func TestKernelCreateClaimWthTrunc(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/fio", true)
	h := newTestHandler(t, f, 1)
	h.opts.Patterns = []string{"*"}

	op := &fs.Op{
		Kind:    fs.OpCreate,
		Path:    "/fio/f8g.bin",
		Flags:   uint32(syscall.O_CREAT | syscall.O_WRONLY | syscall.O_TRUNC),
		Mode:    0o644,
		HasMode: true,
		Kernel:  &gofuse.CreateIn{},
		Caller:  fs.Caller{Pid: 1},
	}
	var nextCalled bool
	res, err := h.Do(context.Background(), op, func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		nextCalled = true
		return fs.Result{Handle: &stubCore{}}, nil
	})
	if err != nil {
		t.Fatalf("kernel create+trunc claim: %v", err)
	}
	if nextCalled {
		t.Fatal("kernel O_CREAT|O_TRUNC matching \"*\" must be claimed by the engine plane")
	}
	if _, ok := res.Handle.(*engineHandle); !ok {
		t.Fatalf("handle type = %T, want *engineHandle", res.Handle)
	}
}
