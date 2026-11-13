package extent

import (
	"context"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

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
