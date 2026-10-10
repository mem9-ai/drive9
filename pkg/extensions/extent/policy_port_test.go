package extent

// Pure-function rows ported from v1 pkg/fuse/extent_policy_test.go: the
// JuiceFS write-offset append rules, open-flag mapping, and the fuse-context
// interrupt semantics.

import (
	"testing"
	"time"
)

// v1 TestJuiceWriteOffset.
func TestPortJuiceWriteOffset(t *testing.T) {
	if got := juiceWriteOffset(0, 216, true); got != 216 {
		t.Fatalf("O_APPEND syscall write off=%d, want EOF 216", got)
	}
	if got := juiceWriteOffset(100, 216, true); got != 100 {
		t.Fatalf("O_APPEND with a kernel offset=%d, want 100", got)
	}
	if got := juiceWriteOffset(0, 216, false); got != 0 {
		t.Fatalf("non-append write off=%d, want 0", got)
	}
}

// v1 TestExtentOpenFlags.
func TestPortExtentOpenFlags(t *testing.T) {
	if got := extentOpenFlags(true); got != 0x20 {
		t.Fatalf("keepCache open flags = %#x, want FOPEN_KEEP_CACHE", got)
	}
	if got := extentOpenFlags(false); got != 0 {
		t.Fatalf("default open flags = %#x, want 0", got)
	}
}

// v1 TestJfsCtxCancelHonorsFuseCancel: JuiceFS Canceled() ignores the FUSE
// interrupt for the first second, then sees it.
func TestPortJfsCtxCancelHonorsFuseCancel(t *testing.T) {
	cancel := make(chan struct{})
	ctx := &jfsFuseContext{cancel: cancel, started: time.Now().Add(-2 * time.Second)}
	if ctx.Canceled() {
		t.Fatal("context canceled before FUSE cancel")
	}
	close(cancel)
	if !ctx.Canceled() {
		t.Fatal("JuiceFS Canceled() must see FUSE cancel after the first second")
	}
	// Fresh context: the interrupt is ignored during the first second.
	fresh := make(chan struct{})
	close(fresh)
	young := &jfsFuseContext{cancel: fresh, started: time.Now()}
	if young.Canceled() {
		t.Fatal("JuiceFS Canceled() must ignore FUSE cancel for the first second")
	}
}

// v1 TestJfsCtxCancelLockBlockValue: blocking lock contexts see interrupts
// immediately.
func TestPortJfsCtxCancelLockBlockValue(t *testing.T) {
	cancel := make(chan struct{})
	ctx := &jfsFuseContext{cancel: cancel, lockBlock: true, started: time.Now()}
	if ctx.Canceled() {
		t.Fatal("blocking lock context canceled before FUSE cancel")
	}
	close(cancel)
	if !ctx.Canceled() {
		t.Fatal("blocking lock context must see FUSE cancel immediately")
	}
}
