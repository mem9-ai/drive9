package git

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// TestGitHandleReleaseBufferReturnsCharge: the close path drops the handle's
// buffer, so its charge must go back to the process memory budget.
func TestGitHandleReleaseBufferReturnsCharge(t *testing.T) {
	budget := membudget.New(1<<20, membudget.ModeEnforce, nil)
	h := &gitHandle{path: "/w.txt", g: &Git{budget: budget}}
	h.buf = writebuffer.NewWriteBuffer(h.path, 0, 0)
	h.buf.SetMemoryBudget(budget)
	if _, err := h.buf.Write(0, make([]byte, 4096)); err != nil {
		t.Fatal(err)
	}
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got == 0 {
		t.Fatal("write did not charge the budget")
	}
	h.releaseBuffer()
	if h.buf != nil {
		t.Fatal("buffer not dropped")
	}
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != 0 {
		t.Fatalf("charge after releaseBuffer = %d, want 0", got)
	}
}
