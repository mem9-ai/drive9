package appendlog

import (
	"context"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
)

// newBudgetedStack builds the appendlog → core chain and hands back the
// extension, so a test can arm the memory budget BEFORE the handle's buffer
// exists.
func newBudgetedStack(t *testing.T, root http.Handler, patterns ...string) (*fs.Chain, *AppendLog) {
	t.Helper()
	ts := httptest.NewServer(root)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Patterns: patterns, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, ext
}

// TestAppendLogFailedCloseReturnsBufferCharge: the facade drops the descriptor
// and calls Release once, so a close whose content commit fails still has to
// return the buffer's memory-budget charge — otherwise every failed close of a
// dirty appendlog file leaks one charge and the mount converges to ENOMEM.
func TestAppendLogFailedCloseReturnsBufferCharge(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/configured.log", []byte("pre"))
	// Every content commit fails (5xx) — the classic "server outage during
	// close" case — while the backend still answers the opens and stats.
	root := wrapBackend(b, func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodPut || r.URL.Query().Has("append-log") || r.URL.Query().Has("chmod") ||
			(r.Method == http.MethodPost && fsPath(r.URL.Path)) {
			w.WriteHeader(http.StatusInternalServerError)
			return true
		}
		return false
	})
	ch, ext := newBudgetedStack(t, root, "**/configured.log")
	budget := membudget.New(8<<20, membudget.ModeEnforce, nil)
	ext.SetMemoryBudget(budget)

	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/configured.log", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h, ok := res.Handle.(*Handle)
	if !ok {
		t.Fatalf("handle = %T", res.Handle)
	}
	if err := h.Write(ctx, 3, []byte("tail")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got == 0 {
		t.Fatal("buffered write did not charge the budget")
	}
	if err := h.Release(ctx); err == nil {
		t.Fatal("release against a failing backend must return the commit error")
	}
	if got := budget.UsedBy(membudget.ClassWriteBuffer); got != 0 {
		t.Fatalf("charge after failed close = %d, want 0 (leak)", got)
	}
}
