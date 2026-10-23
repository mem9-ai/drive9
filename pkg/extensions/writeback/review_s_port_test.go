package writeback

// qiffang sixteenth round: opens join the writeback registry protocol
// (a post-CAS promotion cannot overwrite a newer claim); the checkout
// sweep and dirty accounting iterate the handle keys; FD truncate was
// already resolved here via writebackHandleOf (pinned below for parity).

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// An open arriving between a release's successful CAS and its survivor
// Store is the newer owner: both run under regMu, so the registration is
// never overwritten by the parked promotion.
func TestPortPostCASOpenKeepsOwnership(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ch, wb := newWritebackStack(t, b)
	ctx := context.Background()

	open := func() *bufferingHandle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return writebackHandleOf(res.Handle)
	}
	h1, h2 := open(), open()
	// Park h2's release inside the survivor scan (it blocks on h1.mu):
	// an open arriving NOW must WAIT for the registry protocol instead of
	// registering underneath the parked promotion.
	h1.mu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); wb.untrack(h2) }()
	time.Sleep(30 * time.Millisecond)
	openDone := make(chan *bufferingHandle, 1)
	go func() { openDone <- open() }()
	select {
	case <-openDone:
		h1.mu.Unlock()
		t.Fatal("open completed while a release promotion was parked — opens do not serialize with the registry protocol")
	case <-time.After(80 * time.Millisecond):
	}
	h1.mu.Unlock()
	<-done
	h3 := <-openDone
	if cur := wb.handleFor("/f"); cur != h3 {
		t.Fatalf("handleFor(/f) = %p after the post-park open, want the newest claim", cur)
	}
}

// The checkout sweep visits every tracked handle (the value-vs-key
// mistake made it visit nothing): a clean buffer is dropped and the next
// read observes the replaced backend image; dirty accounting sees the
// dirty writer.
func TestPortCheckoutSweepAndDirtyCountUseHandleKeys(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("AAAA"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	// Write-back policy: the write stays dirty in the buffer (no inline
	// commit clearing it before the accounting check).
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatal(err)
	}
	h := writebackHandleOf(res.Handle)
	if err := h.Write(ctx, 0, []byte("AAAA")); err != nil {
		t.Fatal(err)
	}
	if wb.DirtyHandleCount() == 0 {
		t.Fatal("DirtyHandleCount reported 0 for a dirty writer")
	}
	// Replace the backend image and reset the view.
	b.Seed("/f", []byte("BBBB"))
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	// The clean-buffer sweep ran (keyed iteration): the dirty buffer is
	// this mount's own data and survives the checkout.
	h.mu.Lock()
	dirty := h.dirty
	h.mu.Unlock()
	if !dirty {
		t.Fatal("checkout dropped a DIRTY buffer (local write)")
	}
}

// FD truncate through the named descriptor (parity row for the layer/git
// fixes): the second open's buffer stays untouched.
func TestPortFDTruncateNamedDescriptorWriteback(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ch, _ := newWritebackStack(t, b)
	ctx := context.Background()

	open := func() fs.Handle {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/f", Flags: uint32(syscall.O_RDWR)})
		if err != nil {
			t.Fatal(err)
		}
		return res.Handle
	}
	h1, h2 := open(), open()
	if err := h1.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/f", Size: 2, Handle: h1, Ino: 1}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 5)
	if n, err := h1.Read(ctx, 0, got); err != nil || string(got[:n]) != "he" {
		t.Fatalf("named descriptor read = %q (%d) err=%v, want he", got[:n], n, err)
	}
	_ = h2
}
