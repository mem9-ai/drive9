package writeback

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// #1029 regression pins (chain level, no kernel): FUSE OPEN carries only
// the nodeid, so a hardlinked inode's open may key the write-plane handle
// at either name. When one name is unlinked and later recreated, handles
// keyed at it must follow a surviving alias — otherwise they keep reading
// the recycled name, which a replacement object now owns.

// A read-only handle rekeyed by RetargetPath must serve the survivor's
// content, including the wrapped terminal handle (core's SyncHandle reads
// by its own path).
func TestPortRetargetRebindsWrappedReadHandle(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/doomed", []byte("REPLACEMENT"))
	b.Seed("/survivor", []byte("PRE-DELETE"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/doomed", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	h := res.Handle
	wb.RetargetPath("/doomed", "/survivor")
	got := make([]byte, len("PRE-DELETE"))
	if _, err := h.Read(ctx, 0, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "PRE-DELETE" {
		t.Fatalf("post-retarget read = %q, want the survivor image", got)
	}
}

// A live writer rekeyed onto the survivor must not shadow the recycled
// name: a fresh reader of the replaced path sees the replacement object's
// bytes, not the lingering writer's buffer.
func TestPortRetargetIsolatesRecycledName(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	// The doomed name's writer buffers bytes that never staged.
	wh := open(t, ch, "/doomed").(*bufferingHandle)
	if err := wh.Write(ctx, 0, []byte("DOOMED-WRITER")); err != nil {
		t.Fatal(err)
	}
	// The facade's deferred-unlink hook rebinds it onto the surviving alias.
	wb.RetargetPath("/doomed", "/survivor")
	// The replacement object lands at the recycled name.
	b.Seed("/doomed", []byte("REPLACEMENT"))

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/doomed", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len("REPLACEMENT"))
	if _, err := res.Handle.Read(ctx, 0, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "REPLACEMENT" {
		t.Fatalf("recycled-name read = %q, want the replacement image (lingering writer leaked across the retarget)", got)
	}
}
