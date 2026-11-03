package appendlog

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A directory rename retargets the tracked append handles under it, even
// though the directory itself does not match the file pattern.
func TestPortDirectoryRenameRetargetsHandles(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/d", 0o755)
	b.Seed("/d/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()
	h := openLog(t, ch, "/d/app.log")
	if err := h.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/d", Path2: "/e"}); err != nil {
		t.Fatalf("directory rename = %v", err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("fsync after directory rename = %v", err)
	}
	if node, ok := b.Node("/e/app.log"); !ok || string(node.Data) != "Xbcdef" {
		t.Fatalf("/e/app.log = %q ok=%v, want Xbcdef", node.Data, ok)
	}
}

// Every landed event that carries a size declares it, so a read-side plane
// can adopt the new size after a growing append.
func TestPortLandedEventsCarrySize(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("base"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	bus := fs.NewEventBus()
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Patterns: []string{"**/*.log"}, Client: cl, Events: bus})
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
	var mu sync.Mutex
	var landed []fs.Event
	bus.On(fs.EventLanded, func(e fs.Event) {
		mu.Lock()
		landed = append(landed, e)
		mu.Unlock()
	})
	ctx := context.Background()
	h := openLog(t, ch, "/app.log")
	if err := h.Write(ctx, 4, []byte("-tail")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	events := append([]fs.Event(nil), landed...)
	mu.Unlock()
	if len(events) == 0 {
		t.Fatal("append sync published no landed event")
	}
	for _, e := range events {
		if !e.HasSize {
			t.Fatalf("landed event %+v does not declare its size", e)
		}
	}
	if last := events[len(events)-1]; last.Size != 9 {
		t.Fatalf("landed size = %d, want the appended 9 bytes", last.Size)
	}
}

// The facade's deferred-unlink flush must delete NOW: a second unlink while
// the fd is open lands the remote delete, and the old fd keeps its image.
func TestPortDeferredUnlinkFlushDeletesNow(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("OTHER"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()
	h := openLog(t, ch, "/app.log")
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	if _, ok := b.Node("/app.log"); !ok {
		t.Fatal("the first unlink should defer, not delete")
	}
	// The facade lands the deferred delete before a same-name create.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/app.log"}); err != nil {
		t.Fatalf("deferred-flush unlink = %v", err)
	}
	if _, ok := b.Node("/app.log"); ok {
		t.Fatal("the deferred flush did not remove the old remote entry")
	}
	// The old fd still reads its own image.
	buf := make([]byte, 16)
	n, rerr := h.Read(ctx, 0, buf)
	if rerr != nil || string(buf[:n]) != "OTHER" {
		t.Fatalf("old fd read = %q err=%v", buf[:n], rerr)
	}
	// And a same-name create no longer collides with the old entry.
	if res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log", Flags: uint32(syscall.O_CREAT | syscall.O_EXCL | syscall.O_RDWR), Mode: 0o644}); err != nil || res.Handle == nil {
		t.Fatalf("recreate = %v", err)
	}
}

// An O_SYNC append publish transfers publication ownership: the later Flush
// must not materialize the lower plane's empty pending create over the
// uploaded bytes.
func TestPortSyncAppendRetiresPendingCreate(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	ext, err := New(Options{Patterns: []string{"**/*.log"}, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(ext, wb, terminal)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/app.log",
		Flags: uint32(syscall.O_CREAT | syscall.O_SYNC | syscall.O_RDWR), Mode: 0o644})
	if err != nil || res.Handle == nil {
		t.Fatalf("open = %v", err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatalf("sync write = %v", err)
	}
	if err := res.Handle.Flush(ctx); err != nil {
		t.Fatalf("flush = %v", err)
	}
	if node, ok := b.Node("/app.log"); !ok || string(node.Data) != "hello" {
		t.Fatalf("/app.log = %q ok=%v, want hello (the sync publish was overwritten)", node.Data, ok)
	}
	_ = res.Handle.Release(ctx)
}

// A tracked sibling's commit is adopted by the other clean FD: its next
// write does not CAS against the pre-commit revision.
func TestPortSiblingCommitAdoptedByCleanFd(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()
	h1 := openLog(t, ch, "/app.log")
	h2 := openLog(t, ch, "/app.log")
	if err := h1.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h1.Sync(ctx); err != nil {
		t.Fatalf("first fsync = %v", err)
	}
	if err := h2.Write(ctx, 1, []byte("Y")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatalf("second fsync = %v (stale baseline was not adopted)", err)
	}
	if node, ok := b.Node("/app.log"); !ok || string(node.Data) != "XYcdef" {
		t.Fatalf("/app.log = %q ok=%v, want XYcdef", node.Data, ok)
	}
	// A loaded clean sibling re-reads the committed image, not its old copy.
	buf := make([]byte, 6)
	if n, rerr := h2.Read(ctx, 0, buf); rerr != nil || string(buf[:n]) != "XYcdef" {
		t.Fatalf("sibling read = %q err=%v", buf[:n], rerr)
	}
}

// A rename out of the pattern keeps the claimed FD's setattr on this plane.
func TestPortSetAttrBypassesPatternAfterRename(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/app.log", []byte("abcdef"))
	ch := newStack(t, b, "**/*.log")
	ctx := context.Background()
	h := openLog(t, ch, "/app.log")
	if err := h.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/app.log", Path2: "/app.txt"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/app.txt", Handle: h, Size: 2, HasSize: true})
	if err != nil {
		t.Fatalf("ftruncate = %v", err)
	}
	_ = res
	if err := h.Sync(ctx); err != nil {
		t.Fatalf("fsync after rename+truncate = %v", err)
	}
	if node, ok := b.Node("/app.txt"); !ok || string(node.Data) != "Xb" {
		t.Fatalf("/app.txt = %q ok=%v, want Xb", node.Data, ok)
	}
}
