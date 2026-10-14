package writeback

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
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

// qiffang C2: the read-plane cache's handle is path-keyed too — after the
// survivor retarget, a cached handle still keyed at the doomed name would
// serve the replacement's pending bytes through the pending seed. The
// cached chain (cache -> writeback -> core, with the mount's seeder
// wiring) must read the survivor's image from the original descriptor.
func TestPortRetargetCoversCachedChain(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/doomed", []byte("OLD"))
	b.Seed("/survivor", []byte("OLD"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyWriteBack, Client: cl, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	rc, cerr := cache.New(cache.Options{PrefetchClient: cl, SequentialPrefetch: true})
	if cerr != nil {
		t.Fatal(cerr)
	}
	wb.SetCommitSeeder(rc.SeedCommit)
	wb.SetPendingSeeder(rc.SeedPending)
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(rc, wb, terminal)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/doomed", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatal(err)
	}
	// The facade's deferred-unlink survivor hook broadcasts to every
	// handler that keys state by path.
	wb.RetargetPath("/doomed", "/survivor")
	rc.RetargetPath("/doomed", "/survivor")

	// The replacement writes at the recycled name; its staging seeds the
	// read cache under that name.
	rres, rerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/doomed", Flags: uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC)})
	if rerr != nil {
		t.Fatal(rerr)
	}
	if err := rres.Handle.Write(ctx, 0, []byte("NEW")); err != nil {
		t.Fatal(err)
	}
	if err := rres.Handle.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, len("OLD"))
	if _, err := res.Handle.Read(ctx, 0, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "OLD" {
		t.Fatalf("cached-chain retargeted descriptor read = %q, want the survivor image OLD", got)
	}
}

// qiffang C3: a successful Sync leaves a clean buffer allocated; that
// stale size must not overwrite a fresher remote listing row. An external
// replace after a synced local write must show through OpList.
func TestPortListingIgnoresCleanSyncedBuffer(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	wb, err := New(Options{Policy: PolicyCloseSync, Client: cl, Staging: mustStaging(t)})
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(wb, terminal)
	ctx := context.Background()

	h := open(t, ch, "/f").(*bufferingHandle)
	if err := h.Write(ctx, 0, []byte("local")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	// The handle stays open with its now-clean buffer while another
	// client replaces the remote file with a larger image.
	b.Seed("/f", make([]byte, 20))

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/"})
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range res.Entries {
		if e.Name == "f" && e.Attr.Size != 20 {
			t.Fatalf("listing size = %d, want the authoritative remote 20 (clean buffer overrode it)", e.Attr.Size)
		}
	}
}
