package git

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

func newStack(t *testing.T, b *testbackend.Backend) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	g, err := New(Options{Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func get(t *testing.T, ch *fs.Chain, path string) (fs.Attr, error) {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpGetAttr, Path: path})
	if err != nil {
		return fs.Attr{}, err
	}
	if res.Attr == nil {
		t.Fatal("no attr")
	}
	return *res.Attr, nil
}

func TestWorkspaceDiscoveryAndScoping(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/repo/base.txt", []byte("base"))
	b.Seed("/outside/x", []byte("x"))
	b.SeedGitWorkspace("ws-1", "/repo")
	ch := newStack(t, b)
	ctx := context.Background()

	// Inside the workspace root writes go to the overlay; outside they go
	// straight to the base namespace.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/ov.txt", Offset: 0, Data: []byte("ws"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node("/repo/ov.txt"); ok && n.Data != nil {
		t.Fatal("workspace write leaked into the base namespace")
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/outside/y", Offset: 0, Data: []byte("plain"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/outside/y"); string(n.Data) != "plain" {
		t.Fatal("non-workspace write did not reach the base")
	}

	// The overlay entry is recorded server-side.
	ov := b.GitOverlay("ws-1")
	e, ok := ov["ov.txt"]
	if !ok || e["op"] != "upsert" {
		t.Fatalf("overlay log = %+v", ov)
	}
}

func TestOverlayMergesOverBase(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/repo/keep.txt", []byte("keep"))
	b.Seed("/repo/gone.txt", []byte("gone"))
	b.SeedGitWorkspace("ws-1", "/repo")
	ch := newStack(t, b)
	ctx := context.Background()

	// overlay delete hides base content without touching it
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/gone.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, ch, "/repo/gone.txt"); err == nil {
		t.Fatal("overlay delete did not hide the base file")
	}
	if n, _ := b.Node("/repo/gone.txt"); n.Data == nil {
		t.Fatal("overlay delete removed base content")
	}
	// untouched base content stays visible
	if attr, err := get(t, ch, "/repo/keep.txt"); err != nil || attr.Size != 4 {
		t.Fatalf("base under workspace = %+v %v", attr, err)
	}
	// overlay write + read-your-write
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/new.txt", Offset: 0, Data: []byte("added"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/new.txt"})
	if err != nil || string(res.Data) != "added" {
		t.Fatalf("overlay read = %q %v", res.Data, err)
	}
	// merged listing contains base + overlay minus deletes
	list, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo"})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range list.Entries {
		names[e.Name] = true
	}
	if !names["keep.txt"] || !names["new.txt"] || names["gone.txt"] {
		t.Fatalf("merged listing = %v", names)
	}
}

func TestRenameMovesOverlayEntry(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedGitWorkspace("ws-1", "/repo")
	ch := newStack(t, b)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/a", Offset: 0, Data: []byte("mv"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/repo/a", Path2: "/repo/b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, ch, "/repo/a"); err == nil {
		t.Fatal("old path visible after rename")
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/b"})
	if err != nil || string(res.Data) != "mv" {
		t.Fatalf("renamed overlay content = %q %v", res.Data, err)
	}
	ov := b.GitOverlay("ws-1")
	if ov["a"]["op"] != "delete" || ov["b"]["op"] != "upsert" {
		t.Fatalf("overlay after rename = %+v", ov)
	}
}

func TestSymlinkThroughOverlay(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedGitWorkspace("ws-1", "/repo")
	ch := newStack(t, b)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSymlink, Path: "/repo/ln", Target: "/repo/target"}); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpReadlink, Path: "/repo/ln"})
	if err != nil || string(res.Data) != "/repo/target" {
		t.Fatalf("readlink = %q %v", res.Data, err)
	}
	if attr, err := get(t, ch, "/repo/ln"); err != nil || !attr.IsSymlink() {
		t.Fatalf("symlink attr = %+v %v", attr, err)
	}
}

// Writes on a claimed workspace file buffer locally: nothing reaches the
// overlay log until a durability boundary, which lands ONE entry with the
// merged image.
func TestWorkspaceBufferedWriteCommitsOnceAtFlush(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedGitWorkspace("ws1", "/")
	ch := newStack(t, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/w.txt", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*gitHandle)
	if !ok {
		t.Fatalf("open not claimed by the workspace write plane: %T", res.Handle)
	}
	for i, chunk := range []string{"one ", "two ", "three"} {
		if err := h.Write(ctx, int64(i*4), []byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if entries := b.GitOverlay("ws1"); len(entries) != 0 {
		t.Fatalf("writes reached the overlay log before the boundary: %d", len(entries))
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	entries := b.GitOverlay("ws1")
	if len(entries) != 1 {
		t.Fatalf("overlay log after flush = %+v, want one upsert", entries)
	}
	entry := entries["w.txt"]
	if entry["op"] != "upsert" {
		t.Fatalf("overlay entry = %+v, want an upsert", entry)
	}
	if string(entry["content"].([]byte)) != "one two three" {
		t.Fatalf("entry content = %v, want the merged image", entry["content"])
	}
}
