package git

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/gitws"
	"github.com/mem9-ai/drive9/pkg/gitwsindex"
)

const testCommit = "c0ffee0000000000000000000000000000000001"

// seedGitWorkspaceFS plants one FS-native workspace record in the fake
// backend: the registry index, the manifest, the state file, the tree
// index, and the clean tree content.
func seedGitWorkspaceFS(t *testing.T, b *testbackend.Backend, id, root string, tree map[string]string) {
	t.Helper()
	entries := make([]gitws.TreeEntry, 0, len(tree))
	dirs := map[string]bool{}
	addParentDirs := func(rel string) {
		for prefix := rel; ; {
			i := strings.LastIndexByte(prefix, '/')
			if i <= 0 {
				return
			}
			prefix = prefix[:i]
			if !dirs[prefix] {
				dirs[prefix] = true
				entries = append(entries, gitws.TreeEntry{Path: prefix, Kind: "dir", Mode: "040755"})
			}
		}
	}
	for rel, content := range tree {
		addParentDirs(rel)
		entries = append(entries, gitws.TreeEntry{Path: rel, Kind: "file", Mode: "100644", Size: int64(len(content))})
		b.Seed(gitws.TreePath(id, testCommit, rel), []byte(content))
	}
	idxBody, err := json.Marshal(&gitwsindex.Index{
		Version:    1,
		Workspaces: []gitwsindex.Entry{{WorkspaceID: id, RootPath: root, WorkspaceKind: "main"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	b.Seed(gitwsindex.Path, idxBody)
	manifestBody, err := gitws.EncodeManifest(&gitws.Manifest{
		WorkspaceID:   id,
		RootPath:      root,
		WorkspaceKind: "main",
		RepoURL:       "https://example.com/repo.git",
		RemoteName:    "origin",
		HeadCommit:    testCommit,
		Mode:          "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	b.Seed(gitws.ManifestPath(id), manifestBody)
	b.Seed(gitws.StatePath(id), []byte(gitws.StateDormant+"\n"))
	treeIdxBody, err := gitws.EncodeTreeIndex(&gitws.TreeIndex{Commit: testCommit, Entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	b.Seed(gitws.TreeIndexPath(id), treeIdxBody)
}

// newStack assembles git (outermost, with the terminal chain as its
// store view) over the core terminal — the shape the default mount wires.
func newStack(t *testing.T, b *testbackend.Backend) (*fs.Chain, *Git) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	storeChain, err := fs.NewChain(terminal)
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Options{Store: fs.HandlerFilesystem(storeChain.Dispatch)})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch, g
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

func read(t *testing.T, ch *fs.Chain, path string) ([]byte, error) {
	t.Helper()
	res, err := ch.Dispatch(context.Background(), &fs.Op{Kind: fs.OpRead, Path: path})
	return res.Data, err
}

// Clean tracked paths serve from the tree layer through the chain (the
// v1 clean-tree gap); untracked paths fall through to the classic
// namespace; the store namespace itself passes through untouched.
func TestCleanTreeServing(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/outside/x", []byte("x"))
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{
		"src/main.go":   "package main\n",
		"src/lib/a.go":  "package lib\n",
		"README.md":     "# repo\n",
		"docs/deepdive": "deep",
	})
	ch, g := newStack(t, b)
	ctx := context.Background()

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo"})
	if err != nil || !res.Attr.IsDir() {
		t.Fatalf("workspace root attr = %+v %v", res.Attr, err)
	}
	attr, err := get(t, ch, "/repo/src/main.go")
	if err != nil || attr.Size != int64(len("package main\n")) {
		t.Fatalf("clean tree attr = %+v %v", attr, err)
	}
	data, err := read(t, ch, "/repo/src/main.go")
	if err != nil || string(data) != "package main\n" {
		t.Fatalf("clean tree read = %q %v", data, err)
	}
	// Untracked path: the classic namespace answers.
	if _, err := read(t, ch, "/repo/untracked.txt"); err == nil {
		t.Fatal("untracked missing file read unexpectedly succeeded")
	}
	// The store namespace is a pass-through.
	data, err = read(t, ch, gitws.TreePath("ws-1", testCommit, "README.md"))
	if err != nil || string(data) != "# repo\n" {
		t.Fatalf("store passthrough read = %q %v", data, err)
	}
	// Outside the workspace root nothing changes.
	if data, err = read(t, ch, "/outside/x"); err != nil || string(data) != "x" {
		t.Fatalf("outside read = %q %v", data, err)
	}
	// Merged root listing: tree entries plus nothing else.
	list, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo"})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range list.Entries {
		names[e.Name] = true
	}
	if !names["src"] || !names["README.md"] {
		t.Fatalf("merged root listing = %v", names)
	}
	// Tracked subdirectory listing.
	list, err = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo/src"})
	if err != nil {
		t.Fatal(err)
	}
	names = map[string]bool{}
	for _, e := range list.Entries {
		names[e.Name] = true
	}
	if !names["main.go"] || !names["lib"] {
		t.Fatalf("merged src listing = %v", names)
	}
	_ = g
}

// Writes to tracked paths land in the dirty mirror (never the classic
// namespace); writes outside the workspace go to the base as before.
func TestWorkspaceDiscoveryAndScoping(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/repo/base.txt", []byte("base"))
	b.Seed("/outside/x", []byte("x"))
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{
		"tracked.txt": "clean",
	})
	ch, _ := newStack(t, b)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/tracked.txt", Offset: 0, Data: []byte("dirty"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if n, ok := b.Node(gitws.OverlayPath("ws-1", "tracked.txt")); !ok || string(n.Data) != "dirty" {
		t.Fatalf("mirror write = %+v", n)
	}
	// The mirror shadows the clean tree; the classic tree file is intact.
	if n, _ := b.Node(gitws.TreePath("ws-1", testCommit, "tracked.txt")); string(n.Data) != "clean" {
		t.Fatalf("clean tree mutated: %q", n.Data)
	}
	if n, ok := b.Node("/repo/tracked.txt"); ok && n.Data != nil {
		t.Fatal("workspace write leaked into the classic namespace")
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/untracked.txt", Offset: 0, Data: []byte("plain"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/repo/untracked.txt"); string(n.Data) != "plain" {
		t.Fatal("untracked write did not reach the classic namespace")
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/outside/y", Offset: 0, Data: []byte("plain"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/outside/y"); string(n.Data) != "plain" {
		t.Fatal("non-workspace write did not reach the base")
	}
	// .git traffic passes through to the classic namespace untouched.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/.git/HEAD", Offset: 0, Data: []byte("ref: refs/heads/main\n"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node("/repo/.git/HEAD"); string(n.Data) != "ref: refs/heads/main\n" {
		t.Fatal(".git write did not pass through")
	}
}

func TestOverlayMergesOverTree(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{
		"keep.txt": "keep",
		"gone.txt": "gone",
		"edit.txt": "clean",
		"sub/deep": "d",
	})
	ch, _ := newStack(t, b)
	ctx := context.Background()

	// A blackout hides the clean tree entry without touching it.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/gone.txt"}); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, ch, "/repo/gone.txt"); err == nil {
		t.Fatal("blackout did not hide the clean tree file")
	}
	if n, _ := b.Node(gitws.TreePath("ws-1", testCommit, "gone.txt")); string(n.Data) != "gone" {
		t.Fatal("blackout removed clean tree content")
	}
	if _, ok := b.Node(gitws.BlackoutPath("ws-1", "gone.txt")); !ok {
		t.Fatal("blackout marker missing")
	}
	// Untouched clean content stays visible.
	if attr, err := get(t, ch, "/repo/keep.txt"); err != nil || attr.Size != 4 {
		t.Fatalf("clean under workspace = %+v %v", attr, err)
	}
	// Mirror write + read-your-write through the chain.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/edit.txt", Offset: 0, Data: []byte("dirty"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	data, err := read(t, ch, "/repo/edit.txt")
	if err != nil || string(data) != "dirty" {
		t.Fatalf("mirror read = %q %v", data, err)
	}
	// A write at an offset merges over the current image.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/edit.txt", Offset: 5, Data: []byte("XX"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node(gitws.OverlayPath("ws-1", "edit.txt")); string(n.Data) != "dirtyXX" {
		t.Fatalf("offset write merged = %q", n.Data)
	}
	// Merged listing contains tree + mirror minus blackouts.
	list, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo"})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range list.Entries {
		names[e.Name] = true
	}
	if !names["keep.txt"] || !names["edit.txt"] || names["gone.txt"] {
		t.Fatalf("merged listing = %v", names)
	}
}

func TestRenameMovesMirrorEntry(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{
		"a": "mv",
		"t": "tracked-clean",
	})
	ch, _ := newStack(t, b)
	ctx := context.Background()

	// Mirror-to-mirror rename moves the store node.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/a", Offset: 0, Data: []byte("mv"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/repo/a", Path2: "/repo/b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := get(t, ch, "/repo/a"); err == nil {
		t.Fatal("old path visible after rename")
	}
	data, err := read(t, ch, "/repo/b")
	if err != nil || string(data) != "mv" {
		t.Fatalf("renamed mirror content = %q %v", data, err)
	}
	// Tracked-to-tracked rename copies into the mirror and blackouts the origin.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/repo/t", Path2: "/repo/t2"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := b.Node(gitws.OverlayPath("ws-1", "t2")); string(n.Data) != "tracked-clean" {
		t.Fatalf("renamed tracked content = %q", n.Data)
	}
	if _, ok := b.Node(gitws.BlackoutPath("ws-1", "t")); !ok {
		t.Fatal("tracked origin not blacked out after rename")
	}
}

func TestSymlinkRouting(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{})
	b.SeedSymlink(gitws.TreePath("ws-1", testCommit, "clean-link"), "/repo/target")
	treeIdx, err := gitws.EncodeTreeIndex(&gitws.TreeIndex{Commit: testCommit, Entries: []gitws.TreeEntry{
		{Path: "clean-link", Kind: "symlink", Mode: "120777", Size: 12},
	}})
	if err != nil {
		t.Fatal(err)
	}
	b.Seed(gitws.TreeIndexPath("ws-1"), treeIdx)
	ch, _ := newStack(t, b)
	ctx := context.Background()

	// Tree-layer symlink readlink goes through the store.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpReadlink, Path: "/repo/clean-link"})
	if err != nil || string(res.Data) != "/repo/target" {
		t.Fatalf("tree readlink = %q %v", res.Data, err)
	}
	// A symlink over a blacked-out tracked path re-mirrors.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/clean-link"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSymlink, Path: "/repo/clean-link", Target: "/other"}); err != nil {
		t.Fatal(err)
	}
	res, err = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpReadlink, Path: "/repo/clean-link"})
	if err != nil || string(res.Data) != "/other" {
		t.Fatalf("mirrored readlink = %q %v", res.Data, err)
	}
	// Untracked symlink creation goes to the classic namespace.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSymlink, Path: "/repo/plain-link", Target: "/x"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.Node("/repo/plain-link"); !ok {
		t.Fatal("classic symlink missing")
	}
}

// Writes on a claimed workspace file buffer locally: nothing reaches the
// mirror until a durability boundary, which lands ONE whole image.
func TestWorkspaceBufferedWriteCommitsOnceAtFlush(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws1", "/", map[string]string{
		"w.txt": "seed",
	})
	ch, _ := newStack(t, b)
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
	if _, ok := b.Node(gitws.OverlayPath("ws1", "w.txt")); ok {
		t.Fatal("writes reached the mirror before the boundary")
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	n, ok := b.Node(gitws.OverlayPath("ws1", "w.txt"))
	if !ok {
		t.Fatal("mirror write missing after flush")
	}
	if string(n.Data) != "one two three" {
		t.Fatalf("mirror content = %q, want the merged image", n.Data)
	}
	// The claim seeds from the clean tree when the offset grows beyond
	// the written prefix: the tree bytes survive underneath.
	if err := h.Write(ctx, 13, []byte("!")); err != nil {
		t.Fatal(err)
	}
	if err := h.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if n, _ = b.Node(gitws.OverlayPath("ws1", "w.txt")); string(n.Data) != "one two three"+"!" {
		t.Fatalf("seeded merge = %q", n.Data)
	}
}

// A reload (the mount-start reload racing the first ops, or an index
// event) re-reads the store and must not lose this mount's mirrored
// state — the store is the truth and the walk re-derives it.
func TestReloadPreservesOverlayMirror(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{})
	ch, g := newStack(t, b)
	ctx := context.Background()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpWrite, Path: "/repo/edit.txt", Offset: 0, Data: []byte("mirrored"), Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	g.Reload(ctx)
	data, err := read(t, ch, "/repo/edit.txt")
	if err != nil || string(data) != "mirrored" {
		t.Fatalf("mirror after reload = %q %v", data, err)
	}
}

// A deleted workspace disappears from the view: the index entry is the
// kill-switch.
func TestDeletedWorkspaceDropsFromView(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws-1", "/repo", map[string]string{"a": "x"})
	ch, g := newStack(t, b)
	ctx := context.Background()

	if _, err := read(t, ch, "/repo/a"); err != nil {
		t.Fatal(err)
	}
	idxBody, err := json.Marshal(&gitwsindex.Index{Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	b.Seed(gitwsindex.Path, idxBody)
	g.Reload(ctx)
	if _, err := get(t, ch, "/repo/a"); err == nil {
		t.Fatal("deleted workspace still serving the view")
	}
	if _, ok := b.Node(gitws.TreePath("ws-1", testCommit, "a")); !ok {
		t.Fatal("store subtree must survive deregistration (crash safety)")
	}
}

// The extensions-branch review round taught three semantics the FS-native
// stack must keep: tenant-absolute roots re-root onto a subtree mount
// (v1 fs.localPath), clean-tree stats carry a stable mtime so git's index
// trust survives status passes, and the .git routing predicate answers
// from the workspace table plus the pending markers.
func TestRemoteRootRerootingAndStableTreeMtime(t *testing.T) {
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws-1", "/sub/repo", map[string]string{"a.go": "package a\n"})
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	storeChain, err := fs.NewChain(terminal)
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Options{Store: fs.HandlerFilesystem(storeChain.Dispatch), RemoteRoot: "/sub"})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	// The tenant-absolute root /sub/repo serves at the mount-local /repo.
	first, err := get(t, ch, "/repo/a.go")
	if err != nil || first.Size != int64(len("package a\n")) {
		t.Fatalf("re-rooted clean attr = %+v %v", first, err)
	}
	second, err := get(t, ch, "/repo/a.go")
	if err != nil || !first.Mtime.Equal(second.Mtime) {
		t.Fatalf("clean-tree mtime not stable: %v vs %v (%v)", first.Mtime, second.Mtime, err)
	}
	if first.Mtime.IsZero() {
		t.Fatal("clean-tree mtime is zero (git would re-hash on every status)")
	}

	// .git of the discovered workspace routes local; anything else does not.
	if !g.RoutesLocal("/repo/.git/config") {
		t.Fatal("workspace .git not declared local")
	}
	if !g.RoutesLocal("/repo/.git") {
		t.Fatal("workspace .git dir not declared local")
	}
	if g.RoutesLocal("/repo/.gitignore") {
		t.Fatal(".gitignore declared local")
	}
	if g.RoutesLocal("/other/.git/config") {
		t.Fatal("unregistered root declared local")
	}
}
