package git

// qiffang twenty-first round: a path-level chmod/truncate of a fast-clone
// file (clean HEAD-tree node, no overlay entry) must materialize the blob
// instead of forwarding to core and answering ENOENT.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPortCleanTreePathSetAttrMaterializes(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	localRoot := t.TempDir()
	repo := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "init", "-q", "-b", "main")
	for name, body := range map[string]string{"trunc.txt": "hello", "chmod.txt": "world", "move.txt": "move-me"} {
		if err := os.WriteFile(filepath.Join(repo, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	run(t, repo, "add", ".")
	run(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, repo, "rev-parse", "HEAD")
	shas := map[string]string{
		"trunc.txt": run(t, repo, "rev-parse", "HEAD:trunc.txt"),
		"chmod.txt": run(t, repo, "rev-parse", "HEAD:chmod.txt"),
		"move.txt":  run(t, repo, "rev-parse", "HEAD:move.txt"),
	}
	b.SeedGitWorkspaceFull("ws1", "/repo", "file://"+repo, head)

	g, cl, _ := newGitExt(t, b, localRoot)
	g.armed = true
	ctx := context.Background()
	g.ensure(ctx)

	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	w, _, ok := g.workspaceFor("/repo/trunc.txt")
	if !ok {
		t.Fatal("workspace not discovered")
	}
	w.mu.Lock()
	w.tree = map[string]client.GitTreeNode{}
	for name, sha := range shas {
		w.tree[name] = client.GitTreeNode{Path: name, Name: name, Kind: "file", Mode: "100644", ObjectSHA: sha, SizeBytes: int64(len(name))}
	}
	w.mu.Unlock()

	// The kernel's O_TRUNC pre-step reaches the path-level branch with no
	// handle and no overlay entry: it must truncate the materialized blob.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/trunc.txt", Size: 0}); err != nil {
		t.Fatalf("clean-tree truncate = %v", err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/trunc.txt", Offset: 0, Limit: 16})
	if err != nil || len(rd.Data) != 0 {
		t.Fatalf("truncated clean-tree file = %q err=%v, want empty", rd.Data, err)
	}

	// A path-level chmod of another clean node lands as an overlay upsert.
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/chmod.txt", HasMode: true, Mode: 0o600, Size: -1}); err != nil {
		t.Fatalf("clean-tree chmod = %v", err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/chmod.txt"})
	if err != nil || res.Attr == nil {
		t.Fatalf("getattr after chmod = %v attr=%v", err, res.Attr)
	}
	if res.Attr.Mode&0o777 != 0o600 {
		t.Fatalf("chmod'd clean-tree mode = %#o, want 0600", res.Attr.Mode)
	}
}

// A rename whose source exists only in the fast-clone HEAD tree resolves
// through the Git content path: the target overlay carries the blob and
// the source is whiteouted (a namespace stat/read would 404).
func TestPortCleanTreeRenameMaterializesSource(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	localRoot := t.TempDir()
	repo := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "move.txt"), []byte("move-me"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "add", ".")
	run(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, repo, "rev-parse", "HEAD")
	sha := run(t, repo, "rev-parse", "HEAD:move.txt")
	b.SeedGitWorkspaceFull("ws1", "/repo", "file://"+repo, head)

	g, cl, _ := newGitExt(t, b, localRoot)
	g.armed = true
	ctx := context.Background()
	g.ensure(ctx)
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	w, _, ok := g.workspaceFor("/repo/move.txt")
	if !ok {
		t.Fatal("workspace not discovered")
	}
	w.mu.Lock()
	w.tree = map[string]client.GitTreeNode{
		"move.txt": {Path: "move.txt", Name: "move.txt", Kind: "file", Mode: "100644", ObjectSHA: sha, SizeBytes: 7},
	}
	w.mu.Unlock()

	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/repo/move.txt", Path2: "/repo/moved.txt"}); err != nil {
		t.Fatalf("clean-tree rename = %v", err)
	}
	rd, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/moved.txt", Offset: 0, Limit: 32})
	if err != nil || string(rd.Data) != "move-me" {
		t.Fatalf("/repo/moved.txt = %q err=%v, want the materialized blob", rd.Data, err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/move.txt"}); err == nil {
		t.Fatal("clean-tree rename source still resolves")
	}
}

// A whiteouted tracked file stays out of fresh listings: the tree pass
// used to re-add every name "not in out", resurrecting the deletion in
// READDIRPLUS.
func TestPortCleanTreeListRespectsWhiteout(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	localRoot := t.TempDir()
	repo := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "gone.txt"), []byte("bye"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "add", ".")
	run(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, repo, "rev-parse", "HEAD")
	sha := run(t, repo, "rev-parse", "HEAD:gone.txt")
	b.SeedGitWorkspaceFull("ws1", "/repo", "file://"+repo, head)

	g, cl, _ := newGitExt(t, b, localRoot)
	g.armed = true
	ctx := context.Background()
	g.ensure(ctx)
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	w, _, ok := g.workspaceFor("/repo/gone.txt")
	if !ok {
		t.Fatal("workspace not discovered")
	}
	w.mu.Lock()
	w.tree = map[string]client.GitTreeNode{
		"gone.txt": {Path: "gone.txt", Name: "gone.txt", Kind: "file", Mode: "100644", ObjectSHA: sha, SizeBytes: 3},
	}
	w.mu.Unlock()

	listed := func() bool {
		res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo"})
		if err != nil {
			t.Fatalf("list = %v", err)
		}
		for _, e := range res.Entries {
			if e.Name == "gone.txt" {
				return true
			}
		}
		return false
	}
	if !listed() {
		t.Fatal("clean-tree file missing before unlink")
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/gone.txt"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	if listed() {
		t.Fatal("whiteouted tree file reappeared in a fresh listing")
	}
}
