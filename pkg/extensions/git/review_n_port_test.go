package git

// qiffang twenty-second round: a linked worktree restores its gitdir under
// the common repository with a .git pointer file, the fetched overlay keeps
// Materialized (including empty images), and a checkpoint whose objects did
// not fit the pack budget is refused rather than published unrestorable.

import (
	"context"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPortLinkedWorktreeGitDirResolution(t *testing.T) {
	common := filepath.Join(t.TempDir(), "repo", ".git")
	got, err := linkedWorktreeGitDir(common, ".git/worktrees/w1")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(common, "worktrees", "w1"); got != want {
		t.Fatalf("gitdir = %q, want %q", got, want)
	}
	if _, err := linkedWorktreeGitDir(common, "../escape"); err == nil {
		t.Fatal("escaping gitdir accepted")
	}
	if _, err := linkedWorktreeGitDir(common, ""); err == nil {
		t.Fatal("empty gitdir accepted")
	}
}

// A real worktree archive: the common repo restores normally, the linked
// workspace lands under <common>/.git/worktrees/<name> with a .git pointer
// file at its root, and git resolves HEAD/status through commondir.
func TestPortLinkedWorktreeRestore(t *testing.T) {
	gitBin(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, src, "rev-parse", "HEAD")

	localRoot := t.TempDir()
	repo := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(filepath.Dir(repo), 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, filepath.Dir(repo), "clone", "-q", "file://"+src, repo)
	run(t, repo, "worktree", "add", "-q", "wt", "-b", "b1")
	wtGit := filepath.Join(repo, ".git", "worktrees", "wt")

	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("main", "/repo", "file://"+src, head)
	b.SeedGitWorktree("linked", "/repo/wt", "file://"+src, head, "main", ".git/worktrees/wt")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	g, err := New(Options{Client: cl, LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	mainWS := &workspace{id: "main", root: "/repo", headCommit: head, repoURL: "file://" + src}
	if err := g.checkpointWorkspace(ctx, mainWS, filepath.Join(repo, ".git")); err != nil {
		t.Fatalf("main checkpoint = %v", err)
	}
	content, err := archiveLocalGitStateDir(wtGit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.UpsertGitState(ctx, "linked", client.GitStateRequest{
		CheckpointCommit: head,
		StorageType:      gitStateStorageTarGzNoObjects,
		SizeBytes:        int64(len(content)),
		Content:          content,
	}); err != nil {
		t.Fatalf("linked state upload = %v", err)
	}

	// Crash-equivalent: the whole overlay (both checkouts) is gone.
	if err := os.RemoveAll(filepath.Join(localRoot, "overlay")); err != nil {
		t.Fatal(err)
	}
	restored := &workspace{id: "main", root: "/repo", headCommit: head, repoURL: "file://" + src}
	linked := &workspace{
		id: "linked", root: "/repo/wt", headCommit: head, repoURL: "file://" + src,
		workspaceKind: "linked", commonID: "main", gitDirRel: ".git/worktrees/wt",
	}
	g.workspaces = []*workspace{restored, linked}
	if err := g.restoreWorkspaceState(ctx, restored); err != nil {
		t.Fatalf("main restore = %v", err)
	}
	if err := g.restoreWorkspaceState(ctx, linked); err != nil {
		t.Fatalf("linked restore = %v", err)
	}
	pointer := filepath.Join(localRoot, "overlay", "repo", "wt", ".git")
	info, err := os.Stat(pointer)
	if err != nil || info.IsDir() {
		t.Fatalf("linked root .git = %v (dir=%v), want a pointer file", err, err == nil && info.IsDir())
	}
	if out := run(t, filepath.Dir(pointer), "rev-parse", "--verify", "HEAD"); out != head {
		t.Fatalf("linked HEAD = %q, want %q", out, head)
	}
	// The checkout files are served by the mount, not by the restore: lay
	// the tracked file down and confirm git resolves the worktree through
	// the common repository (index + branch + shared objects).
	wtDir := filepath.Dir(pointer)
	if err := os.WriteFile(filepath.Join(wtDir, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out := run(t, wtDir, "status", "--porcelain"); strings.TrimSpace(out) != "" {
		t.Fatalf("linked status = %q, want clean", out)
	}
	if out := run(t, wtDir, "symbolic-ref", "--short", "HEAD"); out != "b1" {
		t.Fatalf("linked branch = %q, want b1", out)
	}
	if out := run(t, filepath.Join(localRoot, "overlay", "repo"), "worktree", "list", "--porcelain"); !strings.Contains(out, wtDir) {
		t.Fatalf("main worktree list missing the linked checkout: %q", out)
	}
}

// The fetched overlay keeps Materialized for inline images (empty
// included): a rebuilt extension's next write composes on the fetched
// bytes, not the stale base image.
func TestPortOverlayReloadKeepsMaterialized(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/repo/f.txt", []byte("BASE"))
	b.SeedGitWorkspace("ws", "/repo")
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	localRoot := t.TempDir()

	g1, err := New(Options{Client: cl, LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	g1.armed = true
	terminal1, _ := core.New(cl, core.Options{})
	ch1, _ := fs.NewChain(g1, terminal1)
	ctx := context.Background()
	g1.ensure(ctx)
	w1, rel, ok := g1.workspaceFor("/repo/f.txt")
	if !ok {
		t.Fatal("workspace not discovered")
	}
	h := openLog2(t, ch1, "/repo/f.txt")
	if err := h.Write(ctx, 0, []byte("EDIT")); err != nil {
		t.Fatal(err)
	}
	if err := h.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if e, hit := w1.lookup(rel); !hit || string(e.Content) != "EDIT" {
		t.Fatalf("overlay after write = %+v hit=%v", e, hit)
	}

	// A fresh extension re-loads the overlay from the server log.
	g2, err := New(Options{Client: cl, LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	g2.armed = true
	terminal2, _ := core.New(cl, core.Options{})
	ch2, _ := fs.NewChain(g2, terminal2)
	g2.ensure(ctx)
	h2 := openLog2(t, ch2, "/repo/f.txt")
	if err := h2.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := h2.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := h2.Release(ctx); err != nil {
		t.Fatal(err)
	}
	rd, err := ch2.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/f.txt", Offset: 0, Limit: 16})
	if err != nil || string(rd.Data) != "XDIT" {
		t.Fatalf("/repo/f.txt = %q err=%v, want XDIT (fetched overlay authoritative)", rd.Data, err)
	}
}
