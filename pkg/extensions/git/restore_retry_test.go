package git

// Review follow-up (third round): the lazy .git restore must retry after
// a missing-checkpoint attempt — ensure-before-upload, upload, then
// ensure-after-upload restores. A sync.Once consumed the failed attempt
// and every later ensure no-oped while restoreLatched stayed false.

import (
	"context"
	"strings"
	"syscall"
	"os"
	"path/filepath"
	"testing"
	"time"

	"net/http/httptest"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/client"
)

func TestPortGitStateRestoreRetriesAfterNotReady(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	localRoot := t.TempDir()
	// The workspace is known, but its checkpoint has not been uploaded
	// yet (discovery races the CLI's upload), and no local .git exists.
	b.SeedGitWorkspace("ws1", "/repo")
	g, _, _ := newGitExt(t, b, localRoot)
	g.armed = true
	ctx := context.Background()
	g.ensure(ctx)
	repo := filepath.Join(localRoot, "overlay", "repo")
	if _, err := os.Stat(filepath.Join(repo, ".git", "HEAD")); err == nil {
		t.Fatal("pre-upload ensure unexpectedly restored .git")
	}

	// The checkpoint arrives: the CLI-side mount builds the repo and
	// drains, uploading the workspace state.
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "add", "f.txt")
	run(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	uploader, _, ubus := newGitExt(t, b, localRoot)
	uploader.armed = true
	uploader.ensure(ctx)
	ubus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/repo/.git/index"})
	if err := uploader.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.GitState("ws1"); !ok {
		t.Fatal("drain did not upload the ws1 checkpoint")
	}
	// The workspace's list row gains its object source (the CLI registers
	// the repo URL and head with the checkpoint).
	bare := filepath.Join(t.TempDir(), "base.git")
	run(t, repo, "clone", "-q", "--bare", repo, bare)
	head := run(t, repo, "rev-parse", "HEAD")
	b.SeedGitWorkspaceFull("ws1", "/repo", "file://"+bare, strings.TrimSpace(head))

	// Crash-equivalent: wipe the local overlay, then ensure again — the
	// retry must now restore .git.
	if err := os.RemoveAll(repo); err != nil {
		t.Fatal(err)
	}
	// Refreshing known workspaces re-enters the restore path (normal
	// reload/rescan).
	g.Reload(ctx)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(repo, ".git", "HEAD")); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("ensure after checkpoint upload did not restore .git/HEAD (retry gate consumed by the earlier miss)")
}

// MaterializeHeadTree must actually hydrate the HEAD tree's blobs on a
// partial clone: ls-tree records are `<mode> <type> <oid>`, and parsing
// the type from meta[0] (the mode) matched nothing — hydration succeeded
// without fetching a single blob.
func TestPortMaterializeHeadTreeHydratesPartialClone(t *testing.T) {
	gitBin(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("blob-content"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	run(t, src, "config", "uploadpack.allowFilter", "true")

	// A real partial clone: no blobs locally.
	partial := t.TempDir()
	run(t, partial, "clone", "-q", "--filter=blob:none", "--no-checkout", "file://"+src, partial)
	if out := run(t, partial, "rev-list", "--objects", "--missing=print", "HEAD"); !strings.Contains(out, "?") {
		t.Fatalf("precondition: partial clone reports no missing blobs (%q)", out)
	}

	gitDir := filepath.Join(partial, ".git")
	b2 := testbackend.NewBackend()
	ts := httptest.NewServer(b2)
	t.Cleanup(ts.Close)
	g, _ := New(Options{Client: client.New(ts.URL, "")})
	if err := g.MaterializeHeadTree(context.Background(), gitDir); err != nil {
		t.Fatal(err)
	}
	if out := run(t, partial, "rev-list", "--objects", "--missing=print", "HEAD"); strings.Contains(out, "?") {
		t.Fatalf("HEAD blob still missing after materialization: %q", out)
	}
}

// mashenjun's row: a created-but-never-written file in a Git workspace
// publishes its empty image at the durability boundary — Sync and Release
// both owe the name's existence.
func TestPortGitEmptyCreatePersists(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	b.SeedGitWorkspace("ws-1", "/repo")
	localRoot := t.TempDir()
	g, cl, _ := newGitExt(t, b, localRoot)
	g.armed = true
	ctx := context.Background()
	g.ensure(ctx)
	terminal, _ := core.New(cl, core.Options{})
	ch, _ := fs.NewChain(g, terminal)

	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/repo/empty", Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644})
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/empty"}); err != nil {
		t.Fatalf("empty create did not survive its durability boundary: %v", err)
	}
}
