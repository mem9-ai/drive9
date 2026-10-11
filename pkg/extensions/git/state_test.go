package git

// Checkpoint/restore rows ported from v1 pkg/fuse/git_workspace_test.go
// (checkpointAllGitWorkspaces / restoreGitStateAtomically behavior): git's
// own file traffic under a workspace's .git debounces a state checkpoint
// (pack + archive) into the /.drive9/git-workspaces store, and a fresh
// extension restores the state into the local overlay atomically.

import (
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/gitws"
)

func gitBin(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
}

func run(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// newGitExt wires the extension over a core terminal, with the terminal
// chain as its store view — the mount shape.
func newGitExt(t *testing.T, b *testbackend.Backend, localRoot string) (*Git, *fs.EventBus) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	storeChain, err := fs.NewChain(terminal)
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Options{Store: fs.HandlerFilesystem(storeChain.Dispatch), Events: bus, LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	return g, bus
}

func mustBytes(t *testing.T, b *testbackend.Backend, path string) []byte {
	t.Helper()
	n, ok := b.Node(path)
	if !ok {
		t.Fatalf("fixture %s missing", path)
	}
	return n.Data
}

// seedWorkspaceWithRepo rewrites the workspace record's repo URL (the
// restore clones its base objects from it).
func seedWorkspaceWithRepo(t *testing.T, b *testbackend.Backend, id, root, repoURL string) {
	t.Helper()
	manifest, err := gitws.ParseManifest(mustBytes(t, b, gitws.ManifestPath(id)))
	if err != nil {
		t.Fatal(err)
	}
	manifest.RepoURL = repoURL
	body, err := gitws.EncodeManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	b.Seed(gitws.ManifestPath(id), body)
}

// v1: writes under .git schedule (debounce) a state checkpoint — object
// pack + tar.gz archive land in the store.
func TestPortGitStateCheckpointUploadsPackAndArchive(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	seedGitWorkspaceFS(t, b, "ws1", "/repo", nil)
	localRoot := t.TempDir()
	g, bus := newGitExt(t, b, localRoot)
	ctx := context.Background()
	g.Reload(ctx)

	// A real repository in the local overlay under the workspace root
	// (<LocalRoot>/overlay/repo/.git is the extension's git dir), with a
	// bare remote the restore clones its base objects from.
	repo := filepath.Join(localRoot, "overlay", "repo")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "add", "f.txt")
	run(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	bare := filepath.Join(t.TempDir(), "base.git")
	run(t, repo, "clone", "-q", "--bare", repo, bare)
	seedWorkspaceWithRepo(t, b, "ws1", "/repo", "file://"+bare)
	g.Reload(ctx)

	// A .git write schedules the checkpoint (scheduleGitStateCheckpoint
	// via the mutation event).
	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/repo/.git/index"})
	if err := g.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	if _, ok := b.Node(gitws.GitStatePath("ws1")); !ok {
		t.Fatal("checkpoint did not store the git state archive")
	}
	packs, err := g.listObjectPacks(ctx, "ws1")
	if err != nil {
		t.Fatal(err)
	}
	if len(packs) == 0 {
		t.Fatal("checkpoint did not store an object pack for the new commit")
	}
}

// v1 restoreGitStateAtomically: a fresh extension start restores the
// checkpointed .git (remote clone + packs unpacked + archive extracted)
// into the local overlay; the repo is usable afterwards.
func TestPortGitStateRestoreOnStart(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	localRoot := t.TempDir()
	repo := filepath.Join(localRoot, "overlay", "repo")
	bare := filepath.Join(t.TempDir(), "base.git")
	if err := os.MkdirAll(repo, 0o755); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(repo, "f.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	run(t, repo, "add", "f.txt")
	run(t, repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	run(t, repo, "clone", "-q", "--bare", repo, bare)
	seedGitWorkspaceFS(t, b, "ws1", "/repo", nil)
	seedWorkspaceWithRepo(t, b, "ws1", "/repo", "file://"+bare)
	g, bus := newGitExt(t, b, localRoot)
	ctx := context.Background()
	g.Reload(ctx)
	head := run(t, repo, "rev-parse", "HEAD")
	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/repo/.git/index"})
	if err := g.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	// Crash: wipe the local overlay (keeping the store's checkpoint).
	if err := os.RemoveAll(repo); err != nil {
		t.Fatal(err)
	}

	// Fresh mount restores (the Start restore path).
	g2, _ := newGitExt(t, b, localRoot)
	if err := g2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(repo, ".git", "HEAD")); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(repo, ".git", "HEAD")); err != nil {
		t.Fatal("restore did not recreate the local .git directory")
	}
	restored := run(t, repo, "rev-parse", "HEAD")
	if restored != head {
		t.Fatalf("restored HEAD = %s, want %s", restored, head)
	}
	if out := run(t, repo, "cat-file", "-p", "HEAD^{commit}"); !strings.Contains(out, "one") {
		t.Fatalf("restored commit content missing: %q", out)
	}
}
