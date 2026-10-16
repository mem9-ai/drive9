package git

// Checkpoint/restore rows ported from v1 pkg/fuse/git_workspace_test.go
// (checkpointAllGitWorkspaces / restoreGitStateAtomically behavior): git's
// own file traffic under a workspace's .git debounces a state checkpoint
// (pack + archive) to the server, and a fresh extension restores the state
// into the local overlay atomically.

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
	"github.com/mem9-ai/drive9/pkg/fs"
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

func newGitExt(t *testing.T, b *testbackend.Backend, localRoot string) (*Git, *client.Client, *fs.EventBus) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	bus := fs.NewEventBus()
	g, err := New(Options{Client: cl, Events: bus, LocalRoot: localRoot})
	if err != nil {
		t.Fatal(err)
	}
	// These suites exercise the armed serving plane directly; the on-demand
	// dormant gate is covered by the discovery e2e.
	g.armed = true
	return g, cl, bus
}

// v1: writes under .git schedule (debounce) a state checkpoint — object
// pack + tar.gz archive land on the server.
func TestPortGitStateCheckpointUploadsPackAndArchive(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	b.SeedGitWorkspace("ws1", "/repo")
	localRoot := t.TempDir()
	g, _, bus := newGitExt(t, b, localRoot)
	ctx := context.Background()
	g.ensure(ctx)

	// A real repository in the local overlay under the workspace root,
	// with a bare remote the restore clones its base objects from (v1
	// restoreGitObjectsFromRemote).
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

	// A .git write schedules the checkpoint (v1 scheduleGitStateCheckpoint
	// via the mutation event).
	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/repo/.git/index"})
	// Drain the debounce (v1 drainGitStateCheckpoints).
	if err := g.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	if _, ok := b.GitState("ws1"); !ok {
		t.Fatal("checkpoint did not store git state on the server")
	}
	if len(b.GitPacks("ws1")) == 0 {
		t.Fatal("checkpoint did not store an object pack for the new commit")
	}
}

// v1 restoreGitStateAtomically: a fresh extension start restores the
// checkpointed .git (packs unpacked, archive extracted) into the local
// overlay; the repo is usable afterwards.
func TestPortGitStateRestoreOnStart(t *testing.T) {
	gitBin(t)
	b := testbackend.NewBackend()
	localRoot := t.TempDir()
	// gitDirFor roots the workspace .git under <localRoot>/overlay.
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
	b.SeedGitWorkspaceFull("ws1", "/repo", "file://"+bare, "")
	g, _, bus := newGitExt(t, b, localRoot)
	ctx := context.Background()
	g.ensure(ctx)
	head := run(t, repo, "rev-parse", "HEAD")
	bus.Emit(fs.Event{Kind: fs.EventWrite, Path: "/repo/.git/index"})
	if err := g.Drain(ctx); err != nil {
		t.Fatal(err)
	}

	// Crash: wipe the local overlay (keeping the server's checkpoint).
	// The overlay roots view paths under <localRoot>/overlay (localize's
	// NewOverlay), and the restored .git lands at the same layout the
	// CLI's LocalGitDir expects.
	overlayRepo := filepath.Join(localRoot, "overlay", "repo")
	if err := os.RemoveAll(overlayRepo); err != nil {
		t.Fatal(err)
	}

	// Fresh mount restores (v1 ensureGitWorkspaces restore path).
	g2, _, _ := newGitExt(t, b, localRoot)
	if err := g2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(overlayRepo, ".git", "HEAD")); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(overlayRepo, ".git", "HEAD")); err != nil {
		t.Fatal("restore did not recreate the local .git directory")
	}
	restored := run(t, overlayRepo, "rev-parse", "HEAD")
	if restored != head {
		t.Fatalf("restored HEAD = %s, want %s", restored, head)
	}
	if out := run(t, overlayRepo, "cat-file", "-p", "HEAD^{commit}"); !strings.Contains(out, "one") {
		t.Fatalf("restored commit content missing: %q", out)
	}
}
