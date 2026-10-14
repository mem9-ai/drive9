package git

// Review follow-up (third round): the lazy .git restore must retry after
// a missing-checkpoint attempt — ensure-before-upload, upload, then
// ensure-after-upload restores. A sync.Once consumed the failed attempt
// and every later ensure no-oped while restoreLatched stayed false.

import (
	"context"
	"strings"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
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
