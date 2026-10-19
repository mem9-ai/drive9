package localize

// qiffang twenty-first round: a local-only .git anchors gitignore routing,
// transient sidecars join a remote parent's listing, and local attribute
// replies keep the special mode bits.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// The restored checkout lives ONLY in the local overlay: the inner chain
// cannot answer .git, so the repository root must come from the overlay or
// the negative answer caches and gitignore-aware rules shadow tracked files.
func TestPortLocalGitRootFromOverlay(t *testing.T) {
	inner := fs.HandlerFilesystem(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, fs.ErrNotFound
	})
	ctx := context.Background()

	dirRepo := newTestRouter(t, nil, nil)
	if err := os.MkdirAll(filepath.Join(dirRepo.overlay.Root(), "repo", ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if root, ok := dirRepo.repoRoot(ctx, inner, "/repo/sub"); !ok || root != "/repo" {
		t.Fatalf(".git DIRECTORY root = %q ok=%v, want /repo", root, ok)
	}

	// A linked worktree/submodule records .git as a FILE.
	fileRepo := newTestRouter(t, nil, nil)
	gitFile := filepath.Join(fileRepo.overlay.Root(), "repo", ".git")
	if err := os.MkdirAll(filepath.Dir(gitFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gitFile, []byte("gitdir: /elsewhere/.git/worktrees/w\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if root, ok := fileRepo.repoRoot(ctx, inner, "/repo/sub"); !ok || root != "/repo" {
		t.Fatalf(".git FILE root = %q ok=%v, want /repo", root, ok)
	}

	// No local .git anywhere: the negative answer stands.
	empty := newTestRouter(t, nil, nil)
	if root, ok := empty.repoRoot(ctx, inner, "/repo/sub"); ok || root != "" {
		t.Fatalf("root without .git = %q ok=%v, want no repository", root, ok)
	}
}

// A transient -shm sidecar created through transient storage must appear in
// a fresh listing of its REMOTE-routed parent (the local overlay pass only
// walked the persistent plane).
func TestPortTransientChildrenJoinRemoteListing(t *testing.T) {
	f, err := New(Options{LocalRoot: t.TempDir(), TransientRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	if f.transient == nil {
		t.Fatal("transient plane disabled")
	}
	sidecar := filepath.Join(f.transient.Root(), "repo", "cache.db-shm")
	if err := os.MkdirAll(filepath.Dir(sidecar), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sidecar, []byte("shm"), 0o600); err != nil {
		t.Fatal(err)
	}

	res, err := remoteListOp(t, f, &fs.Op{Kind: fs.OpList, Path: "/repo"}, []fs.Entry{
		{Name: "main.go", Attr: fs.Attr{Kind: fs.KindFile}},
	})
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range res.Entries {
		names[e.Name] = true
	}
	if !names["main.go"] || !names["cache.db-shm"] {
		t.Fatalf("listing = %v, want the remote row and the transient sidecar", names)
	}
}

// A successful local chmod(01777/04755) must read back through Lookup/
// GetAttr: FileMode.Perm() masks setuid/setgid/sticky, so the overlay's own
// marks vanished from every reply (renameguard's sticky preflight included).
func TestPortLocalAttrsKeepSpecialModeBits(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpMkdir, Path: "/repo/node_modules/d", Mode: 0o755}); err != nil {
		t.Fatalf("mkdir = %v", err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/node_modules/d", HasMode: true, Mode: 0o1777, Size: -1}); err != nil {
		t.Fatalf("chmod dir = %v", err)
	}
	res, err := doOp(t, f, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/node_modules/d"})
	if err != nil || res.Attr == nil {
		t.Fatalf("getattr dir = %v attr=%v", err, res.Attr)
	}
	if res.Attr.Mode != 0o1777 {
		t.Fatalf("dir mode = %#o, want 01777 (sticky lost)", res.Attr.Mode)
	}

	writeBacking(t, f, "repo/node_modules/suid", "x")
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/node_modules/suid", HasMode: true, Mode: 0o4755, Size: -1}); err != nil {
		t.Fatalf("chmod file = %v", err)
	}
	res, err = doOp(t, f, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/node_modules/suid"})
	if err != nil || res.Attr == nil {
		t.Fatalf("getattr file = %v attr=%v", err, res.Attr)
	}
	if res.Attr.Mode != 0o4755 {
		t.Fatalf("file mode = %#o, want 04755 (setuid lost)", res.Attr.Mode)
	}
}
