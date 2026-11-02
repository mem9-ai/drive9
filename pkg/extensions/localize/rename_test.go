package localize

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// stubView is a minimal lower chain: attribute lookups answer from a fixed
// table (missing otherwise), and renames are recorded instead of executed.
type stubView struct {
	renames []string
	getattr []string
	attrs   map[string]fs.Attr
}

func (s *stubView) dispatch(ctx context.Context, op *fs.Op) (fs.Result, error) {
	switch op.Kind {
	case fs.OpGetAttr:
		s.getattr = append(s.getattr, op.Path)
		if a, ok := s.attrs[op.Path]; ok {
			return fs.Result{Attr: &a}, nil
		}
		return fs.Result{}, fs.ErrNotFound
	case fs.OpRename:
		s.renames = append(s.renames, op.Path+"->"+op.Path2)
		return fs.Result{}, nil
	}
	return fs.Result{}, nil
}

func newTestRouter(t *testing.T, localOnly, remoteOnly []string) *Localize {
	t.Helper()
	f, err := New(Options{LocalRoot: t.TempDir(), LocalOnlyPatterns: localOnly, RemoteOnlyPatterns: remoteOnly})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func writeBacking(t *testing.T, f *Localize, rel, body string) {
	t.Helper()
	abs := filepath.Join(f.overlay.Root(), filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(abs, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func doRename(t *testing.T, f *Localize, view *stubView, oldP, newP string) error {
	t.Helper()
	_, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpRename, Path: oldP, Path2: newP}, view.dispatch)
	return err
}

func TestRenameSamePathIsNoop(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	view := &stubView{}
	if err := doRename(t, f, view, "/repo/x", "/repo/x"); err != nil {
		t.Fatalf("same-path rename = %v, want nil", err)
	}
	if len(view.renames) != 0 || len(view.getattr) != 0 {
		t.Fatalf("same-path rename reached the chain: renames=%v getattr=%v", view.renames, view.getattr)
	}
}

func TestRenameCrossPlaneRefused(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/x.js", "x")
	view := &stubView{}
	if err := doRename(t, f, view, "/repo/node_modules/x.js", "/repo/lib.js"); err != syscall.EXDEV {
		t.Fatalf("local->remote rename = %v, want EXDEV", err)
	}
	if err := doRename(t, f, view, "/repo/lib.js", "/repo/node_modules/x.js"); err != syscall.EXDEV {
		t.Fatalf("remote->local rename = %v, want EXDEV", err)
	}
	if len(view.renames) != 0 {
		t.Fatalf("cross-plane renames reached the chain: %v", view.renames)
	}
}

func TestRenameLocalPlaneMovesBacking(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**", "**/vendor/**"}, nil)
	writeBacking(t, f, "repo/node_modules/x.js", "x")
	view := &stubView{}
	if err := doRename(t, f, view, "/repo/node_modules", "/repo/vendor"); err != nil {
		t.Fatalf("local rename = %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.overlay.Root(), "repo", "vendor", "x.js")); err != nil {
		t.Fatalf("backing not moved: %v", err)
	}
	if len(view.renames) != 0 {
		t.Fatalf("local rename reached the chain: %v", view.renames)
	}
}

func TestRemoteRenameMigratesBackingSubtree(t *testing.T) {
	// /repo/staging is remote-classified but holds a local-only child; the
	// backing subtree must move with the remote rename.
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/staging/node_modules/x.js", "x")
	view := &stubView{attrs: map[string]fs.Attr{"/repo/staging": {Kind: fs.KindDir}}}
	if err := doRename(t, f, view, "/repo/staging", "/repo/other"); err != nil {
		t.Fatalf("remote rename = %v", err)
	}
	if len(view.renames) != 1 {
		t.Fatalf("renames = %v", view.renames)
	}
	if _, err := os.Stat(filepath.Join(f.overlay.Root(), "repo", "other", "node_modules", "x.js")); err != nil {
		t.Fatalf("backing subtree not migrated: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.overlay.Root(), "repo", "staging")); !os.IsNotExist(err) {
		t.Fatalf("stale backing left behind: %v", err)
	}
}

func TestRemoteRenameRejectsLocalDestContent(t *testing.T) {
	// The destination is invisible to the remote view but its backing
	// holds real local-only entries; the replacement must be rejected
	// before the remote rename commits.
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/dst/node_modules/keep.js", "keep")
	view := &stubView{attrs: map[string]fs.Attr{"/repo/src": {Kind: fs.KindDir}}}
	err := doRename(t, f, view, "/repo/src", "/repo/dst")
	if err != syscall.ENOTEMPTY {
		t.Fatalf("rename onto local content = %v, want ENOTEMPTY", err)
	}
	if len(view.renames) != 0 {
		t.Fatalf("rejected rename still reached the chain: %v", view.renames)
	}
	if _, err := os.Stat(filepath.Join(f.overlay.Root(), "repo", "dst", "node_modules", "keep.js")); err != nil {
		t.Fatalf("local content disturbed: %v", err)
	}
}

func TestRemoteRenamePrunesDestScaffolding(t *testing.T) {
	// An empty backing directory at the destination is scaffolding; it is
	// pruned so the rename can replace the destination cleanly.
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	if err := os.MkdirAll(filepath.Join(f.overlay.Root(), "repo", "dst"), 0o755); err != nil {
		t.Fatal(err)
	}
	view := &stubView{attrs: map[string]fs.Attr{"/repo/src": {Kind: fs.KindDir}}}
	if err := doRename(t, f, view, "/repo/src", "/repo/dst"); err != nil {
		t.Fatalf("rename over scaffolding = %v", err)
	}
	if len(view.renames) != 1 {
		t.Fatalf("renames = %v", view.renames)
	}
}

func TestLinkCrossPlaneRefused(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/x.js", "x")
	view := &stubView{}
	if _, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpLink, Path: "/repo/node_modules/x.js", Path2: "/repo/hard.js"}, view.dispatch); err != syscall.EXDEV {
		t.Fatalf("local->remote link = %v, want EXDEV", err)
	}
	if _, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpLink, Path: "/repo/remote.js", Path2: "/repo/node_modules/hard.js"}, view.dispatch); err != syscall.EXDEV {
		t.Fatalf("remote->local link = %v, want EXDEV", err)
	}
}

func TestLinkLocalPlane(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/x.js", "x")
	view := &stubView{}
	if _, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpLink, Path: "/repo/node_modules/x.js", Path2: "/repo/node_modules/y.js"}, view.dispatch); err != nil {
		t.Fatalf("local link = %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.overlay.Root(), "repo", "node_modules", "y.js")); err != nil {
		t.Fatalf("hard link missing: %v", err)
	}
}
