package localize

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// macOS can reject a directory rename beyond its path limit even with
// directory-relative syscalls. The affected production mounts use Linux.
func TestOverlayLongPathRename(t *testing.T) {
	o := NewOverlay(t.TempDir())
	if err := o.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(o.Root())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	long := longOverlayPath()
	if err := root.MkdirAll(long, 0755); err != nil {
		t.Fatal(err)
	}
	if err := root.Symlink("missing", long+"/link"); err != nil {
		t.Fatal(err)
	}
	if err := o.Rename("/"+long, "/moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Lstat("moved/link"); err != nil {
		t.Fatalf("child not moved: %v", err)
	}
	if _, err := root.Lstat(long); !os.IsNotExist(err) {
		t.Fatalf("old long name remains: %v", err)
	}
	// The destination's parents must also be created without passing a
	// path longer than PATH_MAX to a single syscall.
	if err := o.Rename("/moved", "/new/"+long); err != nil {
		t.Fatal(err)
	}
	if _, err := root.Lstat("new/" + long + "/link"); err != nil {
		t.Fatalf("long destination missing: %v", err)
	}
}

func TestRemoteRenameLongDirectoryWithLocalChildren(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "short/node_modules/keep", "payload")
	// The logical child path is valid; only its backing-root prefix exceeds
	// Linux PATH_MAX. Exercise the long state before returning to /short.
	long := "/" + longOverlayPath()[:4070]
	child := long + "/node_modules/keep"
	if len(child) >= 4096 || len(filepath.Join(f.overlay.Root(), child)) < 4096 {
		t.Fatal("fixture must cross PATH_MAX only after the backing prefix")
	}
	view := &stubView{attrs: map[string]fs.Attr{"/short": {Kind: fs.KindDir}, long: {Kind: fs.KindDir}}}
	if err := doRename(t, f, view, "/short", long); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	res, err := f.Do(ctx, &fs.Op{Kind: fs.OpRead, Path: child}, view.dispatch)
	if err != nil || string(res.Data) != "payload" {
		t.Fatalf("read through long view = %q, %v", res.Data, err)
	}
	res, err = f.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: child, Flags: syscall.O_RDWR}, view.dispatch)
	if err != nil || res.Handle == nil {
		t.Fatalf("open through long view = %v, %v", res.Handle, err)
	}
	if err := res.Handle.Write(ctx, 0, []byte("updated")); err != nil {
		_ = res.Handle.Release(ctx)
		t.Fatal(err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	alias := long + "/node_modules/remove"
	if _, err := f.Do(ctx, &fs.Op{Kind: fs.OpLink, Path: child, Path2: alias}, view.dispatch); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Do(ctx, &fs.Op{Kind: fs.OpUnlink, Path: alias}, view.dispatch); err != nil {
		t.Fatalf("unlink through long view: %v", err)
	}
	if _, err := f.Do(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: alias}, view.dispatch); !os.IsNotExist(err) {
		t.Fatalf("deleted child stat = %v", err)
	}
	if err := doRename(t, f, view, long, "/short"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.overlay.Root(), "short", "node_modules", "keep"))
	if err != nil || string(data) != "updated" {
		t.Fatalf("local child lost: %q, %v", data, err)
	}
}
