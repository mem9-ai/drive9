package localize

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func longOverlayPath() string {
	return strings.TrimSuffix(strings.Repeat(strings.Repeat("a", 127)+"/", 32), "/")
}

func TestOverlayLongPath(t *testing.T) {
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
	if _, err := o.Lstat("/" + long); !os.IsNotExist(err) {
		t.Fatalf("missing long path = %v, want ENOENT", err)
	}
	if err := root.MkdirAll(long, 0755); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"z", "a"} {
		if err := root.Mkdir(long+"/"+name, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if info, err := o.Lstat("/" + long); err != nil || !info.IsDir() {
		t.Fatalf("long directory stat = %v, %v", info, err)
	}
	entries, err := o.ReadDir("/" + long)
	if err != nil || len(entries) != 2 || entries[0].Name() != "a" || entries[1].Name() != "z" {
		t.Fatalf("long directory listing = %v, %v", entries, err)
	}
	for _, entry := range entries {
		if info, err := entry.Info(); err != nil || !info.IsDir() {
			t.Fatalf("listed child stat = %v, %v", info, err)
		}
	}
	if err := root.Symlink("missing", long+"/link"); err != nil {
		t.Fatal(err)
	}
	if info, err := o.Lstat("/" + long + "/link"); err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("long symlink lstat = %v, %v", info, err)
	}
}

func TestOverlayLongPathErrors(t *testing.T) {
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
	if err := root.Mkdir(long+"/keep", 0755); err != nil {
		t.Fatal(err)
	}
	if err := root.Mkdir("source", 0755); err != nil {
		t.Fatal(err)
	}
	if err := o.Rename("/source", "/"+long); !errors.Is(err, syscall.ENOTEMPTY) && !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("replace nonempty long directory = %v", err)
	}
	bad := "/" + long + "/" + strings.Repeat("b", 256)
	if _, err := o.Lstat(bad); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("oversized component lstat = %v", err)
	}
	if err := o.Rename("/source", bad); !errors.Is(err, syscall.ENAMETOOLONG) {
		t.Fatalf("oversized component rename = %v", err)
	}
	if _, err := root.Lstat("source"); err != nil {
		t.Fatalf("failed rename lost source: %v", err)
	}
	if err := root.Symlink("loop", "loop"); err != nil {
		t.Fatal(err)
	}
	if _, err := o.Lstat("/loop/" + long); !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("loop error was hidden: %v", err)
	}
	if err := os.WriteFile(filepath.Join(o.Root(), "file"), []byte("keep"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ReadDir("/file/" + long); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("not-directory error was hidden: %v", err)
	}
}

func TestRemoteRenameLongPathRoundTrip(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	long := "/" + longOverlayPath()
	view := &stubView{attrs: map[string]fs.Attr{"/short": {Kind: fs.KindFile}, long: {Kind: fs.KindFile}}}
	if err := doRename(t, f, view, "/short", long); err != nil {
		t.Fatal(err)
	}
	if err := doRename(t, f, view, long, "/short"); err != nil {
		t.Fatal(err)
	}
	if len(view.renames) != 2 {
		t.Fatalf("remote renames = %v", view.renames)
	}
}

func TestOverlayLongPathFileOperations(t *testing.T) {
	o := NewOverlay(t.TempDir())
	if err := o.EnsureRoot(); err != nil {
		t.Fatal(err)
	}
	dir := "/" + longOverlayPath()
	if err := o.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	name := dir + "/file"
	file, err := o.OpenFile(name, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte("payload")); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.Truncate(name, 3); err != nil {
		t.Fatal(err)
	}
	if err := o.Chmod(name, 0600); err != nil {
		t.Fatal(err)
	}
	stamp := time.Unix(1700000000, 0)
	if err := o.Chtimes(name, stamp); err != nil {
		t.Fatal(err)
	}
	info, err := o.Lstat(name)
	if err != nil || info.Size() != 3 || info.Mode().Perm() != 0600 || !info.ModTime().Equal(stamp) {
		t.Fatalf("long file attrs = %v, %v", info, err)
	}
	if err := o.Link(name, dir+"/alias"); err != nil {
		t.Fatal(err)
	}
	if err := o.Symlink("file", dir+"/symlink"); err != nil {
		t.Fatal(err)
	}
	if target, err := o.Readlink(dir + "/symlink"); err != nil || target != "file" {
		t.Fatalf("long readlink = %q, %v", target, err)
	}
	if err := o.SyncDir(dir); err != nil {
		t.Fatal(err)
	}
	for _, child := range []string{"file", "alias", "symlink"} {
		if err := o.Remove(dir + "/" + child); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Remove(dir); err != nil {
		t.Fatal(err)
	}
}
