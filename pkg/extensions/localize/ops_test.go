package localize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func doOp(t *testing.T, f *Localize, op *fs.Op) (fs.Result, error) {
	t.Helper()
	next := fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, fs.ErrUnsupported
	})
	return f.Do(context.Background(), op, next)
}

func TestSetAttrMtimeAppliesChtimes(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/a.js", "a")
	mtime := time.Now().Add(-time.Hour).Round(0)
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/node_modules/a.js", Mtime: mtime, Size: -1}); err != nil {
		t.Fatalf("setattr mtime = %v", err)
	}
	info, err := f.overlay.Lstat("/repo/node_modules/a.js")
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().Unix() != mtime.Unix() {
		t.Fatalf("mtime = %v, want %v", info.ModTime(), mtime)
	}
}

func TestSetAttrMtimeSwallowedAfterUnlink(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/a.js", "a")
	view := &stubView{}
	res, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: "/repo/node_modules/a.js", Flags: uint32(syscall.O_RDWR)}, view.dispatch)
	if err != nil {
		t.Fatal(err)
	}
	h, ok := res.Handle.(*token)
	if !ok {
		t.Fatalf("handle = %T, want *token", res.Handle)
	}
	if _, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpUnlink, Path: "/repo/node_modules/a.js"}, view.dispatch); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	// The kernel flushes inode times through the open handle after the
	// unlink; that flush must succeed (the inode is alive while the
	// handle is open).
	op := &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/node_modules/a.js", Mtime: time.Now(), Size: -1, Handle: h}
	if _, err := f.Do(context.Background(), op, view.dispatch); err != nil {
		t.Fatalf("post-unlink mtime flush = %v, want nil", err)
	}
	if _, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpRelease, Path: "/repo/node_modules/a.js", Handle: h}, view.dispatch); err != nil {
		t.Fatal(err)
	}
	// With no live handle the path is simply gone: ENOENT.
	op2 := &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/node_modules/a.js", Mtime: time.Now(), Size: -1}
	if _, err := f.Do(context.Background(), op2, view.dispatch); !os.IsNotExist(err) {
		t.Fatalf("post-release mtime = %v, want ENOENT", err)
	}
}

func TestUnlinkGuards(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/a.js", "a")
	if err := os.MkdirAll(filepath.Join(f.overlay.Root(), "repo", "node_modules", "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/node_modules/d"}); err != syscall.EISDIR {
		t.Fatalf("unlink of dir = %v, want EISDIR", err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/node_modules/missing.js"}); !os.IsNotExist(err) {
		t.Fatalf("unlink of missing = %v, want ENOENT", err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpUnlink, Path: "/repo/node_modules/a.js"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	if _, err := f.overlay.Lstat("/repo/node_modules/a.js"); !os.IsNotExist(err) {
		t.Fatal("file not removed")
	}
}

func TestRmdirGuards(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	root := f.overlay.Root()
	if err := os.MkdirAll(filepath.Join(root, "repo", "node_modules", "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "repo", "node_modules", "full"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "repo", "node_modules", "full", "x"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "repo", "node_modules", "file"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRmdir, Path: "/repo/node_modules/file"}); err != syscall.ENOTDIR {
		t.Fatalf("rmdir of file = %v, want ENOTDIR", err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRmdir, Path: "/repo/node_modules/full"}); !errors.Is(err, syscall.ENOTEMPTY) {
		t.Fatalf("rmdir of non-empty = %v, want ENOTEMPTY", err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRmdir, Path: "/repo/node_modules/empty"}); err != nil {
		t.Fatalf("rmdir of empty = %v", err)
	}
}

func TestListFiltersRemoteClassifiedChildren(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, []string{"**/skipdir/**"})
	root := f.overlay.Root()
	if err := os.MkdirAll(filepath.Join(root, "repo", "node_modules", "skipdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "repo", "node_modules", "x.js"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "repo", "node_modules", "skipdir", "y.js"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("x.js", filepath.Join(root, "repo", "node_modules", "link")); err != nil {
		t.Fatal(err)
	}
	res, err := doOp(t, f, &fs.Op{Kind: fs.OpList, Path: "/repo/node_modules"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Entries) != 2 {
		t.Fatalf("entries = %v, want x.js + link only", res.Entries)
	}
	for _, e := range res.Entries {
		switch e.Name {
		case "x.js":
			if e.Attr.Kind != fs.KindFile {
				t.Fatalf("x.js kind = %v", e.Attr.Kind)
			}
		case "link":
			if e.Attr.Kind != fs.KindSymlink {
				t.Fatalf("link kind = %v", e.Attr.Kind)
			}
		default:
			t.Fatalf("unexpected entry %q", e.Name)
		}
	}
}

func TestCreateFlagPassthrough(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/a.js", "a")
	// O_EXCL on an existing file must surface EEXIST, not a silent reopen.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpCreate, Path: "/repo/node_modules/a.js", Flags: uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL)}); !os.IsExist(err) {
		t.Fatalf("O_EXCL create = %v, want EEXIST", err)
	}
	// A read-only open of a missing file reports ENOENT; it must not
	// materialize an empty file.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpCreate, Path: "/repo/node_modules/missing.js", Flags: uint32(syscall.O_RDONLY)}); !os.IsNotExist(err) {
		t.Fatalf("read-only open of missing = %v, want ENOENT", err)
	}
	if _, err := f.overlay.Lstat("/repo/node_modules/missing.js"); !os.IsNotExist(err) {
		t.Fatal("read-only open materialized a file")
	}
}

func TestMkdirKeepsModeBits(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpMkdir, Path: "/repo/node_modules", Mode: 0o0750}); err != nil {
		t.Fatalf("mkdir = %v", err)
	}
	info, err := f.overlay.Lstat("/repo/node_modules")
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o750 {
		t.Fatalf("mode = %v, want 0750", info.Mode())
	}
}

func TestFsyncDirFlushesBacking(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	if err := os.MkdirAll(filepath.Join(f.overlay.Root(), "repo", "node_modules"), 0o755); err != nil {
		t.Fatal(err)
	}
	// Directory fsync arrives with the directory-stream handle and no
	// data-plane handle; the backing directory must still be flushed
	// locally instead of passing to the remote chain.
	op := &fs.Op{Kind: fs.OpFsync, Path: "/repo/node_modules", Fh: 42}
	if _, err := doOp(t, f, op); err != nil {
		t.Fatalf("dir fsync = %v", err)
	}
}

func TestReadWithoutHandleServesOverlay(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	writeBacking(t, f, "repo/node_modules/a.js", "payload")
	res, err := doOp(t, f, &fs.Op{Kind: fs.OpRead, Path: "/repo/node_modules/a.js"})
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Data) != "payload" {
		t.Fatalf("read = %q", res.Data)
	}
}
