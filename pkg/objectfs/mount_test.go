//go:build !windows

package objectfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

func testVFS(t *testing.T) (*rcloneFUSE, string) {
	t.Helper()
	configfile.Install()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "n.txt"), []byte("n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := fs.NewFs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeOff
	return newRcloneFUSE(vfs.New(f, &opt), false), dir
}

func TestRcloneFUSELookupRead(t *testing.T) {
	ofs, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatalf("Lookup hello.txt: %v", st)
	}
	if ent.Size != 5 {
		t.Fatalf("size=%d", ent.Size)
	}

	var attr gofuse.AttrOut
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.Ino}}, &attr); st != gofuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}

	var openOut gofuse.OpenOut
	if st := ofs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ent.Ino}}, &openOut); st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}
	buf := make([]byte, 16)
	res, st := ofs.Read(nil, &gofuse.ReadIn{Fh: openOut.Fh, Offset: 0}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read: %v", st)
	}
	got, rst := res.Bytes(nil)
	if rst != gofuse.OK {
		t.Fatalf("Read bytes: %v", rst)
	}
	if string(got) != "hello" {
		t.Fatalf("read %q", got)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})
}

func TestRcloneFUSECreateFlushThenWrite(t *testing.T) {
	ofs, dir := testVFS(t)
	var createOut gofuse.CreateOut
	st := ofs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:     0o644,
	}, "from-mount.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if st := ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush after Create: %v", st)
	}
	n, st := ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh}, []byte("via-fuse"))
	if st != gofuse.OK {
		t.Fatalf("Write after Flush: %v (kernel FLUSH after create must leave the handle writable)", st)
	}
	if n != 8 {
		t.Fatalf("n=%d", n)
	}
	if st := ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush after Write: %v", st)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	got, err := os.ReadFile(filepath.Join(dir, "from-mount.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "via-fuse" {
		t.Fatalf("got %q", got)
	}
}

func TestRcloneFUSEReadDir(t *testing.T) {
	ofs, _ := testVFS(t)
	buf := make([]byte, 4096)
	out := gofuse.NewDirEntryList(buf, 0)
	if st := ofs.ReadDir(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}}, out); st != gofuse.OK {
		t.Fatalf("ReadDir: %v", st)
	}

	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "sub", &ent); st != gofuse.OK {
		t.Fatalf("Lookup sub: %v", st)
	}
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: ent.Ino}, "n.txt", &ent); st != gofuse.OK {
		t.Fatalf("Lookup sub/n.txt: %v", st)
	}
}

func TestRcloneFUSEInstancesDoNotShareHandles(t *testing.T) {
	a, _ := testVFS(t)
	b, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := a.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	var openOut gofuse.OpenOut
	if st := a.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ent.Ino}}, &openOut); st != gofuse.OK {
		t.Fatal(st)
	}
	if b.getHandle(openOut.Fh) != nil {
		t.Fatal("handle leaked across rcloneFUSE instances")
	}
}

func TestMapVFSErr(t *testing.T) {
	if mapVFSErr(nil) != gofuse.OK {
		t.Fatal("nil")
	}
	if mapVFSErr(os.ErrNotExist) != gofuse.ENOENT {
		t.Fatal("not exist")
	}
}

func TestRel(t *testing.T) {
	ofs := &rcloneFUSE{}
	if got := ofs.rel("", "a"); got != "a" {
		t.Fatalf("got %q", got)
	}
	if got := ofs.rel("dir/", "a"); got != "dir/a" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveObjectCacheDir(t *testing.T) {
	got := resolveObjectCacheDir("/tmp/obj-cache", "s3://bucket/prefix/")
	if !filepath.IsAbs(got) || !strings.HasPrefix(got, "/tmp/obj-cache"+string(filepath.Separator)) {
		t.Fatalf("got %q", got)
	}
	def := resolveObjectCacheDir("", "s3://bucket/prefix/")
	if !filepath.IsAbs(def) {
		t.Fatal("default cache dir should be absolute")
	}
	if !strings.Contains(def, filepath.Join(".cache", "drive9", "object")) {
		t.Fatalf("default = %q, want under ~/.cache/drive9/object", def)
	}
	other := resolveObjectCacheDir("", "s3://other/prefix/")
	if def == other {
		t.Fatal("different URIs must not share a cache directory")
	}
}

func TestForgetKeepsLiveLookups(t *testing.T) {
	ofs, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	ofs.Forget(ent.NodeId, 1)
	var attr gofuse.AttrOut
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.NodeId}}, &attr); st != gofuse.OK {
		t.Fatalf("live inode after Forget(1): %v", st)
	}
	ofs.Forget(ent.NodeId, 1)
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.NodeId}}, &attr); st != gofuse.ENOENT {
		t.Fatalf("after last Forget: %v", st)
	}
}

func TestRenameUpdatesInodePath(t *testing.T) {
	ofs, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := ofs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "hello.txt", "moved.txt"); st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}
	var attr gofuse.AttrOut
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.NodeId}}, &attr); st != gofuse.OK {
		t.Fatalf("GetAttr after rename: %v", st)
	}
}

func TestObjectCacheHasPendingNil(t *testing.T) {
	if objectCacheHasPending(nil) {
		t.Fatal("nil VFS")
	}
}
