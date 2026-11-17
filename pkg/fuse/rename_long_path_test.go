package fuse

import (
	"context"
	"path"
	"strings"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/extensions/localize"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestRenameLongPathKeepsFacadeInSync(t *testing.T) {
	long := "/" + strings.TrimSuffix(strings.Repeat(strings.Repeat("a", 127)+"/", 32), "/")
	local, err := localize.New(localize.Options{LocalRoot: t.TempDir(), Profile: "coding-agent", ApplyBuiltinDefaults: true})
	if err != nil {
		t.Fatal(err)
	}
	remote := "/short"
	terminal := fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
		switch op.Kind {
		case fs.OpRename:
			if remote != op.Path {
				return fs.Result{}, syscall.ENOENT
			}
			remote = op.Path2
			return fs.Result{}, nil
		case fs.OpGetAttr, fs.OpLookup:
			if remote == op.Path {
				return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: 0644}}, nil
			}
			if op.Path == "/" || strings.HasPrefix(long, op.Path+"/") {
				return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: 0755}}, nil
			}
			return fs.Result{}, syscall.ENOENT
		}
		return fs.Result{}, nil
	})
	f, err := New(Options{Extensions: []fs.Handler{local, terminal}})
	if err != nil {
		t.Fatal(err)
	}
	f.Init(nil)
	parent := f.inodes.EnsureInode(path.Dir(long), true, 0, time.Now())
	ino := f.inodes.EnsureInode("/short", false, 0, time.Now())
	outward := f.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: parent}, "short", path.Base(long))
	if outward != gofuse.OK {
		t.Fatalf("outward=%v", outward)
	}
	inward := f.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: parent}, Newdir: 1}, path.Base(long), "short")
	if inward != gofuse.OK {
		t.Fatalf("inward=%v", inward)
	}
	mapped, _ := f.inodes.GetInode("/short")
	if remote != "/short" || mapped != ino {
		t.Fatalf("remote=%s old inode mapped=%d want %d", remote, mapped, ino)
	}
	t.Logf("outward=%s inward=%s remote_is_short=%t facade_inode_back_at_short_path=%t", outward, inward, remote == "/short", mapped == ino)
}
