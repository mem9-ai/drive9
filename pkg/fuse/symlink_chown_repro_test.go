package fuse

import (
	"context"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// TestLchownKeepsSymlinkVisible pins the lchown-on-symlink flow: after a
// root lchown (setup) and an owner-group lchown, the link must still
// LOOKUP (pjdfstest chown/00 lchown rows).
func TestLchownKeepsSymlinkVisible(t *testing.T) {
	core := newMemCore()
	core.writeAt("/f", 0, []byte("x"))
	f := newTestFS(t, core)
	var out gofuse.EntryOut
	if st := f.Symlink(nil, &gofuse.InHeader{NodeId: 1}, "f", "s2", &out); st != gofuse.OK {
		t.Fatalf("symlink: %v", st)
	}
	sino := out.NodeId

	// root lchown 65534 65533
	si := &gofuse.SetAttrIn{}
	si.NodeId = sino
	si.Valid = gofuse.FATTR_UID | gofuse.FATTR_GID
	si.Uid, si.Gid = 65534, 65533
	var sout gofuse.AttrOut
	if st := f.SetAttr(nil, si, &sout); st != gofuse.OK {
		t.Fatalf("setup lchown: %v", st)
	}
	// owner-group lchown: gid to supplementary
	si2 := &gofuse.SetAttrIn{}
	si2.NodeId = sino
	si2.Valid = gofuse.FATTR_GID
	si2.Gid = 65532
	if st := f.SetAttr(nil, si2, &sout); st != gofuse.OK {
		t.Fatalf("owner lchown: %v", st)
	}
	// the link must still lookup
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "s2", &lout); st != gofuse.OK {
		t.Fatalf("lookup after lchown: %v", st)
	}
	_ = fs.EventCheckout
	_ = syscall.ENOENT
	_ = context.Background
}
