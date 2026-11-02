// v1 extent_policy rows that are facade-level in the new plane: the
// non-owner chmod gate over the inode entry (kill-priv clear-setid
// allowed, other modes EPERM) and the hardlink-extent routing exclusion.
package fuse

import (
	"context"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestSetAttrModeAllowsNonOwnerClearSetid.
func TestPortSetAttrModeNonOwnerGate(t *testing.T) {
	f, err := New(Options{
		Extensions: []fs.Handler{fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
			return fs.Result{}, fs.ErrUnsupported
		})},
		Uid: 0, Gid: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	ino := f.inodes.Lookup("/suid.bin", false, 0, time.Now())
	f.inodes.UpdateMode(ino, 0o4777)
	f.inodes.UpdateOwner(ino, 0, 0, true, true)
	entry, ok := f.inodes.GetEntry(ino)
	if !ok {
		t.Fatal("missing inode")
	}

	// Non-owner (65534) clearing only SUID/SGID: the kernel kill-priv
	// merge, allowed.
	mk := func(mode uint32) *gofuse.SetAttrIn {
		in := &gofuse.SetAttrIn{}
		in.Mode = mode
		return in
	}
	nonOwner := fs.Caller{Uid: 65534, Gid: 65534}
	owner := fs.Caller{Uid: 0, Gid: 0}

	got, merr := f.checkSetAttrModeForCaller(nonOwner, entry, mk(0o777))
	if merr != nil || got&0o7777 != 0o777 {
		t.Fatalf("clear setid mode=%o err=%v, want 0777 nil", got, merr)
	}
	if _, merr := f.checkSetAttrModeForCaller(nonOwner, entry, mk(0o222)); merr == nil {
		t.Fatal("non-owner chmod 0222 accepted, want EPERM")
	}
	if _, merr := f.checkSetAttrModeForCaller(owner, entry, mk(0o600)); merr != nil {
		t.Fatalf("owner chmod rejected: %v", merr)
	}
	if _, merr := f.checkSetAttrModeForCaller(owner, entry, mk(0o4777)); merr != nil {
		t.Fatalf("root chmod rejected: %v", merr)
	}
}
