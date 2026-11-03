package fuse

import (
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestReexecPreflightGates(t *testing.T) {
	b := newXattrFS(t)
	if r := b.ReexecPreflight(); !r.OK {
		t.Fatalf("clean mount refused: %+v", r.Refusals)
	}

	// open handle gate
	var cout gofuse.CreateOut
	ci := &gofuse.CreateIn{Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644}
	ci.NodeId = 1
	if st := b.Create(nil, ci, "h", &cout); st != gofuse.OK {
		t.Fatal(st)
	}
	r := b.ReexecPreflight()
	if r.OK || r.Refusals[0].Gate != "file_handles" {
		t.Fatalf("handle gate = %+v", r.Refusals)
	}
	b.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: cout.NodeId}, Fh: cout.Fh})

	// xattr gate
	si := &gofuse.SetXAttrIn{}
	si.NodeId = 1
	_ = b.SetXAttr(nil, si, "user.k", []byte("v"))
	if r = b.ReexecPreflight(); r.OK {
		t.Fatalf("xattr gate missing: %+v", r.Refusals)
	}
	found := false
	for _, ref := range r.Refusals {
		// the store now lives in the xattr extension and surfaces through
		// the chain's ReexecGater capability
		if ref.Gate == "extension_state" && ref.Reason == "in-memory xattrs exist" {
			found = true
		}
	}
	if !found {
		t.Fatalf("xattr refusal absent: %+v", r.Refusals)
	}

	// lock gate
	li := &gofuse.LkIn{}
	li.NodeId = 1
	li.Owner = 9
	li.Lk = gofuse.FileLock{Start: 0, End: 9, Typ: uint32(syscall.F_WRLCK)}
	if st := b.SetLk(nil, li); st != gofuse.OK {
		t.Fatal(st)
	}
	if r = b.ReexecPreflight(); r.OK {
		t.Fatalf("lock gate missing")
	}
	lockFound := false
	for _, ref := range r.Refusals {
		if ref.Gate == "fuse_locks" {
			lockFound = true
		}
	}
	if !lockFound {
		t.Fatalf("lock refusal absent: %+v", r.Refusals)
	}
	// guard semantics
	g := &reexecGuard{}
	if !g.tryAcquire() || g.tryAcquire() {
		t.Fatal("double acquire")
	}
	g.release()
	if !g.tryAcquire() {
		t.Fatal("release did not free")
	}
}
