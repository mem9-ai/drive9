package fuse

import (
	"net/http/httptest"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// TestLookupAfterRemoteRenameReturnsENOENT: after a successful remote rename
// A -> B, a LOOKUP of A must miss (ENOENT). A stale positive entry for the
// renamed-away name is what turns git's lockfile recycle into a kernel-level
// EEXIST without ever reaching the daemon.
func TestLookupAfterRemoteRenameReturnsENOENT(t *testing.T) {
	backend := newDeferredUnlinkBackend()
	ts := httptest.NewServer(backend.handler())
	defer ts.Close()
	fs := newDeferredUnlinkFS(t, ts, t.TempDir())

	// Seed a remote-backed file the way a committed upload leaves it.
	const oldP = "/config.lock"
	backend.seedFile(oldP, 1)
	ino := fs.inodes.Lookup(oldP, false, 8, time.Now())
	fs.inodes.UpdateRevision(ino, 1)
	fs.recordCommittedRevision(oldP, 1)

	if st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Newdir:   1,
	}, "config.lock", "config"); st != gofuse.OK {
		t.Fatalf("Rename status = %v, want OK", st)
	}

	// LOOKUP of the renamed-away name must miss.
	var lk gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "config.lock", &lk); st == gofuse.OK {
		t.Fatalf("Lookup of renamed-away name returned OK (stale positive entry); inode=%d", lk.NodeId)
	} else if st != gofuse.ENOENT {
		t.Fatalf("Lookup status = %v, want ENOENT", st)
	}
}
