package fuse

import (
	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// A lookup reply is authoritative for ownership: the open gate must see the
// real owner even before any GetAttr. Split presence flags stay independent;
// HasOwner is only the coarse either-field form.
func TestPortFillEntryOutPersistsSplitOwner(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	f, _ := facadeOverCore(t, ts)
	var out gofuse.EntryOut
	f.fillEntryOut("/extent.bin", &fs.Attr{
		Kind: fs.KindFile, Mode: 0o600, Size: 7, Rev: 3,
		Uid: 2000, HasUID: true,
		HasOwner: true,
	}, false, &out)
	entry, ok := f.inodes.GetEntry(out.NodeId)
	if !ok {
		t.Fatal("inode missing")
	}
	if !entry.HasUID || entry.Uid != 2000 {
		t.Fatalf("uid = %d hasUID=%v, want 2000/true", entry.Uid, entry.HasUID)
	}
	if entry.HasGID {
		t.Fatalf("gid presence = %v, want false (only UID was supplied)", entry.HasGID)
	}
	if entry.Revision != 3 {
		t.Fatalf("revision = %d, want 3", entry.Revision)
	}
}

// The lookup owner is what the open gate must use on a mount without
// default_permissions. Without persisting it, the mount uid becomes the
// owner class and a legitimate uid 2000 open of a 0600 extent file is
// denied before the extent plane can authorize it.
func TestPortLookupOwnerAuthorizesOpen(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	f, _ := facadeOverCore(t, ts)

	var entry gofuse.EntryOut
	f.fillEntryOut("/extent.bin", &fs.Attr{
		Kind: fs.KindFile, Mode: 0o600, Size: 1, Rev: 1,
		Uid: 2000, HasUID: true, HasOwner: true,
	}, false, &entry)
	var opened gofuse.OpenOut
	st := f.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{
			NodeId: entry.NodeId,
			Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 2000, Gid: 2000}},
		},
		Flags: uint32(syscall.O_RDONLY),
	}, &opened)
	if st != gofuse.OK {
		t.Fatalf("open as uid 2000 = %v, want OK", st)
	}
}
