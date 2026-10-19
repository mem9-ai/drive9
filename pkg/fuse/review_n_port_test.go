package fuse

// qiffang/codex twenty-third round: the Open permission gate covers
// O_TRUNC (needs W_OK), ancestor search, Access answers local directories
// from the inode entry, READDIR rows keep server identity, and a checkout
// reset fences in-flight listings.

import (
	"context"
	"strings"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func lookupForTest(t *testing.T, f *FS, path string) *gofuse.EntryOut {
	t.Helper()
	parts := strings.Split(strings.Trim(path, "/"), "/")
	node := uint64(1)
	for i, seg := range parts {
		var out gofuse.EntryOut
		if st := f.Lookup(nil, &gofuse.InHeader{NodeId: node}, seg, &out); st != gofuse.OK {
			t.Fatalf("lookup %s = %v", strings.Join(parts[:i+1], "/"), st)
		}
		if i == len(parts)-1 {
			return &out
		}
		node = out.NodeId
	}
	t.Fatalf("empty path")
	return nil
}

// O_RDONLY|O_TRUNC truncates: it must need W_OK even though the access
// mode says read-only.
func TestPortOpenTruncNeedsWritePermission(t *testing.T) {
	core := newMemCore()
	core.paths["/f"] = &memEntry{mode: 0o444}
	f := newTestFS(t, core)
	entry := lookupForTest(t, f, "/f")
	out := &gofuse.OpenOut{}
	// A caller outside the owner class reads 0444 fine...
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 2000, Gid: 2000}, Pid: 1}}, Flags: uint32(syscall.O_RDONLY)}, out); st != gofuse.OK {
		t.Fatalf("O_RDONLY = %v, want OK", st)
	}
	// ...but a truncating open needs the write bit.
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 2000, Gid: 2000}, Pid: 1}}, Flags: uint32(syscall.O_RDONLY | syscall.O_TRUNC)}, &gofuse.OpenOut{}); st != gofuse.EACCES {
		t.Fatalf("O_RDONLY|O_TRUNC = %v, want EACCES", st)
	}
}

// A caller without search permission on an ancestor cannot open a child by
// absolute path.
func TestPortOpenRequiresAncestorSearch(t *testing.T) {
	core := newMemCore()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o700}
	core.paths["/d/f"] = &memEntry{mode: 0o644}
	f := newTestFS(t, core)
	entry := lookupForTest(t, f, "/d/f")
	// The mount owner (uid 1000) traverses its own 0700 directory.
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.OK {
		t.Fatalf("owner open = %v, want OK", st)
	}
	// A stranger cannot.
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 2000, Gid: 2000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.EACCES {
		t.Fatalf("stranger open = %v, want EACCES", st)
	}
}

// READDIR rows keep mtime/revision/resource/extent identity.
func TestPortDirEntriesKeepServerIdentity(t *testing.T) {
	rows := dirEntriesFrom([]fs.Entry{{Name: "f", Attr: fs.Attr{
		Kind: fs.KindFile, Mode: 0o644, Size: 3, Nlink: 2,
		Rev: 7, ResourceID: "res-1", ExtentIno: 42,
	}}})
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	row := rows[0]
	if row.Rev != 7 || row.ResourceID != "res-1" || row.ExtentIno != 42 || row.Nlink != 2 {
		t.Fatalf("row = %+v, want the server identity preserved", row)
	}
}

// ACCESS on a local directory answers from the inode entry: the chain is
// never asked to stat it.
func TestPortAccessLocalDirSkipsDispatch(t *testing.T) {
	terminal := &errStatTerminal{err: syscall.EIO}
	f, err := New(Options{Extensions: []fs.Handler{terminal}, Uid: 1000, Gid: 1000, LocalDirAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	// Seed the root's inode entry through a lookup-free path: the facade
	// creates NodeId 1 at construction.
	f.inodes.UpdateMode(1, uint32(syscall.S_IFDIR)|0o755)
	if st := f.Access(nil, &gofuse.AccessIn{InHeader: gofuse.InHeader{NodeId: 1, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}}, Mask: 0b100 | 0b001}); st != gofuse.OK {
		t.Fatalf("access = %v, want OK from the local dir entry", st)
	}
}

type errStatTerminal struct{ err error }

func (e *errStatTerminal) Do(ctx context.Context, op *fs.Op, _ fs.Next) (fs.Result, error) {
	return fs.Result{}, e.err
}

// A checkout reset drops the open directory handles' snapshots.
func TestPortCheckoutResetsOpenDirSnapshots(t *testing.T) {
	core := newMemCore()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	core.paths["/d/f"] = &memEntry{mode: 0o644}
	bus := fs.NewEventBus()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	entry := lookupForTest(t, f, "/d")
	var oout gofuse.OpenOut
	if st := f.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}}, &oout); st != gofuse.OK {
		t.Fatalf("opendir = %v", st)
	}
	dh, ok := f.dirs.get(oout.Fh)
	if !ok {
		t.Fatal("dir handle missing")
	}
	dh.entries = []fs.Entry{{Name: "stale"}}
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if dh.entries != nil {
		t.Fatal("checkout left the open dir snapshot in place")
	}
}
