package fuse

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/specials"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestKernelCacheBypassLifecycle(t *testing.T) {
	tt := newKernelCacheBypassTable()
	if tt.active(7) {
		t.Fatal("empty table active")
	}
	tt.arm(7, "/f", 3, 10, "sync-commit")
	if !tt.active(7) {
		t.Fatal("armed marker not active")
	}
	if tt.active(8) {
		t.Fatal("marker leaked to other ino")
	}
	tt.armUntil(7, "/f", 4, 11, "test", time.Now().Add(-time.Second))
	if tt.active(7) {
		t.Fatal("expired marker still active")
	}
	tt.armUntil(9, "/g", 1, 1, "test", time.Now().Add(time.Minute))
	tt.clear()
	if tt.active(9) {
		t.Fatal("clear left markers")
	}
}

func TestOpenForcesDirectIOUnderBypass(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("data"))
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	terminal, _ := core.New(cl, core.Options{})
	f, _ := New(Options{Extensions: []fs.Handler{terminal}})
	f.Init(nil)

	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &lout); st != gofuse.OK {
		t.Fatal(st)
	}
	f.bypass.arm(lout.NodeId, "/f", 1, 4, "sync-commit")

	oi := &gofuse.OpenIn{}
	oi.NodeId = lout.NodeId
	var oout gofuse.OpenOut
	if st := f.Open(nil, oi, &oout); st != gofuse.OK {
		t.Fatalf("open: %v", st)
	}
	if oout.OpenFlags&gofuse.FOPEN_DIRECT_IO == 0 {
		t.Fatalf("open flags = %#x, want DIRECT_IO under bypass", oout.OpenFlags)
	}
}

func TestSpecialNodeMknodAndLookup(t *testing.T) {
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	terminal, _ := core.New(cl, core.Options{})
	specialsExt, err := specials.New(specials.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, _ := New(Options{Extensions: []fs.Handler{specialsExt, terminal}})
	f.Init(nil)

	mi := &gofuse.MknodIn{Mode: uint32(syscall.S_IFIFO) | 0o644}
	mi.NodeId = 1
	var out gofuse.EntryOut
	if st := f.Mknod(nil, mi, "pipe", &out); st != gofuse.OK {
		t.Fatalf("mknod fifo: %v", st)
	}
	if out.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFIFO) {
		t.Fatalf("mode = %o, want FIFO", out.Mode)
	}
	// lookup synthesizes the entry without a remote round trip
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "pipe", &lout); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}
	if lout.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFIFO) {
		t.Fatalf("lookup mode = %o", lout.Mode)
	}
	if _, hit := b.Node("/pipe"); hit {
		t.Fatal("special node leaked to the remote namespace")
	}
}

func TestOpenHandleIndexLifecycle(t *testing.T) {
	idx := NewOpenHandleIndex()
	a := &fileHandle{ino: 2, path: "/a"}
	b := &fileHandle{ino: 3, path: "/a/sub"}
	idx.Add(a)
	idx.Add(b)
	if !idx.Has(2, "/a") {
		t.Fatal("Has by path failed")
	}
	if got := idx.SnapshotPath("/a"); len(got) != 1 || got[0] != a {
		t.Fatalf("snapshot /a = %v", got)
	}
	// unlink marks anonymous: Has by inode stays true, Path goes false
	idx.UnlinkPath("/a")
	if p, ok := idx.Path(a); ok || p != "" {
		t.Fatalf("anonymous Path = %q %v", p, ok)
	}
	// v1 semantics: an anonymous fd no longer counts as an open handle for
	// its (former) path or inode — e.g. WAL-index confirmation must not see it.
	if idx.Has(2, "") {
		t.Fatal("anonymous handle still counts as open")
	}
	// relink onto a fresh incarnation
	idx.RelinkPath("/relinked", []*fileHandle{a})
	if p, ok := idx.Path(a); !ok || p != "/relinked" {
		t.Fatalf("relinked = %q %v", p, ok)
	}
	// rename prefix moves descendants
	renamed := idx.RenamePathPrefix("/a", "/z")
	if len(renamed) != 1 {
		t.Fatalf("renamed = %v", renamed)
	}
	if b.path != "/z/sub" {
		t.Fatalf("b.path = %q", b.path)
	}
	idx.Remove(b)
	if idx.Has(3, "/z/sub") {
		t.Fatal("remove left handle")
	}
}

func TestFacadeRegistersHandlesInIndex(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("data"))
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	terminal, _ := core.New(cl, core.Options{})
	f, _ := New(Options{Extensions: []fs.Handler{terminal}})
	f.Init(nil)

	var cout gofuse.CreateOut
	ci := &gofuse.CreateIn{Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644}
	ci.NodeId = 1
	if st := f.Create(nil, ci, "f2", &cout); st != gofuse.OK {
		t.Fatal(st)
	}
	if !f.openIdx.Has(cout.NodeId, "/f2") {
		t.Fatal("created handle not in index")
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: cout.NodeId}, Fh: cout.Fh})
	if f.openIdx.Has(cout.NodeId, "/f2") {
		t.Fatal("released handle still indexed")
	}
}
