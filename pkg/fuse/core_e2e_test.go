package fuse

import (
	"github.com/mem9-ai/drive9/pkg/fs"

	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
)

// End-to-end over the real mount wiring pieces: facade + sync-CAS core
// terminal against the shared fake backend. Write-sync durability: the
// bytes are committed the moment Write returns.
func TestCoreTerminalEndToEnd(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("hello"))
	ts := httptest.NewServer(b)
	defer ts.Close()

	cl := client.New(ts.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{Extensions: []fs.Handler{terminal}})
	if err != nil {
		t.Fatal(err)
	}
	f.Init(nil)

	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &lout); st != gofuse.OK {
		t.Fatalf("lookup: %v", st)
	}

	var cout gofuse.CreateOut
	ci := &gofuse.CreateIn{Flags: syscall.O_CREAT | syscall.O_RDWR, Mode: 0o644}
	ci.NodeId = 1
	if st := f.Create(nil, ci, "out", &cout); st != gofuse.OK {
		t.Fatalf("create: %v", st)
	}
	if cout.Fh == 0 {
		t.Fatal("no handle")
	}

	wi := &gofuse.WriteIn{Offset: 0}
	wi.NodeId, wi.Fh = cout.NodeId, cout.Fh
	if n, st := f.Write(nil, wi, []byte("hello fuse")); st != gofuse.OK || n != 10 {
		t.Fatalf("write = %d %v", n, st)
	}
	if n, ok := b.Node("/out"); !ok || string(n.Data) != "hello fuse" {
		t.Fatalf("backend after write = %+v", n)
	}

	ri := &gofuse.ReadIn{Offset: 6, Size: 4}
	ri.NodeId, ri.Fh = cout.NodeId, cout.Fh
	res, st := f.Read(nil, ri, make([]byte, 4))
	if st != gofuse.OK {
		t.Fatalf("read: %v", st)
	}
	buf := make([]byte, 4)
	data, _ := res.Bytes(buf)
	if string(data) != "fuse" {
		t.Fatalf("read = %q", data)
	}

	fi := &gofuse.FlushIn{}
	fi.NodeId, fi.Fh = cout.NodeId, cout.Fh
	if st := f.Flush(nil, fi); st != gofuse.OK {
		t.Fatalf("flush: %v", st)
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: cout.NodeId}, Fh: cout.Fh})

	excl := &gofuse.CreateIn{Flags: syscall.O_CREAT | syscall.O_EXCL | syscall.O_RDWR, Mode: 0o644}
	excl.NodeId = 1
	if st := f.Create(nil, excl, "f", &gofuse.CreateOut{}); st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("O_EXCL = %v, want EEXIST", st)
	}
}
