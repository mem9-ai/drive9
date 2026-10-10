package fuse

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestUnlinkSurvivorFacadeLevel(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/keep", []byte("survivor"))
	ts := httptest.NewServer(b)
	defer ts.Close()
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	wb, _ := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	terminal, _ := core.New(cl, core.Options{})
	f, _ := New(Options{Extensions: []fs.Handler{wb, terminal}})
	f.Init(nil)
	ctx := context.Background()

	var out gofuse.CreateOut
	ci := &gofuse.CreateIn{Flags: syscall.O_RDWR}
	ci.NodeId = 1
	if st := f.Create(nil, ci, "keep", &out); st != gofuse.OK {
		t.Fatal(st)
	}
	wi := &gofuse.WriteIn{Offset: 0}
	wi.NodeId, wi.Fh = out.NodeId, out.Fh
	if _, st := f.Write(nil, wi, []byte("SURVIVOR")); st != gofuse.OK {
		t.Fatal(st)
	}
	// unlink while open; the anonymous fd stays readable
	if st := f.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "keep"); st != gofuse.OK {
		t.Fatal(st)
	}
	ri := &gofuse.ReadIn{Offset: 0, Size: 8}
	ri.NodeId, ri.Fh = out.NodeId, out.Fh
	res, st := f.Read(nil, ri, make([]byte, 8))
	if st != gofuse.OK {
		t.Fatalf("post-unlink read: %v", st)
	}
	buf := make([]byte, 8)
	data, _ := res.Bytes(buf)
	if string(data) != "SURVIVOR" {
		t.Fatalf("survivor read = %q", data)
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
	if n, ok := b.Node("/keep"); ok && n.Data != nil {
		t.Fatal("release resurrected the unlinked path")
	}
	_ = ctx
}
