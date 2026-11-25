package fuse

import (
	"context"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/fs"
)

type leaseTestHandle struct {
	fs.Handle
	writes  int
	syncs   int
	flushes int
	onWrite func()
}

func (h *leaseTestHandle) Write(context.Context, int64, []byte) error {
	h.writes++
	if h.onWrite != nil {
		h.onWrite()
	}
	return nil
}
func (h *leaseTestHandle) Sync(context.Context) error  { h.syncs++; return nil }
func (h *leaseTestHandle) Flush(context.Context) error { h.flushes++; return nil }
func (h *leaseTestHandle) Read(_ context.Context, _ int64, p []byte) (int, error) {
	return copy(p, "held"), nil
}

func TestLeaseFenceAppliesToRawHandle(t *testing.T) {
	core := newMemCore()
	core.paths["/f"] = &memEntry{data: []byte("held"), mode: 0644}
	f := newTestFS(t, core)
	binding := &layerMountBinding{}
	f.layerFence = &binding.fenced
	id := lookupForTest(t, f, "/f").NodeId
	header := gofuse.InHeader{NodeId: id}
	raw := &leaseTestHandle{}
	f.handles.put(7, &fileHandle{ino: id, path: "/f", flags: syscall.O_RDWR, h: raw})
	input := &gofuse.WriteIn{InHeader: header, Fh: 7}
	if n, st := f.Write(nil, input, []byte("ok")); n != 2 || st != gofuse.OK {
		t.Fatalf("before fence: %d %v", n, st)
	}
	raw.onWrite = func() { binding.triggerSelfFence("lease expired during write") }
	if n, st := f.Write(nil, input, []byte("in flight")); n != 0 || st != gofuse.EIO {
		t.Fatalf("in flight: %d %v", n, st)
	}
	if n, st := f.Write(nil, input, []byte("late")); n != 0 || st != gofuse.EIO || raw.writes != 2 {
		t.Fatalf("after fence: %d %v, calls %d", n, st, raw.writes)
	}
	if st := f.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: 7}); st != gofuse.EIO || raw.syncs != 0 {
		t.Fatalf("fsync: %v, calls %d", st, raw.syncs)
	}
	if st := f.Flush(nil, &gofuse.FlushIn{InHeader: header, Fh: 7}); st != gofuse.EIO || raw.flushes != 1 {
		t.Fatalf("flush: %v, calls %d", st, raw.flushes)
	}
	if st := f.Fallocate(nil, &gofuse.FallocateIn{InHeader: header, Fh: 7, Length: 2}); st != gofuse.EIO {
		t.Fatalf("allocate: %v", st)
	}
	if _, st := f.CopyFileRange(nil, &gofuse.CopyFileRangeIn{InHeader: header, NodeIdOut: id, FhIn: 7, FhOut: 7, Len: 1}); st != gofuse.EIO {
		t.Fatalf("copy: %v", st)
	}
	buf := make([]byte, 4)
	if _, st := f.Read(nil, &gofuse.ReadIn{InHeader: header, Fh: 7, Size: 4}, buf); st != gofuse.OK || string(buf) != "held" {
		t.Fatalf("read: %v %q", st, buf)
	}
}
