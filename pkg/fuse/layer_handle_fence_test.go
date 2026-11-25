package fuse

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
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
	f.layerFence = binding
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

func TestFencedMetadataAndOpenUnlink(t *testing.T) {
	core := newMemCore()
	f := newTestFS(t, core)
	var out gofuse.CreateOut
	if st := f.Create(nil, exclusiveCreateInput(), "held", &out); st != gofuse.OK {
		t.Fatal(st)
	}
	binding := &layerMountBinding{}
	f.layerFence = binding
	binding.triggerSelfFence("lease expired")
	old, _ := f.inodes.GetEntry(out.NodeId)
	input := &gofuse.SetAttrIn{}
	input.NodeId, input.Valid, input.Mtime = out.NodeId, gofuse.FATTR_MTIME, 12345
	if st := f.SetAttr(nil, input, &gofuse.AttrOut{}); st != gofuse.EIO {
		t.Fatalf("setattr: %v", st)
	}
	current, _ := f.inodes.GetEntry(out.NodeId)
	if current.Mtime != old.Mtime {
		t.Fatal("rejected setattr changed mtime")
	}
	if st := f.Unlink(nil, rootHeader(), "held"); st != gofuse.EIO {
		t.Fatalf("unlink: %v", st)
	}
	if id, ok := f.inodes.GetInode("/held"); !ok || id != out.NodeId || !f.openIdx.HasOpenPath("/held") || f.isDeferredUnlink("/held") {
		t.Fatal("rejected unlink changed open path")
	}
}

func TestDeferredUnlinkCrossingLeaseLossKeepsRemote(t *testing.T) {
	core := newMemCore()
	f := newTestFS(t, core)
	var out gofuse.CreateOut
	if st := f.Create(nil, exclusiveCreateInput(), "held", &out); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := f.Unlink(nil, rootHeader(), "held"); st != gofuse.OK {
		t.Fatal(st)
	}
	binding := &layerMountBinding{}
	f.layerFence = binding
	binding.triggerSelfFence("lease expired before last close")
	releaseUnlinkFixture(f, out)
	if _, ok := core.get("/held"); !ok || !f.isDeferredUnlink("/held") {
		t.Fatal("deferred unlink lost remote object or its pending cleanup")
	}
}

type preparedLeaseUnlink struct {
	fs.HandlerFunc
	prepared   chan struct{}
	finished   chan bool
	marked     bool
	retargeted bool
}

func (p *preparedLeaseUnlink) PreparePathUnlink(string, bool) func(bool) error {
	close(p.prepared)
	return func(commit bool) error { p.finished <- commit; return nil }
}
func (p *preparedLeaseUnlink) RetargetPath(string, string) { p.retargeted = true }
func (p *preparedLeaseUnlink) MarkPathUnlinked(string)     { p.marked = true }

func TestLeaseLossBetweenUnlinkPreparationAndPublication(t *testing.T) {
	core := newMemCore()
	p := &preparedLeaseUnlink{prepared: make(chan struct{}), finished: make(chan bool, 1)}
	p.HandlerFunc = func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) { return next(ctx, op) }
	f := newTestFS(t, core, p)
	var out gofuse.CreateOut
	if st := f.Create(nil, exclusiveCreateInput(), "held", &out); st != gofuse.OK {
		t.Fatal(st)
	}
	if !f.inodes.AddAlias(out.NodeId, "/survivor", "", 2, false, 0, time.Now()) {
		t.Fatal("adding survivor alias failed")
	}
	b := &layerMountBinding{}
	f.layerFence = b
	// Hold the same mutex used by the renewer while the facade prepares.
	// Once preparation returns, the lease loss wins the publication race.
	b.fenceMu.Lock()
	result := make(chan gofuse.Status, 1)
	go func() { result <- f.Unlink(nil, rootHeader(), "held") }()
	<-p.prepared
	b.fenced.Store(true)
	b.fenceMu.Unlock()
	if st := <-result; st != gofuse.EIO {
		t.Fatalf("unlink: %v", st)
	}
	if <-p.finished || p.marked || p.retargeted || !f.openIdx.HasOpenPath("/held") || f.isDeferredUnlink("/held") {
		t.Fatal("rejected unlink left committed plane or handle flags")
	}
	if id, ok := f.inodes.GetInode("/held"); !ok || id != out.NodeId {
		t.Fatal("name lost after fence")
	}
	if id, ok := f.inodes.GetInode("/survivor"); !ok || id != out.NodeId {
		t.Fatal("survivor lost after fence")
	}
	releaseUnlinkFixture(f, out)
	if _, ok := core.get("/held"); !ok {
		t.Fatal("rejected unlink removed remote file")
	}
}

type unlinkUploadBarrier struct {
	*writeback.Writeback
	entered, proceed chan struct{}
	once             sync.Once
}

func (b *unlinkUploadBarrier) wait() {
	b.once.Do(func() { close(b.entered); <-b.proceed })
}
func (b *unlinkUploadBarrier) WaitPathUnlinkUploads(string) { b.wait() }

func (b *unlinkUploadBarrier) PreparePathUnlink(path string, survivor bool) func(bool) error {
	finish := b.Writeback.PreparePathUnlink(path, survivor)
	b.wait()
	return finish
}

func TestSecondOpenDuringUnlinkKeepsAnonymousImage(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/held", []byte("start"))
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	cl := client.New(srv.URL, "")
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	wb, err := writeback.New(writeback.Options{Policy: writeback.PolicyCloseSync, Client: cl})
	if err != nil {
		t.Fatal(err)
	}
	pause := &unlinkUploadBarrier{Writeback: wb, entered: make(chan struct{}), proceed: make(chan struct{})}
	var resume sync.Once
	unblock := func() { resume.Do(func() { close(pause.proceed) }) }
	t.Cleanup(unblock)
	f, err := New(Options{Extensions: []fs.Handler{pause, terminal}, DefaultPermissions: true})
	if err != nil {
		t.Fatal(err)
	}
	inode := lookupFacade(t, f, "held")
	header := gofuse.InHeader{NodeId: inode}
	open := func(flags uint32) uint64 {
		var out gofuse.OpenOut
		if st := f.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: flags}, &out); st != gofuse.OK {
			t.Fatal(st)
		}
		return out.Fh
	}
	first := open(syscall.O_RDONLY)
	unlinked := make(chan gofuse.Status, 1)
	go func() { unlinked <- f.Unlink(nil, rootHeader(), "held") }()
	awaitUnlinkTest(t, pause.entered)
	second := open(syscall.O_RDWR)
	if n, st := f.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: second}, []byte("saved")); n != 5 || st != gofuse.OK {
		t.Fatalf("write: %d %v", n, st)
	}
	unblock()
	if st := awaitUnlinkTest(t, unlinked); st != gofuse.OK {
		t.Fatal(st)
	}
	buf := make([]byte, 5)
	if _, st := f.Read(nil, &gofuse.ReadIn{InHeader: header, Fh: second, Size: 5}, buf); st != gofuse.OK || string(buf) != "saved" {
		t.Fatalf("read: %q %v", buf, st)
	}
	if st := f.Flush(nil, &gofuse.FlushIn{InHeader: header, Fh: second}); st != gofuse.OK {
		t.Fatal(st)
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: second})
	if n, ok := b.Node("/held"); !ok || string(n.Data) != "start" {
		t.Fatalf("anonymous close published %q", n.Data)
	}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: syscall.O_RDWR}, &gofuse.OpenOut{}); st != gofuse.ENOENT {
		t.Fatalf("open of removed name: %v", st)
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: first})
	if _, ok := b.Node("/held"); ok {
		t.Fatal("deleted name resurrected")
	}
}
