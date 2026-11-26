package fuse

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
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

func (b *unlinkUploadBarrier) PrepareOpenUnlink(path, survivor string) (func(func() bool) error, func() error, func(), func()) {
	acquireLock, commit, finalize, abort := b.Writeback.PrepareOpenUnlink(path, survivor)
	b.wait()
	return acquireLock, commit, finalize, abort
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

type blockingCommitUnlink struct {
	fs.HandlerFunc
	commitEntered chan struct{}
	commitProceed chan struct{}
	rollbackSeen  chan struct{}
	onceEnter     sync.Once
	onceRollback  sync.Once
}

func (b *blockingCommitUnlink) PrepareOpenUnlink(string, string) (func(func() bool) error, func() error, func(), func()) {
	acquire := func(func() bool) error {
		b.onceEnter.Do(func() { close(b.commitEntered) })
		<-b.commitProceed
		return nil
	}
	commit := func() error { return nil }
	finalize := func() {}
	abort := func() {
		b.onceRollback.Do(func() { close(b.rollbackSeen) })
	}
	return acquire, commit, finalize, abort
}

// TestSelfFenceNotBlockedBySlowUnlinkCommit verifies that holding an open-file
// unlink inside finish(true) (simulating WAL fsync / commit-queue wait) does
// NOT block triggerSelfFence from acquiring binding.fenceMu, fencing other
// paths immediately, and causing the in-flight unlink to fail with EIO at the
// publication boundary without removing the path.
func TestSelfFenceNotBlockedBySlowUnlinkCommit(t *testing.T) {
	coreFS := newMemCore()
	gate := &blockingCommitUnlink{
		commitEntered: make(chan struct{}),
		commitProceed: make(chan struct{}),
		rollbackSeen:  make(chan struct{}),
	}
	gate.HandlerFunc = func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) { return next(ctx, op) }
	f := newTestFS(t, coreFS, gate)

	var heldOut, otherOut gofuse.CreateOut
	if st := f.Create(nil, exclusiveCreateInput(), "held", &heldOut); st != gofuse.OK {
		t.Fatalf("create held: %v", st)
	}
	if st := f.Create(nil, exclusiveCreateInput(), "other", &otherOut); st != gofuse.OK {
		t.Fatalf("create other: %v", st)
	}
	otherRaw := &leaseTestHandle{}
	f.handles.put(otherOut.Fh, &fileHandle{ino: otherOut.NodeId, path: "/other", flags: syscall.O_RDWR, h: otherRaw})

	binding := &layerMountBinding{}
	f.layerFence = binding

	unlinkStatus := make(chan gofuse.Status, 1)
	go func() {
		unlinkStatus <- f.Unlink(nil, rootHeader(), "held")
	}()
	// Wait until unlink has entered finish(true) and is blocked in WAL/queue wait.
	awaitUnlinkTest(t, gate.commitEntered)

	// triggerSelfFence must complete immediately without waiting for finish(true).
	fenceDone := make(chan struct{})
	go func() {
		binding.triggerSelfFence("lease expired while unlink waited in commit queue")
		close(fenceDone)
	}()
	select {
	case <-fenceDone:
	case <-time.After(2 * time.Second):
		close(gate.commitProceed)
		t.Fatal("triggerSelfFence blocked behind unlink finish(true)")
	}

	// Other paths must reject writes immediately while unlink is still blocked.
	if n, st := f.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: otherOut.NodeId}, Fh: otherOut.Fh}, []byte("blocked")); n != 0 || st != gofuse.EIO {
		close(gate.commitProceed)
		t.Fatalf("write on sibling path after self-fence = (%d, %v), want (0, EIO)", n, st)
	}
	if otherRaw.writes != 0 {
		close(gate.commitProceed)
		t.Fatalf("sibling raw handle saw %d writes after self-fence, want 0", otherRaw.writes)
	}

	// Unblock finish(true): unlink must notice lease loss at the publication boundary,
	// roll back local unlink state, return EIO, and leave /held intact.
	close(gate.commitProceed)
	if st := awaitUnlinkTest(t, unlinkStatus); st != gofuse.EIO {
		t.Fatalf("unlink status = %v, want EIO", st)
	}
	awaitUnlinkTest(t, gate.rollbackSeen)
	if id, ok := f.inodes.GetInode("/held"); !ok || id != heldOut.NodeId {
		t.Fatal("rejected unlink removed /held inode mapping")
	}
	if !f.openIdx.HasOpenPath("/held") || f.isDeferredUnlink("/held") {
		t.Fatal("rejected unlink altered open index or queued deferred unlink")
	}
	releaseUnlinkFixture(f, heldOut)
	if _, ok := coreFS.get("/held"); !ok {
		t.Fatal("rejected unlink deleted remote /held on last close")
	}
}

// TestRejectedUnlinkKeepsCleanReaderSeeingLiveWriteBuffer verifies that when a
// clean reader and a second writer with unflushed buffered writes share a path,
// a lease-loss-rejected unlink does NOT leave a stale remote snapshot pinned on
// the clean reader — subsequent reads on the clean reader still observe the
// writer's acknowledged write-buffer bytes.
func TestRejectedUnlinkKeepsCleanReaderSeeingLiveWriteBuffer(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/doc", []byte("old-remote!!"))
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
	binding := &layerMountBinding{}
	binding.OnSelfFence(func() { wb.AbandonLayerForRollback() })
	f.layerFence = binding

	inode := lookupFacade(t, f, "doc")
	header := gofuse.InHeader{NodeId: inode}
	open := func(flags uint32) uint64 {
		var out gofuse.OpenOut
		if st := f.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: flags}, &out); st != gofuse.OK {
			t.Fatalf("open(0x%x): %v", flags, st)
		}
		return out.Fh
	}

	// Open clean reader H1 and writer H2; write unflushed data through H2.
	readerFh := open(syscall.O_RDONLY)
	writerFh := open(syscall.O_RDWR)
	if n, st := f.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: writerFh}, []byte("new-buffered")); n != 12 || st != gofuse.OK {
		t.Fatalf("write: %d %v", n, st)
	}

	// Start unlink, let PreparePathUnlink complete and pause before publication,
	// then trigger self-fence so unlink is rejected.
	unlinked := make(chan gofuse.Status, 1)
	go func() { unlinked <- f.Unlink(nil, rootHeader(), "doc") }()
	awaitUnlinkTest(t, pause.entered)
	binding.triggerSelfFence("lease expired before unlink publication")
	unblock()

	if st := awaitUnlinkTest(t, unlinked); st != gofuse.EIO {
		t.Fatalf("unlink status = %v, want EIO", st)
	}
	if !f.openIdx.HasOpenPath("/doc") || f.isDeferredUnlink("/doc") {
		t.Fatal("rejected unlink altered open path state")
	}

	// H1 must NOT have a stale "old-remote!!" snapshot pinned; it must read H2's
	// acknowledged write-buffer bytes ("new-buffered").
	buf := make([]byte, 12)
	res, st := f.Read(nil, &gofuse.ReadIn{InHeader: header, Fh: readerFh, Size: 12}, buf)
	if st != gofuse.OK {
		t.Fatalf("reader Read: %v", st)
	}
	got, _ := res.Bytes(buf)
	if string(got) != "new-buffered" {
		t.Fatalf("clean reader saw %q after rejected unlink, want live write-buffer %q", string(got), "new-buffered")
	}

	f.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: writerFh})
	f.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: readerFh})
}

type phasedUnlinkPauseBarrier struct {
	*writeback.Writeback
	pauseBeforeCancel bool
	pauseAfterCancel  bool
	entered           chan struct{}
	proceed           chan struct{}
	once              sync.Once
}

func (b *phasedUnlinkPauseBarrier) wait() {
	b.once.Do(func() { close(b.entered); <-b.proceed })
}

func (b *phasedUnlinkPauseBarrier) PrepareOpenUnlink(path, survivor string) (func(func() bool) error, func() error, func(), func()) {
	acquireLock, commit, finalize, abort := b.Writeback.PrepareOpenUnlink(path, survivor)
	wrappedAcquire := func(aborted func() bool) error {
		if b.pauseBeforeCancel {
			b.wait()
		}
		err := acquireLock(aborted)
		if err == nil && b.pauseAfterCancel {
			b.wait()
		}
		return err
	}
	return wrappedAcquire, commit, finalize, abort
}

// TestSelfFenceDuringRealWritebackUnlinkPreservesStagedState exercises a real
// *writeback.Writeback (with staging Shadow, Pending index, WAL Journal, and
// CommitQueue) across both self-fence windows of open-file unlink:
//  1. self-fence fires while CancelQueue is blocked (must NOT block
//     triggerSelfFence, must immediately fence sibling writes, and must leave
//     Shadow/Pending/handles/namespace 100% intact);
//  2. self-fence fires right after CancelQueue returns and before fenceMu
//     publication (must return EIO and leave Shadow/Pending/handles/namespace
//     100% intact without running irreversible publish/cache-eviction work).
func TestSelfFenceDuringRealWritebackUnlinkPreservesStagedState(t *testing.T) {
	cases := []struct {
		name              string
		pauseBeforeCancel bool
		pauseAfterCancel  bool
	}{
		{name: "fence_during_cancel_queue", pauseBeforeCancel: true},
		{name: "fence_between_cancel_and_publish", pauseAfterCancel: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.Seed("/held", []byte("initial-held"))
			b.Seed("/other", []byte("other-data"))
			srv := httptest.NewServer(b)
			t.Cleanup(srv.Close)
			cl := client.New(srv.URL, "")
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			ledger, err := staging.NewStaging(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			wb, err := writeback.New(writeback.Options{
				Policy:   writeback.PolicyWriteBack,
				SyncMode: "interactive",
				Client:   cl,
				Staging:  ledger,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wb.Stop(context.Background()) })

			// Stage local recovery bytes for /held directly into Shadow + Pending
			// + WAL Journal (representing an fsync'd write awaiting remote commit).
			stagedPayload := []byte("staged-recovery-bytes")
			if err := ledger.Shadow.WriteFull("/held", stagedPayload, 1); err != nil {
				t.Fatalf("seed shadow: %v", err)
			}
			if _, err := ledger.Pending.PutWithBaseRev("/held", int64(len(stagedPayload)), staging.PendingNew, 1); err != nil {
				t.Fatalf("seed pending: %v", err)
			}
			if err := ledger.Journal.AppendSync(staging.JournalEntry{
				Op:      staging.JournalFsync,
				Path:    "/held",
				Length:  int64(len(stagedPayload)),
				BaseRev: 1,
			}); err != nil {
				t.Fatalf("seed journal: %v", err)
			}

			pause := &phasedUnlinkPauseBarrier{
				Writeback:         wb,
				pauseBeforeCancel: tc.pauseBeforeCancel,
				pauseAfterCancel:  tc.pauseAfterCancel,
				entered:           make(chan struct{}),
				proceed:           make(chan struct{}),
			}
			var resumeOnce sync.Once
			unblock := func() { resumeOnce.Do(func() { close(pause.proceed) }) }
			t.Cleanup(unblock)

			f, err := New(Options{Extensions: []fs.Handler{pause, terminal}, DefaultPermissions: true})
			if err != nil {
				t.Fatal(err)
			}
			binding := &layerMountBinding{}
			binding.OnSelfFence(func() { wb.AbandonLayerForRollback() })
			f.layerFence = binding

			heldIno := lookupFacade(t, f, "held")
			otherIno := lookupFacade(t, f, "other")
			var heldOpen, otherOpen gofuse.OpenOut
			if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: heldIno}, Flags: syscall.O_RDWR}, &heldOpen); st != gofuse.OK {
				t.Fatalf("open held: %v", st)
			}
			if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: otherIno}, Flags: syscall.O_RDWR}, &otherOpen); st != gofuse.OK {
				t.Fatalf("open other: %v", st)
			}
			// Buffer a live write on /held so an open reader also verifies handle state.
			if n, st := f.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: heldIno}, Fh: heldOpen.Fh}, stagedPayload); n != uint32(len(stagedPayload)) || st != gofuse.OK {
				t.Fatalf("write held: %d %v", n, st)
			}

			unlinked := make(chan gofuse.Status, 1)
			go func() { unlinked <- f.Unlink(nil, rootHeader(), "held") }()
			awaitUnlinkTest(t, pause.entered)

			// triggerSelfFence must complete immediately without blocking behind unlink.
			fenceDone := make(chan struct{})
			go func() {
				binding.triggerSelfFence("lease expired during real writeback unlink")
				close(fenceDone)
			}()
			select {
			case <-fenceDone:
			case <-time.After(2 * time.Second):
				unblock()
				t.Fatal("triggerSelfFence blocked behind real writeback unlink")
			}

			// Sibling writes must immediately fail with EIO while unlink is still paused.
			if n, st := f.Write(nil, &gofuse.WriteIn{InHeader: gofuse.InHeader{NodeId: otherIno}, Fh: otherOpen.Fh}, []byte("blocked")); n != 0 || st != gofuse.EIO {
				unblock()
				t.Fatalf("sibling write after self-fence = (%d, %v), want (0, EIO)", n, st)
			}

			// Resume unlink: must fail with EIO and preserve all local staged + handle + namespace state.
			unblock()
			if st := awaitUnlinkTest(t, unlinked); st != gofuse.EIO {
				t.Fatalf("unlink status = %v, want EIO", st)
			}
			if !f.openIdx.HasOpenPath("/held") || f.isDeferredUnlink("/held") {
				t.Fatal("rejected unlink altered open path or deferred unlink state")
			}
			if id, ok := f.inodes.GetInode("/held"); !ok || id != heldIno {
				t.Fatal("rejected unlink removed /held inode mapping")
			}
			if !ledger.Pending.HasPending("/held") {
				t.Fatal("rejected unlink removed staged Pending index entry for /held")
			}
			shadowBytes, err := ledger.Shadow.ReadAll("/held")
			if err != nil || string(shadowBytes) != string(stagedPayload) {
				t.Fatalf("staged shadow corrupted after rejected unlink: %q err=%v", string(shadowBytes), err)
			}
			var fsyncFrames, unlinkFrames int
			if err := ledger.Journal.Replay(func(e staging.JournalEntry) {
				if e.Path != "/held" {
					return
				}
				switch e.Op {
				case staging.JournalFsync:
					fsyncFrames++
				case staging.JournalUnlink:
					unlinkFrames++
				}
			}); err != nil {
				t.Fatalf("replay journal: %v", err)
			}
			if fsyncFrames != 1 || unlinkFrames != 0 {
				t.Fatalf("WAL frames for /held after rejected unlink: fsync=%d unlink=%d, want fsync=1 unlink=0", fsyncFrames, unlinkFrames)
			}
			buf := make([]byte, len(stagedPayload))
			res, st := f.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: heldIno}, Fh: heldOpen.Fh, Size: uint32(len(stagedPayload))}, buf)
			if st != gofuse.OK {
				t.Fatalf("read held after rejected unlink: %v", st)
			}
			got, _ := res.Bytes(buf)
			if string(got) != string(stagedPayload) {
				t.Fatalf("held handle read = %q, want %q", string(got), string(stagedPayload))
			}

			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: otherIno}, Fh: otherOpen.Fh})
			f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: heldIno}, Fh: heldOpen.Fh})
		})
	}
}

func TestRestoreUnlinkedPathSkipsConcurrentlyRemovedHandle(t *testing.T) {
	idx := NewOpenHandleIndex()
	h1 := &fileHandle{ino: 42, path: "/held"}
	h2 := &fileHandle{ino: 42, path: "/held"}
	idx.Add(h1)
	idx.Add(h2)

	saved := idx.SnapshotPath("/held")
	idx.UnlinkPath("/held")
	// Simulate h1 closing during Phase 3 WAL persistence before rollback runs.
	idx.Remove(h1)

	idx.RestoreUnlinkedPath("/held", saved)
	if !idx.HasOpenPath("/held") {
		t.Fatal("expected surviving handle h2 to remain open on /held")
	}
	got := idx.SnapshotPath("/held")
	if len(got) != 1 || got[0] != h2 {
		t.Fatalf("restored handles = %v, want only surviving handle h2", got)
	}
	// Closing h2 must leave zero leaked entries for /held.
	idx.Remove(h2)
	if idx.HasOpenPath("/held") || idx.HasOpenInode(42) {
		t.Fatal("leaked closed handle in OpenHandleIndex after rollback")
	}
}

type spyRetargetHandler struct {
	fs.HandlerFunc
	retargets int
	marks     int
}

func (s *spyRetargetHandler) RetargetPath(string, string) { s.retargets++ }
func (s *spyRetargetHandler) MarkPathUnlinked(string)     { s.marks++ }

// TestOpenUnlinkPersistFailureRollsBackAndAllowsContinuedWrite exercises a
// real FUSE Phase 3 failure where both WAL JournalUnlink and fallback shadow
// removal fail: FS.Unlink must fail, roll back all Phase 2 in-memory state
// (inodes, openIdx, deferredUnlinks, handle unlinked flag) without invoking
// RetargetPath/MarkPathUnlinked, preserve Pending + staged bytes for reads,
// and allow continued writes + remote landing after the fault is repaired.
func TestOpenUnlinkPersistFailureRollsBackAndAllowsContinuedWrite(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("chmod read-only shadow dir does not block root")
	}
	for _, withHardlink := range []bool{false, true} {
		name := "single_link"
		if withHardlink {
			name = "hardlink_survivor"
		}
		t.Run(name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.Seed("/held", []byte("orig"))
			var holdPut atomic.Bool
			holdPut.Store(true)
			putStarted := make(chan struct{}, 4)
			release := make(chan struct{})
			var releaseOnce sync.Once
			closeRelease := func() {
				releaseOnce.Do(func() { close(release) })
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/held") && holdPut.Load() && (r.Method == http.MethodPut || r.Method == http.MethodPost) {
					select {
					case putStarted <- struct{}{}:
					default:
					}
					select {
					case <-r.Context().Done():
						http.Error(w, "canceled", http.StatusRequestTimeout)
						return
					case <-release:
					}
				}
				b.ServeHTTP(w, r)
			}))
			t.Cleanup(srv.Close)

			cl := client.New(srv.URL, "")
			cl.SetSmallFileThresholdForTests(1 << 20)
			dir := t.TempDir()
			ledger, err := staging.NewStaging(dir)
			if err != nil {
				t.Fatal(err)
			}
			wb, err := writeback.New(writeback.Options{
				Policy:                  writeback.PolicyWriteBack,
				SyncMode:                "interactive",
				Client:                  cl,
				Staging:                 ledger,
				RemoteCommitWaitTimeout: 200 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = wb.Stop(context.Background()) })
			t.Cleanup(func() {
				holdPut.Store(false)
				closeRelease()
			})
			terminal, err := core.New(cl, core.Options{})
			if err != nil {
				t.Fatal(err)
			}
			spy := &spyRetargetHandler{
				HandlerFunc: func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
					return next(ctx, op)
				},
			}
			f, err := New(Options{Extensions: []fs.Handler{wb, spy, terminal}, DefaultPermissions: true})
			if err != nil {
				t.Fatal(err)
			}

			heldIno := lookupFacade(t, f, "held")
			header := gofuse.InHeader{NodeId: heldIno}
			var openOut gofuse.OpenOut
			if st := f.Open(nil, &gofuse.OpenIn{InHeader: header, Flags: syscall.O_RDWR}, &openOut); st != gofuse.OK {
				t.Fatalf("open: %v", st)
			}
			if withHardlink {
				if !f.inodes.AddAlias(heldIno, "/survivor", "", 2, false, 0, time.Now()) {
					t.Fatal("failed adding /survivor alias")
				}
			}
			v1 := []byte("persisted-v1")
			if n, st := f.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: openOut.Fh}, v1); n != uint32(len(v1)) || st != gofuse.OK {
				t.Fatalf("write v1: %d %v", n, st)
			}
			if st := f.Fsync(nil, &gofuse.FsyncIn{InHeader: header, Fh: openOut.Fh}); st != gofuse.OK {
				t.Fatalf("fsync v1: %v", st)
			}
			select {
			case <-putStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for background commit upload to start")
			}

			// Break WAL + make shadow directory read-only so Phase 3 fails.
			if err := ledger.Journal.Close(); err != nil {
				t.Fatalf("close journal: %v", err)
			}
			shadowDir := filepath.Join(dir, "shadow")
			if err := os.Chmod(shadowDir, 0o500); err != nil {
				t.Fatalf("chmod shadow dir: %v", err)
			}
			t.Cleanup(func() { _ = os.Chmod(shadowDir, 0o700) })

			if st := f.Unlink(nil, rootHeader(), "held"); st == gofuse.OK {
				t.Fatal("expected FS.Unlink to fail when WAL and shadow removal both fail")
			}

			// Verify full Phase 2 rollback and zero stray RetargetPath/MarkPathUnlinked calls.
			if spy.retargets != 0 || spy.marks != 0 {
				t.Fatalf("failed Phase 3 unlink invoked retarget=%d mark=%d, want 0/0", spy.retargets, spy.marks)
			}
			if !f.openIdx.HasOpenPath("/held") || f.isDeferredUnlink("/held") {
				t.Fatal("failed Phase 3 unlink left openIdx or deferredUnlinks corrupted")
			}
			if id, ok := f.inodes.GetInode("/held"); !ok || id != heldIno {
				t.Fatalf("inode mapping for /held lost after rollback: ok=%v id=%d", ok, id)
			}
			if withHardlink {
				if id, ok := f.inodes.GetInode("/survivor"); !ok || id != heldIno {
					t.Fatalf("survivor mapping lost after rollback: ok=%v id=%d", ok, id)
				}
			}
			if !ledger.Pending.HasPending("/held") {
				t.Fatal("failed Phase 3 unlink dropped Pending entry")
			}
			buf := make([]byte, len(v1))
			res, st := f.Read(nil, &gofuse.ReadIn{InHeader: header, Fh: openOut.Fh, Size: uint32(len(v1))}, buf)
			if st != gofuse.OK {
				t.Fatalf("read after failed unlink: %v", st)
			}
			got, _ := res.Bytes(buf)
			if string(got) != string(v1) {
				t.Fatalf("read after failed unlink = %q, want %q", string(got), string(v1))
			}

			// Repair shadow permissions and lift PUT hold: the re-driven upload
			// of v1 must land remotely (failed unlink did not strand it), and
			// continued writes/reads on the same open handle must still work.
			if err := os.Chmod(shadowDir, 0o700); err != nil {
				t.Fatalf("restore shadow dir: %v", err)
			}
			holdPut.Store(false)
			closeRelease()
			deadline := time.Now().Add(5 * time.Second)
			for {
				if node, ok := b.Node("/held"); ok && string(node.Data) == string(v1) {
					break
				}
				if time.Now().After(deadline) {
					node, _ := b.Node("/held")
					t.Fatalf("timed out waiting for re-driven v1 upload on /held; last backend bytes=%q", string(node.Data))
				}
				time.Sleep(20 * time.Millisecond)
			}
			v2 := []byte("persisted-v2!")
			if n, st := f.Write(nil, &gofuse.WriteIn{InHeader: header, Fh: openOut.Fh}, v2); n != uint32(len(v2)) || st != gofuse.OK {
				t.Fatalf("write v2 after rollback: %d %v", n, st)
			}
			buf2 := make([]byte, len(v2))
			res2, st := f.Read(nil, &gofuse.ReadIn{InHeader: header, Fh: openOut.Fh, Size: uint32(len(v2))}, buf2)
			if st != gofuse.OK {
				t.Fatalf("read v2 after rollback: %v", st)
			}
			got2, _ := res2.Bytes(buf2)
			if string(got2) != string(v2) {
				t.Fatalf("read v2 after rollback = %q, want %q", string(got2), string(v2))
			}
			f.Release(nil, &gofuse.ReleaseIn{InHeader: header, Fh: openOut.Fh})
			// Release of the handle must not trigger a stray deferred unlink on remote.
			if node, ok := b.Node("/held"); !ok || string(node.Data) != string(v1) {
				t.Fatalf("after release, remote /held missing or corrupted: ok=%v data=%q", ok, string(node.Data))
			}
			var lookupOut gofuse.EntryOut
			if st := f.Lookup(nil, rootHeader(), "held", &lookupOut); st != gofuse.OK {
				t.Fatalf("lookup /held after release: %v", st)
			}
		})
	}
}
