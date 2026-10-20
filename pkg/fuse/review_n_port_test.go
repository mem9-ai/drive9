package fuse

// qiffang/codex twenty-third round: the Open permission gate covers
// O_TRUNC (needs W_OK), ancestor search, Access answers local directories
// from the inode entry, READDIR rows keep server identity, and a checkout
// reset fences in-flight listings.

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
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

// A mount-local chown recorded on the inode entry governs ancestor search
// even after the namespace cache is dropped (the remote stat still carries
// the pre-chown owner).
func TestPortAncestorSearchUsesInodeOwnership(t *testing.T) {
	core := newMemCore()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o700}
	core.paths["/d/f"] = &memEntry{mode: 0o644}
	bus := fs.NewEventBus()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	dEntry := lookupForTest(t, f, "/d")
	fEntry := lookupForTest(t, f, "/d/f")
	// Mount-local chown to uid/gid 2000 (the remote keeps 1000).
	f.inodes.UpdateOwner(dEntry.NodeId, 2000, 2000, true, true)
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})

	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: fEntry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 2000, Gid: 2000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.OK {
		t.Fatalf("new owner open = %v, want OK", st)
	}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: fEntry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.EACCES {
		t.Fatalf("old owner open = %v, want EACCES", st)
	}
	// A UID-only chown leaves HasGID false: the UID override still applies
	// (the presence flags are independent).
	f.inodes.UpdateOwner(dEntry.NodeId, 3000, 0, true, false)
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: fEntry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 3000, Gid: 1000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.OK {
		t.Fatalf("UID-only chown owner open = %v, want OK", st)
	}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: fEntry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 2000, Gid: 2000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.EACCES {
		t.Fatalf("previous owner open after UID-only chown = %v, want EACCES", st)
	}
}

// blockingDirCache pauses inside Put so a concurrent reset can interleave.
type blockingDirCache struct {
	inner   fs.DirListingCache
	entered chan struct{}
	release chan struct{}
}

func (b *blockingDirCache) Get(dir string) ([]fs.DirEntryInfo, bool) { return b.inner.Get(dir) }
func (b *blockingDirCache) Put(dir string, entries []fs.DirEntryInfo) {
	close(b.entered)
	<-b.release
	b.inner.Put(dir, entries)
}
func (b *blockingDirCache) Upsert(parent string, e fs.DirEntryInfo) { b.inner.Upsert(parent, e) }
func (b *blockingDirCache) Invalidate(dir string)                   { b.inner.Invalidate(dir) }
func (b *blockingDirCache) InvalidatePrefix(dir string)             { b.inner.InvalidatePrefix(dir) }
func (b *blockingDirCache) InvalidateAll()                          { b.inner.InvalidateAll() }

// The generation check and the cache publication are atomic against a
// reset: a listing whose Put was in flight cannot land AFTER the reset's
// sweep.
func TestPortListingPublishIsAtomicWithReset(t *testing.T) {
	core := newMemCore()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	core.paths["/d/f"] = &memEntry{mode: 0o644}
	bus := fs.NewEventBus()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	bc := &blockingDirCache{
		inner:   dircache.NewNamespaceCache(time.Minute, time.Second, 0),
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	f.Ctx.DirCache = bc
	dEntry := lookupForTest(t, f, "/d")
	dh := &dirHandle{path: "/d", ino: dEntry.NodeId}

	loaded := make(chan gofuse.Status, 1)
	go func() {
		loaded <- f.loadDirEntries(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: dEntry.NodeId}}, dh)
	}()
	<-bc.entered
	resetDone := make(chan struct{})
	go func() {
		bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
		close(resetDone)
	}()
	select {
	case <-resetDone:
		t.Fatal("reset completed while a listing publish was in flight")
	case <-time.After(100 * time.Millisecond):
	}
	close(bc.release)
	if st := <-loaded; st != gofuse.OK {
		t.Fatalf("load = %v", st)
	}
	<-resetDone
	if rows, ok := f.Ctx.DirCache.Get("/d"); ok && len(rows) > 0 {
		t.Fatalf("pre-reset listing survived the reset: %+v", rows)
	}
}

// A cached READDIR row round-trips the full identity.
func TestPortDirCacheRoundTripsIdentity(t *testing.T) {
	core := newMemCore()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	core.paths["/d/f"] = &memEntry{mode: 0o644}
	f := newTestFS(t, core)
	f.Ctx.DirCache = dircache.NewNamespaceCache(time.Minute, time.Second, 0)
	f.Ctx.DirCache.Put("/d", []fs.DirEntryInfo{{
		Name: "f", Size: 3, Mode: 0o644, HasMode: true, Nlink: 2,
		Rev: 7, ResourceID: "res-1", ExtentIno: 42,
		Uid: 2000, Gid: 2000, HasUID: true, HasGID: true,
	}})
	dEntry := lookupForTest(t, f, "/d")
	var oout gofuse.OpenOut
	if st := f.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: dEntry.NodeId}}, &oout); st != gofuse.OK {
		t.Fatalf("opendir = %v", st)
	}
	dh, _ := f.dirs.get(oout.Fh)
	if st := f.loadDirEntries(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: dEntry.NodeId}}, dh); st != gofuse.OK {
		t.Fatalf("load = %v", st)
	}
	if len(dh.entries) != 1 {
		t.Fatalf("entries = %d", len(dh.entries))
	}
	a := dh.entries[0].Attr
	if a.Rev != 7 || a.ResourceID != "res-1" || a.ExtentIno != 42 || !a.HasOwner || a.Uid != 2000 {
		t.Fatalf("entry attr = %+v, want the cached identity preserved", a)
	}
}

// A reset that lands while a directory handle is busy must still invalidate
// its snapshot once the page releases the token.
func TestPortBusyDirHandleRemembersReset(t *testing.T) {
	core := newMemCore()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	core.paths["/d/old"] = &memEntry{mode: 0o644}
	bus := fs.NewEventBus()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	dEntry := lookupForTest(t, f, "/d")
	var oout gofuse.OpenOut
	if st := f.OpenDir(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: dEntry.NodeId}}, &oout); st != gofuse.OK {
		t.Fatalf("opendir = %v", st)
	}
	dh, _ := f.dirs.get(oout.Fh)
	dh.entries = []fs.Entry{{Name: "stale", Attr: fs.Attr{Kind: fs.KindFile, Mode: 0o644}}}
	// The page holds the read token while the checkout arrives.
	dh.lockRead(context.Background())
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	if dh.entries == nil {
		t.Fatal("reset cleared the snapshot without the token")
	}
	dh.unlockRead()
	// The next enumeration must drop it and refetch the reset view.
	core.mu.Lock()
	delete(core.paths, "/d/old")
	core.paths["/d/new"] = &memEntry{mode: 0o644}
	core.mu.Unlock()
	var out gofuse.DirEntryList
	if st := f.ReadDir(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: dEntry.NodeId}, Fh: oout.Fh}, &out); st != gofuse.OK {
		t.Fatalf("readdir = %v", st)
	}
	if len(dh.entries) != 1 || dh.entries[0].Name != "new" {
		t.Fatalf("entries = %+v, want the post-reset listing", dh.entries)
	}
}

// A path-level setattr is a mutation of the name: the caller needs SEARCH
// permission on every ancestor. A FATTR_FH descriptor op is exempt (it was
// gated at open).
func TestPortSetAttrRequiresAncestorSearch(t *testing.T) {
	core := newMemCore()
	core.paths["/locked"] = &memEntry{isDir: true, mode: 0o644}
	core.paths["/locked/f"] = &memEntry{mode: 0o600}
	f := newTestFS(t, core)
	ino := lookupForTest(t, f, "/locked/f").NodeId

	pathOp := &gofuse.SetAttrIn{}
	pathOp.NodeId = ino
	pathOp.Valid = gofuse.FATTR_MODE
	pathOp.Mode = 0o600
	pathOp.Caller = gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, pathOp, &aout); st != gofuse.EACCES {
		t.Fatalf("path setattr without ancestor search = %v, want EACCES", st)
	}
	handleOp := &gofuse.SetAttrIn{}
	handleOp.NodeId = ino
	handleOp.Valid = gofuse.FATTR_MODE | gofuse.FATTR_FH
	handleOp.Mode = 0o600
	handleOp.Fh = 7
	handleOp.Caller = gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}
	if st := f.SetAttr(nil, handleOp, &aout); st != gofuse.OK {
		t.Fatalf("descriptor setattr = %v, want OK", st)
	}
}

// gatedReadHandle blocks its Read until released.
type gatedReadHandle struct {
	gate    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedReadHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	g.once.Do(func() { close(g.gate) })
	<-g.release
	return copy(dest, []byte("OLD")), nil
}
func (g *gatedReadHandle) Write(context.Context, int64, []byte) error { return nil }
func (g *gatedReadHandle) Sync(context.Context) error                 { return nil }
func (g *gatedReadHandle) Flush(context.Context) error                { return nil }
func (g *gatedReadHandle) Release(context.Context) error              { return nil }

// A READ reply produced from a view a reset replaced must be EAGAIN (the
// kernel retries) instead of handing the old bytes to the page cache.
func TestPortReadReplyFencedByViewReset(t *testing.T) {
	core := newMemCore()
	core.paths["/f"] = &memEntry{data: []byte("OLD"), mode: 0o644}
	bus := fs.NewEventBus()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	entry := lookupForTest(t, f, "/f")
	gh := &gatedReadHandle{gate: make(chan struct{}), release: make(chan struct{})}
	f.handles.put(7, &fileHandle{ino: entry.NodeId, path: "/f", h: gh})

	type result struct {
		res gofuse.ReadResult
		st  gofuse.Status
	}
	done := make(chan result, 1)
	go func() {
		res, st := f.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: 7, Size: 4}, make([]byte, 4))
		done <- result{res, st}
	}()
	<-gh.gate
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	close(gh.release)
	got := <-done
	if got.st != gofuse.EAGAIN {
		t.Fatalf("stale read status = %v, want EAGAIN", got.st)
	}
	f.handles.drop(7)
}

// An explicit owner of 0:0 is a real identity, not the unset default.
func TestPortExplicitZeroOwnerIsPreserved(t *testing.T) {
	core := newMemCore()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 0, Gid: 0, OwnerSet: true})
	if err != nil {
		t.Fatal(err)
	}
	var out gofuse.Attr
	f.fillAttr(fs.Attr{Kind: fs.KindFile, Mode: 0o644}, 1, &out)
	if out.Uid != 0 || out.Gid != 0 {
		t.Fatalf("explicit owner = %d:%d, want 0:0", out.Uid, out.Gid)
	}
	plain, err := New(Options{Extensions: []fs.Handler{core}})
	if err != nil {
		t.Fatal(err)
	}
	plain.fillAttr(fs.Attr{Kind: fs.KindFile, Mode: 0o644}, 1, &out)
	if out.Uid != uint32(os.Getuid()) {
		t.Fatalf("default owner = %d, want the mounting uid", out.Uid)
	}
}

// The DISPATCH read path carries the same fence.
type blockReadExt struct {
	gate    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockReadExt) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	if op.Kind == fs.OpRead {
		b.once.Do(func() { close(b.gate) })
		<-b.release
	}
	return next(ctx, op)
}

func TestPortDispatchReadReplyFencedByViewReset(t *testing.T) {
	core := newMemCore()
	core.paths["/f"] = &memEntry{data: []byte("OLD"), mode: 0o644}
	bus := fs.NewEventBus()
	ext := &blockReadExt{gate: make(chan struct{}), release: make(chan struct{})}
	f, err := New(Options{Extensions: []fs.Handler{ext, core}, Uid: 1000, Gid: 1000, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	entry := lookupForTest(t, f, "/f")
	done := make(chan gofuse.Status, 1)
	go func() {
		_, st := f.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Size: 4}, make([]byte, 4))
		done <- st
	}()
	<-ext.gate
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	close(ext.release)
	if st := <-done; st != gofuse.EAGAIN {
		t.Fatalf("stale dispatch read status = %v, want EAGAIN", st)
	}
}

// eofHandle reports a zero-byte EOF.
type eofHandle struct{ data []byte }

func (e *eofHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	if off >= int64(len(e.data)) {
		return 0, io.EOF
	}
	return copy(dest, e.data[off:]), nil
}
func (e *eofHandle) Write(context.Context, int64, []byte) error { return nil }
func (e *eofHandle) Sync(context.Context) error                 { return nil }
func (e *eofHandle) Flush(context.Context) error                { return nil }
func (e *eofHandle) Release(context.Context) error              { return nil }

// A zero-byte EOF must be a NON-NIL empty result: a nil result with OK makes
// go-fuse ship the whole request buffer as zeros.
func TestPortReadEOFReturnsEmptyResult(t *testing.T) {
	core := newMemCore()
	core.paths["/f"] = &memEntry{data: []byte("hello"), mode: 0o644}
	f := newTestFS(t, core)
	entry := lookupForTest(t, f, "/f")
	f.handles.put(7, &fileHandle{ino: entry.NodeId, path: "/f", h: &eofHandle{data: []byte("hello")}})
	defer f.handles.drop(7)
	res, st := f.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: 7, Offset: 5, Size: 64}, make([]byte, 64))
	if st != gofuse.OK {
		t.Fatalf("EOF read status = %v", st)
	}
	if res == nil {
		t.Fatal("EOF read returned a nil result (kernel would receive a full zero buffer)")
	}
	data, _ := res.Bytes(make([]byte, 64))
	if len(data) != 0 {
		t.Fatalf("EOF read returned %d bytes, want 0", len(data))
	}
}

// The generation fence applies to empty replies too.
func TestPortEOFReadFencedByViewReset(t *testing.T) {
	core := newMemCore()
	core.paths["/f"] = &memEntry{data: []byte("hello"), mode: 0o644}
	bus := fs.NewEventBus()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000, Events: bus})
	if err != nil {
		t.Fatal(err)
	}
	entry := lookupForTest(t, f, "/f")
	gh := &gatedEOFHandle{gate: make(chan struct{}), release: make(chan struct{})}
	f.handles.put(7, &fileHandle{ino: entry.NodeId, path: "/f", h: gh})
	defer f.handles.drop(7)
	done := make(chan gofuse.Status, 1)
	go func() {
		_, st := f.Read(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: 7, Offset: 5, Size: 64}, make([]byte, 64))
		done <- st
	}()
	<-gh.gate
	bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
	close(gh.release)
	if st := <-done; st != gofuse.EAGAIN {
		t.Fatalf("stale EOF read status = %v, want EAGAIN", st)
	}
}

type gatedEOFHandle struct {
	gate    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *gatedEOFHandle) Read(ctx context.Context, off int64, dest []byte) (int, error) {
	g.once.Do(func() { close(g.gate) })
	<-g.release
	return 0, io.EOF
}
func (g *gatedEOFHandle) Write(context.Context, int64, []byte) error { return nil }
func (g *gatedEOFHandle) Sync(context.Context) error                 { return nil }
func (g *gatedEOFHandle) Flush(context.Context) error                { return nil }
func (g *gatedEOFHandle) Release(context.Context) error              { return nil }

// mutableModeTerminal serves one file whose permission set can change.
type mutableModeTerminal struct {
	mu   sync.Mutex
	mode uint32
}

func (m *mutableModeTerminal) set(mode uint32) {
	m.mu.Lock()
	m.mode = mode
	m.mu.Unlock()
}

func (m *mutableModeTerminal) Do(ctx context.Context, op *fs.Op, _ fs.Next) (fs.Result, error) {
	switch op.Kind {
	case fs.OpLookup, fs.OpGetAttr:
		if op.Path == "/" {
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: 0o755, Size: 0}}, nil
		}
		m.mu.Lock()
		mode := m.mode
		m.mu.Unlock()
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: 7}}, nil
	case fs.OpRead:
		return fs.Result{Data: []byte("private")}, nil
	}
	return fs.Result{}, nil
}

// A remote chmod(000) must close the open gate: the authoritative stat's
// permission set (including 000) is written back to the inode.
func TestPortRemoteChmodZeroClosesOpenGate(t *testing.T) {
	term := &mutableModeTerminal{mode: 0o644}
	f, err := New(Options{Extensions: []fs.Handler{term}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	entry := lookupForTest(t, f, "/f")
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.OK {
		t.Fatalf("pre-chmod open = %v", st)
	}
	// Remote chmod(000): the next authoritative stat and lookup must both
	// record the new permissions.
	term.set(0)
	var aout gofuse.AttrOut
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}}}, &aout); st != gofuse.OK {
		t.Fatalf("getattr = %v", st)
	}
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.EACCES {
		t.Fatalf("post-chmod open = %v, want EACCES", st)
	}
}

// gatedStatTerminal blocks every stat until released, then serves the
// post-reset mode.
type gatedStatTerminal struct {
	mu      sync.Mutex
	mode    uint32
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (g *gatedStatTerminal) Do(ctx context.Context, op *fs.Op, _ fs.Next) (fs.Result, error) {
	if op.Path == "/" {
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: 0o755}}, nil
	}
	g.mu.Lock()
	block := g.mode == 0o644 // pre-reset view answers block; post-reset ones pass
	g.mu.Unlock()
	if block {
		g.once.Do(func() { close(g.entered) })
		<-g.release
	}
	g.mu.Lock()
	mode := g.mode
	g.mu.Unlock()
	return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: mode, Size: 3}}, nil
}

// A stat/lookup that loses the view-generation race is re-dispatched
// against the NEW view (the kernel does not retry namespace ops on EAGAIN);
// it must never answer the pre-reset attributes.
func TestPortStatReplyFencedByViewReset(t *testing.T) {
	for _, kind := range []fs.OpKind{fs.OpGetAttr, fs.OpLookup} {
		term := &gatedStatTerminal{mode: 0o644, entered: make(chan struct{}), release: make(chan struct{})}
		bus := fs.NewEventBus()
		f, err := New(Options{Extensions: []fs.Handler{term}, Uid: 1000, Gid: 1000, Events: bus})
		if err != nil {
			t.Fatal(err)
		}
		// Seed the inode without blocking: "/" answers locally and the file
		// stat below is the first blocked one.
		term.mu.Lock()
		term.mode = 0o644
		term.mu.Unlock()
		type reply struct {
			st   gofuse.Status
			mode uint32
		}
		done := make(chan reply, 1)
		go func() {
			switch kind {
			case fs.OpGetAttr:
				var aout gofuse.AttrOut
				ino := f.inodes.EnsureInode("/f", false, 3, time.Now())
				st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &aout)
				done <- reply{st, aout.Mode & 0o777}
			default:
				var out gofuse.EntryOut
				st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &out)
				done <- reply{st, out.Mode & 0o777}
			}
		}()
		<-term.entered
		// The reset lands while the old stat is in flight; the NEW view
		// answers 0700 from then on.
		term.mu.Lock()
		term.mode = 0o700
		term.mu.Unlock()
		bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
		close(term.release)
		got := <-done
		if got.st != gofuse.OK {
			t.Fatalf("%v after reset = %v, want the retried current view", kind, got.st)
		}
		if got.mode != 0o700 {
			t.Fatalf("%v after reset served mode %#o, want the post-reset 0700", kind, got.mode)
		}
	}
}

// fillAttr must not project an unset chown field (the preserve sentinel).
func TestPortSingleFieldChownKeepsOtherField(t *testing.T) {
	core := newMemCore()
	f := newTestFS(t, core)
	var out gofuse.Attr
	f.fillAttr(fs.Attr{Kind: fs.KindFile, Mode: 0o600, Size: 3, Uid: ^uint32(0), Gid: 2000, HasOwner: true, HasGID: true}, 1, &out)
	if out.Uid != 1000 {
		t.Fatalf("uid = %d, want the mount uid (the unset field must not take the sentinel)", out.Uid)
	}
	if out.Gid != 2000 {
		t.Fatalf("gid = %d, want 2000", out.Gid)
	}
}

// mapModeTerminal serves per-path modes.
type mapModeTerminal struct {
	mu    sync.Mutex
	modes map[string]uint32
}

func (m *mapModeTerminal) Do(ctx context.Context, op *fs.Op, _ fs.Next) (fs.Result, error) {
	if op.Kind == fs.OpRead {
		return fs.Result{Data: []byte("private")}, nil
	}
	m.mu.Lock()
	mode, ok := m.modes[op.Path]
	m.mu.Unlock()
	if !ok {
		if op.Path == "/" || op.Path == "/d" {
			return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: 0o755}}, nil
		}
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: 0o644, Size: 7}}, nil
	}
	kind := fs.KindFile
	if op.Path == "/" || op.Path == "/d" {
		kind = fs.KindDir
	}
	return fs.Result{Attr: &fs.Attr{Kind: kind, Mode: mode, Size: 7}}, nil
}

// A directory's authoritative zero permissions close ancestor search.
func TestPortDirChmodZeroClosesAncestorSearch(t *testing.T) {
	term := &mapModeTerminal{modes: map[string]uint32{"/d": 0}}
	f, err := New(Options{Extensions: []fs.Handler{term}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	dEntry := lookupForTest(t, f, "/d")
	fEntry := lookupForTest(t, f, "/d/f")
	// The fresh Lookup recorded /d's authoritative 000; the ancestor gate
	// must consult it (not an older 0755).
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: fEntry.NodeId, Caller: gofuse.Caller{Owner: gofuse.Owner{Uid: 1000, Gid: 1000}, Pid: 1}}}, &gofuse.OpenOut{}); st != gofuse.EACCES {
		t.Fatalf("open under 000 directory = %v, want EACCES (dir ino %d)", st, dEntry.NodeId)
	}
}

// seqStatTerminal: the first file stat is gated (old view), the second
// answers ENOENT (the reset view).
type seqStatTerminal struct {
	calls   atomic.Int32
	gate    chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *seqStatTerminal) Do(ctx context.Context, op *fs.Op, _ fs.Next) (fs.Result, error) {
	if op.Path == "/" {
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindDir, Mode: 0o755}}, nil
	}
	if s.calls.Add(1) == 1 {
		s.once.Do(func() { close(s.gate) })
		<-s.release
		return fs.Result{Attr: &fs.Attr{Kind: fs.KindFile, Mode: 0o644, Size: 3}}, nil
	}
	return fs.Result{}, fs.ErrNotFound
}

// An error confirmed in the CURRENT view is preserved: a previous attempt's
// stale flag must not rewrite ENOENT into EAGAIN.
func TestPortStatRetryPreservesCurrentViewError(t *testing.T) {
	for _, kind := range []fs.OpKind{fs.OpGetAttr, fs.OpLookup} {
		term := &seqStatTerminal{gate: make(chan struct{}), release: make(chan struct{})}
		bus := fs.NewEventBus()
		f, err := New(Options{Extensions: []fs.Handler{term}, Uid: 1000, Gid: 1000, Events: bus})
		if err != nil {
			t.Fatal(err)
		}
		ino := f.inodes.EnsureInode("/f", false, 3, time.Now())
		done := make(chan gofuse.Status, 1)
		go func() {
			switch kind {
			case fs.OpGetAttr:
				var aout gofuse.AttrOut
				done <- f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &aout)
			default:
				var out gofuse.EntryOut
				done <- f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &out)
			}
		}()
		<-term.gate
		bus.Emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
		close(term.release)
		if st := <-done; st != gofuse.ENOENT {
			t.Fatalf("%v after reset = %v, want the current view's ENOENT", kind, st)
		}
	}
}

// Recording a successful stat's mode keeps sticky/setuid/setgid bits.
func TestPortStatWriteBackKeepsSpecialBits(t *testing.T) {
	core := newMemCore()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o1777}
	core.paths["/f"] = &memEntry{mode: 0o4755, data: []byte("x")}
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000, LocalDirAttrs: true})
	if err != nil {
		t.Fatal(err)
	}
	dEntry := lookupForTest(t, f, "/d")
	if dEntry.Attr.Mode != gofuse.S_IFDIR|0o1777 {
		t.Fatalf("lookup /d mode = %#o", dEntry.Attr.Mode)
	}
	var dout gofuse.AttrOut
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: dEntry.NodeId}}, &dout); st != gofuse.OK {
		t.Fatalf("getattr /d = %v", st)
	}
	if dout.Mode != gofuse.S_IFDIR|0o1777 {
		t.Fatalf("getattr /d mode = %#o, want the sticky bits preserved", dout.Mode)
	}
	fEntry := lookupForTest(t, f, "/f")
	if fEntry.Attr.Mode != gofuse.S_IFREG|0o4755 {
		t.Fatalf("lookup /f mode = %#o", fEntry.Attr.Mode)
	}
}
