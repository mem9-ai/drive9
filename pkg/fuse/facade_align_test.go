package fuse

// v1 behavioral alignment rows for the gofuse facade: offset-0 readdir
// replays the same snapshot (no duplicated entries), READDIRPLUS emits
// "." / ".." without pinning lookups, FORGET keeps mappings of handles with
// pending write-plane state, and copy_file_range stays ENOSYS (the kernel
// falls back to a read+write loop through the handle chains).

import (
	"context"
	"encoding/binary"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/extensions/specials"
	"github.com/mem9-ai/drive9/pkg/fs"
)

const direntSize = 24 // ino(8) + off(8) + namelen(4) + type(4)

type rawDirent struct {
	ino  uint64
	off  uint64
	name string
}

// parseDirents decodes serialized _Dirent records from a READDIR buffer.
func parseDirents(buf []byte) []rawDirent {
	var out []rawDirent
	for len(buf) >= direntSize {
		ino := binary.LittleEndian.Uint64(buf[0:8])
		off := binary.LittleEndian.Uint64(buf[8:16])
		nameLen := binary.LittleEndian.Uint32(buf[16:20])
		if nameLen == 0 || int(direntSize+nameLen) > len(buf) {
			break
		}
		out = append(out, rawDirent{ino: ino, off: off, name: string(buf[direntSize : direntSize+nameLen])})
		total := int(direntSize+nameLen) + (8-int(nameLen)&7)&7
		buf = buf[total:]
	}
	return out
}

func readdirWalk(t *testing.T, f *FS, dirIno uint64, plus bool) []rawDirent {
	t.Helper()
	var oout gofuse.OpenOut
	oi := &gofuse.OpenIn{}
	oi.NodeId = dirIno
	if st := f.OpenDir(nil, oi, &oout); st != gofuse.OK {
		t.Fatal(st)
	}
	var names []rawDirent
	for off := uint64(0); ; {
		buf := make([]byte, 4096)
		out := gofuse.NewDirEntryList(buf, 0)
		ri := &gofuse.ReadIn{Size: 4096}
		ri.NodeId, ri.Fh, ri.Offset = dirIno, oout.Fh, off
		var st gofuse.Status
		if plus {
			st = f.ReadDirPlus(nil, ri, out)
		} else {
			st = f.ReadDir(nil, ri, out)
		}
		if st != gofuse.OK {
			t.Fatal(st)
		}
		ents := parseDirents(buf)
		if len(ents) == 0 {
			return names
		}
		names = append(names, ents...)
		off = ents[len(ents)-1].off
	}
}

func openTestDir(t *testing.T, f *FS, name string) uint64 {
	t.Helper()
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), name, &lout); st != gofuse.OK {
		t.Fatalf("lookup %s: %v", name, st)
	}
	return lout.NodeId
}

func TestReadDirOffsetZeroReplayKeepsSingleCopy(t *testing.T) {
	core := newMemCore()
	core.writeAt("/a", 0, []byte("a"))
	core.mu.Lock()
	core.paths["/c"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	f := newTestFS(t, core)
	ino := openTestDir(t, f, "c")
	core.writeAt("/c/x", 0, []byte("x"))
	core.mu.Lock()
	core.paths["/c/y"] = &memEntry{mode: 0o644}
	core.mu.Unlock()

	first := readdirWalk(t, f, ino, false)
	// A rewind (offset-0 restart — gVisor reuses one host fd this way)
	// must replay the same snapshot, not append a second copy.
	second := readdirWalk(t, f, ino, false)
	if len(first) == 0 || len(second) != len(first) {
		t.Fatalf("replay entry count = %d, want %d", len(second), len(first))
	}
	seen := map[string]int{}
	for _, e := range second {
		seen[e.name]++
	}
	if seen["."] != 1 || seen[".."] != 1 || seen["x"] != 1 || seen["y"] != 1 {
		t.Fatalf("replayed listing has duplicates or gaps: %v", seen)
	}
	// v1 offset scheme: dots occupy 1/2, real entry i occupies i+3.
	for _, e := range second {
		switch e.name {
		case ".":
			if e.off != 1 {
				t.Fatalf(". offset = %d, want 1", e.off)
			}
		case "..":
			if e.off != 2 {
				t.Fatalf(".. offset = %d, want 2", e.off)
			}
		case "x":
			if e.off != 3 {
				t.Fatalf("x offset = %d, want 3", e.off)
			}
		case "y":
			if e.off != 4 {
				t.Fatalf("y offset = %d, want 4", e.off)
			}
		}
	}
}

func TestReadDirPlusDotsDoNotPinLookups(t *testing.T) {
	core := newMemCore()
	core.writeAt("/d/a", 0, []byte("a"))
	core.mu.Lock()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	f := newTestFS(t, core)
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "d", &lout); st != gofuse.OK {
		t.Fatal(st)
	}
	dh := &dirHandle{path: "/d", ino: lout.NodeId, attr: fs.Attr{Kind: fs.KindDir, Mode: 0o755}}
	dots := f.dotDotEntries(dh)
	if len(dots) != 2 || dots[0].Name != "." || dots[1].Name != ".." {
		t.Fatalf("dots = %v", dots)
	}
	// The parent resolves best-effort; the root is its own parent.
	rootDh := &dirHandle{path: "/", ino: 1, attr: fs.Attr{Kind: fs.KindDir, Mode: 0o755}}
	rootDots := f.dotDotEntries(rootDh)
	if rootDots[1].Ino != 1 {
		t.Fatalf("root .. ino = %d, want 1 (root is its own parent)", rootDots[1].Ino)
	}
	// ReadDirPlus emits dots with Ino=0/FUSE_UNKNOWN_INO and no lookup pin:
	// the emit path is covered by TestReadDirOffsetZeroReplay's plain walk
	// plus the explicit Ino reset in ReadDirPlus.
	var oout gofuse.OpenOut
	oi := &gofuse.OpenIn{}
	oi.NodeId = lout.NodeId
	if st := f.OpenDir(nil, oi, &oout); st != gofuse.OK {
		t.Fatal(st)
	}
	buf := make([]byte, 512)
	out := gofuse.NewDirEntryList(buf, 0)
	ri := &gofuse.ReadIn{Size: 512}
	ri.NodeId, ri.Fh = lout.NodeId, oout.Fh
	if st := f.ReadDirPlus(nil, ri, out); st != gofuse.OK {
		t.Fatal(st)
	}
	if out.Offset < 2 {
		t.Fatalf("readdirplus first page ended at offset %d, want >= 2 (dots emitted)", out.Offset)
	}
}

type forgetKeeperProbe struct {
	keep map[string]bool
}

func (p *forgetKeeperProbe) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	return next(ctx, op)
}
func (p *forgetKeeperProbe) KeepForgotten(path string) bool {
	return p.keep[path]
}

func TestForgetKeepsMappingForPendingWriteState(t *testing.T) {
	core := newMemCore()
	core.writeAt("/f", 0, []byte("data"))
	wrap := &forgetKeeperProbe{keep: map[string]bool{"/f": true}}
	f := newTestFS(t, core, wrap)
	ino := openTestDir(t, f, "f")
	f.Forget(ino, 1)
	if _, ok := f.inodes.GetPath(ino); !ok {
		t.Fatal("forget dropped the mapping of a handle with pending write state")
	}
	// Without pending state the mapping is released normally.
	wrap.keep["/f"] = false
	f.Forget(ino, 1)
	if _, ok := f.inodes.GetPath(ino); ok {
		t.Fatal("forget kept the mapping after pending state drained")
	}
}

func TestCopyFileRangeStaysENOSYS(t *testing.T) {
	core := newMemCore()
	core.writeAt("/a", 0, []byte("data"))
	core.writeAt("/b", 0, nil)
	f := newTestFS(t, core)
	var la, lb gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "a", &la); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := f.Lookup(nil, rootHeader(), "b", &lb); st != gofuse.OK {
		t.Fatal(st)
	}
	in := &gofuse.CopyFileRangeIn{}
	in.NodeId, in.NodeIdOut = la.NodeId, lb.NodeId
	in.OffIn, in.OffOut, in.Len = 0, 0, 4
	if _, st := f.CopyFileRange(nil, in); st != gofuse.Status(syscall.ENOSYS) {
		t.Fatalf("copy_file_range = %v, want ENOSYS (v1: kernel falls back to read+write)", st)
	}
}

func TestMknodRegularCreatesAndSpecialEEXIST(t *testing.T) {
	core := newMemCore()
	sp, err := specials.New(specials.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f := newTestFS(t, core, sp)

	// Regular mknod creates the remote file with the requested mode (v1
	// mknodRegular); a second mknod on the same name is EEXIST.
	mi := &gofuse.MknodIn{Mode: uint32(syscall.S_IFREG) | 0o755}
	mi.NodeId = 1
	var eout gofuse.EntryOut
	if st := f.Mknod(nil, mi, "reg", &eout); st != gofuse.OK {
		t.Fatalf("mknod reg = %v", st)
	}
	if eout.Mode&0o777 != 0o755 {
		t.Fatalf("mknod mode = %o, want 755", eout.Mode&0o777)
	}
	if st := f.Mknod(nil, mi, "reg", &eout); st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("second mknod = %v, want EEXIST", st)
	}

	// Metadata-only special: registered locally, visible to lookup, and a
	// repeat mknod (or a mknod over an existing regular file) is EEXIST.
	fi := &gofuse.MknodIn{Mode: uint32(syscall.S_IFIFO) | 0o644}
	fi.NodeId = 1
	if st := f.Mknod(nil, fi, "pipe", &eout); st != gofuse.OK {
		t.Fatalf("mknod fifo = %v", st)
	}
	if st := f.Mknod(nil, fi, "pipe", &eout); st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("second fifo mknod = %v, want EEXIST", st)
	}
	if st := f.Mknod(nil, fi, "reg", &eout); st != gofuse.Status(syscall.EEXIST) {
		t.Fatalf("fifo mknod over existing file = %v, want EEXIST", st)
	}
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "pipe", &lout); st != gofuse.OK || lout.Mode&syscall.S_IFMT != syscall.S_IFIFO {
		t.Fatalf("lookup pipe = %v mode=%o", st, lout.Mode)
	}
}

// v1 gvisor compat row: gVisor rewinds one host directory FD; an offset-0
// restart must refetch the listing (new children become visible) instead of
// replaying the stale snapshot.
func TestGVisorCompatOffsetZeroRescanRefetches(t *testing.T) {
	core := newMemCore()
	core.mu.Lock()
	core.paths["/g"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	f := newTestFS(t, core)
	f.gvisorCompat = true
	ino := openTestDir(t, f, "g")

	first := readdirWalk(t, f, ino, false)
	if len(first) != 2 { // just the dots
		t.Fatalf("first walk = %d entries, want 2", len(first))
	}
	core.mu.Lock()
	core.paths["/g/late"] = &memEntry{mode: 0o644}
	core.mu.Unlock()

	second := readdirWalk(t, f, ino, false)
	found := false
	for _, e := range second {
		if e.name == "late" {
			found = true
		}
	}
	if !found {
		t.Fatal("offset-0 rescan under gvisor compat did not refetch the listing")
	}
}

// v1 chown rows: ownership is mount-local (UpdateOwner), the preserve
// sentinel (-1) changes nothing, and a non-owner caller gets EPERM.
func TestSetAttrChownSemantics(t *testing.T) {
	core := newMemCore()
	core.writeAt("/f", 0, []byte("data"))
	f := newTestFS(t, core)
	ino := openTestDir(t, f, "f")

	owner := &gofuse.SetAttrIn{}
	owner.NodeId = ino
	owner.Valid = gofuse.FATTR_UID | gofuse.FATTR_GID
	owner.Uid = 1234
	owner.Gid = 5678
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, owner, &aout); st != gofuse.OK {
		t.Fatalf("chown by mount owner = %v", st)
	}
	if aout.Uid != 1234 || aout.Gid != 5678 {
		t.Fatalf("owner after chown = %d/%d, want 1234/5678", aout.Uid, aout.Gid)
	}
	// The override survives a fresh GetAttr (v1: UpdateOwner is local state).
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &aout); st != gofuse.OK {
		t.Fatal(st)
	}
	if aout.Uid != 1234 {
		t.Fatalf("getattr owner = %d, want 1234", aout.Uid)
	}

	// A different caller cannot move ownership (EPERM, v1
	// checkSetAttrOwnerForCaller).
	h := rootHeader()
	h.NodeId = ino
	h.Uid = 999
	owner2 := &gofuse.SetAttrIn{}
	owner2.NodeId = ino
	owner2.Valid = gofuse.FATTR_UID
	owner2.Uid = 4321
	owner2.InHeader = *h
	if st := f.SetAttr(nil, owner2, &aout); st != gofuse.EPERM {
		t.Fatalf("chown by non-owner = %v, want EPERM", st)
	}

	// The preserve sentinel (uint32(-1)) changes nothing.
	preserve := &gofuse.SetAttrIn{}
	preserve.NodeId = ino
	preserve.Valid = gofuse.FATTR_UID | gofuse.FATTR_GID
	preserve.Uid = ^uint32(0)
	preserve.Gid = ^uint32(0)
	if st := f.SetAttr(nil, preserve, &aout); st != gofuse.OK {
		t.Fatalf("preserve chown = %v", st)
	}
	if aout.Uid != 1234 || aout.Gid != 5678 {
		t.Fatalf("owner after preserve chown = %d/%d, want unchanged", aout.Uid, aout.Gid)
	}
}

// v1 utimensat rows: mtimes are mount-local overrides that later stats must
// serve instead of the server-derived value.
func TestSetAttrMtimeOverrideServed(t *testing.T) {
	core := newMemCore()
	core.writeAt("/t", 0, []byte("data"))
	f := newTestFS(t, core)
	ino := openTestDir(t, f, "t")

	want := time.Unix(1600000000, 0)
	si := &gofuse.SetAttrIn{}
	si.NodeId = ino
	si.Valid = gofuse.FATTR_MTIME
	si.Mtime = uint64(want.Unix())
	si.Mtimensec = 0
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, si, &aout); st != gofuse.OK {
		t.Fatalf("utimensat = %v", st)
	}
	if aout.Mtime != uint64(want.Unix()) {
		t.Fatalf("mtime after utimensat = %d, want %d", aout.Mtime, want.Unix())
	}
	// A fresh GetAttr serves the override (remote stats cannot clobber it).
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ino}}, &aout); st != gofuse.OK {
		t.Fatal(st)
	}
	if aout.Mtime != uint64(want.Unix()) {
		t.Fatalf("getattr mtime = %d, want override %d", aout.Mtime, want.Unix())
	}
}

// v1 nlink rows: directories floor at 2, files at 1, and the inode's live
// alias count (hardlinks) wins when present.
func TestFillAttrNlinkSemantics(t *testing.T) {
	core := newMemCore()
	core.mu.Lock()
	core.paths["/d"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	f := newTestFS(t, core)
	dino := openTestDir(t, f, "d")
	core.writeAt("/w.txt", 0, []byte("x"))
	fino := openTestDir(t, f, "w.txt")

	var aout gofuse.AttrOut
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: dino}}, &aout); st != gofuse.OK {
		t.Fatal(st)
	}
	if aout.Nlink < 2 {
		t.Fatalf("dir nlink = %d, want >= 2", aout.Nlink)
	}
	if st := f.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: fino}}, &aout); st != gofuse.OK {
		t.Fatal(st)
	}
	if aout.Nlink < 1 {
		t.Fatalf("file nlink = %d, want >= 1", aout.Nlink)
	}
}

// v1 Access rows: class-based permission checks with mount-owner fallback.
func TestAccessClassChecks(t *testing.T) {
	core := newMemCore()
	core.writeAt("/secret", 0, []byte("s"))
	core.writeAt("/open", 0, []byte("o"))
	core.mu.Lock()
	core.paths["/secret"].mode = 0o600
	core.paths["/open"].mode = 0o644
	core.mu.Unlock()
	f := newTestFS(t, core)

	// Mount owner (uid 1000): owner class bits apply.
	ai := &gofuse.AccessIn{}
	ai.Mask = 4 // R_OK
	h := rootHeader()
	h.Uid = 1000
	h.NodeId = openTestDir(t, f, "secret")
	ai.InHeader = *h
	ai.NodeId = h.NodeId
	if st := f.Access(nil, ai); st != gofuse.OK {
		t.Fatalf("owner read on 0600 = %v, want OK", st)
	}
	// Other caller: no read permission (0600 other = 0).
	h.Uid = 1234
	ai.InHeader = *h
	ai.NodeId = h.NodeId
	if st := f.Access(nil, ai); st != gofuse.EACCES {
		t.Fatalf("other read on 0600 = %v, want EACCES", st)
	}
	// Root passes.
	h.Uid = 0
	ai.InHeader = *h
	ai.NodeId = h.NodeId
	if st := f.Access(nil, ai); st != gofuse.OK {
		t.Fatalf("root read = %v, want OK", st)
	}
	// World-readable file: other read OK.
	ai.Mask = 4
	h.NodeId = openTestDir(t, f, "open")
	h.Uid = 1234
	ai.InHeader = *h
	ai.NodeId = h.NodeId
	ai.NodeId = h.NodeId
	if st := f.Access(nil, ai); st != gofuse.OK {
		t.Fatalf("other read on 0644 = %v, want OK", st)
	}
}

// TestLandedSettlePublishesCommittedSize pins the v1 commit-settle contract
// (inodes.UpdateSize + cacheFileForPath at every settle site): a Landed event
// settles the inode size and the parent's cached listing entry to the
// committed size — zero included (empty creates and truncations commit size
// 0) — while a size-less EventWrite (the facade's namespace mutations) must
// not touch the size.
func TestLandedSettlePublishesCommittedSize(t *testing.T) {
	core := newMemCore()
	core.writeAt("/c/x", 0, []byte("xxxxxx"))
	core.mu.Lock()
	core.paths["/c"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	f := newTestFS(t, core)

	var cout gofuse.EntryOut
	if st := f.Lookup(nil, rootHeader(), "c", &cout); st != gofuse.OK {
		t.Fatalf("lookup /c: %v", st)
	}
	var xout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: cout.NodeId}, "x", &xout); st != gofuse.OK {
		t.Fatalf("lookup x: %v", st)
	}
	ino := xout.NodeId

	// A complete listing as a prior readdir would install it; settle upserts
	// patch entries inside it (Get serves complete listings only).
	f.Ctx.DirCache.Put("/c", []fs.DirEntryInfo{{Name: "x", Size: 6}})

	listingSize := func(t *testing.T) (int64, bool) {
		t.Helper()
		ents, _ := f.Ctx.DirCache.Get("/c")
		for _, en := range ents {
			if en.Name == "x" {
				return en.Size, true
			}
		}
		return 0, false
	}

	// A nonzero settle (flush of a 10-byte image).
	f.events.Emit(fs.Event{Kind: fs.EventLanded, Path: "/c/x", Rev: 3, Size: 10})
	if e, ok := f.inodes.GetEntry(ino); !ok || e.Size != 10 {
		t.Fatalf("inode size after settle = %d %v", e.Size, ok)
	}
	if sz, ok := listingSize(t); !ok || sz != 10 {
		t.Fatalf("listing size after settle = %d %v", sz, ok)
	}

	// A zero settle (empty create / truncation commit) publishes size 0:
	// v1 cacheFileForPath has no zero guard.
	f.events.Emit(fs.Event{Kind: fs.EventLanded, Path: "/c/x", Rev: 4, Size: 0})
	if e, ok := f.inodes.GetEntry(ino); !ok || e.Size != 0 {
		t.Fatalf("inode size after zero settle = %d %v", e.Size, ok)
	}
	if sz, ok := listingSize(t); !ok || sz != 0 {
		t.Fatalf("listing size after zero settle = %d %v", sz, ok)
	}

	// A size-less EventWrite (facade namespace mutation) must not zero the
	// freshly settled size.
	f.events.Emit(fs.Event{Kind: fs.EventLanded, Path: "/c/x", Rev: 5, Size: 7})
	f.events.Emit(fs.Event{Kind: fs.EventWrite, Path: "/c/x"})
	if e, ok := f.inodes.GetEntry(ino); !ok || e.Size != 7 {
		t.Fatalf("inode size after size-less write event = %d %v", e.Size, ok)
	}
}

// TestSetAttrRefreshesCachedViews locks the v1 settle contract: a successful
// chmod/truncate updates the inode entry and the namespace-cache listing
// entry immediately — the next GetAttr inside the attr TTL must serve the
// new mode/size, not the pre-setattr values (v1 UpdateMode/UpdateSize +
// cacheEntryForPath).
func TestSetAttrRefreshesCachedViews(t *testing.T) {
	core := newMemCore()
	core.writeAt("/c/m.txt", 0, []byte("0123456789"))
	core.mu.Lock()
	core.paths["/c"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	f := newTestFS(t, core)
	ino := openTestDir(t, f, "c")

	// Warm the namespace cache with a complete listing (readdir).
	if ents := readdirWalk(t, f, ino, false); len(ents) == 0 {
		t.Fatal("no entries")
	}
	var xout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: ino}, "m.txt", &xout); st != gofuse.OK {
		t.Fatalf("lookup m.txt: %v", st)
	}

	// chmod 0111 through SETATTR.
	si := &gofuse.SetAttrIn{}
	si.NodeId = xout.NodeId
	si.Valid = gofuse.FATTR_MODE
	si.Mode = 0o111
	var sout gofuse.AttrOut
	if st := f.SetAttr(nil, si, &sout); st != gofuse.OK {
		t.Fatalf("setattr mode: %v", st)
	}
	if sout.Mode&0o7777 != 0o111 {
		t.Fatalf("setattr reply mode = %o", sout.Mode&0o7777)
	}
	// The next GetAttr must see the new mode without waiting out the TTL.
	gi := &gofuse.GetAttrIn{}
	gi.NodeId = xout.NodeId
	var gout gofuse.AttrOut
	if st := f.GetAttr(nil, gi, &gout); st != gofuse.OK {
		t.Fatalf("getattr: %v", st)
	}
	if gout.Mode&0o7777 != 0o111 {
		t.Fatalf("getattr mode after chmod = %o, want 111", gout.Mode&0o7777)
	}
	if e, ok := f.inodes.GetEntry(xout.NodeId); !ok || e.Mode&0o7777 != 0o111 {
		t.Fatalf("inode mode after chmod = %o", e.Mode&0o7777)
	}
	ents, _ := f.Ctx.DirCache.Get("/c")
	for _, en := range ents {
		if en.Name == "m.txt" && en.Mode&0o7777 != 0o111 {
			t.Fatalf("cached listing mode after chmod = %o", en.Mode&0o7777)
		}
	}

	// Truncate to 3 bytes: size must settle the same way.
	si2 := &gofuse.SetAttrIn{}
	si2.NodeId = xout.NodeId
	si2.Valid = gofuse.FATTR_SIZE
	si2.Size = 3
	if st := f.SetAttr(nil, si2, &sout); st != gofuse.OK {
		t.Fatalf("setattr size: %v", st)
	}
	if st := f.GetAttr(nil, gi, &gout); st != gofuse.OK || gout.Size != 3 {
		t.Fatalf("getattr size after truncate = %d (%v)", gout.Size, st)
	}
}
