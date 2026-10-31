package fuse

// Ports of v1 pkg/fuse/inode_test.go for the chain facade's InodeToPath
// table: listing reseeds never shrink a live inode's size, hardlink aliases
// share one FUSE inode, rename consumes the replaced target's link, and
// fillAttr serves the entry's mode/nlink rows. The v1 fillAttr rows moved
// to the new (fs.Attr, ino) signature; the zero-mode default (0644 file /
// 0755 dir) now lives in entryToAttr and is pinned here alongside.

import (
	"errors"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestEnsureInodeDoesNotShrinkSizeOnReseed(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/final/w0/file-000.txt", false, 32820, time.Now())
	got := m.EnsureInodeWithIdentity("/final/w0/file-000.txt", "", 0, false, 0, time.Now())
	if got != ino {
		t.Fatalf("ino=%d, want %d", got, ino)
	}
	entry, ok := m.GetEntry(ino)
	if !ok {
		t.Fatal("missing entry")
	}
	if entry.Size != 32820 {
		t.Fatalf("listing size=0 shrank the live inode to %d", entry.Size)
	}
}

func TestUpdateMode(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/test.txt", false, 100, time.Now())

	m.UpdateMode(ino, 0o600)

	entry, ok := m.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Mode != 0o600 {
		t.Errorf("mode=%o, want 0o600", entry.Mode)
	}
}

func TestUpdateModeUnknownInode(t *testing.T) {
	m := NewInodeToPath()
	// Should not panic for unknown inode.
	m.UpdateMode(999, 0o600)
}

func TestInodeHardlinkAliasesShareIdentity(t *testing.T) {
	m := NewInodeToPath()
	now := time.Now()

	inoA := m.LookupWithIdentity("/a.txt", "file-1", 2, false, 10, now)
	inoB := m.LookupWithIdentity("/b.txt", "file-1", 2, false, 10, now)
	if inoA != inoB {
		t.Fatalf("hardlink aliases got different inodes: %d != %d", inoA, inoB)
	}
	entry, ok := m.GetEntry(inoA)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Nlink != 2 {
		t.Fatalf("nlink = %d, want 2", entry.Nlink)
	}
	if _, ok := entry.Paths["/a.txt"]; !ok {
		t.Fatalf("entry paths missing /a.txt: %+v", entry.Paths)
	}
	if _, ok := entry.Paths["/b.txt"]; !ok {
		t.Fatalf("entry paths missing /b.txt: %+v", entry.Paths)
	}

	m.RemoveLink("/a.txt")
	if _, ok := m.GetInode("/a.txt"); ok {
		t.Fatal("/a.txt mapping survived RemoveLink")
	}
	if got, ok := m.GetInode("/b.txt"); !ok || got != inoA {
		t.Fatalf("/b.txt inode = %d/%v, want %d/true", got, ok, inoA)
	}
	entry, ok = m.GetEntry(inoA)
	if !ok {
		t.Fatal("entry removed while alias still exists")
	}
	if entry.Nlink != 1 {
		t.Fatalf("nlink after removing one alias = %d, want 1", entry.Nlink)
	}
}

func TestInodeRemoveMappingDoesNotConsumeLink(t *testing.T) {
	m := NewInodeToPath()
	now := time.Now()

	inoA := m.LookupWithIdentity("/a.txt", "file-1", 2, false, 10, now)
	inoB := m.LookupWithIdentity("/b.txt", "file-1", 2, false, 10, now)
	if inoA != inoB {
		t.Fatalf("hardlink aliases got different inodes: %d != %d", inoA, inoB)
	}
	m.Remove("/a.txt")
	entry, ok := m.GetEntry(inoA)
	if !ok {
		t.Fatal("entry removed while alias still exists")
	}
	if entry.Nlink != 2 {
		t.Fatalf("nlink after mapping removal = %d, want 2", entry.Nlink)
	}
}

func TestInodeRenameOverwriteConsumesTargetLinkAndUpdatesCtime(t *testing.T) {
	m := NewInodeToPath()
	now := time.Now()
	oldCtime := now.Add(-time.Hour)

	srcIno := m.LookupWithIdentity("/src.txt", "file-src", 1, false, 10, now)
	targetIno := m.LookupWithIdentity("/target.txt", "file-target", 2, false, 20, now)
	aliasIno := m.LookupWithIdentity("/target-link.txt", "file-target", 2, false, 20, now)
	if aliasIno != targetIno {
		t.Fatalf("target aliases got different inodes: %d != %d", aliasIno, targetIno)
	}
	m.UpdateCtime(targetIno, oldCtime)

	m.Rename("/src.txt", "/target.txt")

	if _, ok := m.GetInode("/src.txt"); ok {
		t.Fatal("source mapping survived rename")
	}
	if got, ok := m.GetInode("/target.txt"); !ok || got != srcIno {
		t.Fatalf("target inode = %d/%v, want %d/true", got, ok, srcIno)
	}
	entry, ok := m.GetEntry(targetIno)
	if !ok {
		t.Fatal("replaced inode removed while alias remains")
	}
	if entry.Nlink != 1 {
		t.Fatalf("replaced alias nlink = %d, want 1", entry.Nlink)
	}
	if !entry.Ctime.After(oldCtime) {
		t.Fatalf("replaced alias ctime = %s, want after %s", entry.Ctime, oldCtime)
	}
	if _, ok := entry.Paths["/target-link.txt"]; !ok {
		t.Fatalf("remaining alias path missing: %+v", entry.Paths)
	}
}

func TestInodeSetIdentityLetsLaterLookupJoinExistingInode(t *testing.T) {
	m := NewInodeToPath()
	now := time.Now()

	inoA := m.Lookup("/a.txt", false, 10, now)
	m.SetIdentity(inoA, "file-1", 1)
	inoB := m.LookupWithIdentity("/b.txt", "file-1", 2, false, 10, now)
	if inoA != inoB {
		t.Fatalf("lookup with identity allocated new inode: %d != %d", inoB, inoA)
	}
	entry, ok := m.GetEntry(inoA)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.Nlink != 2 {
		t.Fatalf("nlink = %d, want 2", entry.Nlink)
	}
}

// newAttrTestFS builds the bare facade fillAttr needs (uid/gid + inode
// table); the chain is irrelevant for these rows.
func newAttrTestFS() *FS {
	return &FS{uid: 1000, gid: 1000, inodes: NewInodeToPath()}
}

func TestFillAttrFileMode(t *testing.T) {
	f := newAttrTestFS()

	tests := []struct {
		name     string
		mode     uint32
		wantMode uint32
	}{
		{"explicit 755", 0o755, syscall.S_IFREG | 0o755},
		{"explicit 600", 0o600, syscall.S_IFREG | 0o600},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out gofuse.Attr
			f.fillAttr(fs.Attr{Kind: fs.KindFile, Mode: tt.mode, Size: 42}, 0, &out)
			if out.Mode != tt.wantMode {
				t.Errorf("mode=%o, want %o", out.Mode, tt.wantMode)
			}
		})
	}
}

func TestFillAttrDirMode(t *testing.T) {
	f := newAttrTestFS()

	tests := []struct {
		name     string
		mode     uint32
		wantMode uint32
	}{
		{"explicit 700", 0o700, syscall.S_IFDIR | 0o700},
		{"explicit 777", 0o777, syscall.S_IFDIR | 0o777},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out gofuse.Attr
			f.fillAttr(fs.Attr{Kind: fs.KindDir, Mode: tt.mode}, 0, &out)
			if out.Mode != tt.wantMode {
				t.Errorf("mode=%o, want %o", out.Mode, tt.wantMode)
			}
		})
	}
}

// v1's "default zero" mode rows: entries without authoritative mode bits
// fall back to 0644 (file) / 0755 (dir) in entryToAttr, and fillAttr serves
// that projection.
func TestEntryToAttrDefaultModes(t *testing.T) {
	f := newAttrTestFS()
	fileIno := f.inodes.Lookup("/a.txt", false, 42, time.Now())
	dirIno := f.inodes.Lookup("/dir", true, 0, time.Now())

	var out gofuse.Attr
	entry, ok := f.inodes.GetEntry(fileIno)
	if !ok {
		t.Fatal("missing file entry")
	}
	f.fillAttr(entryToAttr(entry), fileIno, &out)
	if out.Mode != syscall.S_IFREG|0o644 {
		t.Errorf("default file mode = %o, want %o", out.Mode, syscall.S_IFREG|0o644)
	}
	entry, ok = f.inodes.GetEntry(dirIno)
	if !ok {
		t.Fatal("missing dir entry")
	}
	f.fillAttr(entryToAttr(entry), dirIno, &out)
	if out.Mode != syscall.S_IFDIR|0o755 {
		t.Errorf("default dir mode = %o, want %o", out.Mode, syscall.S_IFDIR|0o755)
	}
}

func TestFillAttrFileNlink(t *testing.T) {
	f := newAttrTestFS()
	var out gofuse.Attr
	f.fillAttr(fs.Attr{Kind: fs.KindFile, Size: 42, Nlink: 3}, 0, &out)
	if out.Nlink != 3 {
		t.Fatalf("nlink = %d, want 3", out.Nlink)
	}
}

func TestFillAttrDirNlink(t *testing.T) {
	f := newAttrTestFS()
	var out gofuse.Attr
	f.fillAttr(fs.Attr{Kind: fs.KindDir, Nlink: 4}, 0, &out)
	if out.Nlink != 4 {
		t.Fatalf("dir nlink = %d, want 4", out.Nlink)
	}
}

// A rmdir'd directory keeps its path→inode mapping until the kernel's last
// reference is forgotten. A later create/mknod/symlink of a FILE at that
// path must not reuse the directory's nodeid: the kernel still holds the
// old inode alive and rejects a reply that retypes a live nodeid with EIO
// (pjdfstest chmod/11, rmdir/01, rename/21, unlink/00 all hit this through
// create-after-rmdir).
func TestLookupKindConflictAllocatesFreshInode(t *testing.T) {
	m := NewInodeToPath()
	dirIno := m.Lookup("/d", true, 0, time.Now())

	fileIno := m.Lookup("/d", false, 0, time.Now())
	if fileIno == dirIno {
		t.Fatalf("create after rmdir reused the directory inode %d", dirIno)
	}
	if got, ok := m.GetInode("/d"); !ok || got != fileIno {
		t.Fatalf("path mapping = %d/%v, want %d/true", got, ok, fileIno)
	}
	if e, ok := m.GetEntry(dirIno); ok && e.IsDir != true {
		t.Fatalf("old directory entry retyped: IsDir=%v", e.IsDir)
	}

	// mkdir over the fresh file's name likewise gets its own inode.
	dirIno2 := m.Lookup("/d", true, 0, time.Now())
	if dirIno2 == fileIno || dirIno2 == dirIno {
		t.Fatalf("mkdir after create reused inode %d", dirIno2)
	}
}

// EnsureInode (readdir reseeds) and EnsureInodeNoUpdate (snapshot recovery)
// share the kind-conflict rule with Lookup.
func TestEnsureInodeKindConflictAllocatesFreshInode(t *testing.T) {
	m := NewInodeToPath()
	dirIno := m.EnsureInode("/d", true, 0, time.Now())

	fileIno := m.EnsureInode("/d", false, 0, time.Now())
	if fileIno == dirIno {
		t.Fatalf("EnsureInode reused the directory inode %d", dirIno)
	}
	if got := m.EnsureInodeNoUpdate("/d", true, 0, time.Now()); got == fileIno {
		t.Fatalf("EnsureInodeNoUpdate reused the file inode %d", fileIno)
	}
}

// The detached directory entry survives until its kernel references drain,
// then Forget releases it instead of keeping a pathless entry forever.
// While it survives it answers GetPath with the OLD path — never an empty
// string, which dispatch would canonicalize to the mount root.
func TestForgetReleasesDetachedDirectoryEntry(t *testing.T) {
	m := NewInodeToPath()
	dirIno := m.Lookup("/d", true, 0, time.Now())
	m.Lookup("/d", false, 0, time.Now()) // kind conflict detaches the path

	// Shared path-resolution contract: the tombstone owns no name — GetPath
	// refuses it so every inode-addressed pathname operation answers
	// ENOENT instead of dispatching through the stale path.
	if p, ok := m.GetPath(dirIno); ok {
		t.Fatalf("tombstone GetPath = %q/true, want not-found", p)
	}
	m.Forget(dirIno, 2)
	if _, ok := m.GetEntry(dirIno); ok {
		t.Fatal("Forget kept a pathless directory entry")
	}
	if got, ok := m.GetInode("/d"); !ok || got == dirIno {
		t.Fatalf("path mapping = %d/%v, want a fresh inode", got, ok)
	}
}

// A READDIR-seeded entry (EnsureInode, Nlookup == 0) never receives a
// FORGET: detaching it must release the entry immediately instead of
// leaking a pathless row per readdir->rmdir->create cycle.
func TestDetachReleasesUnreferencedSeededEntry(t *testing.T) {
	m := NewInodeToPath()
	dirIno := m.EnsureInode("/d", true, 0, time.Now())

	fileIno := m.Lookup("/d", false, 0, time.Now())
	if fileIno == dirIno {
		t.Fatalf("kind conflict reused the seeded inode %d", dirIno)
	}
	if _, ok := m.GetEntry(dirIno); ok {
		t.Fatal("unreferenced detached entry was not released")
	}
	if got, ok := m.GetInode("/d"); !ok || got != fileIno {
		t.Fatalf("path mapping = %d/%v, want %d/true", got, ok, fileIno)
	}
}

// A hardlink alias keeps the entry's other paths when one alias's name is
// taken over by a directory.
func TestDetachKeepsSiblingAliases(t *testing.T) {
	m := NewInodeToPath()
	ino := m.LookupWithIdentity("/a", "res-1", 2, false, 0, time.Now())
	m.AddAlias(ino, "/b", "res-1", 2, false, 0, time.Now())

	if got := m.Lookup("/b", true, 0, time.Now()); got == ino {
		t.Fatalf("mkdir over alias reused file inode %d", ino)
	}
	if got, ok := m.GetInode("/a"); !ok || got != ino {
		t.Fatalf("sibling alias /a = %d/%v, want %d/true", got, ok, ino)
	}
}

// A hardlink-identity rejoin of a detached entry (a tombstone whose name a
// different-kind object now owns) must adopt the NEW path as its primary,
// clear the unlinked marker, and never seed the stale tombstone path back
// into its alias set — inode-addressed operations would otherwise keep
// dispatching through the dead name.
func TestReattachDetachedEntryAdoptsLivePath(t *testing.T) {
	m := NewInodeToPath()
	ino := m.LookupWithIdentity("/a", "res-1", 1, false, 0, time.Now())
	m.Lookup("/a", true, 0, time.Now()) // kind conflict detaches /a

	if got := m.LookupWithIdentity("/b", "res-1", 1, false, 0, time.Now()); got != ino {
		t.Fatalf("identity rejoin = %d, want %d", got, ino)
	}
	if p, ok := m.GetPath(ino); !ok || p != "/b" {
		t.Fatalf("reattached Path = %q/%v, want /b", p, ok)
	}
	e, ok := m.GetEntry(ino)
	if !ok || e.Unlinked {
		t.Fatal("reattached entry still marked unlinked")
	}
	if _, stale := e.Paths["/a"]; stale {
		t.Fatal("stale tombstone path seeded into the alias set")
	}
}

// An open descriptor whose dentry was already forgotten (Nlookup 0 via
// ForgetKeepMapping) still resolves through its nodeid: detaching its path
// must keep the entry until the descriptor closes (HasLiveUse veto).
func TestDetachKeepsEntryWhileDescriptorLive(t *testing.T) {
	m := NewInodeToPath()
	m.HasLiveUse = func(ino uint64) bool { return true }
	dirIno := m.EnsureInode("/d", true, 0, time.Now())

	fileIno := m.Lookup("/d", false, 0, time.Now())
	if fileIno == dirIno {
		t.Fatalf("kind conflict reused the seeded inode %d", dirIno)
	}
	if _, ok := m.GetEntry(dirIno); !ok {
		t.Fatal("entry released while a live descriptor still resolves it")
	}
}

// A path-level setattr (no descriptor) on a detached inode has no name to
// mutate: it must answer ENOENT without dispatching through the tombstone
// path, which a later reclaim can point at a NEW object at that name.
func TestSetAttrOnDetachedInodeWithoutHandleIsENOENT(t *testing.T) {
	f, err := New(Options{Extensions: []fs.Handler{&errCore{err: errors.New("dispatched on detached path")}}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	dirIno := f.inodes.Lookup("/d", true, 0, time.Now())
	f.inodes.Lookup("/d", false, 0, time.Now()) // kind conflict detaches /d

	var out gofuse.AttrOut
	input := &gofuse.SetAttrIn{}
	input.NodeId = dirIno
	input.Uid = 1000
	input.Valid = gofuse.FATTR_MODE
	input.Mode = 0o755
	if st := f.SetAttr(nil, input, &out); st != gofuse.ENOENT {
		t.Fatalf("SetAttr on detached inode = %v, want ENOENT", st)
	}
}

// Shared path-resolution contract, facade level: a mutation arriving by
// inode on a detached directory (SetXAttr here, the reviewer's probe)
// must answer ENOENT without any dispatch — the tombstone path names an
// object the inode no longer owns, and a later reclaim can point it at a
// NEW object, so dispatching would mutate the replacement.
func TestDetachedInodeMutationsDoNotDispatch(t *testing.T) {
	f, err := New(Options{Extensions: []fs.Handler{&errCore{err: errors.New("dispatched on detached path")}}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	dirIno := f.inodes.Lookup("/d", true, 0, time.Now())
	f.inodes.Lookup("/d", false, 0, time.Now()) // kind conflict detaches /d

	input := &gofuse.SetXAttrIn{}
	input.NodeId = dirIno
	if st := f.SetXAttr(nil, input, "user.probe", []byte("x")); st != gofuse.ENOENT {
		t.Fatalf("SetXAttr on detached inode = %v, want ENOENT", st)
	}
	// A child lookup through the stale directory nodeid must not resolve
	// against the mount root either: pathname resolution refuses the
	// tombstone before childOf can join "/victim".
	var out gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: dirIno}, "victim", &out); st != gofuse.ENOENT {
		t.Fatalf("Lookup under detached directory = %v, want ENOENT", st)
	}
}

// End-to-end detached lifecycle: open a descriptor, let the kernel forget
// the inode (Nlookup 0 while the fd lives), detach the name via a kind
// conflict (the live-use veto retains the entry), then close the last
// descriptor — the tombstone must be reaped exactly then.
func TestDetachedEntryReapedWhenLastLiveUseEnds(t *testing.T) {
	f, err := New(Options{Extensions: []fs.Handler{&errCore{err: errors.New("unused")}}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	dirIno := f.inodes.Lookup("/d", true, 0, time.Now())
	h := &fileHandle{ino: dirIno, path: "/d"}
	f.openIdx.Add(h)
	f.inodes.ForgetKeepMapping(dirIno, 2) // kernel refs drained, fd open

	f.inodes.Lookup("/d", false, 0, time.Now()) // kind conflict detaches /d
	if _, ok := f.inodes.GetEntry(dirIno); !ok {
		t.Fatal("detached entry released while its descriptor was live")
	}

	f.openIdx.Remove(h)
	f.inodes.CollectDetached(dirIno)
	if _, ok := f.inodes.GetEntry(dirIno); ok {
		t.Fatal("detached entry not reaped after the last live use ended")
	}
}

// A directory descriptor (opendir handle) counts as a live use exactly
// like a file descriptor: the veto retains the detached entry until the
// directory handle is released.
func TestDirectoryHandleKeepsDetachedEntryAlive(t *testing.T) {
	f, err := New(Options{Extensions: []fs.Handler{&errCore{err: errors.New("unused")}}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	dirIno := f.inodes.Lookup("/d", true, 0, time.Now())
	f.dirs.put(1, &dirHandle{ino: dirIno, path: "/d"})
	f.inodes.ForgetKeepMapping(dirIno, 2)

	f.inodes.Lookup("/d", false, 0, time.Now())
	if _, ok := f.inodes.GetEntry(dirIno); !ok {
		t.Fatal("detached entry released while a directory handle was live")
	}

	f.dirs.drop(1)
	f.inodes.CollectDetached(dirIno)
	if _, ok := f.inodes.GetEntry(dirIno); ok {
		t.Fatal("detached entry not reaped after the directory handle closed")
	}
}

// fstat through a directory handle on a detached directory keeps serving
// the entry attrs: the dirfd's recorded path resolves through dispatchPath
// like a file descriptor's.
func TestGetAttrByDirHandleOnDetachedInodeServesEntry(t *testing.T) {
	f, err := New(Options{Extensions: []fs.Handler{&errCore{err: errors.New("dispatched on detached path")}}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	dirIno := f.inodes.Lookup("/d", true, 0, time.Now())
	f.dirs.put(7, &dirHandle{ino: dirIno, path: "/d"})
	f.inodes.Lookup("/d", false, 0, time.Now()) // kind conflict detaches /d

	input := &gofuse.GetAttrIn{}
	input.NodeId = dirIno
	input.Flags_ = gofuse.FUSE_GETATTR_FH
	input.Fh_ = 7
	var out gofuse.AttrOut
	if st := f.GetAttr(nil, input, &out); st != gofuse.OK {
		t.Fatalf("GetAttr by dir handle on detached inode = %v, want OK (entry attrs)", st)
	}
	// Without the descriptor the tombstone still answers ENOENT.
	bare := &gofuse.GetAttrIn{}
	bare.NodeId = dirIno
	if st := f.GetAttr(nil, bare, &out); st != gofuse.ENOENT {
		t.Fatalf("GetAttr without descriptor on detached inode = %v, want ENOENT", st)
	}
}

// The reviewer's descriptor-identity probe, end to end through the real
// facade flow: create a file, keep its descriptor, let a directory take
// over the name, then SETATTR(mode) with the old nodeid + FATTR_FH. The
// backend executes setattr path-side (no object-bound capability), so the
// operation must be rejected (ESTALE) and the replacement directory's
// mode must never change.
func TestSetAttrByHandleAfterKindReplacementNeverMutatesReplacement(t *testing.T) {
	core := newMemCore()
	f, err := New(Options{Extensions: []fs.Handler{core}, Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}

	// Real create: allocates the kernel nodeid and a file descriptor.
	createIn := &gofuse.CreateIn{}
	createIn.NodeId = 1
	createIn.Mode = 0o644
	createIn.Flags = uint32(syscall.O_CREAT | syscall.O_WRONLY)
	var cout gofuse.CreateOut
	if st := f.Create(nil, createIn, "gone", &cout); st != gofuse.OK {
		t.Fatalf("create /gone = %v", st)
	}
	nodeid, fh := cout.NodeId, cout.Fh
	if fh == 0 {
		t.Fatal("create returned no file handle")
	}

	// Sanity: a descriptor chmod on the LIVE file applies.
	setIn := &gofuse.SetAttrIn{}
	setIn.NodeId = nodeid
	setIn.Fh = fh
	setIn.Valid = gofuse.FATTR_FH | gofuse.FATTR_MODE
	setIn.Mode = 0o600
	var aout gofuse.AttrOut
	if st := f.SetAttr(nil, setIn, &aout); st != gofuse.OK {
		t.Fatalf("live descriptor chmod = %v", st)
	}

	// The name is taken over by a directory (another client's view); the
	// next lookup of the path observes the kind change and detaches the
	// old file inode.
	core.mu.Lock()
	core.paths["/gone"] = &memEntry{isDir: true, mode: 0o755}
	core.mu.Unlock()
	f.inodes.Lookup("/gone", true, 0, time.Now())

	setIn.Mode = 0o700
	if st := f.SetAttr(nil, setIn, &aout); st != gofuse.Status(syscall.ESTALE) {
		t.Fatalf("descriptor chmod on detached inode = %v, want ESTALE", st)
	}
	core.mu.Lock()
	replacement := core.paths["/gone"]
	core.mu.Unlock()
	if replacement == nil || !replacement.isDir || replacement.mode != 0o755 {
		t.Fatalf("replacement object mutated: %+v", replacement)
	}
}
