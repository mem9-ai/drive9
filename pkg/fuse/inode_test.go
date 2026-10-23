package fuse

// Ports of v1 pkg/fuse/inode_test.go for the chain facade's InodeToPath
// table: extent-ino bindings survive Forget and never shrink on reseeds,
// hardlink aliases share one FUSE inode, rename consumes the replaced
// target's link, and fillAttr serves the entry's mode/nlink rows. The v1
// fillAttr rows moved to the new (fs.Attr, ino) signature; the zero-mode
// default (0644 file / 0755 dir) now lives in entryToAttr and is pinned
// here alongside.

import (
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestEnsureInodeDoesNotShrinkExtentSize(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/final/w0/file-000.txt", false, 32820, time.Now())
	m.SetExtentIno(ino, 9)
	got := m.EnsureInodeWithIdentity("/final/w0/file-000.txt", "", 0, false, 0, time.Now())
	if got != ino {
		t.Fatalf("ino=%d, want %d", got, ino)
	}
	entry, ok := m.GetEntry(ino)
	if !ok {
		t.Fatal("missing entry")
	}
	if entry.Size != 32820 {
		t.Fatalf("listing size=0 shrank extent inode to %d", entry.Size)
	}
}

func TestForgetKeepsExtentInodeMapping(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/final/w0/file-000.txt", false, 32768, time.Now())
	m.SetExtentIno(ino, 42)
	m.Forget(ino, 1)
	entry, ok := m.GetEntry(ino)
	if !ok {
		t.Fatal("Forget dropped juicefs inode mapping")
	}
	if entry.ExtentIno != 42 {
		t.Fatalf("ExtentIno=%d, want 42", entry.ExtentIno)
	}
	if entry.Size != 32768 {
		t.Fatalf("size=%d, want 32768 after Forget", entry.Size)
	}
	got, ok := m.GetInode("/final/w0/file-000.txt")
	if !ok || got != ino {
		t.Fatalf("path mapping = %d/%v, want %d/true", got, ok, ino)
	}
}

func TestSetExtentIno(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/test.txt", false, 100, time.Now())
	m.SetExtentIno(ino, 99)
	entry, ok := m.GetEntry(ino)
	if !ok {
		t.Fatal("entry not found")
	}
	if entry.ExtentIno != 99 {
		t.Fatalf("ExtentIno=%d, want 99", entry.ExtentIno)
	}
	m.SetExtentIno(ino, 0) // ignored
	entry, _ = m.GetEntry(ino)
	if entry.ExtentIno != 99 {
		t.Fatalf("zero ExtentIno overwrote cache: %d", entry.ExtentIno)
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

// v1 TestFindByExtentInoReusesFuseInode: the extent-ino reverse index
// resolves back to the FUSE inode (hardlink aliasing across paths).
func TestPortFindByExtentInoReusesFuseInode(t *testing.T) {
	m := NewInodeToPath()
	ino := m.Lookup("/a.txt", false, 1, time.Now())
	m.SetExtentIno(ino, 77)
	got, ok := m.FindByExtentIno(77)
	if !ok || got != ino {
		t.Fatalf("FindByExtentIno = %d/%v, want %d", got, ok, ino)
	}
	if _, ok := m.FindByExtentIno(78); ok {
		t.Fatal("missing juicefs ino must not match")
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
func TestForgetReleasesDetachedDirectoryEntry(t *testing.T) {
	m := NewInodeToPath()
	dirIno := m.Lookup("/d", true, 0, time.Now())
	m.Lookup("/d", false, 0, time.Now()) // kind conflict detaches the path

	m.Forget(dirIno, 2)
	if _, ok := m.GetEntry(dirIno); ok {
		t.Fatal("Forget kept a pathless directory entry")
	}
	if got, ok := m.GetInode("/d"); !ok || got == dirIno {
		t.Fatalf("path mapping = %d/%v, want a fresh inode", got, ok)
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
