package fuse

import (
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

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

func TestFillAttrFileMode(t *testing.T) {
	fs := &Dat9FS{uid: 1000, gid: 1000}

	tests := []struct {
		name     string
		mode     uint32
		wantMode uint32
		hasMode  bool
	}{
		{"default zero", 0, syscall.S_IFREG | 0o644, false},
		{"explicit 755", 0o755, syscall.S_IFREG | 0o755, true},
		{"explicit 600", 0o600, syscall.S_IFREG | 0o600, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &InodeEntry{Ino: 2, Path: "/a.txt", IsDir: false, Size: 42, Mode: tt.mode, HasMode: tt.hasMode}
			var out gofuse.Attr
			fs.fillAttr(entry, &out)
			if out.Mode != tt.wantMode {
				t.Errorf("mode=%o, want %o", out.Mode, tt.wantMode)
			}
		})
	}
}

func TestFillAttrFileNlink(t *testing.T) {
	fs := &Dat9FS{uid: 1000, gid: 1000}
	entry := &InodeEntry{Ino: 2, Path: "/a.txt", IsDir: false, Size: 42, Nlink: 3}
	var out gofuse.Attr
	fs.fillAttr(entry, &out)
	if out.Nlink != 3 {
		t.Fatalf("nlink = %d, want 3", out.Nlink)
	}
}

func TestFillAttrDirNlink(t *testing.T) {
	fs := &Dat9FS{uid: 1000, gid: 1000}
	entry := &InodeEntry{Ino: 2, Path: "/dir", IsDir: true, Nlink: 4}
	var out gofuse.Attr
	fs.fillAttr(entry, &out)
	if out.Nlink != 4 {
		t.Fatalf("dir nlink = %d, want 4", out.Nlink)
	}
}

func TestFillAttrDirMode(t *testing.T) {
	fs := &Dat9FS{uid: 1000, gid: 1000}

	tests := []struct {
		name     string
		mode     uint32
		wantMode uint32
		hasMode  bool
	}{
		{"default zero", 0, syscall.S_IFDIR | 0o755, false},
		{"explicit 700", 0o700, syscall.S_IFDIR | 0o700, true},
		{"explicit 777", 0o777, syscall.S_IFDIR | 0o777, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &InodeEntry{Ino: 2, Path: "/dir", IsDir: true, Size: 0, Mode: tt.mode, HasMode: tt.hasMode}
			var out gofuse.Attr
			fs.fillAttr(entry, &out)
			if out.Mode != tt.wantMode {
				t.Errorf("mode=%o, want %o", out.Mode, tt.wantMode)
			}
		})
	}
}
