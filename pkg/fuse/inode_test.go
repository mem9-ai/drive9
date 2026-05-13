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
