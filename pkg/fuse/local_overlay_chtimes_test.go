package fuse

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestLocalOverlayChtimesToleratesOpenUnlinkedFile covers the close(2) EIO
// seen with sqlite's WAL index: unixShmPurge unlinks the -shm file, and the
// kernel's writeback-time SETATTR (fuse_write_inode -> fuse_flush_times)
// arrives afterwards. A path-based Chtimes fails with ENOENT, which the kernel
// records as a mapping error, and the following close(2) returns EIO
// (SQLITE_IOERR_CLOSE / errcode=4106).
func TestLocalOverlayChtimesToleratesOpenUnlinkedFile(t *testing.T) {
	overlay := NewLocalOverlay(t.TempDir())
	if err := overlay.EnsureRoot(); err != nil {
		t.Fatalf("EnsureRoot: %v", err)
	}
	const path = "/db.sqlite-shm"
	file, err := overlay.OpenFile(path, uint32(os.O_RDWR|os.O_CREATE), 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer func() { _ = file.Close() }()
	if err := overlay.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	mtime := time.Now()

	if err := localOverlayChtimes(overlay, path, true, mtime); err != nil {
		t.Fatalf("open-unlinked Chtimes = %v, want nil", err)
	}
	err = localOverlayChtimes(overlay, path, false, mtime)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("linked-path Chtimes = %v, want ENOENT", err)
	}
}

// TestLocalOverlayChtimesStillUpdatesLiveFile guards the normal path: the
// helper must not swallow errors for a file that still exists.
func TestLocalOverlayChtimesStillUpdatesLiveFile(t *testing.T) {
	root := t.TempDir()
	overlay := NewLocalOverlay(root)
	if err := overlay.EnsureRoot(); err != nil {
		t.Fatalf("EnsureRoot: %v", err)
	}
	const path = "/db.sqlite-shm"
	if _, err := overlay.OpenFile(path, uint32(os.O_RDWR|os.O_CREATE), 0o644); err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	want := time.Unix(1700000000, 0)
	if err := localOverlayChtimes(overlay, path, false, want); err != nil {
		t.Fatalf("Chtimes = %v, want nil", err)
	}
	info, err := os.Stat(filepath.Join(root, "overlay", "db.sqlite-shm"))
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if got := info.ModTime().Unix(); got != want.Unix() {
		t.Fatalf("mtime = %d, want %d", got, want.Unix())
	}
}
