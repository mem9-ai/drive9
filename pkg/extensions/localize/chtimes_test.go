package localize

// Ports of v1 pkg/fuse/local_overlay_chtimes_test.go (d39a6d1b3): the
// os-level chtimes tolerance for the close(2) EIO seen with sqlite's WAL
// index. unixShmPurge unlinks the -shm file while the descriptor is open,
// and the kernel's writeback-time SETATTR arrives afterwards; a path-based
// Chtimes then fails with ENOENT, which the kernel records as a mapping
// error and surfaces as EIO on close (SQLITE_IOERR_CLOSE). The flush through
// a live-but-unlinked handle must be swallowed; a linked path must not be.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// v1 TestLocalOverlayChtimesToleratesOpenUnlinkedFile: the timestamp flush
// for a path removed under an open handle is already complete in memory, so
// ENOENT is swallowed; a path with no unlinked state still reports ENOENT.
func TestChtimesToleratesOpenUnlinkedFile(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	if err := f.overlay.EnsureRoot(); err != nil {
		t.Fatalf("EnsureRoot: %v", err)
	}
	const path = "/repo/node_modules/db.sqlite-shm"
	view := &stubView{}
	res, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpCreate, Path: path, Flags: uint32(syscall.O_RDWR | syscall.O_CREAT), Mode: 0o644}, view.dispatch)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	h, ok := res.Handle.(*token)
	if !ok {
		t.Fatalf("handle = %T, want *token", res.Handle)
	}
	defer func() {
		_, _ = f.Do(context.Background(), &fs.Op{Kind: fs.OpRelease, Path: path, Handle: h}, view.dispatch)
	}()
	if _, err := f.Do(context.Background(), &fs.Op{Kind: fs.OpUnlink, Path: path}, view.dispatch); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if !f.wasUnlinked(path) {
		t.Fatal("unlink under an open handle did not record unlinked state")
	}

	mtime := time.Now()
	if err := f.chtimes(f.overlay, path, mtime); err != nil {
		t.Fatalf("open-unlinked chtimes = %v, want nil", err)
	}
	// A path with no open-handle history keeps the raw ENOENT.
	err = f.chtimes(f.overlay, path+"-other", mtime)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("linked-path chtimes = %v, want ENOENT", err)
	}
}

// v1 TestLocalOverlayChtimesStillUpdatesLiveFile: the tolerance must not
// swallow errors for a file that still exists — the backing mtime moves.
func TestChtimesStillUpdatesLiveFile(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	if err := f.overlay.EnsureRoot(); err != nil {
		t.Fatalf("EnsureRoot: %v", err)
	}
	const path = "/repo/node_modules/db.sqlite-shm"
	writeBacking(t, f, "repo/node_modules/db.sqlite-shm", "x")

	want := time.Unix(1700000000, 0)
	if err := f.chtimes(f.overlay, path, want); err != nil {
		t.Fatalf("chtimes = %v, want nil", err)
	}
	info, err := os.Stat(filepath.Join(f.overlay.Root(), "repo", "node_modules", "db.sqlite-shm"))
	if err != nil {
		t.Fatalf("stat backing: %v", err)
	}
	if got := info.ModTime().Unix(); got != want.Unix() {
		t.Fatalf("mtime = %d, want %d", got, want.Unix())
	}
}
