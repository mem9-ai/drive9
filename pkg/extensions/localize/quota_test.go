package localize

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// newQuotaRouter builds a router whose persistent overlay carries the given
// byte quota. freeRatio 0 disables the free-space guard so the tests can drive
// the byte cap without depending on the host disk.
func newQuotaRouter(t *testing.T, quotaBytes int64) *Localize {
	t.Helper()
	f, err := New(Options{
		LocalRoot:         t.TempDir(),
		LocalOnlyPatterns: []string{"**/node_modules/**"},
		QuotaBytes:        quotaBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func openLocal(t *testing.T, f *Localize, view string) any {
	t.Helper()
	res, err := doOp(t, f, &fs.Op{
		Kind:  fs.OpCreate,
		Path:  view,
		Flags: uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:  0o644,
	})
	if err != nil {
		t.Fatalf("create %s: %v", view, err)
	}
	return res.Handle
}

func writeLocal(t *testing.T, f *Localize, view string, h any, off int64, n int) error {
	t.Helper()
	_, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Handle: h, Offset: off, Data: make([]byte, n)})
	return err
}

func TestOverlayQuotaRefusesWritePastLimit(t *testing.T) {
	f := newQuotaRouter(t, 1000)
	const view = "/repo/node_modules/big.bin"
	h := openLocal(t, f, view)

	if err := writeLocal(t, f, view, h, 0, 600); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 600 {
		t.Fatalf("quota used = %d, want 600", got)
	}
	// Another 600 would exceed the 1000-byte quota: ENOSPC, and the file
	// keeps its committed bytes.
	err := writeLocal(t, f, view, h, 600, 600)
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("over-quota write = %v, want ENOSPC", err)
	}
	info, serr := f.overlay.Lstat(view)
	if serr != nil {
		t.Fatal(serr)
	}
	if info.Size() != 600 {
		t.Fatalf("backing size = %d, want 600 (refused write must not land)", info.Size())
	}
	if got := f.overlay.Quota().Refusals(); got != 1 {
		t.Fatalf("refusals = %d, want 1", got)
	}

	// Truncating to 0 gives the bytes back, so the next write fits.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: view, Size: 0, Handle: h}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 0 {
		t.Fatalf("quota used after truncate = %d, want 0", got)
	}
	if err := writeLocal(t, f, view, h, 0, 900); err != nil {
		t.Fatalf("write after truncate: %v", err)
	}
}

func TestOverlayQuotaSeedsExistingBackingUsage(t *testing.T) {
	root := t.TempDir()
	// An overlay tree that already holds most of the quota (a mount over an
	// existing local-only subtree).
	existing := filepath.Join(root, "overlay", "repo", "node_modules", "old.bin")
	if err := os.MkdirAll(filepath.Dir(existing), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(existing, make([]byte, 900), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{
		LocalRoot:         root,
		LocalOnlyPatterns: []string{"**/node_modules/**"},
		QuotaBytes:        1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.overlay.Quota().Used(); got != 900 {
		t.Fatalf("seeded usage = %d, want 900", got)
	}
	const view = "/repo/node_modules/new.bin"
	h := openLocal(t, f, view)
	if err := writeLocal(t, f, view, h, 0, 200); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("write = %v, want ENOSPC", err)
	}
	if err := writeLocal(t, f, view, h, 0, 100); err != nil {
		t.Fatalf("write within quota: %v", err)
	}
}

func TestOverlayQuotaReleasedOnUnlink(t *testing.T) {
	f := newQuotaRouter(t, 1000)
	const view = "/repo/node_modules/a.bin"
	h := openLocal(t, f, view)
	if err := writeLocal(t, f, view, h, 0, 800); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRelease, Path: view, Handle: h}); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpUnlink, Path: view}); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 0 {
		t.Fatalf("quota used after unlink = %d, want 0", got)
	}
}

func TestOverlayFreeRatioRefusesWhenHostIsFull(t *testing.T) {
	f := newQuotaRouter(t, 0)
	if f.overlay.Quota() != nil {
		t.Fatal("quota built without bounds")
	}
	// Rebuild with only the free-space guard, then drive capacity from the
	// test (the host disk's real capacity is irrelevant here).
	if err := f.overlay.SetQuota(0, defaultOverlayFreeRatio); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	if q == nil {
		t.Fatal("quota not installed")
	}
	q.statfs = func(string) (int64, int64, error) { return 1000, 50, nil }

	const view = "/repo/node_modules/broken.bin"
	h := openLocal(t, f, view)
	err := writeLocal(t, f, view, h, 0, 100)
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("write below the free-space floor = %v, want ENOSPC", err)
	}
	// 50 free of 1000 is below the 10% floor, but a write that keeps the
	// floor is allowed.
	q.statfs = func(string) (int64, int64, error) { return 1000, 500, nil }
	q.mu.Lock()
	q.lastCheck = 0
	q.mu.Unlock()
	if err := writeLocal(t, f, view, h, 0, 100); err != nil {
		t.Fatalf("write above the free-space floor: %v", err)
	}
}

func TestOverlayQuotaDisabledIsUnbounded(t *testing.T) {
	f := newTestRouter(t, []string{"**/node_modules/**"}, nil)
	if f.overlay.Quota() != nil {
		t.Fatal("quota enabled without configuration")
	}
	const view = "/repo/node_modules/plain.bin"
	h := openLocal(t, f, view)
	if err := writeLocal(t, f, view, h, 0, 1<<20); err != nil {
		t.Fatalf("write without quota: %v", err)
	}
}

// TestOverlayQuotaOverwriteDoesNotReleaseBytes pins the write-vs-truncate
// distinction: an in-place overwrite through a handle whose cached size is
// stale must not return the file's still-allocated bytes to the quota.
func TestOverlayQuotaOverwriteDoesNotReleaseBytes(t *testing.T) {
	f := newQuotaRouter(t, 1000)
	const view = "/repo/node_modules/shared.bin"
	a := openLocal(t, f, view)
	b := openLocal(t, f, view) // opened before A writes: its cached size is 0
	if err := writeLocal(t, f, view, a, 0, 1000); err != nil {
		t.Fatalf("grow write: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 1000 {
		t.Fatalf("quota used = %d, want 1000", got)
	}
	// B overwrites the first 10 bytes: the file stays 1000 bytes long.
	if err := writeLocal(t, f, view, b, 0, 10); err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 1000 {
		t.Fatalf("quota used after in-place overwrite = %d, want 1000", got)
	}
}

// TestOverlayQuotaPathWriteRefusalKeepsExistingImage pins the ordering fix: a
// path-addressed write that the quota refuses must not have truncated the file
// first.
func TestOverlayQuotaPathWriteRefusalKeepsExistingImage(t *testing.T) {
	f := newQuotaRouter(t, 1000)
	const view = "/repo/node_modules/keep.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 600)}); err != nil {
		t.Fatalf("seed write: %v", err)
	}
	// A 1200-byte replacement does not fit: the call must fail without
	// destroying the 600 committed bytes.
	_, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 1200)})
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("replacement write = %v, want ENOSPC", err)
	}
	info, serr := f.overlay.Lstat(view)
	if serr != nil {
		t.Fatal(serr)
	}
	if info.Size() != 600 {
		t.Fatalf("backing size = %d, want 600 (refused write truncated the file)", info.Size())
	}
	if got := f.overlay.Quota().Used(); got != 600 {
		t.Fatalf("quota used = %d, want 600", got)
	}
}
