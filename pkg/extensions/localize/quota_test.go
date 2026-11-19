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

// TestOverlayQuotaRenameOverDestinationReleasesBytes pins POSIX replacement:
// renaming over an existing file frees the replaced bytes, so the quota must
// not keep charging them.
func TestOverlayQuotaRenameOverDestinationReleasesBytes(t *testing.T) {
	f := newQuotaRouter(t, 2000)
	const src = "/repo/node_modules/new.bin"
	const dst = "/repo/node_modules/old.bin"
	for _, p := range []string{src, dst} {
		if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: p, Offset: 0, Data: make([]byte, 600)}); err != nil {
			t.Fatalf("seed %s: %v", p, err)
		}
	}
	if got := f.overlay.Quota().Used(); got != 1200 {
		t.Fatalf("quota used = %d, want 1200", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRename, Path: src, Path2: dst}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 600 {
		t.Fatalf("quota used after replacing rename = %d, want 600", got)
	}
}

// TestOverlayQuotaRenameHardlinkAliasKeepsBytes: a rename between two names of
// the same inode frees nothing (the file keeps its link).
func TestOverlayQuotaRenameHardlinkAliasKeepsBytes(t *testing.T) {
	f := newQuotaRouter(t, 2000)
	const a = "/repo/node_modules/a.bin"
	const b = "/repo/node_modules/b.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: a, Offset: 0, Data: make([]byte, 600)}); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpLink, Path: a, Path2: b}); err != nil {
		t.Fatalf("link: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 600 {
		t.Fatalf("quota used after link = %d, want 600", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRename, Path: a, Path2: b}); err != nil {
		t.Fatalf("rename over alias: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 600 {
		t.Fatalf("quota used after alias rename = %d, want 600", got)
	}
}

// TestOverlayQuotaRenameOverDestinationWithAliasKeepsBytes: renaming over a
// destination that another hardlink still references removes only the name; the
// inode's bytes stay on disk and must stay charged.
func TestOverlayQuotaRenameOverDestinationWithAliasKeepsBytes(t *testing.T) {
	f := newQuotaRouter(t, 3000)
	const src = "/repo/node_modules/src.bin"
	const dst = "/repo/node_modules/dst.bin"
	const alias = "/repo/node_modules/alias.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: src, Offset: 0, Data: make([]byte, 600)}); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: dst, Offset: 0, Data: make([]byte, 600)}); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpLink, Path: dst, Path2: alias}); err != nil {
		t.Fatalf("link: %v", err)
	}
	if got := f.overlay.Quota().Used(); got != 1200 {
		t.Fatalf("quota used = %d, want 1200", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRename, Path: src, Path2: dst}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// src's 600 bytes replaced dst's name, but the alias keeps dst's inode.
	if got := f.overlay.Quota().Used(); got != 1200 {
		t.Fatalf("quota used after alias-protected rename = %d, want 1200", got)
	}
}

// TestOverlayQuotaConcurrentAppendsChargeOncePerGrowth: two handles that append
// to the same overlay file must not each charge the same growth. The quota
// tracks the reserved end per path, so the second admission sees the first
// one's reservation instead of the (not yet visible) on-disk size.
func TestOverlayQuotaConcurrentAppendsChargeOncePerGrowth(t *testing.T) {
	f := newQuotaRouter(t, 4000)
	const view = "/repo/node_modules/appended.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	if got := q.Used(); got != 100 {
		t.Fatalf("used = %d, want 100", got)
	}
	// Two admissions for the same target end, as two open handles appending
	// concurrently would produce before either write reaches the file.
	if _, err := q.admitWrite("/repo/node_modules/appended.bin", 500, 400); err != nil {
		t.Fatalf("first append admission: %v", err)
	}
	if _, err := q.admitWrite("/repo/node_modules/appended.bin", 500, 400); err != nil {
		t.Fatalf("second append admission: %v", err)
	}
	if got := q.Used(); got != 500 {
		t.Fatalf("used after two admissions of the same growth = %d, want 500", got)
	}
}

// TestOverlayQuotaInPlaceOverwriteKeepsFreeEstimate: an overwrite grows nothing
// and must not drain the free-space estimate for the throttle window.
func TestOverlayQuotaInPlaceOverwriteKeepsFreeEstimate(t *testing.T) {
	f := newQuotaRouter(t, 0)
	if err := f.overlay.SetQuota(0, 0.10); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	q.statfs = func(string) (int64, int64, error) { return 1000, 500, nil }
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: "/repo/node_modules/ow.bin", Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	q.mu.Lock()
	before := q.cachedFree
	q.mu.Unlock()
	for i := 0; i < 5; i++ {
		if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: "/repo/node_modules/ow.bin", Offset: 0, Data: make([]byte, 100)}); err != nil {
			t.Fatalf("overwrite %d: %v", i, err)
		}
	}
	q.mu.Lock()
	after := q.cachedFree
	q.mu.Unlock()
	if after != before {
		t.Fatalf("in-place overwrites drained the free estimate: %d -> %d", before, after)
	}
}
