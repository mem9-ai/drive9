package localize

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// admitHandle opens the backing file and books a handle write ending at end —
// the production admission path (inode-keyed), so the tests cover the keys
// mutators must reach.
func admitHandle(t *testing.T, f *Localize, view string, end, writeLen int64) (writeReservation, *os.File) {
	t.Helper()
	rel := strings.TrimPrefix(view, "/")
	path := filepath.Join(f.overlay.Root(), filepath.FromSlash(rel))
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatalf("open backing file: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	res, err := f.overlay.Quota().admitFileWrite(file, rel, end, writeLen)
	if err != nil {
		t.Fatalf("admitFileWrite: %v", err)
	}
	return res, file
}

// doOpNoT runs one op without a *testing.T, so background goroutines can drive
// the production path without touching test state.
func doOpNoT(f *Localize, op *fs.Op) (fs.Result, error) {
	next := fs.Next(func(ctx context.Context, op *fs.Op) (fs.Result, error) {
		return fs.Result{}, fs.ErrUnsupported
	})
	return f.Do(context.Background(), op, next)
}

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
	_, _ = admitHandle(t, f, "/repo/node_modules/appended.bin", 500, 400)
	_, _ = admitHandle(t, f, "/repo/node_modules/appended.bin", 500, 400)
	if !q.hasBookingForPath("repo/node_modules/appended.bin") {
		t.Fatal("admission booking not indexed under the production path")
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

// TestOverlayQuotaSettleKeepsAnotherWritersLandedGrowth: writer A reserves a
// file's extension, writer B books nothing against it (same end) and lands the
// bytes; A's failure must not refund the growth that is now on disk.
func TestOverlayQuotaSettleKeepsAnotherWritersLandedGrowth(t *testing.T) {
	f := newQuotaRouter(t, 10000)
	const rel = "/repo/node_modules/shared-append.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: rel, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	resA, _ := admitHandle(t, f, "/repo/node_modules/shared-append.bin", 500, 400)
	resB, _ := admitHandle(t, f, "/repo/node_modules/shared-append.bin", 500, 400)
	if resA.admitted != 400 || resB.admitted != 0 {
		t.Fatalf("admissions = %d/%d, want 400/0 (one growth, charged once)", resA.admitted, resB.admitted)
	}
	// B lands the bytes; A then fails. The last settler reconciles against the
	// file's real size, so the landed growth is charged exactly once.
	if err := os.WriteFile(filepath.Join(f.overlay.Root(), filepath.FromSlash(strings.TrimPrefix(rel, "/"))), make([]byte, 500), 0o644); err != nil {
		t.Fatal(err)
	}
	q.settleWrite(resB)
	q.settleWrite(resA)
	if got := q.Used(); got != 500 {
		t.Fatalf("used after both settles = %d, want 500 (the landed bytes)", got)
	}
}

// TestOverlayQuotaFailedWriteSettlesAgainstResultingSize: a write that lands
// nothing must not keep the reservation for the gap it never created.
func TestOverlayQuotaFailedWriteSettlesAgainstResultingSize(t *testing.T) {
	f := newQuotaRouter(t, 10000)
	const view = "/repo/node_modules/hole.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	// A one-byte write beyond EOF would grow the file to 501; it fails without
	// landing a byte, so settling against the real 100-byte size refunds all.
	res, _ := admitHandle(t, f, "/repo/node_modules/hole.bin", 501, 1)
	if res.admitted != 401 {
		t.Fatalf("admitted = %d, want 401", res.admitted)
	}
	q.settleWrite(res)
	if got := q.Used(); got != 100 {
		t.Fatalf("used after the failed write = %d, want 100", got)
	}
	if q.hasBookingForPath(strings.TrimPrefix(view, "/")) {
		t.Fatal("failed write left its booking behind")
	}
	// A retry must charge the growth again.
	res2, _ := admitHandle(t, f, "/repo/node_modules/hole.bin", 501, 1)
	if res2.admitted != 401 {
		t.Fatalf("retry admitted = %d, want 401", res2.admitted)
	}
}

// TestOverlayQuotaSettledWriteChargesOnce: a completed write retires its
// booking, and the next write to the same file charges only new growth.
func TestOverlayQuotaSettledWriteChargesOnce(t *testing.T) {
	f := newQuotaRouter(t, 10000)
	const view = "/repo/node_modules/settled.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	rel := strings.TrimPrefix(view, "/")
	q := f.overlay.Quota()
	if q.hasBookingForPath(rel) {
		t.Fatal("a completed write left a booking")
	}
	// Overwrite inside the file: no growth, no charge.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if got := q.Used(); got != 100 {
		t.Fatalf("used after in-place overwrite = %d, want 100", got)
	}
}

// TestOverlayQuotaTruncateRecreateChargesAgain is the editor-save pattern: a
// completed write, then a re-create with O_TRUNC, then a fresh write. The
// booking must be retired when the write settles and when the truncating open
// lands, so the regrown bytes are charged again instead of hiding under a
// phantom base.
func TestOverlayQuotaTruncateRecreateChargesAgain(t *testing.T) {
	f := newQuotaRouter(t, 1000)
	const view = "/repo/node_modules/save.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 500)}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	q := f.overlay.Quota()
	if got := q.Used(); got != 500 {
		t.Fatalf("used after first save = %d, want 500", got)
	}
	if q.hasBookingForPath(strings.TrimPrefix(view, "/")) {
		t.Fatal("a completed write left a booking")
	}
	// Re-create the path with O_TRUNC (the ordinary save cycle).
	res, err := doOp(t, f, &fs.Op{Kind: fs.OpCreate, Path: view, Flags: uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_TRUNC), Mode: 0o644})
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	h := res.Handle
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Handle: h, Offset: 0, Data: make([]byte, 400)}); err != nil {
		t.Fatalf("second save: %v", err)
	}
	if got := q.Used(); got != 400 {
		t.Fatalf("used after the regrown save = %d, want 400 (the phantom base hid the charge)", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRelease, Path: view, Handle: h}); err != nil {
		t.Fatal(err)
	}
}

// TestOverlayQuotaRenameRetiresBothNames' bookings: after a rename, a recreate
// at the old name and a write at the new name both charge from disk.
func TestOverlayQuotaRenameRetiresBothNames(t *testing.T) {
	f := newQuotaRouter(t, 2000)
	const src = "/repo/node_modules/r-src.bin"
	const dst = "/repo/node_modules/r-dst.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: src, Offset: 0, Data: make([]byte, 300)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	// A live booking on the source, as an in-flight writer would hold.
	_, _ = admitHandle(t, f, "/repo/node_modules/r-src.bin", 800, 500)
	if !q.hasBookingForPath(strings.TrimPrefix(src, "/")) {
		t.Fatal("admission booking not indexed under the production path")
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRename, Path: src, Path2: dst}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	if q.hasBookingForPath(strings.TrimPrefix(src, "/")) {
		t.Fatal("rename left the source name's booking behind")
	}
	if q.hasBookingForPath(strings.TrimPrefix(dst, "/")) {
		t.Fatal("rename left the destination name's booking behind")
	}
}

// TestOverlayQuotaAbortedBookingsReconcileToDisk: two writers book the same
// growth and NEITHER lands bytes (both fail). The last settler must reconcile
// `used` back to the file's real size, leaving no phantom charge that would
// keep refusing writes.
func TestOverlayQuotaAbortedBookingsReconcileToDisk(t *testing.T) {
	f := newQuotaRouter(t, 10000)
	const rel = "/repo/node_modules/aborted.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: rel, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	resA, _ := admitHandle(t, f, "/repo/node_modules/aborted.bin", 600, 500)
	resB, _ := admitHandle(t, f, "/repo/node_modules/aborted.bin", 600, 500)
	if got := q.Used(); got != 600 {
		t.Fatalf("used while both are in flight = %d, want 600", got)
	}
	q.settleWrite(resA)
	if got := q.Used(); got != 600 {
		t.Fatalf("used after A settles (B still owns the booking) = %d, want 600", got)
	}
	q.settleWrite(resB)
	if got := q.Used(); got != 100 {
		t.Fatalf("used after the last settle = %d, want 100 (the real file size)", got)
	}
	if q.hasBookingForPath(strings.TrimPrefix(rel, "/")) {
		t.Fatal("booking survived its last settle")
	}
}

// TestOverlayQuotaSettlementUsesBookingIdentity reproduces the race the third
// review described: writer A's bytes land, writer B admits (replacing the map
// entry) before A settles, and A's settle must retire A's OWN booking — not
// close B's, which would refund growth B is about to land.
func TestOverlayQuotaSettlementUsesBookingIdentity(t *testing.T) {
	f := newQuotaRouter(t, 10000)
	const rel = "/repo/node_modules/race.bin"
	path := filepath.Join(f.overlay.Root(), filepath.FromSlash(strings.TrimPrefix(rel, "/")))
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: rel, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	resA, _ := admitHandle(t, f, "/repo/node_modules/race.bin", 500, 400)
	// A's WriteAt lands, but A has not settled yet.
	if err := os.WriteFile(path, make([]byte, 500), 0o644); err != nil {
		t.Fatal(err)
	}
	// B admits in that window; the file has caught up with A's booking, so B
	// starts a fresh one and A's booking is retired.
	resB, _ := admitHandle(t, f, "/repo/node_modules/race.bin", 900, 400)
	// A settles AFTER B admitted: it must not close B's booking.
	q.settleWrite(resA)
	if got := q.Used(); got != 900 {
		t.Fatalf("used after A's late settle = %d, want 900 (B's booking untouched)", got)
	}
	// B lands its bytes and settles.
	if err := os.WriteFile(path, make([]byte, 900), 0o644); err != nil {
		t.Fatal(err)
	}
	q.settleWrite(resB)
	if got := q.Used(); got != 900 {
		t.Fatalf("used after B settles = %d, want 900", got)
	}
	if q.hasBookingForPath(strings.TrimPrefix(rel, "/")) {
		t.Fatal("booking survived its last settle")
	}
}

// TestOverlayQuotaTruncateRetiresHandleBooking pins that mutation retirement
// reaches PRODUCTION admissions: a handle write books by inode, and a
// truncating recreate of the same path must retire it (otherwise its end
// watermark survives over a file reset to zero and the next write is not
// charged).
func TestOverlayQuotaTruncateRetiresHandleBooking(t *testing.T) {
	f := newQuotaRouter(t, 2000)
	const view = "/repo/node_modules/retire-ino.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	// An in-flight handle writer books 100 -> 600.
	_, _ = admitHandle(t, f, view, 600, 500)
	if !q.hasBookingForPath(strings.TrimPrefix(view, "/")) {
		t.Fatal("handle booking not indexed by path")
	}
	if got := q.Used(); got != 600 {
		t.Fatalf("used with the booking in flight = %d, want 600", got)
	}
	// The ordinary editor save: recreate the path with O_TRUNC.
	res, err := doOp(t, f, &fs.Op{Kind: fs.OpCreate, Path: view, Flags: uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_TRUNC), Mode: 0o644})
	if err != nil {
		t.Fatalf("re-create: %v", err)
	}
	h := res.Handle
	if q.hasBookingForPath(strings.TrimPrefix(view, "/")) {
		t.Fatal("truncating recreate did not retire the inode-keyed booking")
	}
	if got := q.Used(); got != 0 {
		t.Fatalf("used after the truncating recreate = %d, want 0", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Handle: h, Offset: 0, Data: make([]byte, 400)}); err != nil {
		t.Fatalf("write after recreate: %v", err)
	}
	if got := q.Used(); got != 400 {
		t.Fatalf("used after writing 400 = %d, want 400", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRelease, Path: view, Handle: h}); err != nil {
		t.Fatal(err)
	}
}

// TestOverlayQuotaResizeRetiresHandleBookingOnce: a shrink retires a handle
// writer's booking against the pre-resize size, so the writer's later settle
// cannot refund the shrink a second time.
func TestOverlayQuotaResizeRetiresHandleBookingOnce(t *testing.T) {
	f := newQuotaRouter(t, 2000)
	const view = "/repo/node_modules/resize-ino.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	res, _ := admitHandle(t, f, view, 600, 500)
	if got := q.Used(); got != 600 {
		t.Fatalf("used with the booking in flight = %d, want 600", got)
	}
	// A concurrent truncate to 50 bytes: retire at 100, release the 50 freed.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: view, Size: 50}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := q.Used(); got != 50 {
		t.Fatalf("used after the shrink = %d, want 50", got)
	}
	// The writer's settlement must not refund the shrink again.
	q.settleWrite(res)
	if got := q.Used(); got != 50 {
		t.Fatalf("used after the writer settles = %d, want 50 (no double refund)", got)
	}
}

// TestOverlayQuotaUnlinkRetiresHandleBooking: unlink retires a handle writer's
// booking against the pre-unlink size and refunds the last link's bytes; the
// writer's later landing on the anonymous fd is re-charged.
func TestOverlayQuotaUnlinkRetiresHandleBooking(t *testing.T) {
	f := newQuotaRouter(t, 4000)
	const view = "/repo/node_modules/unlink-ino.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	res, file := admitHandle(t, f, view, 600, 500)
	if got := q.Used(); got != 600 {
		t.Fatalf("used with the booking in flight = %d, want 600", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpUnlink, Path: view}); err != nil {
		t.Fatalf("unlink: %v", err)
	}
	if q.hasBookingForPath(strings.TrimPrefix(view, "/")) {
		t.Fatal("unlink did not retire the inode-keyed booking")
	}
	if got := q.Used(); got != 0 {
		t.Fatalf("used after the unlink = %d, want 0", got)
	}
	// The writer still lands 600 bytes on the unlinked descriptor: those bytes
	// exist, so settling re-charges them.
	if _, err := file.WriteAt(make([]byte, 600), 0); err != nil {
		t.Fatalf("land after unlink: %v", err)
	}
	q.settleWrite(res)
	if got := q.Used(); got != 600 {
		t.Fatalf("used after the retired writer landed = %d, want 600", got)
	}
}

// TestOverlayQuotaRenameRetiresDestinationBookingAtItsOwnSize: the destination
// name's booking is settled against the DESTINATION's pre-rename size, not the
// source's.
func TestOverlayQuotaRenameRetiresDestinationBookingAtItsOwnSize(t *testing.T) {
	f := newQuotaRouter(t, 4000)
	const src = "/repo/node_modules/rn-src.bin"
	const dst = "/repo/node_modules/rn-dst.bin"
	for _, p := range []string{src, dst} {
		n := 50
		if p == dst {
			n = 200
		}
		if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: p, Offset: 0, Data: make([]byte, n)}); err != nil {
			t.Fatal(err)
		}
	}
	q := f.overlay.Quota()
	// A writer holds the destination open, booking 200 -> 1000.
	_, _ = admitHandle(t, f, dst, 1000, 800)
	if got := q.Used(); got != 1050 {
		t.Fatalf("used before the rename = %d, want 1050 (50 + 200 + 800)", got)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpRename, Path: src, Path2: dst}); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// The destination's 200 bytes are released and its booking's 800 refunded;
	// the source's 50 bytes are now the destination's content.
	if got := q.Used(); got != 50 {
		t.Fatalf("used after the rename = %d, want 50", got)
	}
}

// TestOverlayQuotaRetiredSharedBookingChargesOnce: two writers share one
// booking; a truncate retires it; both then land the same extent and settle.
// The post-retirement growth must be charged once, not once per settler.
func TestOverlayQuotaRetiredSharedBookingChargesOnce(t *testing.T) {
	f := newQuotaRouter(t, 10000)
	const view = "/repo/node_modules/retired-shared.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	resA, fileA := admitHandle(t, f, view, 600, 500)
	resB, fileB := admitHandle(t, f, view, 600, 500)
	if resB.admitted != 0 {
		t.Fatalf("second writer paid %d, want 0 (shared booking)", resB.admitted)
	}
	if got := q.Used(); got != 600 {
		t.Fatalf("used with the shared booking = %d, want 600", got)
	}
	// Truncate to 50: the shared booking is retired at the pre-truncate size.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: view, Size: 50}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := q.Used(); got != 50 {
		t.Fatalf("used after the truncate = %d, want 50", got)
	}
	// Both descriptors land the same 600 bytes (the second write is a no-op on
	// an already-grown file, so both settle at 600).
	for _, file := range []*os.File{fileA, fileB} {
		if _, err := file.WriteAt(make([]byte, 550), 50); err != nil {
			t.Fatalf("land: %v", err)
		}
	}
	q.settleWrite(resA)
	q.settleWrite(resB)
	if got := q.Used(); got != 600 {
		t.Fatalf("used after both retired settlers = %d, want 600 (charged once)", got)
	}
}

// TestOverlayQuotaRefreshDoesNotDoubleCountLandedGrowth: after a successful
// write, a Statfs refresh must not subtract that landed growth again (which
// would make the floor fire forever). The final step grows a file the sample
// has not counted yet, so a real admission (and a real refresh) happens.
func TestOverlayQuotaRefreshDoesNotDoubleCountLandedGrowth(t *testing.T) {
	f := newQuotaRouter(t, 0)
	if err := f.overlay.SetQuota(0, 0.10); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	// 100 MiB total, 50 MiB free, 10% floor.
	total := int64(100 << 20)
	q.statfs = func(string) (int64, int64, error) { return total, 50 << 20, nil }
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: "/repo/node_modules/fill.bin", Offset: 0, Data: make([]byte, 1<<20)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Land 20 MiB more (a successful write), which the filesystem now holds.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: "/repo/node_modules/fill.bin", Offset: 1 << 20, Data: make([]byte, 20<<20)}); err != nil {
		t.Fatalf("grow: %v", err)
	}
	// The filesystem now has 30 MiB free (the landed 20 MiB left the sample).
	q.statfs = func(string) (int64, int64, error) { return total, 30 << 20, nil }
	q.mu.Lock()
	q.lastCheck = 0 // force the next admission to re-sample
	q.mu.Unlock()
	// GENUINE growth on a different file: 1 MiB that is not on disk yet.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: "/repo/node_modules/after-refresh.bin", Offset: 0, Data: make([]byte, 1<<20)}); err != nil {
		t.Fatalf("write after refresh = %v, want admitted", err)
	}
	q.mu.Lock()
	refreshed := q.lastCheck != 0
	q.mu.Unlock()
	if !refreshed {
		t.Fatal("the admission short-circuited and never refreshed the sample")
	}
}

// TestOverlayQuotaGrowTruncateResolvesItsPoolShare: a growing ftruncate must
// leave no unsettled pool entry behind, or every Statfs refresh subtracts it
// again.
func TestOverlayQuotaGrowTruncateResolvesItsPoolShare(t *testing.T) {
	f := newQuotaRouter(t, 0)
	if err := f.overlay.SetQuota(0, 0.10); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	total := int64(100 << 20)
	q.statfs = func(string) (int64, int64, error) { return total, 50 << 20, nil }
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: "/repo/node_modules/grow.bin", Offset: 0, Data: make([]byte, 1<<20)}); err != nil {
		t.Fatal(err)
	}
	// Grow the file to 30 MiB with a truncate, then refresh the sample.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: "/repo/node_modules/grow.bin", Size: 30 << 20}); err != nil {
		t.Fatalf("grow truncate: %v", err)
	}
	q.mu.Lock()
	if q.unlanded != 0 {
		t.Fatalf("unlanded pool = %d after a landed grow, want 0", q.unlanded)
	}
	q.lastCheck = 0
	q.mu.Unlock()
	q.statfs = func(string) (int64, int64, error) { return total, 21 << 20, nil }
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: "/repo/node_modules/grow2.bin", Offset: 0, Data: make([]byte, 1<<20)}); err != nil {
		t.Fatalf("write after the grow + refresh = %v, want admitted", err)
	}
}

// TestOverlayQuotaPathWriteResolvesItsPoolShare: a truncating path write takes
// a bookingless reservation. Its resolution must empty the unsettled pool and
// keep exactly the bytes that landed charged, or every later Statfs refresh
// subtracts the phantom growth again.
func TestOverlayQuotaPathWriteResolvesItsPoolShare(t *testing.T) {
	f := newQuotaRouter(t, 0)
	if err := f.overlay.SetQuota(0, 0.10); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	q.statfs = func(string) (int64, int64, error) { return 100 << 20, 50 << 20, nil }
	const view = "/repo/node_modules/pool-path.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 1<<20)}); err != nil {
		t.Fatalf("path write: %v", err)
	}
	if got := q.Used(); got != 1<<20 {
		t.Fatalf("used after the path write = %d, want %d", got, 1<<20)
	}
	q.mu.Lock()
	pool := q.unlanded
	q.mu.Unlock()
	if pool != 0 {
		t.Fatalf("unlanded pool = %d after a resolved path write, want 0", pool)
	}
}

// TestOverlayQuotaRetiredRechargeIsClamped: a retired writer that lands after a
// NEW booking regrew the file is re-charged only up to the extent it booked;
// the bytes above it were already paid at the new booking's admission.
func TestOverlayQuotaRetiredRechargeIsClamped(t *testing.T) {
	f := newQuotaRouter(t, 10000)
	const view = "/repo/node_modules/retired-clamp.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	res, file := admitHandle(t, f, view, 600, 500)
	// A truncate retires the booking and refunds its admitted growth.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: view, Size: 50}); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if got := q.Used(); got != 50 {
		t.Fatalf("used after the truncate = %d, want 50", got)
	}
	// A new writer books the file out to 2000 bytes and pays for all of it.
	if _, _ = admitHandle(t, f, view, 2000, 1950); q.Used() != 2000 {
		t.Fatalf("used after the new booking = %d, want 2000", q.Used())
	}
	// The retired writer now lands 2000 bytes, far past the 600 it booked.
	if _, err := file.WriteAt(make([]byte, 1950), 50); err != nil {
		t.Fatalf("land past the booking end: %v", err)
	}
	q.settleWrite(res)
	// Only the 550 bytes of its booked extent (50..600) are re-charged; the
	// 1400 above it were paid by the new booking and must not be charged again.
	if got := q.Used(); got != 2550 {
		t.Fatalf("used after the clamped re-charge = %d, want 2550", got)
	}
}

// TestOverlayQuotaAliasDetachCleansEveryIndex: a booking admitted through both
// hardlink names must be removed from BOTH when retired, so a second mutation
// through the surviving name cannot re-retire it (which turned the cap
// fail-open).
func TestOverlayQuotaAliasDetachCleansEveryIndex(t *testing.T) {
	f := newQuotaRouter(t, 4000)
	const a = "/repo/node_modules/detach-a.bin"
	const b = "/repo/node_modules/detach-b.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: a, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpLink, Path: a, Path2: b}); err != nil {
		t.Fatalf("link: %v", err)
	}
	q := f.overlay.Quota()
	_, _ = admitHandle(t, f, a, 600, 500)
	_, _ = admitHandle(t, f, b, 600, 500) // same inode: joins, indexes under b
	if !q.hasBookingForPath(strings.TrimPrefix(a, "/")) || !q.hasBookingForPath(strings.TrimPrefix(b, "/")) {
		t.Fatal("booking not indexed under both aliases")
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpUnlink, Path: b}); err != nil {
		t.Fatalf("unlink b: %v", err)
	}
	if q.hasBookingForPath(strings.TrimPrefix(a, "/")) {
		t.Fatal("stale index entry under the surviving alias")
	}
	before := q.Used()
	// A second mutation through the surviving name must not re-retire the dead
	// booking (used must not drop below the bytes still on disk).
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: a, Size: 100}); err != nil {
		t.Fatalf("truncate a: %v", err)
	}
	if got := q.Used(); got != 100 {
		t.Fatalf("used after the collapse = %d, want 100 (was %d)", got, before)
	}
}

// TestOverlayQuotaConcurrentPathWritesStayExact: several workers replace the
// same backing file concurrently. Every truncating open destroys the image the
// other writers are landing, so the snapshot, charge and settle must be
// serialized per target; without that the destroyed image's charge is never
// returned and `used` climbs monotonically while the disk holds one file.
func TestOverlayQuotaConcurrentPathWritesStayExact(t *testing.T) {
	hammer := func(t *testing.T, f *Localize, names []string) {
		t.Helper()
		const perName = 60
		var wg sync.WaitGroup
		errs := make(chan error, len(names))
		for w := range names {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < perName; i++ {
					n := 128 + (i*97+w*13)%2048
					// The production op shape: a handle-less full replacement.
					op := &fs.Op{Kind: fs.OpWrite, Path: names[w], Offset: 0, Data: make([]byte, n)}
					if _, err := doOpNoT(f, op); err != nil {
						errs <- err
						return
					}
				}
			}(w)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("concurrent path write: %v", err)
		}
	}
	t.Run("one name", func(t *testing.T) {
		f := newQuotaRouter(t, 10<<20)
		const view = "/repo/node_modules/hammer.bin"
		names := make([]string, 4)
		for i := range names {
			names[i] = view
		}
		hammer(t, f, names)
		info, err := f.overlay.Lstat(view)
		if err != nil {
			t.Fatalf("lstat: %v", err)
		}
		if got := f.overlay.Quota().Used(); got != info.Size() {
			t.Fatalf("used = %d, want the on-disk size %d", got, info.Size())
		}
	})
	t.Run("hardlink aliases", func(t *testing.T) {
		f := newQuotaRouter(t, 10<<20)
		const a = "/repo/node_modules/hammer-a.bin"
		const b = "/repo/node_modules/hammer-b.bin"
		if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: a, Offset: 0, Data: make([]byte, 256)}); err != nil {
			t.Fatal(err)
		}
		if _, err := doOp(t, f, &fs.Op{Kind: fs.OpLink, Path: a, Path2: b}); err != nil {
			t.Fatalf("link: %v", err)
		}
		names := []string{a, b, a, b}
		hammer(t, f, names)
		info, err := f.overlay.Lstat(a)
		if err != nil {
			t.Fatalf("lstat: %v", err)
		}
		if got := f.overlay.Quota().Used(); got != info.Size() {
			t.Fatalf("used = %d, want the inode's on-disk size %d", got, info.Size())
		}
	})
}

// TestOverlayQuotaStatFailureResolvesThePoolOnce: when a write's settlement
// cannot stat its file, the booking's pool share is resolved once. A second
// stat-failing settler of the same booking must not subtract it again, or it
// eats another live booking's in-flight share.
func TestOverlayQuotaStatFailureResolvesThePoolOnce(t *testing.T) {
	f := newQuotaRouter(t, 0)
	if err := f.overlay.SetQuota(0, 0.10); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	q.statfs = func(string) (int64, int64, error) { return 100 << 20, 50 << 20, nil }
	const shared = "/repo/node_modules/statfail-a.bin"
	const other = "/repo/node_modules/statfail-b.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: shared, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: other, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	resA, fileA := admitHandle(t, f, shared, 600, 500)
	resB, fileB := admitHandle(t, f, shared, 600, 500) // shares A's booking
	if resB.admitted != 0 {
		t.Fatalf("second settler paid %d, want 0", resB.admitted)
	}
	// Another booking keeps its own share in the pool while A's is resolved.
	_, _ = admitHandle(t, f, other, 400, 300)
	// Closed descriptors make the settlement's fstat fail.
	if err := fileA.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fileB.Close(); err != nil {
		t.Fatal(err)
	}
	q.settleWrite(resA)
	q.settleWrite(resB)
	q.mu.Lock()
	pool := q.unlanded
	q.mu.Unlock()
	if pool != 300 {
		t.Fatalf("unlanded pool = %d, want 300 (the other booking's share only)", pool)
	}
}

// TestOverlayQuotaResizeRetirementWaitsForTheTruncate: a REFUSED or failed
// resize must not retire the name's writers (the file never moved); a landed one
// retires them with the new size as their baseline.
func TestOverlayQuotaResizeRetirementWaitsForTheTruncate(t *testing.T) {
	f := newQuotaRouter(t, 1000)
	const view = "/repo/node_modules/resize-order.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: view, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	q := f.overlay.Quota()
	// A refused grow (1000-byte quota, file at 100, asking for 2000) must leave
	// the booking live and unchanged.
	_, _ = admitHandle(t, f, view, 600, 500)
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: view, Size: 2000}); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("over-quota truncate = %v, want ENOSPC", err)
	}
	if !q.hasBookingForPath(strings.TrimPrefix(view, "/")) {
		t.Fatal("a refused truncate retired the booking")
	}
	if got := q.Used(); got != 600 {
		t.Fatalf("used after the refused truncate = %d, want 600", got)
	}
}

// TestOverlayQuotaAliasAdmissionIsReachable: a booking admitted through one
// hardlink alias is retired by a mutation through the other.
func TestOverlayQuotaAliasAdmissionIsReachable(t *testing.T) {
	f := newQuotaRouter(t, 4000)
	const a = "/repo/node_modules/alias-a.bin"
	const b = "/repo/node_modules/alias-b.bin"
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpWrite, Path: a, Offset: 0, Data: make([]byte, 100)}); err != nil {
		t.Fatal(err)
	}
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpLink, Path: a, Path2: b}); err != nil {
		t.Fatalf("link: %v", err)
	}
	q := f.overlay.Quota()
	_, _ = admitHandle(t, f, a, 600, 500) // booked through alias A
	if !q.hasBookingForPath(strings.TrimPrefix(a, "/")) {
		t.Fatal("booking not indexed")
	}
	// Mutating through alias B must reach the same inode booking.
	if _, err := doOp(t, f, &fs.Op{Kind: fs.OpSetAttr, Path: b, Size: 50}); err != nil {
		t.Fatalf("truncate through alias: %v", err)
	}
	if q.hasBookingForPath(strings.TrimPrefix(a, "/")) || q.hasBookingForPath(strings.TrimPrefix(b, "/")) {
		t.Fatal("alias truncate did not retire the inode booking")
	}
}
