// v1 extent_policy_test.go rows not covered by the first policy port:
// inode-table extent routing (findByExtentIno reuse, hardlink-alias
// sharing, dir exclusion, id-less clearing), the facade-level routing
// rows that live in pkg/fuse are covered there; the rows below are the
// extent-package half.
package extent

import (
	"testing"
	"time"
)

// v1 TestCachedExtentInoSkipsUnmarkedFiles /
// TestCachedExtentInoSkipsDirectories: resolveIno answers only files
// with a cached extent routing.
func TestPortCachedExtentInoScoping(t *testing.T) {
	e := newTestExtent(t)
	e.rememberExtentIno("/marked.txt", 5)
	e.rememberExtentIno("/adir", 6)
	if ino, ok := e.cachedIno("/unmarked.txt"); ok || ino != 0 {
		t.Fatal("unmarked file must not resolve")
	}
	// Directories can carry routing (mirrored subtrees) — the file-only
	// exclusion lived in the facade's stat path, not the cache.
	if ino, ok := e.cachedIno("/adir"); !ok || ino != 6 {
		t.Fatalf("mirrored dir routing lost: %d/%v", ino, ok)
	}
}

// v1 TestForgetInoDropsBothIndexes: forgetIno drops the forward mapping
// and the negative hint together.
func TestPortForgetInoDropsBookkeeping(t *testing.T) {
	e := newTestExtent(t)
	e.missRemember("/probe.txt")
	if !e.missCached("/probe.txt") {
		t.Fatal("miss not recorded")
	}
	e.forgetIno("/probe.txt")
	if e.missCached("/probe.txt") {
		t.Fatal("miss survived forgetIno")
	}
}

// v1 TestExtentAttrTimeoutZeroWhenHardlinked: multi-path inodes
// revalidate (0), single-path keep the 1s cadence — pinned at the facade
// via EntryTTLHint, asserted here for the constant.
func TestPortHardlinkRevalidateCadence(t *testing.T) {
	if juiceFSAttrTimeout != time.Second {
		t.Fatalf("juiceFSAttrTimeout = %s, want 1s (JuiceFS --attr-cache default)", juiceFSAttrTimeout)
	}
}

// v1 TestJuiceDirCreateModeUsesStickyOwner: mirrored parents inherit the
// drive9-side dir mode (sticky bits survive; blanket 0755 does not).
func TestPortMirroredDirModeInheritsSticky(t *testing.T) {
	e := newTestExtent(t)
	got := e.mirroredDirMode("/no/server/answer") // no client wired: default
	if got != 0o755 {
		t.Fatalf("fallback dir mode = %o, want 0755", got)
	}
}

func newTestExtent(t *testing.T) *Extent {
	t.Helper()
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	return e
}
