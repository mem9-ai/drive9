// v1 dat9fs sticky-rename variants: the caller owns neither the sticky
// parent nor the file (EPERM), owns the file (allowed), and the
// non-owner directory rename across writable parents (allowed). These
// complement renameguard_test.go's sticky row with the ownership matrix.
package renameguard

import (
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
)

// v1 TestRenameRejectsStickySourceParentWhenCallerOwnsNeitherParentNorSource:
// the sticky SOURCE parent refuses a caller owning neither the parent nor
// the file (the existing suite covers same-parent moves; this is the
// cross-parent shape).
func TestPortStickySourceParentEPERMWhenCallerOwnsNothing(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/sticky", 0o777|0o1000)
	b.SeedDir("/dst", 0o777)
	b.Seed("/sticky/file", []byte("x"))
	ch := newStack(t, b)
	// Caller 65534 owns neither the sticky parent (mount uid 1000), nor
	// /dst, nor the file.
	if err := rename(ch, "/sticky/file", "/dst/file", 65534); !errnoIs(err, syscall.EPERM) {
		t.Fatalf("err = %v, want EPERM", err)
	}
	if n, ok := b.Node("/sticky/file"); !ok || string(n.Data) != "x" {
		t.Fatalf("source disturbed: %v", n)
	}
}

// v1 sticky source-parent companion: the caller OWNS the file — the
// sticky parent must not block its own file's rename out of the tree.
func TestPortStickySourceParentAllowsFileOwner(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/sticky", 0o777|0o1000)
	b.SeedDir("/dst", 0o777)
	b.Seed("/sticky/file", []byte("x"))
	ch := newStack(t, b)
	// Entries resolve to the mount owner (1000): same-uid rename passes.
	if err := rename(ch, "/sticky/file", "/dst/file", 1000); errnoIs(err, syscall.EPERM) {
		t.Fatalf("file-owner rename refused: %v", err)
	}
}

// v1 TestRenameRejectsStickyTargetParentWhenCallerOwnsNeitherParentNorTarget:
// replacing INTO a sticky directory requires owning the parent, the
// victim, or being root.
func TestPortStickyTargetParentEPERMWhenCallerOwnsNothing(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/src", 0o777)
	b.SeedDir("/sticky", 0o777|0o1000)
	b.Seed("/src/file", []byte("x"))
	b.Seed("/sticky/victim", []byte("v"))
	ch := newStack(t, b)
	if err := rename(ch, "/src/file", "/sticky/victim", 65534); !errnoIs(err, syscall.EPERM) {
		t.Fatalf("err = %v, want EPERM", err)
	}
	// The sticky parent's owner (1000) may replace the victim.
	if err := rename(ch, "/src/file", "/sticky/victim", 1000); errnoIs(err, syscall.EPERM) {
		t.Fatalf("parent-owner replace refused: %v", err)
	}
}

// v1 TestRenamePreflightAllowsNonOwnerDirectoryAcrossWritableParents: a
// non-root caller renaming a DIRECTORY it does not own across parents
// both writable-by-all is allowed (no sticky bits involved).
func TestPortNonOwnerDirectoryAcrossWritableParentsAllowed(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/a", 0o777)
	b.SeedDir("/b", 0o777)
	b.SeedDir("/a/dir", 0o755)
	ch := newStack(t, b)
	if err := rename(ch, "/a/dir", "/b/dir", 65534); errnoIs(err, syscall.EPERM) || errnoIs(err, syscall.EACCES) {
		t.Fatalf("non-owner dir rename refused: %v", err)
	}
}
