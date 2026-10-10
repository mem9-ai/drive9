package renameguard

import (
	"context"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// newStack builds [renameguard → core] over the in-memory fstest backend.
func newStack(t *testing.T, b *testbackend.Backend) *fs.Chain {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	g, err := New(Options{Uid: 1000, Gid: 1000})
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, terminal)
	if err != nil {
		t.Fatal(err)
	}
	return ch
}

func rename(ch *fs.Chain, oldP, newP string, uid uint32) error {
	_, err := ch.Dispatch(context.Background(), &fs.Op{
		Kind: fs.OpRename, Path: oldP, Path2: newP,
		Caller: fs.Caller{Uid: uid, Gid: 1000},
	})
	return err
}

func errnoIs(err error, want syscall.Errno) bool {
	if err == nil {
		return false
	}
	return syscall.Errno(want) == err || err.Error() == want.Error()
}

func TestRenamePreflightClassMismatches(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/file", []byte("f"))
	b.SeedDir("/dir", 0o755)
	ch := newStack(t, b)

	if err := rename(ch, "/dir", "/file", 0); !errnoIs(err, syscall.ENOTDIR) {
		t.Fatalf("dir→file = %v, want ENOTDIR", err)
	}
	if err := rename(ch, "/file", "/dir", 0); !errnoIs(err, syscall.EISDIR) {
		t.Fatalf("file→dir = %v, want EISDIR", err)
	}
	if err := rename(ch, "/missing", "/x", 0); !errnoIs(err, syscall.ENOENT) {
		t.Fatalf("missing source = %v, want ENOENT", err)
	}
	if err := rename(ch, "/dir", "/dir/nested", 0); !errnoIs(err, syscall.EINVAL) {
		t.Fatalf("into-own-subtree = %v, want EINVAL", err)
	}
}

func TestRenamePreflightSticky(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/sticky", 0o777|0o1000) // sticky
	b.Seed("/sticky/owned", []byte("x"))
	ch := newStack(t, b)

	// The entries resolve to the mount owner (uid 1000); a different
	// caller is refused by the sticky rule (check order: parent W|X first
	// — renaming inside the 0777 sticky dir passes that — then the sticky
	// ownership test), root bypasses.
	if err := rename(ch, "/sticky/owned", "/sticky/moved", 1234); !errnoIs(err, syscall.EPERM) {
		t.Fatalf("sticky rename by non-owner = %v, want EPERM", err)
	}
	// The sticky directory's owner may move entries it does not own
	// (caller == parent owner).
	if err := rename(ch, "/sticky/owned", "/sticky/moved2", 1000); err != nil {
		t.Fatalf("rename by sticky dir owner = %v, want nil", err)
	}
	if err := rename(ch, "/sticky/moved2", "/moved", 0); err != nil {
		t.Fatalf("root bypass = %v", err)
	}
}

func TestRenamePreflightEmptyDirTarget(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/emptyd", 0o755)
	b.SeedDir("/full", 0o755)
	b.Seed("/full/child", []byte("c"))
	ch := newStack(t, b)

	if err := rename(ch, "/emptyd", "/full", 0); !errnoIs(err, syscall.ENOTEMPTY) {
		t.Fatalf("dir→full = %v, want ENOTEMPTY", err)
	}
}

func TestRenameAllowedCasesPass(t *testing.T) {
	b := testbackend.NewBackend()
	b.Seed("/a.txt", []byte("x"))
	b.SeedDir("/d1", 0o755)
	b.SeedDir("/d2", 0o755)
	ch := newStack(t, b)

	if err := rename(ch, "/a.txt", "/b.txt", 1000); err != nil {
		t.Fatalf("file rename: %v", err)
	}
	if n, ok := b.Node("/b.txt"); !ok || string(n.Data) != "x" {
		t.Fatalf("renamed file = %q %v", n.Data, ok)
	}
	if err := rename(ch, "/d1", "/d2", 1000); err != nil {
		t.Fatalf("dir onto empty dir: %v", err)
	}
	if _, ok := b.Node("/d2"); !ok {
		t.Fatal("target dir missing after rename")
	}
}

func TestRenamePreflightSymlinkParentELOOP(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/real", 0o755)
	b.Seed("/real/f", []byte("x"))
	b.SeedSymlink("/link", "/real")
	ch := newStack(t, b)

	// The old path itself is stat'ed first — under a symlinked parent it
	// simply does not exist (ENOENT); the ELOOP row fires for the NEW
	// parent.
	if err := rename(ch, "/link/f", "/real/g", 0); !errnoIs(err, syscall.ENOENT) {
		t.Fatalf("rename of missing old under symlink = %v, want ENOENT", err)
	}
	if err := rename(ch, "/real/f", "/link/g", 0); !errnoIs(err, syscall.ELOOP) {
		t.Fatalf("rename via symlinked new parent = %v, want ELOOP", err)
	}
}
