package bulkfile

// qiffang twenty-second round: a surrendered descriptor stays in the
// namespace-tracking registry, so a rename follows it instead of its later
// session recreating the old name.

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func TestPortSurrenderedDescriptorFollowsRename(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	first := openForWrite(t, ch, "/a")
	if err := first.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	// A second writable open hands the path to the lower plane and turns
	// the first descriptor into a passthrough.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatalf("second open = %v", err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRename, Path: "/a", Path2: "/b"}); err != nil {
		t.Fatalf("rename = %v", err)
	}
	if err := first.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	if err := first.Sync(ctx); err != nil {
		t.Fatalf("sync = %v", err)
	}
	if _, ok := b.Node("/a"); ok {
		t.Fatal("surrendered descriptor recreated the old name")
	}
	n, ok := b.Node("/b")
	if !ok {
		t.Fatal("/b missing after the write")
	}
	if string(n.Data) != "Xello" {
		t.Fatalf("/b = %q, want the write composed on the renamed image", n.Data)
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

// An unlink retires EVERY descriptor's name binding: a surrendered fd keeps
// its anonymous backing (bound before the delete) and must not re-open the
// deleted path with its O_CREAT flags.
func TestPortSurrenderedDescriptorSurvivesUnlink(t *testing.T) {
	b := testbackend.NewBackend()
	ch := newWritebackStack(t, b)
	ctx := context.Background()

	first := openForWrite(t, ch, "/a")
	if err := first.Write(ctx, 0, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/a", Flags: uint32(syscall.O_RDWR)})
	if err != nil {
		t.Fatalf("second open = %v", err)
	}
	if err := res.Handle.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpUnlink, Path: "/a"}); err != nil {
		t.Fatalf("unlink = %v", err)
	}
	if err := first.Write(ctx, 0, []byte("X")); err != nil {
		t.Fatalf("write after unlink = %v", err)
	}
	if err := first.Sync(ctx); err != nil {
		t.Fatalf("sync after unlink = %v", err)
	}
	if _, ok := b.Node("/a"); ok {
		t.Fatal("surrendered descriptor resurrected the unlinked name")
	}
	if err := first.Release(ctx); err != nil {
		t.Fatal(err)
	}
}
