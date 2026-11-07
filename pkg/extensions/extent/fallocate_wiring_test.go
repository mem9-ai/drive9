package extent

import (
	"context"
	"syscall"
	"testing"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// fallocate(2) on an open extent file must reach the engine, not fall through
// to the chain terminal as ENOSYS.
//
// JuiceFS's VFS already implements Fallocate (validation, the handle's write
// lock, Meta.Fallocate, and the writer/reader length bookkeeping). The extent
// plane only has to ROUTE the op to it; without that routing an operation the
// engine fully supports reported "not supported" the moment the file became
// extent layout.
func TestFallocateOnOpenExtentFileReachesVFS(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err, _ := dispatchCreate(ctx, h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("handle type %T", res.Handle)
	}

	fellThrough := false
	_, err = h.Do(ctx, &fs.Op{
		Kind: fs.OpFallocate, Path: "/d/a.db", Fh: eh.fh, Offset: 0, Size: 8192,
	}, func(context.Context, *fs.Op) (fs.Result, error) {
		fellThrough = true
		return fs.Result{}, fs.ErrUnsupported
	})
	if err != nil {
		t.Fatalf("fallocate on an open extent file: %v", err)
	}
	if fellThrough {
		t.Fatal("fallocate fell through to the chain terminal instead of reaching VFS")
	}
}

// A preallocation that must NOT change the length (FALLOC_FL_KEEP_SIZE) stays
// a no-op, as POSIX requires.
func TestFallocateKeepSizeDoesNotGrowTheFile(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err, _ := dispatchCreate(ctx, h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(ctx, 0, []byte("abc")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := h.Do(ctx, &fs.Op{
		Kind: fs.OpFallocate, Path: "/d/a.db", Fh: eh.fh,
		Offset: 0, Size: 1 << 20, Flags: 0x01, // FALLOC_FL_KEEP_SIZE
	}, func(context.Context, *fs.Op) (fs.Result, error) { return fs.Result{}, fs.ErrUnsupported }); err != nil {
		t.Fatalf("fallocate keep-size: %v", err)
	}
	if size, ok := h.opts.Engine.LocalAttr(ctx, "/d/a.db"); !ok || size != 3 {
		t.Fatalf("KEEP_SIZE changed the length: got %d (ok=%v), want 3", size, ok)
	}
}
