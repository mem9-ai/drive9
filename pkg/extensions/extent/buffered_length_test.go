package extent

import (
	"context"
	"syscall"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// A stat between write(2) and publication must report the length the caller
// just wrote — not the last published size.
//
// This is the exact sequence strace captured from mptest's parent: pwrite64 of
// page 1 and page 2 succeeds, the very next newfstatat still reports st_size=0,
// and SQLite's next pread of the schema cookie comes back as EOF. A stat
// answering 0 makes the kernel reset its own i_size and serve every following
// read as EOF, so a sqlite3_exec batch that creates a table and then creates an
// index on it reports "no such table".
//
// POSIX grows the file on write(2). The engine's writer buffers bytes before
// publishing them, and that buffer is still part of the file's observable
// length.
func TestStatReportsBytesStillBufferedByTheWriter(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err, _ := dispatchCreate(ctx, h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)

	// Nothing written: an empty file.
	if size, ok := h.opts.Engine.LocalAttr(ctx, "/d/a.db"); !ok || size != 0 {
		t.Fatalf("fresh file size = %d (ok=%v), want 0", size, ok)
	}

	// SQLite's first page write (4096 bytes at 0).
	page := make([]byte, 4096)
	copy(page, "SQLite format 3\x00")
	if err := eh.Write(ctx, 0, page); err != nil {
		t.Fatalf("write page 1: %v", err)
	}
	// THE STAT THE KERNEL ISSUES — before any flush or commit.
	if size, ok := h.opts.Engine.LocalAttr(ctx, "/d/a.db"); !ok || size != 4096 {
		t.Fatalf("stat after write reported size %d (ok=%v), want 4096; the kernel resets i_size on a smaller answer and then serves EOF", size, ok)
	}
	// Second page, same batch.
	if err := eh.Write(ctx, 4096, page); err != nil {
		t.Fatalf("write page 2: %v", err)
	}
	if size, ok := h.opts.Engine.LocalAttr(ctx, "/d/a.db"); !ok || size != 8192 {
		t.Fatalf("stat after second write reported size %d (ok=%v), want 8192", size, ok)
	}
	// A getattr through the plane must agree.
	attr, ok := h.lookupLocalOnly(ctx, "/d/a.db")
	if !ok || attr == nil {
		t.Fatal("getattr did not serve the still-unpublished file")
	}
	if attr.Size != 8192 {
		t.Fatalf("getattr reported %d bytes, want 8192", attr.Size)
	}
}

// A sparse write past EOF grows the file to the write's end, not to a whole
// chunk.
func TestStatReportsBufferedExtentWriteEnd(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err, _ := dispatchCreate(ctx, h, "/d/sparse", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(ctx, 1<<20, []byte("tail")); err != nil {
		t.Fatalf("write: %v", err)
	}
	want := int64(1<<20) + 4
	if size, ok := h.opts.Engine.LocalAttr(ctx, "/d/sparse"); !ok || size != want {
		t.Fatalf("stat = %d (ok=%v), want %d", size, ok, want)
	}
}

// The buffered mark must not outlive the publication that covered it: once the
// bytes are published, the committed size is the truth and a truncate must be
// able to shrink the file again.
func TestTruncateShrinksAfterBufferedBytesArePublished(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err, _ := dispatchCreate(ctx, h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(ctx, 0, make([]byte, 8192)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := eh.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	// Now shrink. The stale buffered mark must not resurrect 8192.
	if _, _, handled := h.tryTruncate(ctx, &fs.Op{
		Kind: fs.OpSetAttr, Path: "/d/a.db", HasSize: true, Size: 4096,
	}, func(context.Context, *fs.Op) (fs.Result, error) { return fs.Result{}, nil }); !handled {
		t.Fatal("truncate on an open engine file was not handled")
	}
	if size, ok := h.opts.Engine.LocalAttr(ctx, "/d/a.db"); !ok || size != 4096 {
		t.Fatalf("size after truncate = %d (ok=%v), want 4096", size, ok)
	}
}

// A truncate must SHRINK the observable length, including the buffered mark.
//
// The mark is monotonic on purpose (a later stat must not report a shorter
// file than an earlier write established), so it has to be clamped when the
// caller explicitly truncates. SQLite deletes its rollback journal by
// truncating it to zero: a journal that still stats as non-empty is a journal
// the next connection tries to roll back.
func TestTruncateClampsTheBufferedMark(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	res, err, _ := dispatchCreate(ctx, h, "/d/j.db-journal", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(ctx, 0, make([]byte, 5120)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if size, _ := h.opts.Engine.LocalAttr(ctx, "/d/j.db-journal"); size != 5120 {
		t.Fatalf("size = %d, want 5120", size)
	}
	// SQLite's journal delete: truncate to zero.
	if err := h.opts.Engine.Bridge.TruncateLocal(ctx, "/d/j.db-journal", 0); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if size, ok := h.opts.Engine.LocalAttr(ctx, "/d/j.db-journal"); !ok || size != 0 {
		t.Fatalf("size after truncate-to-zero = %d (ok=%v), want 0; the buffered mark was not clamped", size, ok)
	}
}

// A SECOND opener must see bytes the first hasn't published yet.
//
// JuiceFS records the length at open time (VFS.Open -> Meta.Open -> GetAttr).
// If that answer comes only from the committed size, a process that opens the
// database while another has buffered writes binds a short length and serves
// EOF for the region the other just wrote. mptest runs five client processes
// against one file, so every one of them is this case.
func TestSecondOpenSeesBufferedLength(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	f.seedExtent(t, "/d/a.db", "committed") // materialized: GetAttr goes remote

	br := h.opts.Engine.Bridge
	// Pretend another descriptor buffered page 1 plus page 2.
	ino := inoFor("/d/a.db")
	br.remember(ino, "/d/a.db")
	br.noteBufferedLength(ino, 8192)

	// The length VFS.Open would report for a fresh opener.
	attr := &meta.Attr{}
	if errno := br.GetAttr(meta.NewContext(1, 0, []uint32{0}), ino, attr); errno != 0 {
		t.Fatalf("GetAttr: %v", errno)
	}
	if attr.Length != 8192 {
		t.Fatalf("open-time length = %d, want the buffered 8192", attr.Length)
	}
}
