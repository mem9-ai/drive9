//go:build !windows

package objectfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfscommon"
)

func testVFS(t *testing.T) (*rcloneFUSE, string) {
	t.Helper()
	configfile.Install()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "sub", "n.txt"), []byte("n"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := fs.NewFs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeOff
	return newRcloneFUSE(vfs.New(f, &opt), false), dir
}

func TestRcloneFUSELookupRead(t *testing.T) {
	ofs, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatalf("Lookup hello.txt: %v", st)
	}
	if ent.Size != 5 {
		t.Fatalf("size=%d", ent.Size)
	}

	var attr gofuse.AttrOut
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.Ino}}, &attr); st != gofuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}

	var openOut gofuse.OpenOut
	if st := ofs.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ent.Ino}}, &openOut); st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}
	buf := make([]byte, 16)
	res, st := ofs.Read(nil, &gofuse.ReadIn{Fh: openOut.Fh, Offset: 0}, buf)
	if st != gofuse.OK {
		t.Fatalf("Read: %v", st)
	}
	got, rst := res.Bytes(nil)
	if rst != gofuse.OK {
		t.Fatalf("Read bytes: %v", rst)
	}
	if string(got) != "hello" {
		t.Fatalf("read %q", got)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})
}

func testWriteVFS(t *testing.T) (*rcloneFUSE, string) {
	t.Helper()
	configfile.Install()
	dir := t.TempDir()
	f, err := fs.NewFs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.WriteBack = 0
	return newRcloneFUSE(vfs.New(f, &opt), false), dir
}

func TestRcloneFUSECreateFlushThenWrite(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	var createOut gofuse.CreateOut
	st := ofs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:     0o644,
	}, "from-mount.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if st := ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush after Create: %v", st)
	}
	n, st := ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh}, []byte("via-fuse"))
	if st != gofuse.OK {
		t.Fatalf("Write after Flush: %v (kernel FLUSH after create must leave the handle writable)", st)
	}
	if n != 8 {
		t.Fatalf("n=%d", n)
	}
	if st := ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush after Write: %v", st)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	got, err := os.ReadFile(filepath.Join(dir, "from-mount.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "via-fuse" {
		t.Fatalf("got %q", got)
	}
}

type failPutFs struct{ fs.Fs }

func (f failPutFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	_, _ = io.Copy(io.Discard, in)
	return nil, io.ErrUnexpectedEOF
}

func TestRcloneFUSEFlushReportsPutFailure(t *testing.T) {
	configfile.Install()
	dir := t.TempDir()
	inner, err := fs.NewFs(context.Background(), dir)
	if err != nil {
		t.Fatal(err)
	}
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.WriteBack = 0
	ofs := newRcloneFUSE(vfs.New(failPutFs{inner}, &opt), false)
	var createOut gofuse.CreateOut
	st := ofs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:     0o644,
	}, "fail.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh}, []byte("data")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	st = ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh})
	if st == gofuse.OK {
		t.Fatal("Flush must surface the remote PUT failure")
	}
}

func TestRcloneFUSEOpCancel(t *testing.T) {
	ofs, _ := testVFS(t)
	ch := make(chan struct{})
	close(ch)
	st := ofs.withOp(ch, func(context.Context) error {
		time.Sleep(time.Hour)
		return nil
	})
	if st != gofuse.Status(syscall.EINTR) {
		t.Fatalf("status=%v want EINTR", st)
	}
}

func TestRcloneFUSEOpCancelDoesNotWait(t *testing.T) {
	ofs, _ := testVFS(t)
	started := make(chan struct{})
	block := make(chan struct{})
	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- ofs.withOp(cancel, func(context.Context) error {
			close(started)
			<-block
			return nil
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("fn did not start")
	}
	close(cancel)
	select {
	case st := <-done:
		if st != gofuse.Status(syscall.EINTR) {
			t.Fatalf("status=%v want EINTR", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("withOp must return EINTR without waiting for in-flight I/O")
	}
	close(block)
}

func TestRcloneFUSEMutatingOpWaitsAndReportsSuccess(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	started := make(chan struct{})
	block := make(chan struct{})
	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- ofs.withMutatingOp(cancel, func(context.Context) error {
			close(started)
			<-block
			return ofs.v.Mkdir("late-dir", 0o755)
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("fn did not start")
	}
	close(cancel)
	select {
	case <-done:
		t.Fatal("mutating op returned before settlement")
	case <-time.After(50 * time.Millisecond):
	}
	close(block)
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("status=%v want OK after settled success", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mutating op did not return after settlement")
	}
	if _, err := os.Stat(filepath.Join(dir, "late-dir")); err != nil {
		t.Fatalf("mkdir should be visible after OK: %v", err)
	}
}

func TestRcloneFUSEMutatingOpDeadlineDoesNotUnblockIgnoredCtx(t *testing.T) {
	ofs, _ := testWriteVFS(t)
	old := fuseOpTimeout
	fuseOpTimeout = 30 * time.Millisecond
	t.Cleanup(func() { fuseOpTimeout = old })

	started := make(chan struct{})
	block := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		done <- ofs.withMutatingOp(nil, func(ctx context.Context) error {
			close(started)
			select {
			case <-block:
				return nil
			case <-ctx.Done():
				<-block
				return nil
			}
		})
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("fn did not start")
	}
	select {
	case <-done:
		t.Fatal("mutating op must stay blocked after fuseOpTimeout when fn ignores ctx")
	case <-time.After(150 * time.Millisecond):
	}
	close(block)
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("status=%v want OK after fn returns", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("mutating op did not return after fn finished")
	}
}

func TestRcloneFUSESetAttrSize(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	if err := os.WriteFile(filepath.Join(dir, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatalf("Lookup: %v", st)
	}
	var openOut gofuse.OpenOut
	if st := ofs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: ent.NodeId},
		Flags:    uint32(os.O_RDWR),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open: %v", st)
	}
	defer ofs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})
	var out gofuse.AttrOut
	st := ofs.SetAttr(nil, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ent.NodeId},
		Valid:    gofuse.FATTR_SIZE,
		Size:     1,
	}}, &out)
	if st != gofuse.OK {
		t.Fatalf("SetAttr: %v", st)
	}
	var attr gofuse.AttrOut
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.NodeId}}, &attr); st != gofuse.OK {
		t.Fatalf("GetAttr: %v", st)
	}
	if attr.Size != 1 {
		t.Fatalf("size=%d", attr.Size)
	}
}

func TestRcloneFUSESetAttrCancel(t *testing.T) {
	ofs, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatalf("Lookup: %v", st)
	}
	ch := make(chan struct{})
	close(ch)
	var out gofuse.AttrOut
	st := ofs.SetAttr(ch, &gofuse.SetAttrIn{SetAttrInCommon: gofuse.SetAttrInCommon{
		InHeader: gofuse.InHeader{NodeId: ent.NodeId},
		Valid:    gofuse.FATTR_SIZE,
		Size:     1,
	}}, &out)
	if st != gofuse.Status(syscall.EINTR) {
		t.Fatalf("status=%v want EINTR (SetAttr must honor kernel cancel)", st)
	}
}

func TestRcloneFUSEReadDirCancel(t *testing.T) {
	ofs, _ := testVFS(t)
	ch := make(chan struct{})
	close(ch)
	buf := make([]byte, 4096)
	out := gofuse.NewDirEntryList(buf, 0)
	st := ofs.ReadDir(ch, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}}, out)
	if st != gofuse.Status(syscall.EINTR) {
		t.Fatalf("status=%v want EINTR", st)
	}
}

func TestRcloneFUSECreateCancel(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	ch := make(chan struct{})
	close(ch)
	var createOut gofuse.CreateOut
	st := ofs.Create(ch, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:     0o644,
	}, "canceled.txt", &createOut)
	if st != gofuse.Status(syscall.EINTR) {
		t.Fatalf("status=%v want EINTR", st)
	}
	if _, err := os.Stat(filepath.Join(dir, "canceled.txt")); !os.IsNotExist(err) {
		t.Fatalf("canceled Create must not leave a file: %v", err)
	}
}

func TestRcloneFUSEDuplicateFlush(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	var createOut gofuse.CreateOut
	st := ofs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:     0o644,
	}, "dup-flush.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh}, []byte("hello")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	var st1, st2 gofuse.Status
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		st1 = ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh})
	}()
	go func() {
		defer wg.Done()
		st2 = ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh})
	}()
	wg.Wait()
	if st1 != gofuse.OK || st2 != gofuse.OK {
		t.Fatalf("Flush statuses %v %v", st1, st2)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	got, err := os.ReadFile(filepath.Join(dir, "dup-flush.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestRcloneFUSEConcurrentFlushAndWrite(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	var createOut gofuse.CreateOut
	st := ofs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:     0o644,
	}, "flush-write.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh}, []byte("hello")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	var flushSt, writeSt gofuse.Status
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		flushSt = ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh})
	}()
	go func() {
		defer wg.Done()
		_, writeSt = ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh, Offset: 5}, []byte("!"))
	}()
	wg.Wait()
	if flushSt != gofuse.OK {
		t.Fatalf("Flush: %v", flushSt)
	}
	if writeSt != gofuse.OK {
		t.Fatalf("Write: %v", writeSt)
	}
	if st := ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("final Flush: %v", st)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	got, err := os.ReadFile(filepath.Join(dir, "flush-write.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello!" {
		t.Fatalf("got %q", got)
	}
}

func TestRcloneFUSERenameDuringFlush(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	var createOut gofuse.CreateOut
	st := ofs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: 1},
		Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
		Mode:     0o644,
	}, "before.txt", &createOut)
	if st != gofuse.OK {
		t.Fatalf("Create: %v", st)
	}
	if _, st := ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh}, []byte("moved")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	var flushSt, renameSt gofuse.Status
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		flushSt = ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh})
	}()
	go func() {
		defer wg.Done()
		renameSt = ofs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "before.txt", "after.txt")
	}()
	wg.Wait()
	if flushSt != gofuse.OK {
		t.Fatalf("Flush: %v", flushSt)
	}
	if renameSt != gofuse.OK {
		t.Fatalf("Rename: %v", renameSt)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	if _, err := os.Stat(filepath.Join(dir, "before.txt")); !os.IsNotExist(err) {
		t.Fatal("successful rename must not leave before.txt")
	}
	got, err := os.ReadFile(filepath.Join(dir, "after.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "moved" {
		t.Fatalf("got %q", got)
	}
}

func TestRcloneFUSEOpenThenRenamePublishesNewPath(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	if err := os.WriteFile(filepath.Join(dir, "before.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "before.txt", &ent); st != gofuse.OK {
		t.Fatalf("Lookup: %v", st)
	}
	started := make(chan struct{})
	resume := make(chan struct{})
	ofs.afterRemoteOpen = func() {
		close(started)
		<-resume
	}
	defer func() { ofs.afterRemoteOpen = nil }()

	var openOut gofuse.OpenOut
	openDone := make(chan gofuse.Status, 1)
	go func() {
		openDone <- ofs.Open(nil, &gofuse.OpenIn{
			InHeader: gofuse.InHeader{NodeId: ent.NodeId},
			Flags:    uint32(os.O_RDWR),
		}, &openOut)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Open did not reach publication fence")
	}
	if st := ofs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "before.txt", "after.txt"); st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}
	close(resume)
	select {
	case st := <-openDone:
		if st != gofuse.OK {
			t.Fatalf("Open: %v", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Open did not return")
	}
	if _, st := ofs.Write(nil, &gofuse.WriteIn{Fh: openOut.Fh}, []byte("data!")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	if st := ofs.Flush(nil, &gofuse.FlushIn{Fh: openOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})
	if _, err := os.Stat(filepath.Join(dir, "before.txt")); !os.IsNotExist(err) {
		t.Fatal("Flush after rename must not recreate before.txt")
	}
	got, err := os.ReadFile(filepath.Join(dir, "after.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data!" {
		t.Fatalf("got %q", got)
	}
}

func TestRcloneFUSECreateThenRenamePublishesNewPath(t *testing.T) {
	ofs, dir := testWriteVFS(t)
	started := make(chan struct{})
	resume := make(chan struct{})
	ofs.afterRemoteOpen = func() {
		close(started)
		<-resume
	}
	defer func() { ofs.afterRemoteOpen = nil }()

	var createOut gofuse.CreateOut
	createDone := make(chan gofuse.Status, 1)
	go func() {
		createDone <- ofs.Create(nil, &gofuse.CreateIn{
			InHeader: gofuse.InHeader{NodeId: 1},
			Flags:    uint32(os.O_WRONLY | os.O_CREATE | os.O_TRUNC),
			Mode:     0o644,
		}, "before.txt", &createOut)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("Create did not reach publication fence")
	}
	if st := ofs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "before.txt", "after.txt"); st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}
	close(resume)
	select {
	case st := <-createDone:
		if st != gofuse.OK {
			t.Fatalf("Create: %v", st)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Create did not return")
	}
	if _, st := ofs.Write(nil, &gofuse.WriteIn{Fh: createOut.Fh}, []byte("created")); st != gofuse.OK {
		t.Fatalf("Write: %v", st)
	}
	if st := ofs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush: %v", st)
	}
	ofs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	if _, err := os.Stat(filepath.Join(dir, "before.txt")); !os.IsNotExist(err) {
		t.Fatal("Flush after rename must not recreate before.txt")
	}
	got, err := os.ReadFile(filepath.Join(dir, "after.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "created" {
		t.Fatalf("got %q", got)
	}
}

func TestRcloneFUSEReadDir(t *testing.T) {
	ofs, _ := testVFS(t)
	buf := make([]byte, 4096)
	out := gofuse.NewDirEntryList(buf, 0)
	if st := ofs.ReadDir(nil, &gofuse.ReadIn{InHeader: gofuse.InHeader{NodeId: 1}}, out); st != gofuse.OK {
		t.Fatalf("ReadDir: %v", st)
	}

	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "sub", &ent); st != gofuse.OK {
		t.Fatalf("Lookup sub: %v", st)
	}
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: ent.Ino}, "n.txt", &ent); st != gofuse.OK {
		t.Fatalf("Lookup sub/n.txt: %v", st)
	}
}

func TestRcloneFUSEInstancesDoNotShareHandles(t *testing.T) {
	a, _ := testVFS(t)
	b, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := a.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	var openOut gofuse.OpenOut
	if st := a.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ent.Ino}}, &openOut); st != gofuse.OK {
		t.Fatal(st)
	}
	if b.getHandle(openOut.Fh) != nil {
		t.Fatal("handle leaked across rcloneFUSE instances")
	}
}

func TestMapVFSErr(t *testing.T) {
	if mapVFSErr(nil) != gofuse.OK {
		t.Fatal("nil")
	}
	if mapVFSErr(os.ErrNotExist) != gofuse.ENOENT {
		t.Fatal("not exist")
	}
	if mapVFSErr(context.Canceled) != gofuse.Status(syscall.EINTR) {
		t.Fatal("canceled")
	}
	if mapVFSErr(context.DeadlineExceeded) != gofuse.Status(syscall.ETIMEDOUT) {
		t.Fatal("deadline")
	}
	if mapVFSErr(syscall.ENOTDIR) != gofuse.Status(syscall.ENOTDIR) {
		t.Fatal("ENOTDIR")
	}
}

func TestRel(t *testing.T) {
	ofs := &rcloneFUSE{}
	if got := ofs.rel("", "a"); got != "a" {
		t.Fatalf("got %q", got)
	}
	if got := ofs.rel("dir/", "a"); got != "dir/a" {
		t.Fatalf("got %q", got)
	}
}

func TestResolveObjectCacheDir(t *testing.T) {
	got := resolveObjectCacheDir("/tmp/obj-cache", "s3://bucket/prefix/")
	if !filepath.IsAbs(got) || !strings.HasPrefix(got, "/tmp/obj-cache"+string(filepath.Separator)) {
		t.Fatalf("got %q", got)
	}
	def := resolveObjectCacheDir("", "s3://bucket/prefix/")
	if !filepath.IsAbs(def) {
		t.Fatal("default cache dir should be absolute")
	}
	if !strings.Contains(def, filepath.Join(".cache", "drive9", "object")) {
		t.Fatalf("default = %q, want under ~/.cache/drive9/object", def)
	}
	other := resolveObjectCacheDir("", "s3://other/prefix/")
	if def == other {
		t.Fatal("different URIs must not share a cache directory")
	}
}

func TestForgetKeepsLiveLookups(t *testing.T) {
	ofs, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	ofs.Forget(ent.NodeId, 1)
	var attr gofuse.AttrOut
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.NodeId}}, &attr); st != gofuse.OK {
		t.Fatalf("live inode after Forget(1): %v", st)
	}
	ofs.Forget(ent.NodeId, 1)
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.NodeId}}, &attr); st != gofuse.ENOENT {
		t.Fatalf("after last Forget: %v", st)
	}
}

func TestRenameUpdatesInodePath(t *testing.T) {
	ofs, _ := testVFS(t)
	var ent gofuse.EntryOut
	if st := ofs.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "hello.txt", &ent); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := ofs.Rename(nil, &gofuse.RenameIn{InHeader: gofuse.InHeader{NodeId: 1}, Newdir: 1}, "hello.txt", "moved.txt"); st != gofuse.OK {
		t.Fatalf("Rename: %v", st)
	}
	var attr gofuse.AttrOut
	if st := ofs.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: ent.NodeId}}, &attr); st != gofuse.OK {
		t.Fatalf("GetAttr after rename: %v", st)
	}
}

func TestObjectCacheHasPendingNil(t *testing.T) {
	if objectCacheHasPending(nil) {
		t.Fatal("nil VFS")
	}
}
