package fuse

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/fusecomp/dircache"
)

func TestUnlinkKeepsSurvivingHardlinkVisible(t *testing.T) {
	for _, state := range []string{"partial", "complete", "session"} {
		t.Run(state, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.Seed("/keep", []byte("survivor"))
			b.SeedDir("/other", 0755)
			f := newPerfFS(t, b)
			keep := lookupFacade(t, f, "keep")
			other := lookupFacade(t, f, "other")
			var alias gofuse.EntryOut
			if st := f.Link(nil, &gofuse.LinkIn{InHeader: gofuse.InHeader{NodeId: other}, Oldnodeid: keep}, "alias", &alias); st != gofuse.OK {
				t.Fatal(st)
			}
			dc := f.Ctx.DirCache.(*dircache.DirCache)
			entry := fs.DirEntryInfo{Name: "keep", Size: 8, Nlink: 2}
			switch state {
			case "complete":
				dc.Put("/", []fs.DirEntryInfo{entry})
			case "session":
				dc.MarkSessionCreatedDir("/")
				dc.Upsert("/", entry)
			default:
				dc.Upsert("/", entry)
			}
			// This listing predates unlink and must not reinstall the stale nlink.
			request := dc.BeginRequest("/")
			if st := f.Unlink(nil, &gofuse.InHeader{NodeId: other}, "alias"); st != gofuse.OK {
				t.Fatal(st)
			}
			dc.PutListing("/", []fs.DirEntryInfo{entry}, request)
			var out gofuse.EntryOut
			if st := f.Lookup(nil, rootHeader(), "keep", &out); st != gofuse.OK || out.Nlink != 1 {
				t.Fatalf("surviving alias lookup = %v, nlink=%d; want OK, 1", st, out.Nlink)
			}
			if _, ok := b.Node("/keep"); !ok {
				t.Fatal("surviving backend name was deleted")
			}
			if _, ok := b.Node("/other/alias"); ok {
				t.Fatal("removed alias remains in backend")
			}
			if st := f.Lookup(nil, rootHeader(), "absent", &out); st != gofuse.ENOENT {
				t.Fatalf("missing name lookup = %v", st)
			}
		})
	}
}

func createUnlinkFixture(t *testing.T, f *FS, name string) gofuse.CreateOut {
	t.Helper()
	var out gofuse.CreateOut
	if st := f.Create(nil, exclusiveCreateInput(), name, &out); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := f.Unlink(nil, rootHeader(), name); st != gofuse.OK {
		t.Fatal(st)
	}
	return out
}

func exclusiveCreateInput() *gofuse.CreateIn {
	return &gofuse.CreateIn{InHeader: *rootHeader(), Flags: syscall.O_CREAT | syscall.O_RDWR | syscall.O_EXCL, Mode: 0644}
}

func releaseUnlinkFixture(f *FS, out gofuse.CreateOut) {
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
}

func awaitUnlinkTest[T any](t *testing.T, c <-chan T) T {
	t.Helper()
	select {
	case result := <-c:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for operation")
		var zero T
		return zero
	}
}

func TestDeferredUnlinkWaitsBeforeRecreate(t *testing.T) {
	for _, initiator := range []string{"last_close", "replacement"} {
		t.Run(initiator, func(t *testing.T) {
			backend := newMemCore()
			entered, proceed := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			finish := func() { unblock.Do(func() { close(proceed) }) }
			t.Cleanup(finish)
			var deletes atomic.Int32
			gate := fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
				if op.Kind == fs.OpUnlink && op.Path == "/same" {
					if deletes.Add(1) == 1 {
						close(entered)
					}
					select {
					case <-proceed:
					case <-ctx.Done():
						return fs.Result{}, ctx.Err()
					}
				}
				if op.Kind == fs.OpCreate && op.Flags&syscall.O_EXCL != 0 {
					if _, exists := backend.get(op.Path); exists {
						return fs.Result{}, syscall.EEXIST
					}
				}
				return next(ctx, op)
			})
			f := newTestFS(t, backend, gate)
			original := createUnlinkFixture(t, f, "same")
			first := make(chan error, 1)
			go func() {
				if initiator == "last_close" {
					releaseUnlinkFixture(f, original)
					first <- nil
				} else {
					first <- f.flushDeferredUnlink("/same")
				}
			}()
			awaitUnlinkTest(t, entered)
			if !f.isDeferredUnlink("/same") {
				t.Error("in-flight deletion lost its registration")
			}
			// Waiting on this path must not hold the filesystem-wide mutex.
			other := make(chan gofuse.Status, 1)
			go func() { other <- f.Create(nil, exclusiveCreateInput(), "other", &gofuse.CreateOut{}) }()
			if st := awaitUnlinkTest(t, other); st != gofuse.OK {
				t.Fatalf("unrelated create = %v", st)
			}
			created := make(chan gofuse.Status, 1)
			var replacement gofuse.CreateOut
			go func() { created <- f.Create(nil, exclusiveCreateInput(), "same", &replacement) }()
			select {
			case st := <-created:
				t.Fatalf("create returned before old deletion completed: %v", st)
			case <-time.After(100 * time.Millisecond):
			}
			finish()
			if err := awaitUnlinkTest(t, first); err != nil {
				t.Fatal(err)
			}
			if st := awaitUnlinkTest(t, created); st != gofuse.OK {
				t.Fatalf("recreate = %v", st)
			}
			// A late close of the old handle must not delete or negatively
			// cache the replacement, even when replacement initiated cleanup.
			releaseUnlinkFixture(f, original)
			var out gofuse.EntryOut
			if st := f.Lookup(nil, rootHeader(), "same", &out); st != gofuse.OK || out.NodeId != replacement.NodeId {
				t.Fatalf("replacement lookup = %v, inode=%d; want %d", st, out.NodeId, replacement.NodeId)
			}
			if n := deletes.Load(); n != 1 {
				t.Fatalf("remote deletes = %d, want one", n)
			}
		})
	}
}

func TestDeferredUnlinkFailureBlocksReplacementAndRetries(t *testing.T) {
	backend := newMemCore()
	var fail atomic.Bool
	fail.Store(true)
	var deletes atomic.Int32
	gate := fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
		if op.Kind == fs.OpUnlink {
			deletes.Add(1)
			if fail.Load() {
				return fs.Result{}, syscall.EIO
			}
		}
		return next(ctx, op)
	})
	f := newTestFS(t, backend, gate)
	old := createUnlinkFixture(t, f, "same")
	releaseUnlinkFixture(f, old)
	if !f.isDeferredUnlink("/same") {
		t.Fatal("failed deletion was forgotten")
	}
	if st := f.Create(nil, exclusiveCreateInput(), "same", &gofuse.CreateOut{}); st != gofuse.EIO {
		t.Fatalf("create during failed cleanup = %v, want EIO", st)
	}
	if !f.isDeferredUnlink("/same") {
		t.Fatal("failed replacement dropped cleanup state")
	}
	fail.Store(false)
	if st := f.Create(nil, exclusiveCreateInput(), "same", &gofuse.CreateOut{}); st != gofuse.OK {
		t.Fatalf("recreate after cleanup recovery = %v", st)
	}
	if f.isDeferredUnlink("/same") || deletes.Load() != 3 {
		t.Fatalf("pending=%v, deletes=%d", f.isDeferredUnlink("/same"), deletes.Load())
	}
}

func TestDeferredUnlinkAlreadyAbsent(t *testing.T) {
	backend := newMemCore()
	f := newTestFS(t, backend)
	old := createUnlinkFixture(t, f, "same")
	if _, err := backend.Do(context.Background(), &fs.Op{Kind: fs.OpUnlink, Path: "/same"}, nil); err != nil {
		t.Fatal(err)
	}
	if st := f.Create(nil, exclusiveCreateInput(), "same", &gofuse.CreateOut{}); st != gofuse.OK {
		t.Fatalf("recreate after ENOENT cleanup = %v", st)
	}
	releaseUnlinkFixture(f, old)
	if _, ok := backend.get("/same"); !ok {
		t.Fatal("old handle deleted replacement")
	}
}

func TestDeferredUnlinkFailureBlocksNamespaceMutations(t *testing.T) {
	for _, operation := range []string{"create", "mkdir", "mknod", "symlink", "link", "rename"} {
		t.Run(operation, func(t *testing.T) {
			backend := newMemCore()
			var armed bool
			var mutations int
			gate := fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
				if armed {
					if op.Kind == fs.OpUnlink {
						return fs.Result{}, syscall.EIO
					}
					mutations++
				}
				return next(ctx, op)
			})
			f := newTestFS(t, backend, gate)
			createUnlinkFixture(t, f, "same")
			var source gofuse.CreateOut
			if st := f.Create(nil, exclusiveCreateInput(), "source", &source); st != gofuse.OK {
				t.Fatal(st)
			}
			armed = true
			var st gofuse.Status
			switch operation {
			case "create":
				st = f.Create(nil, exclusiveCreateInput(), "same", &gofuse.CreateOut{})
			case "mkdir":
				st = f.Mkdir(nil, &gofuse.MkdirIn{InHeader: *rootHeader(), Mode: 0755}, "same", &gofuse.EntryOut{})
			case "mknod":
				st = f.Mknod(nil, &gofuse.MknodIn{InHeader: *rootHeader(), Mode: syscall.S_IFREG | 0644}, "same", &gofuse.EntryOut{})
			case "symlink":
				st = f.Symlink(nil, rootHeader(), "source", "same", &gofuse.EntryOut{})
			case "link":
				st = f.Link(nil, &gofuse.LinkIn{InHeader: *rootHeader(), Oldnodeid: source.NodeId}, "same", &gofuse.EntryOut{})
			case "rename":
				st = f.Rename(nil, &gofuse.RenameIn{InHeader: *rootHeader(), Newdir: 1}, "source", "same")
			}
			if st != gofuse.EIO || mutations != 0 || !f.isDeferredUnlink("/same") {
				t.Fatalf("status=%v downstream mutations=%d pending=%v", st, mutations, f.isDeferredUnlink("/same"))
			}
		})
	}
}

func TestDeferredUnlinkDirectoryBoundaryAndFailure(t *testing.T) {
	backend := newMemCore()
	var fail bool
	gate := fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
		if fail && op.Kind == fs.OpUnlink {
			return fs.Result{}, syscall.EIO
		}
		return next(ctx, op)
	})
	f := newTestFS(t, backend, gate)
	for _, name := range []string{"dir", "directory"} {
		var dir gofuse.EntryOut
		if st := f.Mkdir(nil, &gofuse.MkdirIn{InHeader: *rootHeader(), Mode: 0755}, name, &dir); st != gofuse.OK {
			t.Fatal(st)
		}
		input := exclusiveCreateInput()
		input.NodeId = dir.NodeId
		if st := f.Create(nil, input, "child", &gofuse.CreateOut{}); st != gofuse.OK {
			t.Fatal(st)
		}
		if st := f.Unlink(nil, &gofuse.InHeader{NodeId: dir.NodeId}, "child"); st != gofuse.OK {
			t.Fatal(st)
		}
	}
	fail = true
	if st := f.Rmdir(nil, rootHeader(), "dir"); st != gofuse.EIO {
		t.Fatalf("rmdir during failed child cleanup = %v", st)
	}
	if _, exists := backend.get("/dir"); !exists {
		t.Fatal("failed child cleanup still removed parent")
	}
	fail = false
	if st := f.Rmdir(nil, rootHeader(), "dir"); st != gofuse.OK {
		t.Fatalf("rmdir after recovery = %v", st)
	}
	if f.isDeferredUnlink("/dir/child") || !f.isDeferredUnlink("/directory/child") {
		t.Fatal("directory cleanup crossed its path boundary")
	}
	if _, exists := backend.get("/directory/child"); !exists {
		t.Fatal("unrelated directory child was deleted")
	}
}

func TestDeferredUnlinkDoesNotDeleteReboundName(t *testing.T) {
	backend := newMemCore()
	var deletes int
	gate := fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
		if op.Kind == fs.OpUnlink {
			deletes++
		}
		return next(ctx, op)
	})
	f := newTestFS(t, backend, gate)
	old := createUnlinkFixture(t, f, "same")
	newIno := f.inodes.LookupWithIdentity("/same", "replacement", 1, false, 0, time.Now())
	if newIno == old.NodeId {
		t.Fatal("fixture did not replace the inode")
	}
	releaseUnlinkFixture(f, old)
	if deletes != 0 || f.isDeferredUnlink("/same") {
		t.Fatalf("stale cleanup: deletes=%d pending=%v", deletes, f.isDeferredUnlink("/same"))
	}
	if _, exists := backend.get("/same"); !exists {
		t.Fatal("rebound name was deleted")
	}
}

func TestDeferredUnlinkKeepsEveryOpenHardlinkAlias(t *testing.T) {
	backend := testbackend.NewBackend()
	backend.Seed("/first", []byte("shared"))
	f := newPerfFS(t, backend)
	ino := lookupFacade(t, f, "first")
	open := func() gofuse.OpenOut {
		var out gofuse.OpenOut
		if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: ino}, Flags: syscall.O_RDONLY}, &out); st != gofuse.OK {
			t.Fatal(st)
		}
		return out
	}
	first := open()
	var alias gofuse.EntryOut
	if st := f.Link(nil, &gofuse.LinkIn{InHeader: *rootHeader(), Oldnodeid: ino}, "second", &alias); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := f.Unlink(nil, rootHeader(), "first"); st != gofuse.OK {
		t.Fatal(st)
	}
	second := open() // The surviving alias now supplies the inode's path.
	if st := f.Unlink(nil, rootHeader(), "second"); st != gofuse.OK {
		t.Fatal(st)
	}
	for _, path := range []string{"/first", "/second"} {
		if !f.isDeferredUnlink(path) {
			t.Fatalf("lost deferred alias %s", path)
		}
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: first.Fh})
	for _, path := range []string{"/first", "/second"} {
		if _, exists := backend.Node(path); !exists {
			t.Fatalf("deleted %s before last handle closed", path)
		}
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: second.Fh})
	for _, path := range []string{"/first", "/second"} {
		if _, exists := backend.Node(path); exists || f.isDeferredUnlink(path) {
			t.Fatalf("last close retained %s: exists=%v pending=%v", path, exists, f.isDeferredUnlink(path))
		}
	}
}

type unlinkMarkGate struct {
	entered chan struct{}
	proceed chan struct{}
}

func (g *unlinkMarkGate) Do(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
	return next(ctx, op)
}

func (g *unlinkMarkGate) MarkPathUnlinked(string) error {
	close(g.entered)
	<-g.proceed
	return nil
}

func TestDeferredUnlinkLastCloseBeforeRegistration(t *testing.T) {
	backend := newMemCore()
	gate := &unlinkMarkGate{entered: make(chan struct{}), proceed: make(chan struct{})}
	f := newTestFS(t, backend, gate)
	var old gofuse.CreateOut
	if st := f.Create(nil, exclusiveCreateInput(), "same", &old); st != gofuse.OK {
		t.Fatal(st)
	}
	var once sync.Once
	unblock := func() { once.Do(func() { close(gate.proceed) }) }
	t.Cleanup(unblock)
	unlinked := make(chan gofuse.Status, 1)
	go func() { unlinked <- f.Unlink(nil, rootHeader(), "same") }()
	awaitUnlinkTest(t, gate.entered)
	releaseUnlinkFixture(f, old)
	unblock()
	if st := awaitUnlinkTest(t, unlinked); st != gofuse.OK {
		t.Fatal(st)
	}
	if _, exists := backend.get("/same"); exists || f.isDeferredUnlink("/same") {
		t.Fatalf("last close missed cleanup: exists=%v pending=%v", exists, f.isDeferredUnlink("/same"))
	}
}

func TestDeferredUnlinkParentRenamePreservesOpenData(t *testing.T) {
	backend := testbackend.NewBackend()
	backend.SeedDir("/dir", 0755)
	backend.Seed("/dir/file", []byte("old content"))
	f := newPerfFS(t, backend)
	dir := lookupFacade(t, f, "dir")
	var entry gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: dir}, "file", &entry); st != gofuse.OK {
		t.Fatal(st)
	}
	var opened gofuse.OpenOut
	if st := f.Open(nil, &gofuse.OpenIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}}, &opened); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := f.Unlink(nil, &gofuse.InHeader{NodeId: dir}, "file"); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := f.Rename(nil, &gofuse.RenameIn{InHeader: *rootHeader(), Newdir: 1}, "dir", "moved"); st != gofuse.OK {
		t.Fatal(st)
	}
	data := make([]byte, 64)
	n, err := f.openHandle(opened.Fh).Read(context.Background(), 0, data)
	if err != nil || string(data[:n]) != "old content" {
		t.Fatalf("anonymous handle after parent rename: %q, %v", data[:n], err)
	}
	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: entry.NodeId}, Fh: opened.Fh})
	for _, path := range []string{"/dir/file", "/moved/file"} {
		if _, exists := backend.Node(path); exists || f.isDeferredUnlink(path) {
			t.Fatalf("hidden child survived rename: %s exists=%v pending=%v", path, exists, f.isDeferredUnlink(path))
		}
	}
	if st := f.Rmdir(nil, rootHeader(), "moved"); st != gofuse.OK {
		t.Fatalf("rmdir after parent rename = %v", st)
	}
}

func TestDeferredUnlinkParentRenameWaitsAndKeepsFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			backend := newMemCore()
			entered, proceed := make(chan struct{}), make(chan struct{})
			var startOnce, finishOnce sync.Once
			unblock := func() { finishOnce.Do(func() { close(proceed) }) }
			t.Cleanup(unblock)
			var renames atomic.Int32
			gate := fs.HandlerFunc(func(ctx context.Context, op *fs.Op, next fs.Next) (fs.Result, error) {
				if op.Kind == fs.OpUnlink {
					startOnce.Do(func() { close(entered) })
					<-proceed
					if fail {
						return fs.Result{}, syscall.EIO
					}
				}
				if op.Kind == fs.OpRename {
					renames.Add(1)
				}
				return next(ctx, op)
			})
			f := newTestFS(t, backend, gate)
			var dir gofuse.EntryOut
			if st := f.Mkdir(nil, &gofuse.MkdirIn{InHeader: *rootHeader(), Mode: 0755}, "dir", &dir); st != gofuse.OK {
				t.Fatal(st)
			}
			input := exclusiveCreateInput()
			input.NodeId = dir.NodeId
			var file gofuse.CreateOut
			if st := f.Create(nil, input, "file", &file); st != gofuse.OK {
				t.Fatal(st)
			}
			if st := f.Unlink(nil, &gofuse.InHeader{NodeId: dir.NodeId}, "file"); st != gofuse.OK {
				t.Fatal(st)
			}
			closed := make(chan struct{}, 1)
			go func() { releaseUnlinkFixture(f, file); closed <- struct{}{} }()
			awaitUnlinkTest(t, entered)
			renamed := make(chan gofuse.Status, 1)
			go func() { renamed <- f.Rename(nil, &gofuse.RenameIn{InHeader: *rootHeader(), Newdir: 1}, "dir", "moved") }()
			select {
			case st := <-renamed:
				t.Fatalf("rename overtook child cleanup: %v", st)
			case <-time.After(100 * time.Millisecond):
			}
			unblock()
			awaitUnlinkTest(t, closed)
			st := awaitUnlinkTest(t, renamed)
			if fail {
				if st != gofuse.EIO || renames.Load() != 0 || !f.isDeferredUnlink("/dir/file") {
					t.Fatalf("failed cleanup: rename=%v dispatched=%d pending=%v", st, renames.Load(), f.isDeferredUnlink("/dir/file"))
				}
			} else if st != gofuse.OK || renames.Load() != 1 || f.isDeferredUnlink("/dir/file") {
				t.Fatalf("successful cleanup: rename=%v dispatched=%d", st, renames.Load())
			}
		})
	}
}
