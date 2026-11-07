package extent

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/mem9-ai/drive9/pkg/fs"
)

// A write lock excludes a different owner; two readers coexist; the same owner
// may replace its own.
func TestLockConflictsFollowPOSIX(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(7)

	if e := lt.Setlk(context.Background(), ino, 1, 111, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatalf("owner1 write: %v", e)
	}
	if e := lt.Setlk(context.Background(), ino, 2, 222, syscall.F_WRLCK, 0, 99, false); e != syscall.EAGAIN {
		t.Fatalf("second writer = %v, want EAGAIN", e)
	}
	if e := lt.Setlk(context.Background(), ino, 2, 222, syscall.F_RDLCK, 0, 99, false); e != syscall.EAGAIN {
		t.Fatalf("reader against a writer = %v, want EAGAIN", e)
	}
	// The owner may upgrade/replace its own range.
	if e := lt.Setlk(context.Background(), ino, 1, 111, syscall.F_RDLCK, 0, 99, false); e != 0 {
		t.Fatalf("owner1 downgrade: %v", e)
	}
	// Now another reader is compatible.
	if e := lt.Setlk(context.Background(), ino, 2, 222, syscall.F_RDLCK, 0, 99, false); e != 0 {
		t.Fatalf("second reader after downgrade: %v", e)
	}
	// ...but a writer is not.
	if e := lt.Setlk(context.Background(), ino, 3, 333, syscall.F_WRLCK, 0, 99, false); e != syscall.EAGAIN {
		t.Fatalf("writer against readers = %v, want EAGAIN", e)
	}
	// Disjoint ranges do not conflict.
	if e := lt.Setlk(context.Background(), ino, 3, 333, syscall.F_WRLCK, 200, 299, false); e != 0 {
		t.Fatalf("disjoint writer: %v", e)
	}
}

// Replacing a range must SPLIT the owner's older range, not drop it: this is
// the rule fcntl(F_SETLK) guarantees and SQLite's pager relies on.
func TestLockReplaceSplitsOwnerRanges(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(9)

	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_WRLCK, 0, 30, false); e != 0 {
		t.Fatalf("initial: %v", e)
	}
	// Punch a hole in the middle with a second owner's range: owner1 must keep
	// [0,9] and [21,30].
	if e := lt.Setlk(context.Background(), ino, 2, 2, syscall.F_WRLCK, 10, 20, false); e != syscall.EAGAIN {
		t.Fatalf("hole against owner1 = %v, want EAGAIN (owner1 still holds it)", e)
	}
	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_UNLCK, 10, 20, false); e != 0 {
		t.Fatalf("owner1 unlock middle: %v", e)
	}
	if e := lt.Setlk(context.Background(), ino, 2, 2, syscall.F_WRLCK, 10, 20, false); e != 0 {
		t.Fatalf("owner2 takes the hole: %v", e)
	}
	// Owner1's surviving head and tail must still exclude others.
	if e := lt.Setlk(context.Background(), ino, 3, 3, syscall.F_WRLCK, 0, 5, false); e != syscall.EAGAIN {
		t.Fatalf("head range = %v, want EAGAIN", e)
	}
	if e := lt.Setlk(context.Background(), ino, 3, 3, syscall.F_WRLCK, 25, 30, false); e != syscall.EAGAIN {
		t.Fatalf("tail range = %v, want EAGAIN", e)
	}
	// Getlk reports the blocking owner for a conflicting request.
	typ, start, end, pid, errno := lt.Getlk(ino, 3, syscall.F_WRLCK, 0, 3)
	if errno != 0 || typ != syscall.F_WRLCK || start != 0 || end != 9 || pid != 1 {
		t.Fatalf("Getlk = (%v,%d,%d,%d,%v), want owner1's WRLCK [0,9]", typ, start, end, pid, errno)
	}
	// A free range reports F_UNLCK.
	typ, _, _, _, errno = lt.Getlk(ino, 3, syscall.F_WRLCK, 100, 200)
	if errno != 0 || typ != syscall.F_UNLCK {
		t.Fatalf("Getlk on a free range = %v/%v, want F_UNLCK", typ, errno)
	}
}

// flock is a separate whole-file namespace.
func TestFlockIsWholeFileAndSeparate(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(11)
	if e := lt.Flock(context.Background(), ino, 1, syscall.F_WRLCK, false); e != 0 {
		t.Fatalf("flock exclusive: %v", e)
	}
	if e := lt.Flock(context.Background(), ino, 2, syscall.F_RDLCK, false); e != syscall.EAGAIN {
		t.Fatalf("shared flock against exclusive = %v, want EAGAIN", e)
	}
	// A byte-range lock is a different namespace and must not be blocked.
	if e := lt.Setlk(context.Background(), ino, 2, 2, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatalf("byte-range lock alongside flock: %v", e)
	}
	// Releasing the flock frees it for others.
	if e := lt.Flock(context.Background(), ino, 1, syscall.F_UNLCK, false); e != 0 {
		t.Fatalf("flock unlock: %v", e)
	}
	if e := lt.Flock(context.Background(), ino, 2, syscall.F_RDLCK, false); e != 0 {
		t.Fatalf("shared flock after release: %v", e)
	}
}

// Closing the file's last descriptor must drop every lock on it, or the next
// opener sees a phantom conflict it can never clear.
func TestReleaseLocksOnLastClose(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(13)
	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatal(e)
	}
	if e := lt.Flock(context.Background(), ino, 1, syscall.F_WRLCK, false); e != 0 {
		t.Fatal(e)
	}
	lt.ReleaseInode(ino)
	if e := lt.Setlk(context.Background(), ino, 2, 2, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatalf("lock survived the last close: %v", e)
	}
	if e := lt.Flock(context.Background(), ino, 2, syscall.F_WRLCK, false); e != 0 {
		t.Fatalf("flock survived the last close: %v", e)
	}
}

// The bridge exposes the table through the Meta contract, including through an
// engine handle, which is what the FUSE facade dispatches to.
func TestEngineHandleServesLocks(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, err, _ := dispatchCreate(context.Background(), h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("handle type %T", res.Handle)
	}
	ctx := context.Background()

	// A FUSE lock from owner 1.
	if err := eh.SetLk(ctx, fsFileLock(1, syscall.F_WRLCK, 0, 99), false); err != nil {
		t.Fatalf("SetLk: %v", err)
	}
	// Owner 2 conflicts.
	if err := eh.SetLk(ctx, fsFileLock(2, syscall.F_WRLCK, 0, 99), false); err == nil {
		t.Fatal("second owner's write lock succeeded; the table is not enforcing")
	}
	// GetLk reports owner 1's lock to owner 2.
	got, err := eh.GetLk(ctx, fsFileLock(2, syscall.F_WRLCK, 0, 99))
	if err != nil {
		t.Fatalf("GetLk: %v", err)
	}
	if got.Type != syscall.F_WRLCK || got.Pid != 1 {
		t.Fatalf("GetLk = %+v, want owner1's WRLCK", got)
	}
	// Listing goes through the Meta contract.
	pls, _, lerr := h.opts.Engine.Bridge.ListLocks(ctx, eh.ino)
	if lerr != nil || len(pls) != 1 {
		t.Fatalf("ListLocks = %d items, err=%v; want 1", len(pls), lerr)
	}
	// flock goes through the same handle.
	if err := eh.SetLk(ctx, fsFlock(1, syscall.F_WRLCK), false); err != nil {
		t.Fatalf("flock: %v", err)
	}
	if err := eh.SetLk(ctx, fsFlock(2, syscall.F_WRLCK), false); err == nil {
		t.Fatal("second flock succeeded against an exclusive one")
	}
}

// F_SETLKW (block=true) must WAIT for the holder, not report "busy".
//
// The kernel does not retry a failed F_SETLKW: it returns the error to the
// program. Reporting EAGAIN here makes fcntl(fd, F_SETLKW) fail immediately
// where the program expects to block.
func TestBlockingLockWaitsForTheHolder(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(31)

	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatalf("holder: %v", e)
	}

	acquired := make(chan syscall.Errno, 1)
	go func() {
		acquired <- lt.Setlk(context.Background(), ino, 2, 2, syscall.F_WRLCK, 0, 99, true)
	}()
	// It must not return while owner 1 holds the range.
	select {
	case e := <-acquired:
		t.Fatalf("blocking request returned %v while the lock was held; it must wait", e)
	case <-time.After(50 * time.Millisecond):
	}
	// Releasing the holder must hand the lock to the waiter.
	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_UNLCK, 0, 99, false); e != 0 {
		t.Fatalf("unlock: %v", e)
	}
	select {
	case e := <-acquired:
		if e != 0 {
			t.Fatalf("blocking request failed after the holder released: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking request never woke after the holder released")
	}
	// And the waiter really owns it now.
	if e := lt.Setlk(context.Background(), ino, 3, 3, syscall.F_WRLCK, 0, 99, false); e != syscall.EAGAIN {
		t.Fatalf("third writer = %v, want EAGAIN (owner 2 should hold it)", e)
	}
}

// A blocking request must stop waiting when it is cancelled, rather than
// pinning the FUSE goroutine forever.
func TestBlockingLockHonoursCancellation(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(32)
	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan syscall.Errno, 1)
	go func() {
		done <- lt.Setlk(ctx, ino, 2, 2, syscall.F_WRLCK, 0, 99, true)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case e := <-done:
		if e != syscall.EINTR {
			t.Fatalf("cancelled blocking request = %v, want EINTR", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled blocking request never returned")
	}
}

// Blocking flock (flock(2) without LOCK_NB) waits for the same reason.
func TestBlockingFlockWaitsForTheHolder(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(33)
	if e := lt.Flock(context.Background(), ino, 1, syscall.F_WRLCK, false); e != 0 {
		t.Fatal(e)
	}
	acquired := make(chan syscall.Errno, 1)
	go func() { acquired <- lt.Flock(context.Background(), ino, 2, syscall.F_WRLCK, true) }()
	select {
	case e := <-acquired:
		t.Fatalf("blocking flock returned %v while held", e)
	case <-time.After(50 * time.Millisecond):
	}
	if e := lt.Flock(context.Background(), ino, 1, syscall.F_UNLCK, false); e != 0 {
		t.Fatalf("unlock: %v", e)
	}
	select {
	case e := <-acquired:
		if e != 0 {
			t.Fatalf("blocking flock failed after release: %v", e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking flock never woke")
	}
}

// JuiceFS's close-time release must actually clear a lock held "to EOF".
//
// VFS.Flush/Release drop a closing descriptor's record locks with
// Setlk(F_UNLCK, 0, 0x7FFFFFFFFFFFFFFF). The table stores "to EOF" as
// lockEOF, so without normalising the sentinel the unlock left the tail
// [2^63, EOF] behind — a phantom lock no owner can ever release, which is
// precisely the wall a second SQLite writer hits.
func TestUnlockToVFSEOFReleasesToEOFLocks(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(41)

	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_WRLCK, 0, lockEOF, false); e != 0 {
		t.Fatalf("lock to EOF: %v", e)
	}
	if e := lt.Setlk(context.Background(), ino, 1, 1, syscall.F_UNLCK, 0, lockVFSUnlockEOF, false); e != 0 {
		t.Fatalf("unlock to VFS EOF: %v", e)
	}
	if pls, fls := lt.ListLocks(ino); len(pls) != 0 || len(fls) != 0 {
		t.Fatalf("unlock-to-EOF left %d posix / %d flock locks: %+v", len(pls), len(fls), pls)
	}
	// The range must be free for a different owner.
	if e := lt.Setlk(context.Background(), ino, 2, 2, syscall.F_WRLCK, 0, lockEOF, false); e != 0 {
		t.Fatalf("next writer blocked by a phantom lock: %v", e)
	}
}

// fsFileLock builds a POSIX byte-range lock payload (inclusive end, as FUSE
// delivers it).
func fsFileLock(owner uint64, typ uint32, start, end int64) fs.FileLock {
	return fs.FileLock{Owner: owner, Pid: uint32(owner), Type: typ, Start: start, End: end}
}

// The multi-process shape SQLite actually runs: several descriptors on ONE
// file, each worker taking and dropping its own locks while the others stay
// open.
//
// Releasing on the path's LAST close is not enough — the last close may never
// come. Every worker in mptester keeps the database open for its whole run, so
// a worker that finishes a transaction and closes its own descriptor must see
// its locks go away while the path is still open, or the next worker's
// EXCLUSIVE waits forever.
func TestFlushReleasesLocksWhileOtherDescriptorsStayOpen(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	ctx := context.Background()

	var handles []*engineHandle
	for i := 0; i < 4; i++ {
		res, err, _ := dispatchCreate(ctx, h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		eh, ok := res.Handle.(*engineHandle)
		if !ok {
			t.Fatalf("handle %d type %T", i, res.Handle)
		}
		handles = append(handles, eh)
	}
	// Four workers, each taking EXCLUSIVE in turn as its transaction commits.
	for i, eh := range handles {
		owner := uint64(i + 1)
		if err := eh.SetLk(ctx, fsFileLock(owner, syscall.F_WRLCK, 0, 99), false); err != nil {
			t.Fatalf("worker %d EXCLUSIVE: %v", i+1, err)
		}
		// A concurrent worker must be refused while this one holds it.
		other := handles[(i+1)%len(handles)]
		if err := other.SetLk(ctx, fsFileLock(uint64(i+5), syscall.F_WRLCK, 0, 99), false); err == nil {
			t.Fatalf("worker %d got EXCLUSIVE while worker %d held it", 5+i, i+1)
		}
		// The transaction ends: this worker closes, the others stay open.
		if err := eh.Flush(fs.WithFlushOwner(ctx, owner)); err != nil {
			t.Fatalf("worker %d flush: %v", i+1, err)
		}
	}
	// Every worker has since closed its transaction; nothing may still be
	// held, whatever the path's handle count.
	pls, fls, lerr := h.opts.Engine.Bridge.ListLocks(ctx, handles[0].ino)
	if lerr != nil {
		t.Fatalf("ListLocks: %v", lerr)
	}
	if len(pls) != 0 || len(fls) != 0 {
		t.Fatalf("%d byte-range and %d flock locks survived every worker's close: %+v", len(pls), len(fls), pls)
	}
}

// close(2) must drop the CLOSING process's locks — not just the last close of
// the path.
//
// The mount negotiates FUSE_POSIX_LOCKS, so the kernel keeps no lock state
// here and never sends a close-time F_UNLCK: whoever answered F_SETLK is the
// only place the lock can die. Dropping it only when the path's last handle
// closes means a process that closed (or crashed) leaves its RESERVED behind,
// and the next SQLite writer's EXCLUSIVE plus hot-journal recovery can never
// run.
func TestFlushReleasesTheClosingOwnersLocks(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	h := newTestHandler(t, f, 0)
	res, err, _ := dispatchCreate(context.Background(), h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh, ok := res.Handle.(*engineHandle)
	if !ok {
		t.Fatalf("handle type %T", res.Handle)
	}
	ctx := context.Background()

	if err := eh.SetLk(ctx, fsFileLock(1, syscall.F_WRLCK, 0, 99), false); err != nil {
		t.Fatalf("owner1 lock: %v", err)
	}
	if err := eh.SetLk(ctx, fsFileLock(2, syscall.F_WRLCK, 0, 99), false); err == nil {
		t.Fatal("second writer succeeded while owner1 held the range")
	}
	// FLUSH carries the closing descriptor's kernel lock owner.
	if err := eh.Flush(fs.WithFlushOwner(ctx, 1)); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := eh.SetLk(ctx, fsFileLock(2, syscall.F_WRLCK, 0, 99), false); err != nil {
		t.Fatalf("owner2 still blocked after owner1 closed: %v", err)
	}
	// The release is scoped to the closing owner: owner 2's live lock still
	// excludes a third writer.
	if err := eh.SetLk(ctx, fsFileLock(3, syscall.F_WRLCK, 0, 99), false); err == nil {
		t.Fatal("flush released locks it did not own")
	}
	// A flush that carries no owner (0) must not touch the table at all.
	if err := eh.Flush(ctx); err != nil {
		t.Fatalf("plain flush: %v", err)
	}
	if err := eh.SetLk(ctx, fsFileLock(4, syscall.F_WRLCK, 0, 99), false); err == nil {
		t.Fatal("a flush without a lock owner released another owner's lock")
	}
	// flock is released at RELEASE, not FLUSH: it belongs to the open file
	// description, so it dies with the descriptor — which is exactly what
	// upstream's VFS.Release does, and why this plane must route through it
	// instead of reaching into the table. The handle is dead afterwards, so
	// the table itself is what reports the result.
	if err := eh.SetLk(ctx, fsFlock(5, syscall.F_WRLCK), false); err != nil {
		t.Fatalf("flock owner5: %v", err)
	}
	if err := eh.SetLk(ctx, fsFlock(6, syscall.F_WRLCK), false); err == nil {
		t.Fatal("second flock succeeded against an exclusive one")
	}
	if err := eh.Release(ctx); err != nil {
		t.Fatalf("release: %v", err)
	}
	pls, fls := h.opts.Engine.Bridge.locks.ListLocks(eh.ino)
	if len(pls) != 0 || len(fls) != 0 {
		t.Fatalf("locks survived the descriptor's release: %d posix, %d flock", len(pls), len(fls))
	}
}

// fsFlock builds a whole-file flock payload.
func fsFlock(owner uint64, typ uint32) fs.FileLock {
	return fs.FileLock{Owner: owner, Type: typ, Flock: true}
}

// Concurrency: exactly one writer may hold the same range at a time, even when
// many goroutines race for it. This is the property SQLite's pager depends on:
// a second writer must be REFUSED, never silently admitted alongside the first.
func TestLockIsMutuallyExclusiveUnderRace(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(21)
	const contenders = 32
	var holders atomic.Int32
	var concurrent atomic.Int32
	var peak atomic.Int32
	done := make(chan struct{}, contenders)

	for i := 0; i < contenders; i++ {
		go func(owner uint64) {
			defer func() { done <- struct{}{} }()
			if e := lt.Setlk(context.Background(), ino, owner, uint32(owner), syscall.F_WRLCK, 0, 99, false); e != 0 {
				return // refused, as it must be
			}
			n := concurrent.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			holders.Add(1)
			time.Sleep(time.Millisecond)
			concurrent.Add(-1)
			_ = lt.Setlk(context.Background(), ino, owner, uint32(owner), syscall.F_UNLCK, 0, 99, false)
		}(uint64(i + 1))
	}
	for i := 0; i < contenders; i++ {
		<-done
	}
	if got := peak.Load(); got > 1 {
		t.Fatalf("%d writers held the same range concurrently; the lock is not exclusive", got)
	}
	if holders.Load() == 0 {
		t.Fatal("no writer ever acquired the lock; the test proves nothing")
	}
}
