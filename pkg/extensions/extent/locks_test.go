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

	if e := lt.Setlk(ino, 1, 111, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatalf("owner1 write: %v", e)
	}
	if e := lt.Setlk(ino, 2, 222, syscall.F_WRLCK, 0, 99, false); e != syscall.EAGAIN {
		t.Fatalf("second writer = %v, want EAGAIN", e)
	}
	if e := lt.Setlk(ino, 2, 222, syscall.F_RDLCK, 0, 99, false); e != syscall.EAGAIN {
		t.Fatalf("reader against a writer = %v, want EAGAIN", e)
	}
	// The owner may upgrade/replace its own range.
	if e := lt.Setlk(ino, 1, 111, syscall.F_RDLCK, 0, 99, false); e != 0 {
		t.Fatalf("owner1 downgrade: %v", e)
	}
	// Now another reader is compatible.
	if e := lt.Setlk(ino, 2, 222, syscall.F_RDLCK, 0, 99, false); e != 0 {
		t.Fatalf("second reader after downgrade: %v", e)
	}
	// ...but a writer is not.
	if e := lt.Setlk(ino, 3, 333, syscall.F_WRLCK, 0, 99, false); e != syscall.EAGAIN {
		t.Fatalf("writer against readers = %v, want EAGAIN", e)
	}
	// Disjoint ranges do not conflict.
	if e := lt.Setlk(ino, 3, 333, syscall.F_WRLCK, 200, 299, false); e != 0 {
		t.Fatalf("disjoint writer: %v", e)
	}
}

// Replacing a range must SPLIT the owner's older range, not drop it: this is
// the rule fcntl(F_SETLK) guarantees and SQLite's pager relies on.
func TestLockReplaceSplitsOwnerRanges(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(9)

	if e := lt.Setlk(ino, 1, 1, syscall.F_WRLCK, 0, 30, false); e != 0 {
		t.Fatalf("initial: %v", e)
	}
	// Punch a hole in the middle with a second owner's range: owner1 must keep
	// [0,9] and [21,30].
	if e := lt.Setlk(ino, 2, 2, syscall.F_WRLCK, 10, 20, false); e != syscall.EAGAIN {
		t.Fatalf("hole against owner1 = %v, want EAGAIN (owner1 still holds it)", e)
	}
	if e := lt.Setlk(ino, 1, 1, syscall.F_UNLCK, 10, 20, false); e != 0 {
		t.Fatalf("owner1 unlock middle: %v", e)
	}
	if e := lt.Setlk(ino, 2, 2, syscall.F_WRLCK, 10, 20, false); e != 0 {
		t.Fatalf("owner2 takes the hole: %v", e)
	}
	// Owner1's surviving head and tail must still exclude others.
	if e := lt.Setlk(ino, 3, 3, syscall.F_WRLCK, 0, 5, false); e != syscall.EAGAIN {
		t.Fatalf("head range = %v, want EAGAIN", e)
	}
	if e := lt.Setlk(ino, 3, 3, syscall.F_WRLCK, 25, 30, false); e != syscall.EAGAIN {
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
	if e := lt.Flock(ino, 1, syscall.F_WRLCK, false); e != 0 {
		t.Fatalf("flock exclusive: %v", e)
	}
	if e := lt.Flock(ino, 2, syscall.F_RDLCK, false); e != syscall.EAGAIN {
		t.Fatalf("shared flock against exclusive = %v, want EAGAIN", e)
	}
	// A byte-range lock is a different namespace and must not be blocked.
	if e := lt.Setlk(ino, 2, 2, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatalf("byte-range lock alongside flock: %v", e)
	}
	// Releasing the flock frees it for others.
	if e := lt.Flock(ino, 1, syscall.F_UNLCK, false); e != 0 {
		t.Fatalf("flock unlock: %v", e)
	}
	if e := lt.Flock(ino, 2, syscall.F_RDLCK, false); e != 0 {
		t.Fatalf("shared flock after release: %v", e)
	}
}

// Closing the file's last descriptor must drop every lock on it, or the next
// opener sees a phantom conflict it can never clear.
func TestReleaseLocksOnLastClose(t *testing.T) {
	lt := newLockTable()
	const ino = meta.Ino(13)
	if e := lt.Setlk(ino, 1, 1, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatal(e)
	}
	if e := lt.Flock(ino, 1, syscall.F_WRLCK, false); e != 0 {
		t.Fatal(e)
	}
	lt.ReleaseInode(ino)
	if e := lt.Setlk(ino, 2, 2, syscall.F_WRLCK, 0, 99, false); e != 0 {
		t.Fatalf("lock survived the last close: %v", e)
	}
	if e := lt.Flock(ino, 2, syscall.F_WRLCK, false); e != 0 {
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

// fsFileLock builds a POSIX byte-range lock payload (inclusive end, as FUSE
// delivers it).
func fsFileLock(owner uint64, typ uint32, start, end int64) fs.FileLock {
	return fs.FileLock{Owner: owner, Pid: uint32(owner), Type: typ, Start: start, End: end}
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
			if e := lt.Setlk(ino, owner, uint32(owner), syscall.F_WRLCK, 0, 99, false); e != 0 {
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
			lt.Setlk(ino, owner, uint32(owner), syscall.F_UNLCK, 0, 99, false)
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
