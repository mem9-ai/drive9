package fuse

// Ports of v1 pkg/fuse/locks_test.go for the chain facade. locks_xattr_test.go
// already pins the fuseLockTable's split/release and blocking-wake rows; these
// add the facade-level POSIX lock contract (SetLk/GetLk/SetLkw through the
// gofuse entry points), the EINTR cancel row, the Release/Flush owner-drop
// rows, the EINVAL type guard, and the EnableLocks mount option.

import (
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestGoFuseMountOptionsEnableLocks(t *testing.T) {
	opts := newGoFuseMountOptions(MountConfig{})
	if !opts.EnableLocks {
		t.Fatalf("EnableLocks = false, want true")
	}
}

// lockTestFS builds a facade whose lock ops never need the chain.
func lockTestFS(t *testing.T) *FS {
	t.Helper()
	core := newMemCore()
	core.writeAt("/f", 0, []byte("locked"))
	return newTestFS(t, core)
}

func lockInput(node uint64, fh uint64, owner uint64, pid uint32, lk gofuse.FileLock) *gofuse.LkIn {
	return &gofuse.LkIn{
		InHeader: gofuse.InHeader{
			NodeId: node,
			Caller: gofuse.Caller{
				Pid: pid,
			},
		},
		Fh:    fh,
		Owner: owner,
		Lk:    lk,
	}
}

func TestFacadeFileLocksConflictAndUnlock(t *testing.T) {
	f := lockTestFS(t)
	ino := openTestDir(t, f, "f")

	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := f.SetLk(nil, lockInput(ino, 2, 2, 1002, readLock)); st != gofuse.EAGAIN {
		t.Fatalf("conflicting SetLk status = %v, want EAGAIN", st)
	}

	var out gofuse.LkOut
	if st := f.GetLk(nil, lockInput(ino, 2, 2, 1002, readLock), &out); st != gofuse.OK {
		t.Fatalf("GetLk status = %v, want OK", st)
	}
	if out.Lk.Typ != uint32(syscall.F_WRLCK) || out.Lk.Pid != 1001 {
		t.Fatalf("GetLk conflict = %+v, want writer pid 1001", out.Lk)
	}

	unlock := writeLock
	unlock.Typ = uint32(syscall.F_UNLCK)
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, unlock)); st != gofuse.OK {
		t.Fatalf("unlock status = %v, want OK", st)
	}
	if st := f.SetLk(nil, lockInput(ino, 2, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after unlock status = %v, want OK", st)
	}
}

func TestFacadeFileLocksPartialUnlock(t *testing.T) {
	f := lockTestFS(t)
	ino := openTestDir(t, f, "f")

	full := gofuse.FileLock{Start: 0, End: 99, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, full)); st != gofuse.OK {
		t.Fatalf("SetLk full status = %v, want OK", st)
	}

	unlock := gofuse.FileLock{Start: 10, End: 19, Typ: uint32(syscall.F_UNLCK), Pid: 1001}
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, unlock)); st != gofuse.OK {
		t.Fatalf("partial unlock status = %v, want OK", st)
	}

	midRead := gofuse.FileLock{Start: 10, End: 19, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := f.SetLk(nil, lockInput(ino, 2, 2, 1002, midRead)); st != gofuse.OK {
		t.Fatalf("SetLk unlocked middle status = %v, want OK", st)
	}

	leftRead := gofuse.FileLock{Start: 0, End: 9, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := f.SetLk(nil, lockInput(ino, 2, 2, 1002, leftRead)); st != gofuse.EAGAIN {
		t.Fatalf("SetLk still-locked left status = %v, want EAGAIN", st)
	}
}

func TestFacadeSetLkwWaitsForUnlock(t *testing.T) {
	f := lockTestFS(t)
	ino := openTestDir(t, f, "f")

	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	done := make(chan gofuse.Status, 1)
	cancel := make(chan struct{})
	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	go func() {
		done <- f.SetLkw(cancel, lockInput(ino, 2, 2, 1002, readLock))
	}()

	select {
	case st := <-done:
		t.Fatalf("SetLkw returned before unlock: %v", st)
	case <-time.After(20 * time.Millisecond):
	}

	unlock := writeLock
	unlock.Typ = uint32(syscall.F_UNLCK)
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, unlock)); st != gofuse.OK {
		t.Fatalf("unlock status = %v, want OK", st)
	}
	select {
	case st := <-done:
		if st != gofuse.OK {
			t.Fatalf("SetLkw status = %v, want OK", st)
		}
	case <-time.After(time.Second):
		close(cancel)
		t.Fatalf("SetLkw did not return after unlock")
	}
}

// v1 row: a blocked SETLKW answers EINTR when its cancel channel fires
// before the conflicting lock goes away.
func TestLockTableBlockingCancelReturnsEINTR(t *testing.T) {
	tt := newFuseLockTable()
	wr := gofuse.FileLock{Start: 0, End: 9, Typ: uint32(syscall.F_WRLCK)}
	if st := tt.set(nil, 1, 1, 1, wr, false); st != gofuse.OK {
		t.Fatal(st)
	}
	cancel := make(chan struct{})
	done := make(chan gofuse.Status, 1)
	go func() {
		rd := gofuse.FileLock{Start: 0, End: 9, Typ: uint32(syscall.F_RDLCK)}
		done <- tt.set(cancel, 1, 2, 2, rd, true)
	}()
	time.Sleep(20 * time.Millisecond)
	close(cancel)
	select {
	case st := <-done:
		if st != gofuse.EINTR {
			t.Fatalf("canceled blocking set = %v, want EINTR", st)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled blocking set did not return")
	}
	// The held lock is untouched by the canceled waiter.
	if _, ok := tt.conflict(1, 3, gofuse.FileLock{Start: 0, End: 9, Typ: uint32(syscall.F_RDLCK)}); !ok {
		t.Fatal("canceled waiter disturbed the held write lock")
	}
}

func TestFacadeReleaseDropsOwnerLocks(t *testing.T) {
	f := lockTestFS(t)
	ino := openTestDir(t, f, "f")

	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	f.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: ino}, Fh: 99, LockOwner: 1})

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := f.SetLk(nil, lockInput(ino, 2, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after release status = %v, want OK", st)
	}
}

func TestFacadeFlushDropsOwnerLocks(t *testing.T) {
	f := lockTestFS(t)
	ino := openTestDir(t, f, "f")

	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	if st := f.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{
			NodeId: ino,
			Caller: gofuse.Caller{
				Pid: 1001,
			},
		},
		Fh:        1,
		LockOwner: 1,
	}); st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := f.SetLk(nil, lockInput(ino, 2, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after flush status = %v, want OK", st)
	}
}

func TestFacadeFileLocksRejectInvalidType(t *testing.T) {
	f := lockTestFS(t)
	ino := openTestDir(t, f, "f")
	lock := gofuse.FileLock{Start: 0, End: 1023, Typ: 99, Pid: 1001}
	if st := f.SetLk(nil, lockInput(ino, 1, 1, 1001, lock)); st != gofuse.EINVAL {
		t.Fatalf("invalid SetLk status = %v, want EINVAL", st)
	}
}

// v1 row: two owners can hold overlapping read locks, but a writer excludes
// every other owner (the rw/rw/ww conflict classes).
func TestLockTableOwnerAndConflictClasses(t *testing.T) {
	tt := newFuseLockTable()
	rd := gofuse.FileLock{Start: 0, End: 99, Typ: uint32(syscall.F_RDLCK)}
	if st := tt.set(nil, 1, 1, 1, rd, false); st != gofuse.OK {
		t.Fatal(st)
	}
	// Shared: another reader from another owner coexists.
	if st := tt.set(nil, 1, 2, 2, rd, false); st != gofuse.OK {
		t.Fatalf("overlapping read locks from different owners conflicted: %v", st)
	}
	// Writer vs reader conflicts for both directions.
	wr := gofuse.FileLock{Start: 50, End: 60, Typ: uint32(syscall.F_WRLCK)}
	if st := tt.set(nil, 1, 3, 3, wr, false); st != gofuse.EAGAIN {
		t.Fatalf("writer over reader = %v, want EAGAIN", st)
	}
	if _, ok := tt.conflict(1, 3, wr); !ok {
		t.Fatal("conflict() lost the writer-over-reader conflict")
	}
	// Same-owner overlap never conflicts (F_SETLK re-arms own ranges); a
	// separate node holds only owner 1's locks so the probe is isolated.
	if st := tt.set(nil, 2, 1, 1, rd, false); st != gofuse.OK {
		t.Fatal(st)
	}
	if st := tt.set(nil, 2, 1, 1, wr, false); st != gofuse.OK {
		t.Fatalf("same-owner overlapping re-arm conflicted: %v", st)
	}
	if _, ok := tt.conflict(2, 1, wr); ok {
		t.Fatal("same owner treated as a conflict")
	}
	// release drops only that owner's locks (per inode); the other owner's
	// read lock keeps conflicting with writer probes.
	tt.release(1, 1)
	if _, ok := tt.conflict(1, 3, wr); !ok {
		t.Fatal("release dropped another owner's lock")
	}
	tt.release(1, 2)
	if _, ok := tt.conflict(1, 3, wr); ok {
		t.Fatal("locks survived releasing every owner")
	}
	tt.release(2, 1)
	if _, ok := tt.conflict(2, 3, wr); ok {
		t.Fatal("locks survived releasing the second inode's owner")
	}
}
