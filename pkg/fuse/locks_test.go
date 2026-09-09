package fuse

import (
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func TestGoFuseMountOptionsEnableLocks(t *testing.T) {
	opts := newGoFuseMountOptions(&MountOptions{})
	if !opts.EnableLocks {
		t.Fatalf("EnableLocks = false, want true")
	}
}

func TestDat9FSFileLocksConflictAndUnlock(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, 1, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, 2, 2, 1002, readLock)); st != gofuse.EAGAIN {
		t.Fatalf("conflicting SetLk status = %v, want EAGAIN", st)
	}

	var out gofuse.LkOut
	if st := fs.GetLk(nil, lockInput(10, 2, 2, 1002, readLock), &out); st != gofuse.OK {
		t.Fatalf("GetLk status = %v, want OK", st)
	}
	if out.Lk.Typ != uint32(syscall.F_WRLCK) || out.Lk.Pid != 1001 {
		t.Fatalf("GetLk conflict = %+v, want writer pid 1001", out.Lk)
	}

	unlock := writeLock
	unlock.Typ = uint32(syscall.F_UNLCK)
	if st := fs.SetLk(nil, lockInput(10, 1, 1, 1001, unlock)); st != gofuse.OK {
		t.Fatalf("unlock status = %v, want OK", st)
	}
	if st := fs.SetLk(nil, lockInput(10, 2, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after unlock status = %v, want OK", st)
	}
}

func TestDat9FSFileLocksPartialUnlock(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	full := gofuse.FileLock{Start: 0, End: 99, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, 1, 1, 1001, full)); st != gofuse.OK {
		t.Fatalf("SetLk full status = %v, want OK", st)
	}

	unlock := gofuse.FileLock{Start: 10, End: 19, Typ: uint32(syscall.F_UNLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, 1, 1, 1001, unlock)); st != gofuse.OK {
		t.Fatalf("partial unlock status = %v, want OK", st)
	}

	midRead := gofuse.FileLock{Start: 10, End: 19, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, 2, 2, 1002, midRead)); st != gofuse.OK {
		t.Fatalf("SetLk unlocked middle status = %v, want OK", st)
	}

	leftRead := gofuse.FileLock{Start: 0, End: 9, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, 2, 2, 1002, leftRead)); st != gofuse.EAGAIN {
		t.Fatalf("SetLk still-locked left status = %v, want EAGAIN", st)
	}
}

func TestDat9FSSetLkwWaitsForUnlock(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, 1, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	done := make(chan gofuse.Status, 1)
	cancel := make(chan struct{})
	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	go func() {
		done <- fs.SetLkw(cancel, lockInput(10, 2, 2, 1002, readLock))
	}()

	select {
	case st := <-done:
		t.Fatalf("SetLkw returned before unlock: %v", st)
	case <-time.After(20 * time.Millisecond):
	}

	unlock := writeLock
	unlock.Typ = uint32(syscall.F_UNLCK)
	if st := fs.SetLk(nil, lockInput(10, 1, 1, 1001, unlock)); st != gofuse.OK {
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

func TestDat9FSReleaseDropsOwnerLocks(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	h := &FileHandle{Ino: 10, Path: "/lock"}
	fh := fs.fileHandles.Allocate(h)
	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, fh, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}
	if h.posixLocks&posixLockBit == 0 || h.ofdOwner != 1 {
		t.Fatalf("posixLocks=%d ofdOwner=%d, want bit 2 and owner 1", h.posixLocks, h.ofdOwner)
	}

	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: 10}, Fh: fh, LockOwner: 1})

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, fh+1, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after release status = %v, want OK", st)
	}
}

func TestDat9FSReleaseUnlocksOfdOwnerWhenLockOwnerZero(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	h := &FileHandle{Ino: 10, Path: "/lock"}
	fh := fs.fileHandles.Allocate(h)
	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, fh, 0xabc, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	// JuiceFS: kernel 3.1- does not pass lock_owner in Release; unlock ofdOwner.
	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: 10}, Fh: fh, LockOwner: 0})

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, fh+1, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after Release(lock_owner=0) status = %v, want OK", st)
	}
}

func TestDat9FSFlushDropsOwnerLocks(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	h := &FileHandle{Ino: 10, Path: "/lock"}
	fh := fs.fileHandles.Allocate(h)
	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, fh, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	if st := fs.Flush(nil, &gofuse.FlushIn{
		InHeader: gofuse.InHeader{
			NodeId: 10,
			Caller: gofuse.Caller{
				Pid: 1001,
			},
		},
		Fh:        fh,
		LockOwner: 1,
	}); st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, fh+1, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after flush status = %v, want OK", st)
	}
}

func TestDat9FSFlushDoesNotDropLocksForHandleThatNeverSetLk(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	writer := &FileHandle{Ino: 10, Path: "/lock"}
	sibling := &FileHandle{Ino: 10, Path: "/lock"}
	wfh := fs.fileHandles.Allocate(writer)
	sfh := fs.fileHandles.Allocate(sibling)
	writeLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, wfh, 1, 1001, writeLock)); st != gofuse.OK {
		t.Fatalf("SetLk writer status = %v, want OK", st)
	}

	// JuiceFS VFS.Flush: sibling h.locks&2==0, so UNLCK is skipped even if
	// FlushIn.LockOwner matches the writer's owner.
	if st := fs.Flush(nil, &gofuse.FlushIn{
		InHeader:  gofuse.InHeader{NodeId: 10},
		Fh:        sfh,
		LockOwner: 1,
	}); st != gofuse.OK {
		t.Fatalf("sibling Flush status = %v, want OK", st)
	}

	readLock := gofuse.FileLock{Start: 0, End: 1023, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, sfh, 2, 1002, readLock)); st != gofuse.EAGAIN {
		t.Fatalf("SetLk after sibling Flush status = %v, want EAGAIN", st)
	}

	if st := fs.Flush(nil, &gofuse.FlushIn{
		InHeader:  gofuse.InHeader{NodeId: 10},
		Fh:        wfh,
		LockOwner: 1,
	}); st != gofuse.OK {
		t.Fatalf("writer Flush status = %v, want OK", st)
	}
	if st := fs.SetLk(nil, lockInput(10, sfh, 2, 1002, readLock)); st != gofuse.OK {
		t.Fatalf("SetLk after writer Flush status = %v, want OK", st)
	}
}

func TestDat9FSFlockDoesNotConflictWithPosixByteLock(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	h := &FileHandle{Ino: 10, Path: "/lock"}
	fh := fs.fileHandles.Allocate(h)
	flock := gofuse.FileLock{Start: 0, End: (1 << 63) - 1, Typ: uint32(syscall.F_WRLCK), Pid: 1001}
	in := lockInput(10, fh, 1, 1001, flock)
	in.LkFlags = gofuse.FUSE_LK_FLOCK
	if st := fs.SetLk(nil, in); st != gofuse.OK {
		t.Fatalf("Flock status = %v, want OK", st)
	}
	if h.posixLocks&flockBit == 0 || h.flockOwner != 1 {
		t.Fatalf("posixLocks=%d flockOwner=%d, want flock bit and owner 1", h.posixLocks, h.flockOwner)
	}

	posix := gofuse.FileLock{Start: 0x40000000, End: 0x40000000, Typ: uint32(syscall.F_RDLCK), Pid: 1002}
	if st := fs.SetLk(nil, lockInput(10, fh+1, 2, 1002, posix)); st != gofuse.OK {
		t.Fatalf("posix SetLk after Flock status = %v, want OK (JuiceFS separates flock and SETLK)", st)
	}

	other := lockInput(10, fh+1, 3, 1003, flock)
	other.LkFlags = gofuse.FUSE_LK_FLOCK
	if st := fs.SetLk(nil, other); st != gofuse.EAGAIN {
		t.Fatalf("conflicting Flock status = %v, want EAGAIN", st)
	}

	fs.Release(nil, &gofuse.ReleaseIn{InHeader: gofuse.InHeader{NodeId: 10}, Fh: fh, LockOwner: 1})
	if st := fs.SetLk(nil, other); st != gofuse.OK {
		t.Fatalf("Flock after Release status = %v, want OK", st)
	}
}

func TestDat9FSFileLocksRejectInvalidType(t *testing.T) {
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), &MountOptions{})
	lock := gofuse.FileLock{Start: 0, End: 1023, Typ: 99, Pid: 1001}
	if st := fs.SetLk(nil, lockInput(10, 1, 1, 1001, lock)); st != gofuse.EINVAL {
		t.Fatalf("invalid SetLk status = %v, want EINVAL", st)
	}
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
