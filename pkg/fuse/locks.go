package fuse

import (
	"sync"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type fuseHeldLock struct {
	owner uint64
	pid   uint32
	start uint64
	end   uint64
	typ   uint32
}

type fuseLockTable struct {
	mu      sync.Mutex
	locks   map[uint64][]fuseHeldLock
	flocks  map[uint64]map[uint64]uint32 // node -> (owner^fh) -> F_RDLCK/F_WRLCK
	changed chan struct{}
}

func newFuseLockTable() *fuseLockTable {
	return &fuseLockTable{
		locks:   make(map[uint64][]fuseHeldLock),
		flocks:  make(map[uint64]map[uint64]uint32),
		changed: make(chan struct{}),
	}
}

func (fs *Dat9FS) GetLk(cancel <-chan struct{}, input *gofuse.LkIn, out *gofuse.LkOut) (code gofuse.Status) {
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	if lock, ok := fs.locks.conflict(input.NodeId, owner, input.Lk); ok {
		out.Lk = gofuse.FileLock{
			Start: lock.start,
			End:   lock.end,
			Typ:   lock.typ,
			Pid:   lock.pid,
		}
		return gofuse.OK
	}
	out.Lk = input.Lk
	out.Lk.Typ = uint32(syscall.F_UNLCK)
	return gofuse.OK
}

func (fs *Dat9FS) SetLk(cancel <-chan struct{}, input *gofuse.LkIn) (code gofuse.Status) {
	if input.LkFlags&gofuse.FUSE_LK_FLOCK != 0 {
		return fs.setFlock(cancel, input, false)
	}
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	return fs.setPosixLock(cancel, input, owner, false)
}

func (fs *Dat9FS) SetLkw(cancel <-chan struct{}, input *gofuse.LkIn) (code gofuse.Status) {
	if input.LkFlags&gofuse.FUSE_LK_FLOCK != 0 {
		return fs.setFlock(cancel, input, true)
	}
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	return fs.setPosixLock(cancel, input, owner, true)
}

func (fs *Dat9FS) setPosixLock(cancel <-chan struct{}, input *gofuse.LkIn, owner uint64, blocking bool) gofuse.Status {
	st := fs.locks.set(cancel, input.NodeId, owner, input.Pid, input.Lk, blocking)
	if st != gofuse.OK || input.Lk.Typ == uint32(syscall.F_UNLCK) {
		return st
	}
	if fh, ok := fs.fileHandles.Get(input.Fh); ok {
		fh.markPosixLock(owner)
	}
	return st
}

func fuseLockOwner(owner uint64, pid uint32, fh uint64) uint64 {
	if owner != 0 {
		return owner
	}
	if pid != 0 {
		return uint64(pid)
	}
	return fh
}

const (
	flockBit     uint8 = 1 // JuiceFS handle.locks bit 1 (Flock)
	posixLockBit uint8 = 2 // JuiceFS handle.locks bit 2 (SETLK)
)

func (fs *Dat9FS) setFlock(cancel <-chan struct{}, input *gofuse.LkIn, blocking bool) gofuse.Status {
	owner := input.Owner
	st := fs.locks.flock(cancel, input.NodeId, owner^input.Fh, input.Lk.Typ, blocking)
	if st != gofuse.OK {
		return st
	}
	fh, ok := fs.fileHandles.Get(input.Fh)
	if !ok {
		return st
	}
	fh.Lock()
	if input.Lk.Typ == uint32(syscall.F_UNLCK) {
		fh.posixLocks &= posixLockBit
	} else {
		fh.posixLocks |= flockBit
		fh.flockOwner = owner
	}
	fh.Unlock()
	return st
}

func (t *fuseLockTable) flock(cancel <-chan struct{}, node uint64, owner uint64, typ uint32, blocking bool) gofuse.Status {
	switch typ {
	case uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK):
	default:
		return gofuse.EINVAL
	}
	for {
		t.mu.Lock()
		if typ == uint32(syscall.F_UNLCK) {
			if owners := t.flocks[node]; owners != nil {
				delete(owners, owner)
				if len(owners) == 0 {
					delete(t.flocks, node)
				}
			}
			t.notifyLocked()
			t.mu.Unlock()
			return gofuse.OK
		}
		conflict := false
		for heldOwner, heldTyp := range t.flocks[node] {
			if heldOwner == owner {
				continue
			}
			if heldTyp == uint32(syscall.F_WRLCK) || typ == uint32(syscall.F_WRLCK) {
				conflict = true
				break
			}
		}
		if !conflict {
			owners := t.flocks[node]
			if owners == nil {
				owners = make(map[uint64]uint32)
				t.flocks[node] = owners
			}
			owners[owner] = typ
			t.notifyLocked()
			t.mu.Unlock()
			return gofuse.OK
		}
		if !blocking {
			t.mu.Unlock()
			return gofuse.EAGAIN
		}
		changed := t.changed
		t.mu.Unlock()
		select {
		case <-cancel:
			return gofuse.EINTR
		case <-changed:
		}
	}
}

func (t *fuseLockTable) flockUnlock(node uint64, owner uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	owners := t.flocks[node]
	if owners == nil {
		return
	}
	delete(owners, owner)
	if len(owners) == 0 {
		delete(t.flocks, node)
	}
	t.notifyLocked()
}

func (fh *FileHandle) markPosixLock(owner uint64) {
	fh.Lock()
	fh.posixLocks |= posixLockBit
	if fh.ofdOwner == 0 {
		fh.ofdOwner = owner
	}
	fh.Unlock()
}

// releasePosixLocksOnFlush mirrors JuiceFS VFS.Flush: Setlk(UNLCK) only when
// this handle issued SETLK (h.locks&2). lockOwner is FlushIn.LockOwner as-is;
// if the kernel omitted it, fall back to the ofdOwner stored at SETLK.
func (fs *Dat9FS) releasePosixLocksOnFlush(node uint64, fh *FileHandle, lockOwner uint64) {
	fh.Lock()
	posix := fh.posixLocks&posixLockBit != 0
	ofd := fh.ofdOwner
	if lockOwner != 0 && lockOwner == ofd {
		fh.ofdOwner = 0
	}
	fh.Unlock()
	if !posix {
		return
	}
	owner := lockOwner
	if owner == 0 {
		owner = ofd
	}
	if owner != 0 {
		fs.locks.release(node, owner)
	}
}

// releasePosixLocksOnRelease mirrors JuiceFS VFS.Release: Setlk(UNLCK) using
// ofdOwner, because Release often has lock_owner=0.
func (fs *Dat9FS) releasePosixLocksOnRelease(node uint64, fh *FileHandle) {
	fh.Lock()
	posix := fh.posixLocks&posixLockBit != 0
	ofd := fh.ofdOwner
	fh.ofdOwner = 0
	fh.Unlock()
	if posix && ofd != 0 {
		fs.locks.release(node, ofd)
	}
}

func (fs *Dat9FS) releaseFlockOnRelease(node uint64, fh *FileHandle, handleID uint64) {
	fh.Lock()
	flock := fh.posixLocks&flockBit != 0
	owner := fh.flockOwner
	fh.posixLocks &= posixLockBit
	fh.flockOwner = 0
	fh.Unlock()
	if flock && owner != 0 {
		fs.locks.flockUnlock(node, owner^handleID)
	}
}

func (t *fuseLockTable) set(cancel <-chan struct{}, node uint64, owner uint64, pid uint32, lk gofuse.FileLock, blocking bool) gofuse.Status {
	switch lk.Typ {
	case uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK):
	default:
		return gofuse.EINVAL
	}

	for {
		t.mu.Lock()
		if lk.Typ == uint32(syscall.F_UNLCK) {
			t.removeRangeLocked(node, owner, lk.Start, lk.End)
			t.notifyLocked()
			t.mu.Unlock()
			return gofuse.OK
		}
		if _, ok := t.conflictLocked(node, owner, lk); !ok {
			t.removeRangeLocked(node, owner, lk.Start, lk.End)
			t.locks[node] = append(t.locks[node], fuseHeldLock{
				owner: owner,
				pid:   pid,
				start: lk.Start,
				end:   lk.End,
				typ:   lk.Typ,
			})
			t.notifyLocked()
			t.mu.Unlock()
			return gofuse.OK
		}
		if !blocking {
			t.mu.Unlock()
			return gofuse.EAGAIN
		}
		changed := t.changed
		t.mu.Unlock()

		select {
		case <-cancel:
			return gofuse.EINTR
		case <-changed:
		}
	}
}

func (t *fuseLockTable) conflict(node uint64, owner uint64, lk gofuse.FileLock) (fuseHeldLock, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.conflictLocked(node, owner, lk)
}

func (t *fuseLockTable) conflictLocked(node uint64, owner uint64, lk gofuse.FileLock) (fuseHeldLock, bool) {
	for _, held := range t.locks[node] {
		if held.owner == owner || !rangesOverlap(held.start, held.end, lk.Start, lk.End) {
			continue
		}
		if held.typ == uint32(syscall.F_WRLCK) || lk.Typ == uint32(syscall.F_WRLCK) {
			return held, true
		}
	}
	return fuseHeldLock{}, false
}

func (t *fuseLockTable) release(node uint64, owner uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	locks := t.locks[node]
	kept := locks[:0]
	for _, held := range locks {
		if held.owner != owner {
			kept = append(kept, held)
		}
	}
	if len(kept) == 0 {
		delete(t.locks, node)
	} else {
		t.locks[node] = kept
	}
	t.notifyLocked()
}

func (t *fuseLockTable) removeRangeLocked(node uint64, owner uint64, start uint64, end uint64) {
	locks := t.locks[node]
	kept := locks[:0]
	for _, held := range locks {
		if held.owner != owner || !rangesOverlap(held.start, held.end, start, end) {
			kept = append(kept, held)
			continue
		}
		if held.start < start {
			left := held
			left.end = start - 1
			kept = append(kept, left)
		}
		if held.end > end {
			right := held
			right.start = end + 1
			kept = append(kept, right)
		}
	}
	if len(kept) == 0 {
		delete(t.locks, node)
		return
	}
	t.locks[node] = kept
}

func (t *fuseLockTable) notifyLocked() {
	close(t.changed)
	t.changed = make(chan struct{})
}

func rangesOverlap(aStart uint64, aEnd uint64, bStart uint64, bEnd uint64) bool {
	return aStart <= bEnd && bStart <= aEnd
}
