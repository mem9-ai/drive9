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
	changed chan struct{}
}

func newFuseLockTable() *fuseLockTable {
	return &fuseLockTable{
		locks:   make(map[uint64][]fuseHeldLock),
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
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	return fs.locks.set(cancel, input.NodeId, owner, input.Pid, input.Lk, false)
}

func (fs *Dat9FS) SetLkw(cancel <-chan struct{}, input *gofuse.LkIn) (code gofuse.Status) {
	owner := fuseLockOwner(input.Owner, input.Pid, input.Fh)
	return fs.locks.set(cancel, input.NodeId, owner, input.Pid, input.Lk, true)
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
