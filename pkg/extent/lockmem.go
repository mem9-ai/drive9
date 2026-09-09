package extent

import (
	"sort"
	"sync"
	"syscall"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

// memLockMeta is JuiceFS sql_lock's in-process wait loop. A single FUSE
// mount is the POSIX coordinator (locks die with the daemon, like the
// kernel table). sql_lock persists because its meta *is* local SQL; an
// HTTP persist after every grant turned each sqlite BEGIN IMMEDIATE into
// a TiDB RPC and could EAGAIN-rollback a local grant if the server still
// held a stale row.
type memLockMeta struct {
	jfsmeta.Meta
	mu     sync.Mutex
	cond   *sync.Cond
	plocks map[jfsmeta.Ino][]memOwnerLocks
	flocks map[jfsmeta.Ino]map[uint64]uint32
}

type memOwnerLocks struct {
	owner uint64
	recs  []memPLock
}

type memPLock struct {
	typ        uint32
	start, end uint64
	pid        uint32
}

func wrapLockMeta(inner jfsmeta.Meta) jfsmeta.Meta {
	m := &memLockMeta{
		Meta:   inner,
		plocks: make(map[jfsmeta.Ino][]memOwnerLocks),
		flocks: make(map[jfsmeta.Ino]map[uint64]uint32),
	}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func memIsUnlock(t uint32) bool {
	return t == uint32(syscall.F_UNLCK) || t == 'U' || t == 2
}

func memIsWrite(t uint32) bool {
	return t == uint32(syscall.F_WRLCK) || t == 'W' || t == 3
}

func memOverlap(a0, a1, b0, b1 uint64) bool {
	return a1 >= b0 && a0 <= b1
}

func (m *memLockMeta) Setlk(ctx jfsmeta.Context, inode jfsmeta.Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	nl := memPLock{typ: ltype, start: start, end: end, pid: pid}
	for {
		m.mu.Lock()
		if !memIsUnlock(ltype) {
			if _, busy := m.plockConflict(inode, owner, nl); busy {
				if !block {
					m.mu.Unlock()
					return syscall.EAGAIN
				}
				if ctx != nil && ctx.Canceled() {
					m.mu.Unlock()
					return syscall.EINTR
				}
				wake := time.AfterFunc(10*time.Millisecond, func() { m.cond.Broadcast() })
				m.cond.Wait()
				wake.Stop()
				m.mu.Unlock()
				continue
			}
		}
		m.plockApply(inode, owner, nl)
		m.cond.Broadcast()
		m.mu.Unlock()
		return 0
	}
}

func (m *memLockMeta) Flock(ctx jfsmeta.Context, inode jfsmeta.Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	for {
		m.mu.Lock()
		if !memIsUnlock(ltype) {
			if m.flockConflict(inode, owner, ltype) {
				if !block {
					m.mu.Unlock()
					return syscall.EAGAIN
				}
				if ctx != nil && ctx.Canceled() {
					m.mu.Unlock()
					return syscall.EINTR
				}
				wake := time.AfterFunc(10*time.Millisecond, func() { m.cond.Broadcast() })
				m.cond.Wait()
				wake.Stop()
				m.mu.Unlock()
				continue
			}
		}
		m.flockApply(inode, owner, ltype)
		m.cond.Broadcast()
		m.mu.Unlock()
		return 0
	}
}

func (m *memLockMeta) WaitWrites(inode jfsmeta.Ino) syscall.Errno {
	type waitWritesMeta interface {
		WaitWrites(jfsmeta.Ino) syscall.Errno
	}
	if b, ok := m.Meta.(waitWritesMeta); ok {
		return b.WaitWrites(inode)
	}
	return 0
}

func (m *memLockMeta) QueueWriteParts(ctx jfsmeta.Context, inode jfsmeta.Ino, indx uint32, parts []jfsmeta.WritePart, mtime time.Time) syscall.Errno {
	type queueWritePartsMeta interface {
		QueueWriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	}
	if b, ok := m.Meta.(queueWritePartsMeta); ok {
		return b.QueueWriteParts(ctx, inode, indx, parts, mtime)
	}
	return m.WriteParts(ctx, inode, indx, parts, mtime)
}

func (m *memLockMeta) WriteParts(ctx jfsmeta.Context, inode jfsmeta.Ino, indx uint32, parts []jfsmeta.WritePart, mtime time.Time) syscall.Errno {
	type writePartsMeta interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	}
	if b, ok := m.Meta.(writePartsMeta); ok {
		return b.WriteParts(ctx, inode, indx, parts, mtime)
	}
	var st syscall.Errno
	for _, p := range parts {
		st = m.Meta.Write(ctx, inode, indx, p.Off, p.Slice, mtime)
		if st != 0 {
			return st
		}
	}
	return 0
}

func (m *memLockMeta) Getlk(ctx jfsmeta.Context, inode jfsmeta.Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	m.mu.Lock()
	defer m.mu.Unlock()
	nl := memPLock{typ: *ltype, start: *start, end: *end, pid: *pid}
	if rec, busy := m.plockConflict(inode, owner, nl); busy {
		*ltype = rec.typ
		*start = rec.start
		*end = rec.end
		*pid = rec.pid
		return 0
	}
	*ltype = uint32(syscall.F_UNLCK)
	return 0
}

func (m *memLockMeta) plockConflict(inode jfsmeta.Ino, owner uint64, nl memPLock) (memPLock, bool) {
	wantWrite := memIsWrite(nl.typ)
	for _, h := range m.plocks[inode] {
		if h.owner == owner {
			continue
		}
		for _, rec := range h.recs {
			if !memOverlap(nl.start, nl.end, rec.start, rec.end) {
				continue
			}
			if wantWrite || memIsWrite(rec.typ) {
				return rec, true
			}
		}
	}
	return memPLock{}, false
}

func (m *memLockMeta) plockApply(inode jfsmeta.Ino, owner uint64, nl memPLock) {
	held := m.plocks[inode]
	idx := -1
	var mine []memPLock
	for i, h := range held {
		if h.owner == owner {
			idx = i
			mine = h.recs
			break
		}
	}
	next := memUpdateLocks(mine, nl)
	if len(next) == 0 {
		if idx >= 0 {
			m.plocks[inode] = append(held[:idx], held[idx+1:]...)
			if len(m.plocks[inode]) == 0 {
				delete(m.plocks, inode)
			}
		}
		return
	}
	if idx >= 0 {
		held[idx].recs = next
		return
	}
	m.plocks[inode] = append(held, memOwnerLocks{owner: owner, recs: next})
}

func (m *memLockMeta) flockConflict(inode jfsmeta.Ino, owner uint64, ltype uint32) bool {
	cur := m.flocks[inode]
	if cur == nil {
		return false
	}
	wantWrite := memIsWrite(ltype)
	for o, t := range cur {
		if o == owner {
			continue
		}
		if wantWrite || memIsWrite(t) {
			return true
		}
	}
	return false
}

func (m *memLockMeta) flockApply(inode jfsmeta.Ino, owner uint64, ltype uint32) {
	cur := m.flocks[inode]
	if memIsUnlock(ltype) {
		if cur != nil {
			delete(cur, owner)
			if len(cur) == 0 {
				delete(m.flocks, inode)
			}
		}
		return
	}
	if cur == nil {
		cur = make(map[uint64]uint32)
		m.flocks[inode] = cur
	}
	if memIsWrite(ltype) {
		cur[owner] = uint32('W')
	} else {
		cur[owner] = uint32('R')
	}
}

func memUpdateLocks(ls []memPLock, nl memPLock) []memPLock {
	size := len(ls)
	for i := 0; i < size && nl.start <= nl.end; i++ {
		l := ls[i]
		if nl.start < l.start && nl.end >= l.start {
			ls = append(ls, nl)
			ls[len(ls)-1].end = l.start - 1
			nl.start = l.start
		}
		if nl.start > l.start && nl.start <= l.end {
			l.end = nl.start - 1
			ls = append(ls, l)
			ls[i].start = nl.start
			l = ls[i]
		}
		if nl.start == l.start {
			ls[i].typ = nl.typ
			ls[i].pid = nl.pid
			if l.end > nl.end {
				ls[i].end = nl.end
				l.start = nl.end + 1
				ls = append(ls, l)
			}
			nl.start = ls[i].end + 1
		}
	}
	if nl.start <= nl.end {
		ls = append(ls, nl)
	}
	// JuiceFS updateLocks sorts by start after split-append so merge/UNLCK
	// see a linear range. sqlite DELETE PENDING@0x40000000 then SHARED@0x40000002
	// appends out of order; without sort a full-range UNLCK can leave a fragment.
	sort.Slice(ls, func(i, j int) bool { return ls[i].start < ls[j].start })
	for i := 0; i < len(ls); {
		if memIsUnlock(ls[i].typ) || ls[i].start > ls[i].end {
			copy(ls[i:], ls[i+1:])
			ls = ls[:len(ls)-1]
			continue
		}
		if i+1 < len(ls) && ls[i].typ == ls[i+1].typ && ls[i].pid == ls[i+1].pid && ls[i].end+1 == ls[i+1].start {
			ls[i].end = ls[i+1].end
			ls[i+1].start = ls[i+1].end + 1
		}
		i++
	}
	return ls
}
