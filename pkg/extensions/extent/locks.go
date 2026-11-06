package extent

import (
	"context"
	"sort"
	"sync"
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
)

// Mount-local advisory locking.
//
// The extent plane serves a single mount: one daemon, one credential, one
// namespace view. JuiceFS's Meta contract includes POSIX byte-range locks
// (Setlk/Getlk) and BSD flock, and the community VFS calls them for
// fcntl(F_SETLK)/flock(2). Without an implementation those calls fall through
// to the embedded nil meta.Meta and PANIC; returning ENOLCK instead makes every
// SQLite pager open fail with "database is locked" because SQLite treats the
// failure as contention and never retries successfully.
//
// The table below is deliberately in-memory and mount-scoped:
//
//   - it is NOT durable and NOT shared between mounts. Two mounts (or two
//     machines) writing one database still do not see each other's locks, so
//     the extent plane gives no cross-client exclusion. That is a real
//     limitation, and it is why the mount refuses rather than pretends:
//     a lock held here is advertised only to this daemon's own descriptors.
//   - Locks die with the process. A crashed mount leaves no stale lock behind,
//     which is the behaviour a single-writer SQLite workload wants.
//
// Semantics follow the POSIX/fcntl rules the kernel relies on:
//
//   - byte ranges are inclusive [Start, End]; End == lockEOF means "to EOF"
//     and is normalised to lockEOF so a later extend does not shrink it;
//   - F_RDLCK/F_WRLCK: the new request must not conflict with ANY range held
//     by a DIFFERENT owner. Same-owner requests replace theirs;
//   - replacing is range-splitting: a new range truncates and fragments the
//     owner's overlapping older ranges, exactly as fcntl(F_SETLK) requires;
//   - F_UNLCK removes/truncates the owner's ranges only;
//   - flock is whole-file and independent of byte ranges (two separate
//     namespaces in POSIX), tracked per owner.
const lockEOF = ^uint64(0)

// lockRange is one held byte-range lock.
type lockRange struct {
	owner uint64
	pid   uint32
	typ   uint32 // F_RDLCK | F_WRLCK
	start uint64
	end   uint64 // inclusive; lockEOF = to EOF
}

// lockTable holds the mount's advisory locks. Every method takes mu; the table
// never calls out while holding it.
type lockTable struct {
	mu sync.Mutex
	// plocks are POSIX byte-range locks, per inode.
	plocks map[meta.Ino][]lockRange
	// flocks are whole-file BSD locks, per inode, keyed by owner.
	flocks map[meta.Ino]map[uint64]uint32
}

func newLockTable() *lockTable {
	return &lockTable{
		plocks: make(map[meta.Ino][]lockRange),
		flocks: make(map[meta.Ino]map[uint64]uint32),
	}
}

// rangesOverlap reports whether [a1,a2] and [b1,b2] intersect, treating
// lockEOF as unbounded.
func rangesOverlap(a1, a2, b1, b2 uint64) bool {
	return a1 <= b2 && b1 <= a2
}

// normalizeRange converts an end past the file into lockEOF so a range that is
// "to EOF now" stays "to EOF" as the file grows (POSIX: the lock extends).
func normalizeRange(start, end uint64) (uint64, uint64) {
	if end < start {
		return start, start - 1 // empty range; caller rejects it
	}
	return start, end
}

// Setlk installs a byte-range lock for owner, replacing that owner's
// overlapping ranges. block=false must not wait: returning EAGAIN lets the
// caller (SQLite) decide, which is what F_SETLK means.
func (t *lockTable) Setlk(ino meta.Ino, owner uint64, pid uint32, typ uint32, start, end uint64, block bool) syscall.Errno {
	if typ != syscall.F_RDLCK && typ != syscall.F_WRLCK && typ != syscall.F_UNLCK {
		return syscall.EINVAL
	}
	start, end = normalizeRange(start, end)
	if end < start {
		return syscall.EINVAL
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if typ != syscall.F_UNLCK && t.conflictsLocked(ino, owner, typ, start, end) {
		// POSIX F_SETLK answers EACCES/EAGAIN on contention; SQLite
		// distinguishes only "busy" from "broken", and EAGAIN is what the
		// kernel returns for fcntl(F_SETLK). A blocking request
		// (F_SETLKW) cannot be serviced here — this table has no wait queue
		// — so it reports the same contention.
		return syscall.EAGAIN
	}
	t.removeOwnerRangesLocked(ino, owner, start, end)
	if typ != syscall.F_UNLCK {
		t.plocks[ino] = append(t.plocks[ino], lockRange{owner: owner, pid: pid, typ: typ, start: start, end: end})
	}
	if len(t.plocks[ino]) == 0 {
		delete(t.plocks, ino)
	}
	return 0
}

// conflictsLocked reports whether any OTHER owner holds a conflicting range.
// Caller holds mu.
func (t *lockTable) conflictsLocked(ino meta.Ino, owner uint64, typ uint32, start, end uint64) bool {
	for _, l := range t.plocks[ino] {
		if l.owner == owner {
			continue
		}
		if !rangesOverlap(l.start, l.end, start, end) {
			continue
		}
		// Two read locks never conflict; anything involving a write does.
		if typ == syscall.F_WRLCK || l.typ == syscall.F_WRLCK {
			return true
		}
	}
	return false
}

// removeOwnerRangesLocked drops (or truncates) the owner's ranges overlapping
// [start,end]. Truncation is what makes fcntl range replacement correct: a new
// [10,20] over an existing [0,30] leaves [0,9] and [21,30].
func (t *lockTable) removeOwnerRangesLocked(ino meta.Ino, owner uint64, start, end uint64) {
	in := t.plocks[ino]
	if len(in) == 0 {
		return
	}
	out := in[:0]
	for _, l := range in {
		if l.owner != owner || !rangesOverlap(l.start, l.end, start, end) {
			out = append(out, l)
			continue
		}
		// Keep the part before the removed range.
		if l.start < start {
			head := l
			head.end = start - 1
			out = append(out, head)
		}
		// Keep the part after it.
		if l.end > end && end != lockEOF {
			tail := l
			tail.start = end + 1
			out = append(out, tail)
		}
	}
	if len(out) == 0 {
		delete(t.plocks, ino)
		return
	}
	t.plocks[ino] = out
}

// Getlk reports the first lock that WOULD block a request by owner. A free
// range answers F_UNLCK. Only conflicts from other owners are reported, which
// is the contract F_GETLK specifies.
func (t *lockTable) Getlk(ino meta.Ino, owner uint64, typ uint32, start, end uint64) (uint32, uint64, uint64, uint32, syscall.Errno) {
	if typ != syscall.F_RDLCK && typ != syscall.F_WRLCK {
		return 0, 0, 0, 0, syscall.EINVAL
	}
	start, end = normalizeRange(start, end)
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, l := range t.plocks[ino] {
		if l.owner == owner || !rangesOverlap(l.start, l.end, start, end) {
			continue
		}
		if l.typ == syscall.F_WRLCK || typ == syscall.F_WRLCK {
			return l.typ, l.start, l.end, l.pid, 0
		}
	}
	return syscall.F_UNLCK, start, end, 0, 0
}

// Flock installs a whole-file BSD lock. flock locks conflict regardless of the
// range namespace, and are per owner.
func (t *lockTable) Flock(ino meta.Ino, owner uint64, typ uint32, block bool) syscall.Errno {
	switch typ {
	case syscall.F_RDLCK, syscall.F_WRLCK, syscall.F_UNLCK:
	default:
		return syscall.EINVAL
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if typ == syscall.F_UNLCK {
		if m := t.flocks[ino]; m != nil {
			delete(m, owner)
			if len(m) == 0 {
				delete(t.flocks, ino)
			}
		}
		return 0
	}
	for other, held := range t.flocks[ino] {
		if other == owner {
			continue
		}
		if held == syscall.F_WRLCK || typ == syscall.F_WRLCK {
			return syscall.EAGAIN
		}
	}
	m := t.flocks[ino]
	if m == nil {
		m = make(map[uint64]uint32)
		t.flocks[ino] = m
	}
	m[owner] = typ
	return 0
}

// ListLocks reports every lock held on an inode, for the Meta contract
// (JuiceFS uses it to display and to clean stale locks).
func (t *lockTable) ListLocks(ino meta.Ino) ([]meta.PLockItem, []meta.FLockItem) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var pls []meta.PLockItem
	for _, l := range t.plocks[ino] {
		var item meta.PLockItem
		item.Owner = l.owner
		item.Type = l.typ
		item.Pid = l.pid
		item.Start = l.start
		item.End = l.end
		pls = append(pls, item)
	}
	sort.Slice(pls, func(i, j int) bool {
		if pls[i].Start != pls[j].Start {
			return pls[i].Start < pls[j].Start
		}
		return pls[i].Owner < pls[j].Owner
	})
	var fls []meta.FLockItem
	for owner, typ := range t.flocks[ino] {
		var item meta.FLockItem
		item.Owner = owner
		if typ == syscall.F_WRLCK {
			item.Type = "WRLCK"
		} else {
			item.Type = "RDLCK"
		}
		fls = append(fls, item)
	}
	sort.Slice(fls, func(i, j int) bool { return fls[i].Owner < fls[j].Owner })
	return pls, fls
}

// ReleaseOwner drops every lock a session/process owned on an inode. Called
// when the last descriptor for an inode closes: POSIX releases all of a
// process's locks on a file when any descriptor for it is closed.
func (t *lockTable) ReleaseOwner(ino meta.Ino, owner uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if m := t.flocks[ino]; m != nil {
		delete(m, owner)
		if len(m) == 0 {
			delete(t.flocks, ino)
		}
	}
	in := t.plocks[ino]
	if len(in) == 0 {
		return
	}
	out := in[:0]
	for _, l := range in {
		if l.owner != owner {
			out = append(out, l)
		}
	}
	if len(out) == 0 {
		delete(t.plocks, ino)
		return
	}
	t.plocks[ino] = out
}

// ReleaseInode drops every lock on an inode, whatever the owner. Used when the
// inode itself goes away (unlink of the last name, or a path forgetting).
func (t *lockTable) ReleaseInode(ino meta.Ino) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.plocks, ino)
	delete(t.flocks, ino)
}

// ---- Meta surface ----

// Flock implements meta.Meta.
func (b *Bridge) Flock(ctx meta.Context, ino meta.Ino, owner uint64, ltype uint32, block bool) syscall.Errno {
	return b.locks.Flock(ino, owner, ltype, block)
}

// Setlk implements meta.Meta.
func (b *Bridge) Setlk(ctx meta.Context, ino meta.Ino, owner uint64, block bool, ltype uint32, start, end uint64, pid uint32) syscall.Errno {
	return b.locks.Setlk(ino, owner, pid, ltype, start, end, block)
}

// Getlk implements meta.Meta.
func (b *Bridge) Getlk(ctx meta.Context, ino meta.Ino, owner uint64, ltype *uint32, start, end *uint64, pid *uint32) syscall.Errno {
	got, s, e, p, errno := b.locks.Getlk(ino, owner, *ltype, *start, *end)
	if errno != 0 {
		return errno
	}
	*ltype, *start, *end = got, s, e
	if got == syscall.F_UNLCK {
		*pid = 0
	} else {
		*pid = p
	}
	return 0
}

// ListLocks implements meta.Meta.
func (b *Bridge) ListLocks(ctx context.Context, inode meta.Ino) ([]meta.PLockItem, []meta.FLockItem, error) {
	pls, fls := b.locks.ListLocks(inode)
	return pls, fls, nil
}

// ReleaseLocksForIno drops every lock on an inode. The plane calls it when a
// file's last descriptor closes.
func (b *Bridge) ReleaseLocksForIno(ino meta.Ino) { b.locks.ReleaseInode(ino) }
