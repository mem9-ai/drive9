// This file implements the extent engine's POSIX record locks (jfs_plock) and
// advisory flocks (jfs_flock), which are authoritative across mounts: two
// mounts of one filesystem exclude each other through these rows rather than
// through per-process state.
package datastore

import (
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"syscall"
)

func jfsIsUnlock(ltype uint32) bool {
	return ltype == jfsLockUnlock || ltype == 'U' || ltype == 2
}
func jfsIsWriteLock(ltype uint32) bool {
	return ltype == jfsLockWrite || ltype == 'W' || ltype == 3
}
func (s *Store) jfsLockInodeTx(tx *sql.Tx, ino uint64) (int, error) {
	var got uint64
	err := tx.QueryRow(`SELECT inode FROM jfs_node WHERE inode = ? FOR UPDATE`, ino).Scan(&got)
	if errors.Is(err, sql.ErrNoRows) {
		return int(syscall.ENOENT), nil
	}
	if err != nil {
		return 0, err
	}
	return 0, nil
}
func (s *Store) jfsFlockTx(tx *sql.Tx, ino, sid, owner uint64, ltype uint32) (int, error) {
	if eno, err := s.jfsLockInodeTx(tx, ino); err != nil || eno != 0 {
		return eno, err
	}
	if jfsIsUnlock(ltype) {
		_, err := tx.Exec(`DELETE FROM jfs_flock WHERE inode = ? AND sid = ? AND owner = ?`, ino, sid, int64(owner))
		return 0, err
	}
	wantWrite := jfsIsWriteLock(ltype)
	var n int
	q := `SELECT COUNT(*) FROM jfs_flock WHERE inode = ? AND NOT (sid = ? AND owner = ?) FOR UPDATE`
	if !wantWrite {
		q = `SELECT COUNT(*) FROM jfs_flock WHERE inode = ? AND NOT (sid = ? AND owner = ?) AND ltype = ? FOR UPDATE`
		if err := tx.QueryRow(q, ino, sid, int64(owner), int('W')).Scan(&n); err != nil {
			return 0, err
		}
	} else if err := tx.QueryRow(q, ino, sid, int64(owner)).Scan(&n); err != nil {
		return 0, err
	}
	if n > 0 {
		return int(syscall.EAGAIN), nil
	}
	lt := byte('R')
	if wantWrite {
		lt = 'W'
	}
	_, err := tx.Exec(`INSERT INTO jfs_flock (inode, sid, owner, ltype) VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE ltype = VALUES(ltype)`, ino, sid, int64(owner), lt)
	return 0, err
}

type jfsPLock struct {
	Type  uint32 `json:"t"`
	Start uint64 `json:"s"`
	End   uint64 `json:"e"`
	Pid   uint32 `json:"p"`
}

func (s *Store) jfsLoadPlocksTx(tx *sql.Tx, ino uint64) ([]struct {
	sid, owner uint64
	recs       []jfsPLock
}, error) {
	return s.jfsLoadPlocksQueryTx(tx, ino, false)
}
func (s *Store) jfsLoadPlocksForUpdateTx(tx *sql.Tx, ino uint64) ([]struct {
	sid, owner uint64
	recs       []jfsPLock
}, error) {
	return s.jfsLoadPlocksQueryTx(tx, ino, true)
}
func (s *Store) jfsLoadPlocksQueryTx(tx *sql.Tx, ino uint64, forUpdate bool) ([]struct {
	sid, owner uint64
	recs       []jfsPLock
}, error) {
	q := `SELECT sid, owner, records FROM jfs_plock WHERE inode = ?`
	if forUpdate {
		q += ` FOR UPDATE`
	}
	rows, err := tx.Query(q, ino)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []struct {
		sid, owner uint64
		recs       []jfsPLock
	}
	for rows.Next() {
		var sid uint64
		var owner int64
		var raw []byte
		if err := rows.Scan(&sid, &owner, &raw); err != nil {
			return nil, err
		}
		var recs []jfsPLock
		if len(raw) > 0 {
			_ = json.Unmarshal(raw, &recs)
		}
		out = append(out, struct {
			sid, owner uint64
			recs       []jfsPLock
		}{sid, uint64(owner), recs})
	}
	return out, rows.Err()
}

// jfsUpdateLocks is JuiceFS meta.updateLocks: split/merge inclusive ranges
// and drop F_UNLCK records. SQLite WAL uses single-byte POSIX locks.
func jfsUpdateLocks(ls []jfsPLock, nl jfsPLock) []jfsPLock {
	size := len(ls)
	for i := 0; i < size && nl.Start <= nl.End; i++ {
		l := ls[i]
		if nl.Start < l.Start && nl.End >= l.Start {
			ls = append(ls, nl)
			ls[len(ls)-1].End = l.Start - 1
			nl.Start = l.Start
		}
		if nl.Start > l.Start && nl.Start <= l.End {
			l.End = nl.Start - 1
			ls = append(ls, l)
			ls[i].Start = nl.Start
			l = ls[i]
		}
		if nl.Start == l.Start {
			ls[i].Type = nl.Type
			ls[i].Pid = nl.Pid
			if l.End > nl.End {
				ls[i].End = nl.End
				l.Start = nl.End + 1
				ls = append(ls, l)
			}
			nl.Start = ls[i].End + 1
		}
	}
	if nl.Start <= nl.End {
		ls = append(ls, nl)
	}
	sort.Slice(ls, func(i, j int) bool { return ls[i].Start < ls[j].Start })
	for i := 0; i < len(ls); {
		if jfsIsUnlock(ls[i].Type) || ls[i].Start > ls[i].End {
			copy(ls[i:], ls[i+1:])
			ls = ls[:len(ls)-1]
			continue
		}
		if i+1 < len(ls) && ls[i].Type == ls[i+1].Type && ls[i].Pid == ls[i+1].Pid && ls[i].End+1 == ls[i+1].Start {
			ls[i].End = ls[i+1].End
			ls[i+1].Start = ls[i+1].End + 1
		}
		i++
	}
	return ls
}
func (s *Store) jfsSavePlockTx(tx *sql.Tx, ino, sid, owner uint64, recs []jfsPLock) error {
	if len(recs) == 0 {
		_, err := tx.Exec(`DELETE FROM jfs_plock WHERE inode = ? AND sid = ? AND owner = ?`, ino, sid, int64(owner))
		return err
	}
	raw, err := json.Marshal(recs)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO jfs_plock (inode, sid, owner, records) VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE records = VALUES(records)`, ino, sid, int64(owner), raw)
	return err
}
func (s *Store) jfsSetlkTx(tx *sql.Tx, ino, sid, owner uint64, ltype uint32, start, end uint64, pid uint32) (int, error) {
	if eno, err := s.jfsLockInodeTx(tx, ino); err != nil || eno != 0 {
		return eno, err
	}
	held, err := s.jfsLoadPlocksForUpdateTx(tx, ino)
	if err != nil {
		return 0, err
	}
	nl := jfsPLock{Type: ltype, Start: start, End: end, Pid: pid}
	if jfsIsUnlock(ltype) {
		for _, h := range held {
			if h.sid != sid || h.owner != owner {
				continue
			}
			return 0, s.jfsSavePlockTx(tx, ino, sid, owner, jfsUpdateLocks(h.recs, nl))
		}
		return 0, nil
	}
	wantWrite := jfsIsWriteLock(ltype)
	for _, h := range held {
		if h.sid == sid && h.owner == owner {
			continue
		}
		for _, rec := range h.recs {
			if end < rec.Start || start > rec.End {
				continue
			}
			otherWrite := jfsIsWriteLock(rec.Type)
			if wantWrite || otherWrite {
				return int(syscall.EAGAIN), nil
			}
		}
	}
	var mine []jfsPLock
	for _, h := range held {
		if h.sid == sid && h.owner == owner {
			mine = h.recs
			break
		}
	}
	return 0, s.jfsSavePlockTx(tx, ino, sid, owner, jfsUpdateLocks(mine, nl))
}
func (s *Store) jfsGetlkTx(tx *sql.Tx, ino, sid, owner uint64, ltype uint32, start, end uint64) (uint32, uint64, uint64, uint32, int, error) {
	held, err := s.jfsLoadPlocksTx(tx, ino)
	if err != nil {
		return 0, 0, 0, 0, 0, err
	}
	wantWrite := jfsIsWriteLock(ltype)
	for _, h := range held {
		if h.sid == sid && h.owner == owner {
			continue
		}
		for _, rec := range h.recs {
			if end < rec.Start || start > rec.End {
				continue
			}
			otherWrite := jfsIsWriteLock(rec.Type)
			if wantWrite || otherWrite {
				return rec.Type, rec.Start, rec.End, rec.Pid, 0, nil
			}
		}
	}
	return jfsLockUnlock, start, end, 0, 0, nil
}
