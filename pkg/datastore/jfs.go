// This file holds the extent engine's shared foundation: the JuiceFS inode
// type enum, the cached-usage and projection helpers every other jfs_* file
// reads, and the tenant initialization / format / session plumbing the
// dispatch runner needs. The operation surface itself lives in jfs_meta.go,
// and the work it dispatches to is split by area:
//
//	jfs_meta.go      the RPC dispatch table and its transaction runner
//	jfs_namespace.go edges, file_nodes projection, attributes, rename
//	jfs_data.go      chunk reads/writes, truncate, compaction, quota bytes
//	jfs_locks.go     POSIX locks and advisory flocks
//	jfs_gc.go        deletion queues, block GC, compaction leases
package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/mysqlutil"
)

const (
	jfsRootIno     = uint64(1)
	jfsTypeFile    = uint8(1)
	jfsTypeDir     = uint8(2)
	jfsTypeSymlink = uint8(3)
	jfsTypeFIFO    = uint8(4)
	jfsTypeBlock   = uint8(5)
	jfsTypeChar    = uint8(6)
	jfsTypeSocket  = uint8(7)
)

// jfsCompactThreshold is the slice-count that enqueues a compact task.
// Tests may lower it via SetCompactThresholdForTest.
var jfsCompactThreshold = extentCompactSlices

// jfsTestFailBeforeProjection is invoked after the JuiceFS node/edge insert
// and before the file_nodes projection. Tests use it to prove the txn rolls back.
var jfsTestFailBeforeProjection func() error

// SetCompactThresholdForTest overrides the compact enqueue threshold.
func SetCompactThresholdForTest(n int) func() {
	old := jfsCompactThreshold
	jfsCompactThreshold = n
	return func() { jfsCompactThreshold = old }
}

// ExtentAttr is the JSON shape of juicefs meta.Attr (exported field names).
type ExtentAttr struct {
	Flags     uint8  `json:"Flags"`
	Typ       uint8  `json:"Typ"`
	Mode      uint16 `json:"Mode"`
	Uid       uint32 `json:"Uid"`
	Gid       uint32 `json:"Gid"`
	Rdev      uint32 `json:"Rdev"`
	Atime     int64  `json:"Atime"`
	Mtime     int64  `json:"Mtime"`
	Ctime     int64  `json:"Ctime"`
	Atimensec uint32 `json:"Atimensec"`
	Mtimensec uint32 `json:"Mtimensec"`
	Ctimensec uint32 `json:"Ctimensec"`
	Nlink     uint32 `json:"Nlink"`
	Length    uint64 `json:"Length"`
	Parent    uint64 `json:"Parent"`
	Full      bool   `json:"Full"`
	Tier      uint8  `json:"Tier"`
}

type ExtentSlice struct {
	Id   uint64 `json:"Id"`
	Size uint32 `json:"Size"`
	Off  uint32 `json:"Off"`
	Len  uint32 `json:"Len"`
}

type ExtentProjection struct {
	Path          string
	ContentLayout ContentLayout
	ExtentIno     uint64
}

func (s *Store) GetExtentProjection(ctx context.Context, path string) (*ExtentProjection, error) {
	var layout sql.NullString
	var ino sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT content_layout, extent_ino FROM file_nodes WHERE `+
		s.scope.And(`path_hash = ? AND path = ?`),
		s.scope.Args(fileNodePathHash(path), path)...).Scan(&layout, &ino)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	p := &ExtentProjection{Path: path, ContentLayout: ContentLayout(layout.String)}
	if ino.Valid {
		p.ExtentIno = uint64(ino.Int64)
	}
	return p, nil
}

// overlayExtentStat copies the JuiceFS inode's length, mode and type onto the
// projection's stat result.
//
// The projection is not maintained on the extent write path: inodes.size_bytes
// stays 0 (see jfsWritePartsTx) and inodes.mode keeps the mode the file was
// created with, because chmod goes to jfs_node through the JuiceFS SetAttr op.
// A remote reader (HEAD/stat/CLI/API/search) therefore has to take both from
// jfs_node, exactly like overlayExtentStatDir does for a directory listing —
// otherwise `chmod 600` on a mounted extent file is visible inside the mount
// and invisible to everyone else.
func (s *Store) overlayExtentStat(ctx context.Context, db execer, nf *NodeWithFile) {
	if nf == nil || nf.Node.IsDirectory || nf.File == nil {
		return
	}
	if nf.File.StorageType != StorageExtent {
		return
	}
	path := nf.Node.Path
	if path == "" {
		return
	}
	var length, typ, perm sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT j.length, j.type, j.mode FROM file_nodes fn
		INNER JOIN jfs_node j ON j.inode = fn.extent_ino
		WHERE `+s.scope.AndAs("fn", `fn.path_hash = ? AND fn.path = ?`)+`
		AND fn.extent_ino IS NOT NULL AND fn.extent_ino <> 0`,
		s.scope.Args(fileNodePathHash(path), path)...).Scan(&length, &typ, &perm)
	if err != nil || !length.Valid {
		return
	}
	nf.File.SizeBytes = length.Int64
	if typ.Valid && perm.Valid {
		// Permission bits only, matching what the classic projection stores in
		// inodes.mode: X-Dat9-Mode carries st_mode & 07777 (the file type a
		// client needs comes from the listing overlay and the FUSE attr
		// refresh, both of which do carry it). perm.Valid guards the same
		// NULL-vs-0 distinction jfsTypeToStatMode makes for listings — both
		// columns are NOT NULL today, so this is symmetry, not reachability.
		nf.Mode = uint32(perm.Int64) & 0o7777
		nf.HasMode = true
	}
}

// jfsTypeToStatMode maps a JuiceFS node type plus permission bits onto the stat
// mode a directory entry carries. The projection's inodes.mode is NULL for
// extent files, so a listing built from it alone would present a symlink as a
// regular file (git reports that as a typechange).
//
// permStored distinguishes a stored permission from a missing one: chmod 000
// stores a real 0, which must be published as 0000 exactly like the per-file
// stat does — substituting a default made ListDir disagree with Stat for the
// same file. Only a NULL mode (no permission stored on the jfs_node row)
// falls back to the type default.
func jfsTypeToStatMode(typ uint8, perm uint32, permStored bool) uint32 {
	kind := uint32(syscall.S_IFREG)
	switch typ {
	case jfsTypeDir:
		kind = uint32(syscall.S_IFDIR)
	case jfsTypeSymlink:
		return uint32(syscall.S_IFLNK) | 0o777
	case jfsTypeFIFO:
		kind = uint32(syscall.S_IFIFO)
	case jfsTypeBlock:
		kind = uint32(syscall.S_IFBLK)
	case jfsTypeChar:
		kind = uint32(syscall.S_IFCHR)
	case jfsTypeSocket:
		kind = uint32(syscall.S_IFSOCK)
	}
	if !permStored {
		if kind == uint32(syscall.S_IFDIR) {
			perm = 0o755
		} else {
			perm = 0o644
		}
	}
	return kind | (perm & 0o7777)
}

// overlayExtentStatDir is overlayExtentStat for a directory listing. The
// per-node query would cost one round trip per entry, and a listing is exactly
// where the missing size does the most damage: readdirplus hands the kernel
// size 0 for every extent file, the kernel caches i_size = 0, and every later
// read of those files returns nothing until the attributes expire.
//
// The overlay also records the JuiceFS inode on the entry. It comes from the
// same row as the length, and it is the only layout signal a listing carries:
// without it a FUSE readdirplus cannot tell an extent child from a single-layout
// one, and every first open has to spend a HEAD probe to find out.
func (s *Store) overlayExtentStatDir(ctx context.Context, db execer, parentPath string, entries []*NodeWithFile) error {
	if parentPath == "" || len(entries) == 0 {
		return nil
	}
	type extentStat struct {
		ino    uint64
		length int64
		mode   uint32
	}
	rows, err := db.QueryContext(ctx, `SELECT fn.path, fn.extent_ino, j.length, j.type, j.mode FROM file_nodes fn
		INNER JOIN jfs_node j ON j.inode = fn.extent_ino
		WHERE `+s.scope.AndAs("fn", `fn.parent_path_hash = ? AND fn.parent_path = ?`)+`
		AND fn.extent_ino IS NOT NULL AND fn.extent_ino <> 0`,
		s.scope.Args(fileNodePathHash(parentPath), parentPath)...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	stats := make(map[string]extentStat)
	for rows.Next() {
		var path string
		var ino, length sql.NullInt64
		var typ, perm sql.NullInt64
		if err := rows.Scan(&path, &ino, &length, &typ, &perm); err != nil {
			return err
		}
		if length.Valid {
			stats[path] = extentStat{
				ino:    uint64(ino.Int64),
				length: length.Int64,
				mode:   jfsTypeToStatMode(uint8(typ.Int64), uint32(perm.Int64), perm.Valid),
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, nf := range entries {
		// Directories keep the projection's values: overlayExtentStat skips them
		// on the single-node path, and readdirplus must not disagree with it.
		if nf == nil || nf.File == nil || nf.Node.IsDirectory {
			continue
		}
		stat, ok := stats[nf.Node.Path]
		if !ok {
			continue
		}
		nf.ContentLayout = ContentLayoutExtent
		nf.ExtentIno = stat.ino
		nf.File.SizeBytes = stat.length
		nf.Mode = stat.mode
		nf.HasMode = true
	}
	return nil
}

func (s *Store) withExtentStat(ctx context.Context, db execer, nf *NodeWithFile, err error) (*NodeWithFile, error) {
	if err != nil || nf == nil {
		return nf, err
	}
	s.overlayExtentStat(ctx, db, nf)
	return nf, nil
}

// RunExtentMetaOp executes one JuiceFS-engine RPC inside a tenant transaction
// and dual-writes the file_nodes projection for namespace ops.
// HasPendingExtentWork reports whether the extent data plane has work the
// server-side maintenance pass should run: files waiting to be drained from the
// deletion queue, blocks whose grace period has passed, queued compactions, or
// a session that can only be expired.
//
// Extent metadata operations enqueue this work without emitting a tenant kick
// (the work mask the outbox poller carries is persisted and deliberately keeps
// WorkExtent clear), so without a probe an extent-only tenant whose mounts did
// nothing else would never have its blocks reclaimed. The periodic safety-net
// scan uses this to decide whether the tenant needs an extent kick.
func (s *Store) HasPendingExtentWork(ctx context.Context) (bool, error) {
	var pending bool
	err := s.db.QueryRowContext(ctx, `SELECT
		EXISTS(SELECT 1 FROM jfs_delfile) OR
		EXISTS(SELECT 1 FROM block_gc_tasks WHERE status = 'PENDING' AND available_at <= CURRENT_TIMESTAMP(3)) OR
		EXISTS(SELECT 1 FROM slice_compact_tasks WHERE status = 'PENDING') OR
		EXISTS(SELECT 1 FROM jfs_session2 WHERE expire > 0 AND expire < ?)`,
		time.Now().Unix()).Scan(&pending)
	if err != nil {
		return false, err
	}
	return pending, nil
}

// ExtentMetaMaybeExists reports whether this tenant can have extent nodes at
// all, so classic-path callers can skip their extent-specific work.
//
// The classic write paths (single-file delete, recursive delete, directory
// rename) each used to probe the projection unconditionally. For a tenant that
// never opts into content_layout=extent that is pure overhead on the hottest
// operations, and it is avoidable on fact rather than on a guess: an extent
// node is exactly a file_nodes row with extent_ino set or content_layout
// 'extent', so if no such row exists there is nothing for those paths to do.
//
// extentMetaPresent caches the tenant-wide answer. It is set by every writer of
// extent_ino (each jfsEnsureInitTx call site: extent meta ops, extent unlink,
// and the classic rename that mirrors a destination parent) and by a positive
// probe. It records that the tenant has proved it uses the extent layout, so it
// never flips back; a process whose probe raced a create re-reads next time.
//
// A probe failure reports true so the caller keeps its previous behavior
// instead of silently dropping extent work on a database hiccup.
func (s *Store) ExtentMetaMaybeExists(ctx context.Context) bool {
	if s == nil {
		return true
	}
	if s.extentMetaPresent.Load() {
		return true
	}
	var present bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM file_nodes WHERE `+
		s.scope.And(`extent_ino IS NOT NULL AND extent_ino <> 0`)+`)`, s.scope.Args()...).Scan(&present)
	if err != nil {
		// Cannot know, so assume the work is needed. Returning false here would
		// turn a transient probe failure into a classic-path delete of an extent
		// file (unreclaimable jfs rows), a recursive delete that skips the jfs
		// subtree, or a directory rename that forks the two trees.
		return true
	}
	if !present {
		return false
	}
	s.extentMetaPresent.Store(true)
	return true
}

// extentUsageCacheTTL bounds how stale the extent bytes *not yet reported* to the
// central counters — the term quota admission adds — may be. The classic path
// reads its usage from a cache with the same property; the value is a soft
// admission input, not an accounting ledger, and the aggregate must not run on
// every 4KiB extent write.
const extentUsageCacheTTL = 2 * time.Second

// jfsEnsureInitTx creates the JuiceFS root node and counters if this tenant has
// never had them. It deliberately re-reads the root row on every call: a stale
// positive jfsInited must never let this become a no-op, or the tenant's very
// first extent meta op would run against a database with no root node and fail
// with EIO on a lookup of inode 1. The read is a primary-key point lookup, and
// the read-only dispatch skips the whole transaction path once jfsInited is set,
// so this does not touch the hot read path.
func (s *Store) jfsEnsureInitTx(tx *sql.Tx) error {
	if tx == nil {
		return fmt.Errorf("jfsEnsureInitTx: nil transaction")
	}
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, jfsRootIno).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		if s != nil {
			s.jfsInited.Store(true)
			s.extentMetaPresent.Store(true)
			s.applyExtentPoolBudget()
		}
		return nil
	}
	now := time.Now().UnixNano()
	atime, asec := jfsSplitTime(now)
	if _, err := tx.Exec(`INSERT IGNORE INTO jfs_node
		(inode, type, flags, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, rdev, parent)
		VALUES (?, ?, 0, ?, 0, 0, ?, ?, ?, ?, ?, ?, 2, ?, 0, ?)`,
		jfsRootIno, jfsTypeDir, 0777, atime, atime, atime, asec, asec, asec, 4096, jfsRootIno); err != nil {
		return err
	}
	counters := []struct {
		name  string
		value int64
	}{
		{"nextInode", 2},
		{"nextChunk", 1},
		{"nextSession", 0},
		// usedSpace/totalInodes are seeded for completeness (JuiceFS's format
		// init writes them) but drive9 never advances them: extent quota is
		// enforced by ExtentQuotaLimit inside the meta transaction, not by
		// JuiceFS's directory counters. They are deliberately not read either,
		// so nothing can mistake a zero here for "no usage".
		{"usedSpace", 0},
		{"totalInodes", 0},
	}
	for _, c := range counters {
		if _, err := tx.Exec(`INSERT IGNORE INTO jfs_counter (name, value) VALUES (?, ?)`, c.name, c.value); err != nil {
			return err
		}
	}
	if s != nil {
		s.jfsInited.Store(true)
		// The root row now exists, so this tenant has extent metadata: the
		// classic paths must keep running their extent probes from here on.
		s.extentMetaPresent.Store(true)
		s.applyExtentPoolBudget()
	}
	return nil
}

// applyExtentPoolBudget widens this tenant's user pool to the extent data
// plane's budget, once, the first time the tenant proves it uses
// content_layout=extent. The established global default (6/2, see
// pkg/mysqlutil) therefore stays exactly as it is for every other tenant and
// for the classic write path; only a tenant whose extent meta is initialized
// pays for the extra connections, and the budget itself is configurable with
// DRIVE9_EXTENT_DB_MAX_OPEN_CONNS / DRIVE9_EXTENT_DB_MAX_IDLE_CONNS.
func (s *Store) applyExtentPoolBudget() {
	if s == nil || s.db == nil || !s.extentPoolRaised.CompareAndSwap(false, true) {
		return
	}
	maxOpen, maxIdle := mysqlutil.ExtentPoolLimits()
	if maxOpen <= 0 {
		return
	}
	s.db.SetMaxOpenConns(maxOpen)
	s.db.SetMaxIdleConns(maxIdle)
}

func jfsSplitTime(ns int64) (int64, int16) {
	return ns / 1e3, int16(ns % 1e3)
}

func jfsAttrFromNode(typ uint8, mode uint16, uid, gid uint32, atime, mtime, ctime int64, asec, msec, csec int16, nlink uint32, length uint64, parent uint64, rdev uint32) ExtentAttr {
	return ExtentAttr{
		Typ:       typ,
		Mode:      mode,
		Uid:       uid,
		Gid:       gid,
		Rdev:      rdev,
		Atime:     atime / 1e6,
		Atimensec: uint32(atime%1e6*1000) + uint32(asec),
		Mtime:     mtime / 1e6,
		Mtimensec: uint32(mtime%1e6*1000) + uint32(msec),
		Ctime:     ctime / 1e6,
		Ctimensec: uint32(ctime%1e6*1000) + uint32(csec),
		Nlink:     nlink,
		Length:    length,
		Parent:    parent,
		Full:      true,
	}
}

func (s *Store) jfsGetCounterTx(db execer, name string) (int64, error) {
	var v int64
	err := db.QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, name).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return v, err
}

func (s *Store) jfsIncrCounterTx(tx *sql.Tx, name string, delta int64) (int64, error) {
	if _, err := tx.Exec(`INSERT INTO jfs_counter (name, value) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE value = value + ?`, name, delta, delta); err != nil {
		return 0, err
	}
	return s.jfsGetCounterTx(tx, name)
}

func (s *Store) jfsSetIfSmallTx(tx *sql.Tx, name string, value, diff int64) (bool, error) {
	var cur int64
	err := tx.QueryRow(`SELECT value FROM jfs_counter WHERE name = ? FOR UPDATE`, name).Scan(&cur)
	if errors.Is(err, sql.ErrNoRows) {
		_, err = tx.Exec(`INSERT INTO jfs_counter (name, value) VALUES (?, ?)`, name, value)
		return true, err
	}
	if err != nil {
		return false, err
	}
	if cur > value-diff {
		return false, nil
	}
	_, err = tx.Exec(`UPDATE jfs_counter SET value = ? WHERE name = ?`, value, name)
	return true, err
}

func (s *Store) jfsLoadFormatTx(tx *sql.Tx) ([]byte, error) {
	var v []byte
	err := tx.QueryRow(`SELECT value FROM jfs_setting WHERE name = 'format'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return v, err
}

func (s *Store) jfsStoreFormatTx(tx *sql.Tx, format json.RawMessage, force bool) error {
	_, err := tx.Exec(`INSERT INTO jfs_setting (name, value) VALUES ('format', ?)
		ON DUPLICATE KEY UPDATE value = IF(?, VALUES(value), value)`, string(format), force)
	return err
}

func (s *Store) jfsUpsertSessionTx(tx *sql.Tx, sid uint64, expire int64, info []byte, update bool) error {
	if update {
		_, err := tx.Exec(`UPDATE jfs_session2 SET expire = ?, info = ? WHERE sid = ?`, expire, info, sid)
		return err
	}
	_, err := tx.Exec(`INSERT INTO jfs_session2 (sid, expire, info) VALUES (?, ?, ?)`, sid, expire, info)
	return err
}

func (s *Store) jfsRefreshSessionTx(tx *sql.Tx, sid uint64, expire int64) error {
	res, err := tx.Exec(`UPDATE jfs_session2 SET expire = ? WHERE sid = ?`, expire, sid)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		_, err = tx.Exec(`INSERT INTO jfs_session2 (sid, expire, info) VALUES (?, ?, ?)`, sid, expire, []byte("{}"))
	}
	return err
}

const (
	jfsRenameNoReplace = uint32(1 << 0)
	jfsRenameExchange  = uint32(1 << 1)
)
