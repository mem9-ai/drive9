// This file implements the extent engine's data plane: chunk slice lists,
// writes, truncate, compaction commits, and the extent byte accounting quota
// admission and the central usage report read.
package datastore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

// ExtentUnreportedUsageBytes returns the extent file bytes that no central report
// has accounted for yet: the tenant's extent total (see extentUsageTotalSQL) minus
// extentUsageReportedKey, the marker that records how much of it
// reportExtentQuotaUsage has already pushed into the central quota counters. That difference — and never the total — is
// the term a write admission may add to the central usage: the reported bytes
// are already in it, so adding the total would count them twice and refuse a
// quota-configured tenant at about half its real limit.
//
// The total's authoritative source is jfs_node.length (see overlayExtentStat),
// not the projection's size_bytes. The query rides idx_file_nodes_extent_ino and
// is cached for extentUsageCacheTTL: it is a soft admission input, not an
// accounting ledger, and the aggregate must not run on every 4KiB extent write.
func (s *Store) ExtentUnreportedUsageBytes(ctx context.Context) (int64, error) {
	if v, ok := s.cachedExtentUsage(); ok {
		return v, nil
	}
	total, reported, err := s.extentUsageTotals(ctx)
	if err != nil {
		return 0, err
	}
	unreported := total - reported
	s.storeCachedExtentUsage(unreported)
	return unreported, nil
}

// extentUsageTotalSQL sums the bytes of the tenant's extent FILES. The inner
// SELECT DISTINCT is what makes it a byte count instead of a dentry count:
// file_nodes has one row per name, so a hardlinked extent file would otherwise
// be charged once per alias, and the mirrored directories of
// jfsEnsureParentsTx/jfsMknodTx — which carry an extent_ino and a 4096-byte jfs
// length — would be charged as if they held data, neither of which the classic
// plane does (a directory inode is created with size_bytes 0, and a hardlink
// does not copy bytes). jfs_node.type keeps symlinks out for the same reason.
//
// The scope filter is applied to the subquery, so the DISTINCT still rides
// idx_file_nodes_extent_ino.
const extentUsageTotalSQL = `SELECT COALESCE(SUM(j.length), 0)
	FROM (SELECT DISTINCT fn.extent_ino AS ino FROM file_nodes fn WHERE %s) d
	INNER JOIN jfs_node j ON j.inode = d.ino AND j.type = ?`

// extentUsageTotals reads the tenant's extent byte total and the reported marker
// in one transaction, so the two cannot skew against each other.
func (s *Store) extentUsageTotals(ctx context.Context) (total, reported int64, err error) {
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var sum sql.NullInt64
		if qerr := tx.QueryRow(fmt.Sprintf(extentUsageTotalSQL,
			s.scope.AndAs("fn", `fn.extent_ino IS NOT NULL AND fn.extent_ino <> 0`)),
			append(append([]any{}, s.scope.Args()...), jfsTypeFile)...).Scan(&sum); qerr != nil {
			return qerr
		}
		total = sum.Int64
		serr := tx.QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, extentUsageReportedKey).Scan(&reported)
		if serr != nil && !errors.Is(serr, sql.ErrNoRows) {
			return serr
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return total, reported, nil
}
func (s *Store) cachedExtentUsage() (int64, bool) {
	s.extentUsageMu.Lock()
	defer s.extentUsageMu.Unlock()
	if s.extentUsageAt.IsZero() || time.Since(s.extentUsageAt) > extentUsageCacheTTL {
		return 0, false
	}
	return s.extentUsageBytes, true
}
func (s *Store) invalidateCachedExtentUsage() {
	s.extentUsageMu.Lock()
	s.extentUsageAt = time.Time{}
	s.extentUsageMu.Unlock()
}

func (s *Store) storeCachedExtentUsage(v int64) {
	s.extentUsageMu.Lock()
	s.extentUsageBytes, s.extentUsageAt = v, time.Now()
	s.extentUsageMu.Unlock()
}

// extentUsageReportedKey is the jfs_counter row that remembers how many extent
// bytes this tenant has already reported to the central quota counters. It is
// what turns the periodic reconciliation into a delta instead of a re-count.
const extentUsageReportedKey = "extentQuotaReportedBytes"

// PeekExtentUsageDelta returns the extent bytes written since the last report
// without recording them. The caller reports the delta to the central counters
// and only then calls CommitExtentUsageDelta, so a failed report is retried
// with the same delta on the next pass instead of being lost: the marker used
// to advance in this transaction, before the report ran, which meant a failed
// mutation-log write silently under-counted quota forever.
//
// The reverse window is possible instead — a report that lands and a commit
// that fails re-reports the same delta, and two pods draining one tenant can
// both report the same delta (the per-tenant slot is process-local) — and the
// counter update is a relative increment that nothing recomputes, so that
// over-count persists rather than decaying. Over-counting a soft quota input is
// still the safer half of the trade, and docs/extent-todos.md tracks the
// report-lease sketch that would close it.
func (s *Store) PeekExtentUsageDelta(ctx context.Context) (total int64, delta int64, err error) {
	total, reported, err := s.extentUsageTotals(ctx)
	if err != nil {
		return 0, 0, err
	}
	return total, total - reported, nil
}

// CommitExtentUsageDelta records the total a report covered as the reported
// extent byte count, but only if the marker still holds the value that report
// was computed against — derived here from the same PeekExtentUsageDelta pair
// the caller reported, so a caller cannot pass the wrong number.
//
// The compare-and-set matters because the reporter is not always alone: shard
// ownership is a hash ring, so a ring transition can hand the tenant to another
// pod while this report is in flight, and that pod then peeks the same marker.
// Whoever commits second would otherwise write a stale total over the newer one
// and make the difference look unreported for ever; instead the loser leaves the
// marker alone and says so.
func (s *Store) CommitExtentUsageDelta(ctx context.Context, peekedTotal, peekedDelta int64) error {
	expected := peekedTotal - peekedDelta
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO jfs_counter (name, value) VALUES (?, ?)
			ON DUPLICATE KEY UPDATE value = IF(value = ?, VALUES(value), value)`,
			extentUsageReportedKey, peekedTotal, expected); err != nil {
			return err
		}
		var after int64
		if err := tx.QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, extentUsageReportedKey).Scan(&after); err != nil {
			return err
		}
		if after != peekedTotal {
			logger.Warn(ctx, "extent_quota_report_raced",
				zap.Int64("expected", expected), zap.Int64("wanted", peekedTotal), zap.Int64("marker", after))
		}
		return nil
	})
	if err != nil {
		return err
	}
	// Drop the cached remainder rather than pinning it to zero: the marker
	// covers the total as of the caller's Peek, and a write that commits between
	// that Peek and this commit is covered by neither, so caching the zero for
	// the TTL would hide it from admission. Re-reading costs one aggregate per
	// report, not one per write.
	s.invalidateCachedExtentUsage()
	return nil
}
func (s *Store) jfsReadTx(db execer, ino uint64, indx uint32) ([]byte, error) {
	var buf []byte
	err := db.QueryRow(`SELECT slices FROM jfs_chunk WHERE inode = ? AND indx = ?`, ino, indx).Scan(&buf)
	if errors.Is(err, sql.ErrNoRows) {
		return []byte{}, nil
	}
	return buf, err
}
func marshalExtentSlice(pos uint32, id uint64, size, off, length uint32) []byte {
	buf := make([]byte, extentSliceBytes)
	binary.BigEndian.PutUint32(buf[0:4], pos)
	binary.BigEndian.PutUint64(buf[4:12], id)
	binary.BigEndian.PutUint32(buf[12:16], size)
	binary.BigEndian.PutUint32(buf[16:20], off)
	binary.BigEndian.PutUint32(buf[20:24], length)
	return buf
}

type extentWritePart struct {
	Off   uint32
	Slice ExtentSlice
}

func (s *Store) jfsWritePartsTx(tx *sql.Tx, ino uint64, indx uint32, parts []extentWritePart, mtime time.Time, quota *ExtentQuotaLimit) (int, *ExtentAttr, int64, int64, int, error) {
	// JuiceFS doWrite: ForUpdate Get node, upsert slice, insert sliceRef,
	// update node. Slice-count SELECT only when the chunk already existed
	// (compact decision). We skip that SELECT and any compact INSERT here:
	// the slice count this write produced is enough, and the compact task is
	// queued by enqueueCompactAfterWrite once this transaction has committed.
	var typ uint8
	var length, parent uint64
	var uid, gid uint32
	// FOR UPDATE is what makes the length update below a serialized
	// read-modify-write: two concurrent writes to one inode would otherwise
	// both read the same old length and the later one would store its shorter
	// stale value, leaving committed slices past the visible EOF.
	err := tx.QueryRow(`SELECT type, length, parent, uid, gid FROM jfs_node WHERE inode = ? FOR UPDATE`, ino).
		Scan(&typ, &length, &parent, &uid, &gid)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, 0, 0, int(syscall.ENOENT), nil
	}
	if err != nil {
		return 0, nil, 0, 0, 0, err
	}
	if typ != jfsTypeFile {
		attr := ExtentAttr{Typ: typ, Length: length, Parent: parent, Uid: uid, Gid: gid, Full: true}
		return 0, &attr, 0, 0, int(syscall.EPERM), nil
	}
	if len(parts) == 0 {
		attr := ExtentAttr{Typ: typ, Length: length, Parent: parent, Uid: uid, Gid: gid, Full: true}
		return 0, &attr, 0, 0, 0, nil
	}
	var buf []byte
	var dlen, dspace int64
	oldLength := length
	refSQL := make([]string, 0, len(parts))
	refArgs := make([]any, 0, len(parts)*2)
	for _, p := range parts {
		newlen := uint64(indx)*ExtentChunkSize + uint64(p.Off) + uint64(p.Slice.Len)
		if newlen > length {
			dlen += int64(newlen - length)
			dspace = dlen
			length = newlen
		}
		buf = append(buf, marshalExtentSlice(p.Off, p.Slice.Id, p.Slice.Size, p.Slice.Off, p.Slice.Len)...)
		refSQL = append(refSQL, "(?, ?, 1)")
		refArgs = append(refArgs, p.Slice.Id, p.Slice.Size)
	}
	if errno := quota.admit(oldLength, length); errno != 0 {
		return 0, nil, 0, 0, errno, nil
	}
	if len(refSQL) > 0 {
		q := `INSERT INTO jfs_chunk_ref (chunkid, size, refs) VALUES ` + strings.Join(refSQL, ",") +
			` ON DUPLICATE KEY UPDATE refs = refs + 1`
		if _, err := tx.Exec(q, refArgs...); err != nil {
			return 0, nil, 0, 0, 0, err
		}
	}
	if _, err := tx.Exec(`INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE slices = CONCAT(slices, VALUES(slices))`, ino, indx, buf); err != nil {
		return 0, nil, 0, 0, 0, err
	}
	now := time.Now().UnixNano()
	mt, msec := jfsSplitTime(mtime.UnixNano())
	ct, csec := jfsSplitTime(now)
	if _, err := tx.Exec(`UPDATE jfs_node SET length=?, mtime=?, ctime=?, mtimensec=?, ctimensec=? WHERE inode=?`,
		length, mt, ct, msec, csec, ino); err != nil {
		return 0, nil, 0, 0, 0, err
	}
	// The projection's inode row is deliberately NOT updated here. Doing it in
	// this transaction (the statement jfsTruncateTx runs) keeps
	// inodes.size_bytes exact, but it measured +40-50% on extent_write on the
	// EC2 test host (9.5/10.8 ms -> 15.2/14.4 ms per write op; 64 MB spill
	// 38/39 s -> 45 s; A/B/A with only the server binary changed), and this is
	// the transaction sqlite's 10 s --wait budget covers. Readers that need the
	// real length take it from jfs_node instead: the attr overlays for
	// stat/readdirplus, and a LEFT JOIN in the search queries. A tenant that
	// wants the raw columns fixed for old rows runs, once:
	//   UPDATE inodes i JOIN file_nodes f ON f.inode_id = i.inode_id
	//     JOIN jfs_node j ON j.inode = f.extent_ino
	//      SET i.size_bytes = j.length
	//    WHERE f.content_layout = 'extent'
	// JuiceFS doWrite returns len(slices)/sliceBytes from the in-memory blob.
	// LENGTH() is the SQL equivalent without pulling the blob (CONCAT write).
	var nbytes int
	if err := tx.QueryRow(`SELECT LENGTH(slices) FROM jfs_chunk WHERE inode = ? AND indx = ?`, ino, indx).Scan(&nbytes); err != nil {
		return 0, nil, 0, 0, 0, err
	}
	attr := ExtentAttr{Typ: typ, Length: length, Parent: parent, Uid: uid, Gid: gid, Full: true}
	return nbytes / extentSliceBytes, &attr, dlen, dspace, 0, nil
}

// jfsTruncateZeroChunksTx records the zero-slice coverage a length change needs
// so the chunk map agrees with jfs_node.length. It is the port of the slice
// bookkeeping in JuiceFS doTruncate (pkg/meta/sql.go), which the first version
// of this function omitted: without it, extending a file reported the new
// length while reads of the gap returned nothing, and shrinking left the
// discarded range's slices in place.
//
// Zero slices carry id 0 and reference no block, so they cost no space; they
// mark the range as explicitly sparse and let a later rewrite of the same range
// supersede them normally.
//
// Superseded slices keep their refcounts, matching upstream doTruncate, which
// appends the same zero slices and frees nothing: the blocks a reader on
// another mount may still be holding are released when a compaction rewrites
// the chunk (jfsCompactTx), not while a truncate is racing those readers.
//
// It returns the chunk indices it appended to, so the caller can schedule
// compaction for one that just crossed the slice threshold — the write path
// does that for writes, and without it a chunk grown only by repeated truncates
// would never be compacted.
func (s *Store) jfsTruncateZeroChunksTx(tx *sql.Tx, ino, oldLength, length uint64) ([]uint32, error) {
	var zeroChunks []uint32
	left, right := oldLength, length
	if left > right {
		right, left = left, right
	}
	// Chunks strictly between the two boundaries lose everything they hold.
	if right/ExtentChunkSize-left/ExtentChunkSize > 1 {
		rows, err := tx.Query(`SELECT indx FROM jfs_chunk WHERE inode = ? AND indx > ? AND indx < ? FOR UPDATE`,
			ino, left/ExtentChunkSize, right/ExtentChunkSize)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var indx uint32
			if err := rows.Scan(&indx); err != nil {
				_ = rows.Close()
				return nil, err
			}
			zeroChunks = append(zeroChunks, indx)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		if err := rows.Err(); err != nil {
			return nil, err
		}
	}

	l := uint32(right - left)
	if right > (left/ExtentChunkSize+1)*ExtentChunkSize {
		l = ExtentChunkSize - uint32(left%ExtentChunkSize)
	}
	if err := s.jfsAppendSliceTx(tx, ino, uint32(left/ExtentChunkSize),
		marshalExtentSlice(uint32(left%ExtentChunkSize), 0, 0, 0, l)); err != nil {
		return nil, err
	}
	full := marshalExtentSlice(0, 0, 0, 0, ExtentChunkSize)
	for _, indx := range zeroChunks {
		if err := s.jfsAppendSliceTx(tx, ino, indx, full); err != nil {
			return nil, err
		}
	}
	if right > (left/ExtentChunkSize+1)*ExtentChunkSize && right%ExtentChunkSize > 0 {
		if err := s.jfsAppendSliceTx(tx, ino, uint32(right/ExtentChunkSize),
			marshalExtentSlice(0, 0, 0, 0, uint32(right%ExtentChunkSize))); err != nil {
			return nil, err
		}
	}
	touched := make([]uint32, 0, len(zeroChunks)+2)
	touched = append(touched, uint32(left/ExtentChunkSize))
	touched = append(touched, zeroChunks...)
	if right > (left/ExtentChunkSize+1)*ExtentChunkSize && right%ExtentChunkSize > 0 {
		touched = append(touched, uint32(right/ExtentChunkSize))
	}
	return touched, nil
}

// truncateCrossedChunksTx reports which of the touched chunks now hold at least
// jfsCompactThreshold slices. LENGTH() reads the slice count without pulling the
// blob (the append is a CONCAT), and the indices are batched so a large truncate
// does not issue one round trip per chunk.
func (s *Store) truncateCrossedChunksTx(tx *sql.Tx, ino uint64, touched []uint32) ([]uint32, error) {
	if len(touched) == 0 {
		return nil, nil
	}
	const batch = 256
	var crossed []uint32
	for start := 0; start < len(touched); start += batch {
		end := start + batch
		if end > len(touched) {
			end = len(touched)
		}
		chunk := touched[start:end]
		placeholders := make([]string, len(chunk))
		args := make([]any, 0, len(chunk)+1)
		args = append(args, ino)
		for i, indx := range chunk {
			placeholders[i] = "?"
			args = append(args, indx)
		}
		rows, qerr := tx.Query(`SELECT indx, LENGTH(slices) FROM jfs_chunk WHERE inode = ? AND indx IN (`+
			strings.Join(placeholders, ",")+`)`, args...)
		if qerr != nil {
			return nil, qerr
		}
		for rows.Next() {
			var indx uint32
			var nbytes int
			if serr := rows.Scan(&indx, &nbytes); serr != nil {
				_ = rows.Close()
				return nil, serr
			}
			if nbytes/extentSliceBytes >= jfsCompactThreshold {
				crossed = append(crossed, indx)
			}
		}
		if cerr := rows.Close(); cerr != nil {
			return nil, cerr
		}
		if rerr := rows.Err(); rerr != nil {
			return nil, rerr
		}
	}
	return crossed, nil
}

func (s *Store) jfsAppendSliceTx(tx *sql.Tx, ino uint64, indx uint32, slice []byte) error {
	_, err := tx.Exec(`INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE slices = CONCAT(slices, VALUES(slices))`, ino, indx, slice)
	return err
}
func (s *Store) jfsTruncateTx(tx *sql.Tx, ino, length uint64, quota *ExtentQuotaLimit, compactOut *[]uint32) (*ExtentAttr, int64, int64, int, error) {
	// FOR UPDATE, like jfsWritePartsTx and upstream doTruncate (JuiceFS
	// pkg/meta/sql.go does s.ForUpdate().Get(&nodeAttr)): a plain read lets a
	// concurrent write commit between this read and the zero-slice append, and
	// because a later slice in the chunk blob wins, the truncate's zero slice
	// would then mask the range the write just committed. The length UPDATE
	// below could equally regress the other writer's extension.
	attr, eno, err := s.jfsGetAttrForUpdateTx(tx, ino)
	if err != nil || eno != 0 {
		return attr, 0, 0, eno, err
	}
	if attr.Length == length {
		return attr, 0, 0, 0, nil
	}
	if errno := quota.admit(attr.Length, length); errno != 0 {
		return attr, 0, 0, errno, nil
	}
	touched, err := s.jfsTruncateZeroChunksTx(tx, ino, attr.Length, length)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	// A chunk that only ever grows through truncates must reach compaction the
	// same way a written one does. One bounded IN query, not one query per
	// chunk: a truncate of a large file can touch thousands of chunks, and this
	// runs inside the node-row lock.
	crossed, err := s.truncateCrossedChunksTx(tx, ino, touched)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	dlen := int64(length) - int64(attr.Length)
	now := time.Now().UnixNano()
	mt, msec := jfsSplitTime(now)
	if _, err := tx.Exec(`UPDATE jfs_node SET length=?, mtime=?, ctime=?, mtimensec=?, ctimensec=? WHERE inode=?`,
		length, mt, mt, msec, msec, ino); err != nil {
		return nil, 0, 0, 0, err
	}
	// The write path deliberately leaves inodes.size_bytes at 0 (readers take
	// the length from jfs_node), but a truncate is exactly where the classic
	// projection can be made exact for free, in the transaction that already
	// holds the row.
	if _, err := tx.Exec(`UPDATE inodes SET size_bytes = ?, mtime = ? WHERE inode_id IN (
		SELECT inode_id FROM file_nodes WHERE extent_ino = ?)`, length, time.Now().UTC(), ino); err != nil {
		return nil, 0, 0, 0, err
	}
	if compactOut != nil {
		*compactOut = append(*compactOut, crossed...)
	}
	out, _, _ := s.jfsGetAttrTx(tx, ino)
	return out, dlen, dlen, 0, nil
}

type extentSliceRef struct {
	id   uint64
	size uint32
}

func (s *Store) jfsCompactTx(tx *sql.Tx, ino uint64, indx uint32, origin []byte, skipped int, pos uint32, id uint64, size uint32) (int, []extentSliceRef, error) {
	var cur []byte
	err := tx.QueryRow(`SELECT slices FROM jfs_chunk WHERE inode = ? AND indx = ? FOR UPDATE`, ino, indx).Scan(&cur)
	if errors.Is(err, sql.ErrNoRows) {
		return int(syscall.EINVAL), nil, nil
	}
	if err != nil {
		return 0, nil, err
	}
	if len(cur) < len(origin) || string(cur[:len(origin)]) != string(origin) {
		return int(syscall.EINVAL), nil, nil
	}
	skipBytes := skipped * extentSliceBytes
	if skipBytes > len(origin) {
		skipBytes = 0
	}
	repl := marshalExtentSlice(pos, id, size, 0, size)
	next := append([]byte{}, cur[:skipBytes]...)
	next = append(next, repl...)
	next = append(next, cur[len(origin):]...)
	if _, err := tx.Exec(`UPDATE jfs_chunk SET slices = ? WHERE inode = ? AND indx = ?`, next, ino, indx); err != nil {
		return 0, nil, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_chunk_ref (chunkid, size, refs) VALUES (?, ?, 1)`, id, size); err != nil {
		return 0, nil, err
	}
	// JuiceFS doCompactChunk: refs-1 in the compact txn, deleteSlice after.
	// Batched IN is the remote-SQL shape of those local Execs. Do not
	// INSERT block_gc_tasks here — that held FOR UPDATE across sqlite fsync.
	tuples := make([]string, 0)
	args := make([]any, 0)
	var olds []extentSliceRef
	for i := skipBytes; i+extentSliceBytes <= len(origin); i += extentSliceBytes {
		oldID := binary.BigEndian.Uint64(origin[i+4 : i+12])
		oldSize := binary.BigEndian.Uint32(origin[i+12 : i+16])
		if oldID == 0 {
			continue
		}
		tuples = append(tuples, "(?, ?)")
		args = append(args, oldID, oldSize)
		olds = append(olds, extentSliceRef{id: oldID, size: oldSize})
	}
	if len(tuples) > 0 {
		q := `UPDATE jfs_chunk_ref SET refs = refs - 1 WHERE (chunkid, size) IN (` + strings.Join(tuples, ",") + `)`
		if _, err := tx.Exec(q, args...); err != nil {
			return 0, nil, err
		}
	}
	// Compact rewrote the chunk, so any queued compaction for it is done. Now
	// that the slices are durable this cannot be allowed to fail silently: a
	// task left PENDING would be re-run against the rewritten chunk.
	if _, err := tx.Exec(`UPDATE slice_compact_tasks SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(3)
		WHERE extent_ino = ? AND chunk = ? AND status IN ('PENDING','LEASED')`, ino, indx); err != nil {
		return 0, nil, err
	}
	return 0, olds, nil
}
