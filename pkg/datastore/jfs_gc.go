// This file implements the extent engine's reclamation: the deleted-file queue
// that drops slice refcounts, the block GC queue that deletes unreferenced
// objects, stale-session sweeps, subtree deletion, and the leased compaction
// queue.
package datastore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
)

func (s *Store) jfsEnqueueSliceGCTx(tx *sql.Tx, id uint64, size uint32, ino uint64) error {
	nblocks := int((size + ExtentBlockSize - 1) / ExtentBlockSize)
	if nblocks < 1 {
		nblocks = 1
	}
	for i := 0; i < nblocks; i++ {
		key := fmt.Sprintf("chunks/%d/%d/%d_%d_%d", id/1000/1000, id/1000, id, i, blockLen(size, i))
		taskID := strings.ToLower(ulid.Make().String())
		if _, err := tx.Exec(`INSERT IGNORE INTO block_gc_tasks (task_id, block_key, extent_ino, size_bytes, status, max_attempts, available_at)
			VALUES (?, ?, ?, ?, 'PENDING', 8, `+extentBlockGCGraceSQL+`)`, taskID, key, nullUint64(ino), blockLen(size, i)); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) jfsEnqueueDeadSliceGCTx(tx *sql.Tx, ino uint64, refs []extentSliceRef) error {
	if len(refs) == 0 {
		return nil
	}
	tuples := make([]string, 0, len(refs))
	args := make([]any, 0, len(refs)*2)
	for _, r := range refs {
		if r.id == 0 {
			continue
		}
		tuples = append(tuples, "(?, ?)")
		args = append(args, r.id, r.size)
	}
	if len(tuples) == 0 {
		return nil
	}
	// The block-GC tasks are durably inserted below; only then may the
	// zero-reference rows go. Keeping them would grow jfs_chunk_ref without
	// bound (one row per superseded slice) and slow every ref update.
	q := `SELECT chunkid, size FROM jfs_chunk_ref WHERE refs <= 0 AND (chunkid, size) IN (` + strings.Join(tuples, ",") + `)`
	rows, err := tx.Query(q, args...)
	if err != nil {
		return err
	}
	var dead []extentSliceRef
	for rows.Next() {
		var o extentSliceRef
		if rows.Scan(&o.id, &o.size) == nil {
			dead = append(dead, o)
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	const batch = 64
	ins := make([]string, 0, batch)
	insArgs := make([]any, 0, batch*4)
	flush := func() error {
		if len(ins) == 0 {
			return nil
		}
		_, err := tx.Exec(`INSERT IGNORE INTO block_gc_tasks (task_id, block_key, extent_ino, size_bytes, status, max_attempts, available_at) VALUES `+
			strings.Join(ins, ","), insArgs...)
		ins = ins[:0]
		insArgs = insArgs[:0]
		return err
	}
	for _, o := range dead {
		nblocks := int((o.size + ExtentBlockSize - 1) / ExtentBlockSize)
		if nblocks < 1 {
			nblocks = 1
		}
		for i := 0; i < nblocks; i++ {
			key := fmt.Sprintf("chunks/%d/%d/%d_%d_%d", o.id/1000/1000, o.id/1000, o.id, i, blockLen(o.size, i))
			taskID := strings.ToLower(ulid.Make().String())
			ins = append(ins, "(?, ?, ?, ?, 'PENDING', 8, "+extentBlockGCGraceSQL+")")
			insArgs = append(insArgs, taskID, key, nullUint64(ino), blockLen(o.size, i))
			if len(ins) >= batch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	// Order matters: the GC tasks are committed first, so a crash between the
	// two statements retries the enqueue (INSERT IGNORE) instead of losing it.
	delTuples := make([]string, 0, len(dead))
	delArgs := make([]any, 0, len(dead)*2)
	for _, o := range dead {
		delTuples = append(delTuples, "(?, ?)")
		delArgs = append(delArgs, o.id, o.size)
	}
	if len(delTuples) > 0 {
		if _, err := tx.Exec(`DELETE FROM jfs_chunk_ref WHERE (chunkid, size) IN (`+strings.Join(delTuples, ",")+`)`, delArgs...); err != nil {
			return err
		}
	}
	return nil
}
func blockLen(size uint32, indx int) uint32 {
	remain := int64(size) - int64(indx)*ExtentBlockSize
	if remain > ExtentBlockSize {
		return ExtentBlockSize
	}
	if remain < 0 {
		return 0
	}
	return uint32(remain)
}
func nullUint64(v uint64) any {
	if v == 0 {
		return nil
	}
	return v
}
func (s *Store) jfsEnqueueFileGCTx(tx *sql.Tx, ino, length uint64) error {
	// Record the inode only. JuiceFS sql_unlink with MaxDeletes=0 does the
	// same; walking every 24-byte slice here made delete-journal COMMIT unlink
	// take seconds (sqlite --wait all is 10s). DrainDeletedFile walks
	// jfs_chunk in batches after the transaction commits, so the slices must
	// survive until then, and the delfile row is the crash-recovery record.
	_, err := tx.Exec(`INSERT IGNORE INTO jfs_delfile (inode, length, expire) VALUES (?, ?, ?)`, ino, length, time.Now().Unix())
	return err
}

// jfsDeleteChunkBatch bounds how many jfs_chunk rows one drain transaction
// walks; the rows are deleted as they are processed, so a big file drains over
// several rounds without holding a long lock.
const jfsDeleteChunkBatch = 64

// jfsReclaimOrphanedInoTx records a jfs inode for reclamation when the extent
// tree no longer references it but the drive9 projection still did: the orphan
// cleanup paths delete such a projection row, and that row may have been the
// last thing naming the inode's chunks. Without a jfs_delfile record nothing
// would ever find them again — no edge for jfsDeleteSubtreeTx, no row for
// DrainDeletedFile — so the node, its jfs_chunk slices and their blocks would
// leak for ever.
//
// It refuses to touch anything that is still referenced: the node must exist,
// must be a regular file, must have no edge left (a hardlink alias keeps the
// inode alive, and the alias has its own projection row) and must have no open
// handle, whose data JuiceFS keeps until the last close.
//
// The node row goes with the record, exactly as in jfsUnlinkTx's last-reference
// branch: the drain reclaims the chunks of an inode it can still look up, and
// leaving the node behind would keep a slice list with no owner.
func (s *Store) jfsReclaimOrphanedInoTx(tx *sql.Tx, ino uint64) error {
	if ino == 0 || ino == jfsRootIno {
		return nil
	}
	var typ uint8
	var length uint64
	err := tx.QueryRow(`SELECT type, length FROM jfs_node WHERE inode = ? FOR UPDATE`, ino).Scan(&typ, &length)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if typ != jfsTypeFile {
		return nil
	}
	var edges int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE inode = ? FOR UPDATE`, ino).Scan(&edges); err != nil {
		return err
	}
	if edges != 0 {
		return nil
	}
	var opened int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_sustained WHERE inode = ? FOR UPDATE`, ino).Scan(&opened); err != nil {
		return err
	}
	if opened != 0 {
		return nil
	}
	if err := s.jfsEnqueueFileGCTx(tx, ino, length); err != nil {
		return err
	}
	_, err = tx.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino)
	return err
}

// DrainDeletedFile reclaims the blocks of one deleted extent file: it walks
// that inode's slices in batches, drops each slice's refcount, queues the
// blocks whose refs hit zero for object deletion, and finally removes the
// jfs_chunk/jfs_delfile rows. Safe to call repeatedly; it is a no-op once the
// delfile row is gone.
func (s *Store) DrainDeletedFile(ctx context.Context, ino uint64) error {
	if ino == 0 {
		return nil
	}
	for {
		done, err := s.drainDeletedFileOnce(ctx, ino)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
func (s *Store) drainDeletedFileOnce(ctx context.Context, ino uint64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	done, err := s.drainDeletedFileTx(tx, ino)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return done, nil
}
func (s *Store) drainDeletedFileTx(tx *sql.Tx, ino uint64) (bool, error) {
	var pending int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = ?`, ino).Scan(&pending); err != nil {
		return false, err
	}
	if pending == 0 {
		return true, nil
	}
	rows, err := tx.Query(`SELECT indx, slices FROM jfs_chunk WHERE inode = ? ORDER BY indx LIMIT ?`, ino, jfsDeleteChunkBatch)
	if err != nil {
		return false, fmt.Errorf("drain select chunks: %w", err)
	}
	type chunkRow struct {
		indx   uint32
		slices []byte
	}
	var batch []chunkRow
	for rows.Next() {
		var cr chunkRow
		if err := rows.Scan(&cr.indx, &cr.slices); err != nil {
			_ = rows.Close()
			return false, err
		}
		batch = append(batch, cr)
	}
	if err := rows.Close(); err != nil {
		return false, err
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(batch) == 0 {
		if _, err := tx.Exec(`DELETE FROM jfs_delfile WHERE inode = ?`, ino); err != nil {
			return false, err
		}
		return true, nil
	}
	// A slice can appear more than once in one file (rewrites append a new
	// record), so the refcount drop is per occurrence.
	counts := make(map[extentSliceRef]int)
	order := make([]extentSliceRef, 0, len(batch))
	indxArgs := make([]any, 0, len(batch))
	indxPlace := make([]string, 0, len(batch))
	for _, cr := range batch {
		indxArgs = append(indxArgs, cr.indx)
		indxPlace = append(indxPlace, "?")
		for i := 0; i+extentSliceBytes <= len(cr.slices); i += extentSliceBytes {
			id := binary.BigEndian.Uint64(cr.slices[i+4 : i+12])
			size := binary.BigEndian.Uint32(cr.slices[i+12 : i+16])
			if id == 0 {
				continue
			}
			ref := extentSliceRef{id: id, size: size}
			if _, seen := counts[ref]; !seen {
				order = append(order, ref)
			}
			counts[ref]++
		}
	}
	for _, ref := range order {
		if _, err := tx.Exec(`UPDATE jfs_chunk_ref SET refs = refs - ? WHERE chunkid = ? AND size = ?`,
			counts[ref], ref.id, ref.size); err != nil {
			return false, fmt.Errorf("drain decrement ref: %w", err)
		}
	}
	if err := s.jfsEnqueueDeadSliceGCTx(tx, ino, order); err != nil {
		return false, fmt.Errorf("drain enqueue gc: %w", err)
	}
	if _, err := tx.Exec(`DELETE FROM jfs_chunk WHERE inode = ? AND indx IN (`+strings.Join(indxPlace, ",")+`)`,
		append([]any{ino}, indxArgs...)...); err != nil {
		return false, fmt.Errorf("drain delete chunks: %w", err)
	}
	return false, nil
}

// DrainPendingDeletedFiles drains up to limit deleted-file records that are
// ready (expire <= now). Used inline after unlink and by the tenant worker as
// the safety net for drains that were interrupted.
func (s *Store) DrainPendingDeletedFiles(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	drained := 0
	for i := 0; i < limit; i++ {
		if err := ctx.Err(); err != nil {
			return drained, err
		}
		var ino uint64
		err := s.db.QueryRowContext(ctx, `SELECT inode FROM jfs_delfile WHERE expire <= ? ORDER BY expire LIMIT 1`,
			time.Now().Unix()).Scan(&ino)
		if errors.Is(err, sql.ErrNoRows) {
			return drained, nil
		}
		if err != nil {
			return drained, err
		}
		if err := s.DrainDeletedFile(ctx, ino); err != nil {
			return drained, err
		}
		drained++
	}
	return drained, nil
}
func (s *Store) jfsDeleteSustainedTx(tx *sql.Tx, sid, ino uint64) error {
	if _, err := tx.Exec(`DELETE FROM jfs_sustained WHERE sid = ? AND inode = ?`, sid, ino); err != nil {
		return err
	}
	// FOR UPDATE, like jfsUnlinkTx's holder count: two sessions holding the
	// same inode (the normal case since open_ref registers every runtime's
	// opens) can close concurrently, and a snapshot count would let each see
	// the other's row survive, skip reclaim in both, and strand a nlink=0
	// node with no edge, no sustained row and no delfile record — a leak
	// nothing walks again. The current-read count serializes the two closers
	// (the loser blocks on the winner's deleted row and then sees zero); the
	// inverse lock order against open_ref's node-then-sustained sequence can
	// deadlock, which RunExtentMetaOp's retry loop resolves.
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_sustained WHERE inode = ? FOR UPDATE`, ino).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	// The attr read is locked too, so the nlink check is ordered against a
	// concurrent open_ref on this inode rather than read from a snapshot.
	attr, eno, err := s.jfsGetAttrForUpdateTx(tx, ino)
	if err != nil || eno != 0 {
		return err
	}
	if attr != nil && attr.Nlink == 0 {
		if err := s.jfsEnqueueFileGCTx(tx, ino, attr.Length); err != nil {
			return err
		}
		// The last reference is gone and the owning session has ended, so the
		// node row is dead. Leaving it behind leaks one row per
		// create -> unlink-while-open -> close cycle (the temp-file pattern
		// editors and sqlite use constantly); a later write on a stale handle
		// must fail on the missing node rather than resurrect it.
		if attr.Typ == jfsTypeSymlink {
			if _, err := tx.Exec(`DELETE FROM jfs_symlink WHERE inode = ?`, ino); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) jfsFindStaleSessionsTx(tx *sql.Tx, limit int) ([]uint64, error) {
	if limit <= 0 {
		limit = 16
	}
	now := time.Now().Unix()
	rows, err := tx.Query(`SELECT sid FROM jfs_session2 WHERE expire > 0 AND expire < ? ORDER BY expire LIMIT ?`, now, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var sids []uint64
	for rows.Next() {
		var sid uint64
		if err := rows.Scan(&sid); err != nil {
			return nil, err
		}
		sids = append(sids, sid)
	}
	if sids == nil {
		sids = []uint64{}
	}
	return sids, rows.Err()
}
func (s *Store) jfsCleanStaleSessionTx(tx *sql.Tx, sid uint64) error {
	if _, err := tx.Exec(`DELETE FROM jfs_flock WHERE sid = ?`, sid); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_plock WHERE sid = ?`, sid); err != nil {
		return err
	}
	rows, err := tx.Query(`SELECT inode FROM jfs_sustained WHERE sid = ?`, sid)
	if err != nil {
		return err
	}
	var inos []uint64
	for rows.Next() {
		var ino uint64
		if err := rows.Scan(&ino); err != nil {
			_ = rows.Close()
			return err
		}
		inos = append(inos, ino)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, ino := range inos {
		if err := s.jfsDeleteSustainedTx(tx, sid, ino); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`DELETE FROM jfs_session2 WHERE sid = ?`, sid)
	return err
}

// UnlinkExtentPath removes an extent file via the JuiceFS Unlink txn (edge + projection).
func (s *Store) UnlinkExtentPath(ctx context.Context, path string, opened bool) error {
	// Capture the extent inode before the projection row disappears: the
	// unlink transaction only records jfs_delfile, so the blocks are reclaimed
	// here right after it commits.
	var ino uint64
	_ = s.db.QueryRowContext(ctx, `SELECT extent_ino FROM file_nodes WHERE `+
		s.scope.And(`path_hash = ? AND path = ?`),
		s.scope.Args(fileNodePathHash(path), path)...).Scan(&ino)
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		if err := s.jfsEnsureInitTx(tx); err != nil {
			return err
		}
		return s.unlinkExtentPathTx(ctx, tx, path, opened)
	})
	if err == nil && ino != 0 {
		_ = s.DrainDeletedFile(ctx, ino)
	}
	return err
}
func (s *Store) unlinkExtentTree(ctx context.Context, dirPath string) error {
	prefix := dirPath
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	// Escape the prefix: a directory name containing % or _ would otherwise
	// over-match and unlink extent files in sibling directories.
	rows, err := s.db.QueryContext(ctx, `SELECT path FROM file_nodes WHERE `+
		s.scope.And(`is_directory = 0 AND content_layout = ? AND (path = ? OR path LIKE ? ESCAPE '\\')`),
		s.scope.Args(string(ContentLayoutExtent), dirPath, likeLiteralPrefixPattern(prefix))...)
	if err != nil {
		return err
	}
	var paths []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			_ = rows.Close()
			return err
		}
		paths = append(paths, p)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, p := range paths {
		if err := s.UnlinkExtentPath(ctx, p, false); err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	return nil
}

// jfsDeleteSubtreeTx removes the JuiceFS subtree rooted at ino: every edge and
// node below it, plus the node itself. Extent files are recorded in
// jfs_delfile so DrainDeletedFile reclaims their blocks. Server-side directory
// deletes only touch file_nodes, so without this the jfs tree keeps orphan
// edges (P0-3): rmdir then fails with ENOTEMPTY and recreating the name binds
// the orphan inode.
func (s *Store) jfsDeleteSubtreeTx(tx *sql.Tx, ino uint64) error {
	if ino == 0 || ino == jfsRootIno {
		return nil
	}
	queue := []uint64{ino}
	seen := map[uint64]struct{}{}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if _, ok := seen[cur]; ok {
			continue
		}
		seen[cur] = struct{}{}
		rows, err := tx.Query(`SELECT inode FROM jfs_edge WHERE parent = ?`, cur)
		if err != nil {
			return err
		}
		for rows.Next() {
			var child uint64
			if err := rows.Scan(&child); err != nil {
				_ = rows.Close()
				return err
			}
			if child != jfsRootIno {
				queue = append(queue, child)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
	}
	now := time.Now().Unix()
	for node := range seen {
		var typ uint8
		var length uint64
		var nlink uint32
		// FOR UPDATE, like jfsUnlinkTx and jfsReclaimOrphanedInoTx: a plain
		// read left a window for a concurrent open_ref to commit its holder
		// row between the sustained count below and this transaction's node
		// DELETE, reclaiming the inode under the handle that was registering.
		err := tx.QueryRow(`SELECT type, length, nlink FROM jfs_node WHERE inode = ? FOR UPDATE`, node).Scan(&typ, &length, &nlink)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// Edge without a node (already half-deleted): drop the edge below.
		case err != nil:
			return err
		default:
			// A node another runtime still holds open is not reclaimable here,
			// whatever this subtree delete says: its blocks must outlive that
			// handle (same rule as jfsUnlinkTx). The name still goes — only the
			// node, its chunks and the holder rows stay, with nlink = 0 so the
			// holders' last close (jfsDeleteSustainedTx) or the stale-session
			// sweep reclaims them.
			var holders int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_sustained WHERE inode = ? FOR UPDATE`, node).Scan(&holders); err != nil {
				return err
			}
			if holders > 0 {
				if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? OR inode = ?`, node, node); err != nil {
					return err
				}
				if _, err := tx.Exec(`UPDATE jfs_node SET nlink = 0 WHERE inode = ?`, node); err != nil {
					return err
				}
				continue
			}
			if typ == jfsTypeFile {
				// Every file in the subtree, including one already unlinked while
				// open (nlink = 0, its blocks still referenced only by this node):
				// the node and its jfs_sustained row go below, so without this
				// record nothing could ever find those chunks again. The drain is
				// idempotent and drops refcounts rather than deleting shared
				// blocks, so recording a node that was already enqueued is safe.
				if _, err := tx.Exec(`INSERT IGNORE INTO jfs_delfile (inode, length, expire) VALUES (?, ?, ?)`,
					node, length, now); err != nil {
					return err
				}
			}
		}
		if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? OR inode = ?`, node, node); err != nil {
			return err
		}
		for _, q := range []string{
			`DELETE FROM jfs_node WHERE inode = ?`,
			`DELETE FROM jfs_symlink WHERE inode = ?`,
			`DELETE FROM jfs_sustained WHERE inode = ?`,
		} {
			if _, err := tx.Exec(q, node); err != nil {
				return err
			}
		}
	}
	return nil
}

// SweepStaleExtentSessions reclaims locks and open-unlinked files for expired sessions.
func (s *Store) SweepStaleExtentSessions(ctx context.Context, limit int) (int, error) {
	n := 0
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		sids, err := s.jfsFindStaleSessionsTx(tx, limit)
		if err != nil {
			return err
		}
		for _, sid := range sids {
			if err := s.jfsCleanStaleSessionTx(tx, sid); err != nil {
				return err
			}
			n++
		}
		return nil
	})
	return n, err
}

// jfsClaimCompactTx takes the oldest claimable compact task under a real lease:
// the row is locked for the surrounding transaction and the UPDATE owns it only
// if it still matches the same "claimable" predicate, so two executors racing
// for one task cannot both win. Tasks are queued by the write path (see
// enqueueCompactAfterWrite); there is no self-feeding discovery scan anymore,
// because `LENGTH(slices)` has no index and the upsert it needed used to
// overwrite the lease of whoever claimed the chunk first.
func (s *Store) jfsClaimCompactTx(tx *sql.Tx) (ino uint64, chunk uint32, taskID string, err error) {
	const claimable = `status = 'PENDING' OR (status = 'LEASED' AND lease_until < CURRENT_TIMESTAMP(3))`
	err = tx.QueryRow(`SELECT task_id, extent_ino, chunk FROM slice_compact_tasks
		WHERE `+claimable+`
		ORDER BY created_at LIMIT 1 FOR UPDATE`).Scan(&taskID, &ino, &chunk)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, "", nil
	}
	if err != nil {
		return 0, 0, "", err
	}
	res, err := tx.Exec(`UPDATE slice_compact_tasks SET status = 'LEASED', leased_at = CURRENT_TIMESTAMP(3),
		lease_until = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 5 MINUTE)
		WHERE task_id = ? AND (`+claimable+`)`, taskID)
	if err != nil {
		return 0, 0, "", err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		// Another executor claimed it between the SELECT and the UPDATE.
		return 0, 0, "", nil
	}
	return ino, chunk, taskID, nil
}

// jfsRequeueCompactTx returns a leased task to the queue after a failed
// attempt, counting the attempt the way RequeueBlockGC does: without the count
// max_attempts is dead, and a chunk whose CAS keeps losing is retried by every
// mount plus the server fallback for the life of the tenant.
func (s *Store) jfsRequeueCompactTx(tx *sql.Tx, taskID string, cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	_, err := tx.Exec(`UPDATE slice_compact_tasks
		SET attempt_count = attempt_count + 1,
			last_error = ?,
			status = CASE WHEN max_attempts > 0 AND attempt_count + 1 >= max_attempts THEN 'FAILED' ELSE 'PENDING' END,
			leased_at = NULL,
			lease_until = NULL
		WHERE task_id = ? AND status = 'LEASED'`, msg, taskID)
	return err
}

// jfsCompleteCompactTx acknowledges a claimed task whose chunk needs no work.
// ExecuteCompact reports success for a chunk that was drained or is already
// thin, and those outcomes have to ack the row: leaving it LEASED meant the
// lease expired and it was reclaimed only to no-op again, so one row per
// (inode, chunk) stayed in the table for the life of the tenant.
func (s *Store) jfsCompleteCompactTx(tx *sql.Tx, taskID string) error {
	if taskID == "" {
		return nil
	}
	_, err := tx.Exec(`UPDATE slice_compact_tasks
		SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(3),
			leased_at = NULL, lease_until = NULL
		WHERE task_id = ? AND status IN ('PENDING','LEASED')`, taskID)
	return err
}

// CompleteCompactTask is jfsCompleteCompactTx for the in-process fallback.
func (s *Store) CompleteCompactTask(ctx context.Context, taskID string) error {
	return s.InTx(ctx, func(tx *sql.Tx) error {
		return s.jfsCompleteCompactTx(tx, taskID)
	})
}

// RequeueCompactTask is jfsRequeueCompactTx for the in-process fallback, so its
// failures count attempts the same way a mount's do.
func (s *Store) RequeueCompactTask(ctx context.Context, taskID string, cause error) error {
	return s.InTx(ctx, func(tx *sql.Tx) error {
		return s.jfsRequeueCompactTx(tx, taskID, cause)
	})
}

// ListPendingBlockGC returns block keys ready for deletion. A task only
// becomes visible once its grace period has passed: the superseded slice is
// already gone from metadata when its blocks are queued, so a reader on
// another mount that still holds the pre-compaction slice list would hit a
// deleted object if the delete ran immediately (the combination JuiceFS's own
// docs warn about for compaction plus write cache).
func (s *Store) ListPendingBlockGC(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT block_key FROM block_gc_tasks
		WHERE status = 'PENDING' AND available_at <= CURRENT_TIMESTAMP(3)
		ORDER BY created_at LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}
func (s *Store) MarkBlockGCDone(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE block_gc_tasks SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(3)
		WHERE block_key = ?`, key)
	return err
}

// RequeueBlockGC records a failed block deletion and makes the task visible
// again immediately. Once max_attempts is exhausted the task is parked as
// FAILED instead of being retried forever, and the caller is expected to log
// it: a block that can never be deleted is a leak, not a transient error.
// The boolean reports whether another attempt is left.
func (s *Store) RequeueBlockGC(ctx context.Context, key string, cause error) (bool, error) {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	res, err := s.db.ExecContext(ctx, `UPDATE block_gc_tasks
		SET attempt_count = attempt_count + 1,
			last_error = ?,
			status = CASE WHEN max_attempts > 0 AND attempt_count + 1 >= max_attempts THEN 'FAILED' ELSE status END,
			available_at = CASE WHEN max_attempts > 0 AND attempt_count + 1 >= max_attempts
				THEN available_at ELSE CURRENT_TIMESTAMP(3) END,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE block_key = ? AND status = 'PENDING'`, msg, key)
	if err != nil {
		return false, err
	}
	if rows, err := res.RowsAffected(); err == nil && rows == 0 {
		// Someone else completed it, or an earlier attempt already parked it.
		return false, nil
	}
	var retry bool
	err = s.db.QueryRowContext(ctx, `SELECT status = 'PENDING' FROM block_gc_tasks WHERE block_key = ?`, key).Scan(&retry)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return retry, err
}
func (s *Store) BlockGCTaskStatus(ctx context.Context, key string) (string, error) {
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM block_gc_tasks WHERE block_key = ?`, key).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	return status, err
}
func (s *Store) ClaimCompactTask(ctx context.Context) (ino uint64, chunk uint32, taskID string, err error) {
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		var e error
		ino, chunk, taskID, e = s.jfsClaimCompactTx(tx)
		return e
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, "", nil
	}
	return ino, chunk, taskID, err
}
