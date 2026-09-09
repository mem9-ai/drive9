package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	PendingBlockSourceFUSE    = "fuse"
	PendingBlockSourceUpload  = "upload"
	PendingBlockSourceCompact = "compact"

	ExtentPendingTTL   = time.Hour
	ExtentBlockGCGrace = time.Hour
	// JuiceFS meta.Read kicks compact when a chunk has >=5 slices.
	ExtentCompactReadRows = 5
	// JuiceFS meta.Write: go compactChunk when numSlices%100==99 || numSlices>350;
	// sync compactChunk(once) when numSlices>=maxSlices (2500).
	ExtentCompactWriteMod    = 100
	ExtentCompactWriteSoft   = 350
	ExtentCompactMaxSlices   = 2500
	ExtentCompactSoftRows    = ExtentCompactWriteSoft
	ExtentCompactSyncRows    = ExtentCompactMaxSlices
	ExtentCompactMaxAttempts = 8
	ExtentCommitOpsRetain    = 64
	ExtentCommitOpsMaxAge    = 24 * time.Hour

	BlockGCTaskQueued    = "queued"
	BlockGCTaskLeased    = "leased"
	BlockGCTaskCompleted = "completed"

	SliceCompactQueued    = "queued"
	SliceCompactLeased    = "leased"
	SliceCompactCompleted = "completed"
)

// ShouldCompactOnWrite is JuiceFS meta.Write: numSlices%100==99 || numSlices>350.
func ShouldCompactOnWrite(numSlices int) bool {
	if numSlices <= 0 {
		return false
	}
	return numSlices%ExtentCompactWriteMod == ExtentCompactWriteMod-1 || numSlices > ExtentCompactWriteSoft
}

// ShouldSyncCompactOnWrite is JuiceFS meta.Write compactChunk(once) at maxSlices.
func ShouldSyncCompactOnWrite(numSlices int) bool {
	return numSlices >= ExtentCompactMaxSlices
}

// PendingBlock is a row in pending_blocks.
type PendingBlock struct {
	BlockKey       string
	InodeID        string
	SizeBytes      int64
	ChecksumSHA256 string
	ReservedBytes  int64
	Source         string
	CreatedAt      time.Time
}

// SliceCommitOp is an idempotency log row.
type SliceCommitOp struct {
	OpID            string
	InodeID         string
	GenerationAfter int64
	RevisionAfter   int64
	SizeAfter       int64
	CreatedAt       time.Time
}

// ChunkRowCount is the number of slice rows in one 64MiB chunk after commit.
type ChunkRowCount struct {
	Chunk int64 `json:"chunk"`
	Rows  int   `json:"rows"`
}

// CommitSlicesResult is returned after a successful (or replayed) commit.
type CommitSlicesResult struct {
	Revision   int64
	Generation int64
	SizeBytes  int64
	Replayed   bool
	ChunkRows  []ChunkRowCount `json:"chunk_rows,omitempty"`
}

type extentCommitState struct {
	inodeID    string
	revision   int64
	generation int64
	sizeBytes  int64
	layout     ContentLayout
}

func (s *Store) InsertPendingBlocksTx(tx execer, blocks []PendingBlock, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	for i := range blocks {
		b := blocks[i]
		if b.BlockKey == "" || b.InodeID == "" || b.SizeBytes <= 0 {
			return fmt.Errorf("invalid pending block")
		}
		source := b.Source
		if source == "" {
			source = PendingBlockSourceFUSE
		}
		_, err := tx.Exec(`INSERT INTO pending_blocks
			(`+s.scope.InsCols(`block_key, inode_id, size_bytes, checksum_sha256, reserved_bytes, source, created_at`)+`)
			VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?, ?`)+`)`,
			s.scope.Args(b.BlockKey, b.InodeID, b.SizeBytes, b.ChecksumSHA256, b.ReservedBytes, source, now)...)
		if err != nil {
			return fmt.Errorf("insert pending block %s: %w", b.BlockKey, err)
		}
	}
	return nil
}

func (s *Store) GetPendingBlock(ctx context.Context, blockKey string) (*PendingBlock, error) {
	row := s.db.QueryRowContext(ctx, `SELECT block_key, inode_id, size_bytes, checksum_sha256, reserved_bytes, source, created_at
		FROM pending_blocks WHERE `+s.scope.And(`block_key = ?`), s.scope.Args(blockKey)...)
	return scanPendingBlock(row)
}

func (s *Store) GetPendingBlocksForInode(ctx context.Context, inodeID string, keys []string) (map[string]PendingBlock, error) {
	out := make(map[string]PendingBlock, len(keys))
	if len(keys) == 0 {
		return out, nil
	}
	placeholders := strings.Repeat("?,", len(keys))
	placeholders = placeholders[:len(placeholders)-1]
	args := s.scope.Args(inodeID)
	for _, k := range keys {
		args = append(args, k)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT block_key, inode_id, size_bytes, checksum_sha256, reserved_bytes, source, created_at
		FROM pending_blocks WHERE `+s.scope.And(`inode_id = ? AND block_key IN (`+placeholders+`)`), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var b PendingBlock
		if err := rows.Scan(&b.BlockKey, &b.InodeID, &b.SizeBytes, &b.ChecksumSHA256, &b.ReservedBytes, &b.Source, &b.CreatedAt); err != nil {
			return nil, err
		}
		out[b.BlockKey] = b
	}
	return out, rows.Err()
}

func scanPendingBlock(row *sql.Row) (*PendingBlock, error) {
	var b PendingBlock
	err := row.Scan(&b.BlockKey, &b.InodeID, &b.SizeBytes, &b.ChecksumSHA256, &b.ReservedBytes, &b.Source, &b.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &b, nil
}

// ExtentReadState is JuiceFS doRead+inode attr from one meta txn: length and
// the chunk slice list are the same snapshot.
type ExtentReadState struct {
	InodeID    string
	Revision   int64
	Generation int64
	SizeBytes  int64
	Layout     ContentLayout
	Rows       []SliceRow
}

func (s *Store) ListSlicesForInode(ctx context.Context, inodeID string) ([]SliceRow, error) {
	// JuiceFS doRead is one chunk blob. Multi-row file_slices must not be
	// observed mid-replace (DELETE+INSERT): that window is all-zero btree
	// pages (wal-multiwrite). Current-read the inode and slice rows so compact
	// FOR UPDATE and this list are serialized (TiDB RR snapshot SELECT of
	// file_slices would otherwise miss a committed replace).
	var out []SliceRow
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		if _, err := s.lockExtentStateTx(ctx, tx, inodeID); err != nil {
			return err
		}
		rows, err := s.listSlicesForInodeTx(ctx, tx, inodeID)
		if err != nil {
			return err
		}
		out = rows
		return nil
	})
	return out, err
}

// LoadExtentReadState is JuiceFS meta.Read: inode length and the slice list
// from one transaction. Plan must not Stat in one txn and list slices in
// another (VACUUM truncate-then-write leaves size covering holes).
func (s *Store) LoadExtentReadState(ctx context.Context, path string) (*ExtentReadState, error) {
	var out *ExtentReadState
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		nf, err := s.StatTx(ctx, tx, path)
		if err != nil {
			return err
		}
		if nf.File == nil || !nf.File.IsExtent() {
			return ErrNotExtent
		}
		st, err := s.lockExtentStateTx(ctx, tx, nf.File.FileID)
		if err != nil {
			return err
		}
		rows, err := s.listSlicesForInodeTx(ctx, tx, nf.File.FileID)
		if err != nil {
			return err
		}
		out = &ExtentReadState{
			InodeID:    st.inodeID,
			Revision:   st.revision,
			Generation: st.generation,
			SizeBytes:  st.sizeBytes,
			Layout:     st.layout,
			Rows:       rows,
		}
		return nil
	})
	return out, err
}

func (s *Store) ListSlicesOverlapping(ctx context.Context, inodeID string, start, end int64) ([]SliceRow, error) {
	if end <= start {
		return nil, nil
	}
	all, err := s.ListSlicesForInode(ctx, inodeID)
	if err != nil {
		return nil, err
	}
	var out []SliceRow
	for _, row := range all {
		if row.FileOff < end && row.FileOff+row.Len > start {
			out = append(out, row)
		}
	}
	return out, nil
}

func (s *Store) CountSlicesInChunk(ctx context.Context, inodeID string, chunk int64) (int, error) {
	var n int
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		blob, getErr := s.getChunkBlobTx(ctx, tx, inodeID, chunk)
		if getErr != nil {
			return getErr
		}
		if len(blob) > 0 {
			c, countErr := countChunkSliceRecords(blob)
			if countErr != nil {
				return countErr
			}
			n = c
			return nil
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM file_slices WHERE `+s.scope.And(`inode_id = ? AND chunk = ?`),
			s.scope.Args(inodeID, chunk)...).Scan(&n)
	})
	return n, err
}

func (s *Store) HasPendingBlocks(ctx context.Context, inodeID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_blocks WHERE `+s.scope.And(`inode_id = ?`),
		s.scope.Args(inodeID)...).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *Store) GetSliceCommitOp(ctx context.Context, opID string) (*SliceCommitOp, error) {
	row := s.db.QueryRowContext(ctx, `SELECT op_id, inode_id, generation_after, revision_after, size_after, created_at
		FROM slice_commit_ops WHERE `+s.scope.And(`op_id = ?`), s.scope.Args(opID)...)
	var op SliceCommitOp
	err := row.Scan(&op.OpID, &op.InodeID, &op.GenerationAfter, &op.RevisionAfter, &op.SizeAfter, &op.CreatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return &op, nil
}

func scanSliceRows(rows *sql.Rows) ([]SliceRow, error) {
	var out []SliceRow
	for rows.Next() {
		var r SliceRow
		if err := rows.Scan(&r.InodeID, &r.Chunk, &r.Seq, &r.FileOff, &r.Len, &r.BlockKey, &r.BlockOff, &r.BlockLen, &r.ChecksumSHA256, &r.Kind, &r.BornGen); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) lockExtentStateTx(ctx context.Context, tx *sql.Tx, inodeID string) (*extentCommitState, error) {
	var st extentCommitState
	var layout string
	err := tx.QueryRowContext(ctx, `SELECT i.inode_id, i.revision, i.size_bytes,
		COALESCE(c.slice_generation, 0), COALESCE(c.content_layout, 'single')
		FROM inodes i JOIN contents c ON c.inode_id = i.inode_id
		WHERE `+s.scope.AndAs("i", `i.inode_id = ?`)+` FOR UPDATE`,
		scopeWhereArgs(s.scope, 2, s.scope.Args(inodeID)...)...).Scan(
		&st.inodeID, &st.revision, &st.sizeBytes, &st.generation, &layout)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	st.layout = ContentLayout(layout)
	return &st, nil
}

func (s *Store) listSlicesForInodeTx(ctx context.Context, tx *sql.Tx, inodeID string) ([]SliceRow, error) {
	// JuiceFS doRead: one blob row per chunk. Current-read the blob; decode
	// is in-memory last-write-wins. file_slices is only a fallback for
	// inodes written before file_chunks existed.
	blobs, err := s.listChunkBlobsTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	if len(blobs) > 0 {
		var out []SliceRow
		for _, item := range blobs {
			rows, decErr := decodeChunkSliceBlob(inodeID, item.chunk, item.blob)
			if decErr != nil {
				return nil, decErr
			}
			out = append(out, rows...)
		}
		return out, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT inode_id, chunk, seq, file_off, len, block_key, block_off, block_len, checksum_sha256, kind, born_gen
		FROM file_slices WHERE `+s.scope.And(`inode_id = ?`)+` ORDER BY chunk, seq, file_off FOR UPDATE`, s.scope.Args(inodeID)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanSliceRows(rows)
}

type chunkBlob struct {
	chunk int64
	blob  []byte
}

func (s *Store) listChunkBlobsTx(ctx context.Context, tx *sql.Tx, inodeID string) ([]chunkBlob, error) {
	rows, err := tx.QueryContext(ctx, `SELECT chunk, slices FROM file_chunks WHERE `+s.scope.And(`inode_id = ?`)+` ORDER BY chunk FOR UPDATE`,
		s.scope.Args(inodeID)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []chunkBlob
	for rows.Next() {
		var item chunkBlob
		if err := rows.Scan(&item.chunk, &item.blob); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) getChunkBlobTx(ctx context.Context, tx *sql.Tx, inodeID string, chunk int64) ([]byte, error) {
	var blob []byte
	err := tx.QueryRowContext(ctx, `SELECT slices FROM file_chunks WHERE `+s.scope.And(`inode_id = ? AND chunk = ?`)+` FOR UPDATE`,
		s.scope.Args(inodeID, chunk)...).Scan(&blob)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return blob, err
}

func (s *Store) appendChunkSliceTx(ctx context.Context, tx *sql.Tx, inodeID string, chunk int64, rec []byte) error {
	if len(rec) == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO file_chunks (`+s.scope.InsCols(`inode_id, chunk, slices`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?`)+`)
		ON DUPLICATE KEY UPDATE slices = CONCAT(slices, VALUES(slices))`,
		s.scope.Args(inodeID, chunk, rec)...)
	if err != nil {
		return fmt.Errorf("append chunk slice: %w", err)
	}
	return nil
}

func (s *Store) putChunkBlobTx(ctx context.Context, tx *sql.Tx, inodeID string, chunk int64, blob []byte) error {
	if len(blob) == 0 {
		_, err := tx.ExecContext(ctx, `DELETE FROM file_chunks WHERE `+s.scope.And(`inode_id = ? AND chunk = ?`),
			s.scope.Args(inodeID, chunk)...)
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO file_chunks (`+s.scope.InsCols(`inode_id, chunk, slices`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?`)+`)
		ON DUPLICATE KEY UPDATE slices = VALUES(slices)`,
		s.scope.Args(inodeID, chunk, blob)...)
	if err != nil {
		return fmt.Errorf("put chunk blob: %w", err)
	}
	return nil
}

func (s *Store) replaceChunkSlicesTx(ctx context.Context, tx *sql.Tx, inodeID string, chunk int64, next []SliceRow) error {
	blob, err := marshalChunkSlices(sliceRowsToOps(next))
	if err != nil {
		return err
	}
	if err := s.putChunkBlobTx(ctx, tx, inodeID, chunk, blob); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_slices WHERE `+s.scope.And(`inode_id = ? AND chunk = ?`), s.scope.Args(inodeID, chunk)...); err != nil {
		return fmt.Errorf("delete chunk slices: %w", err)
	}
	return nil
}

func (s *Store) replaceSlicesTx(ctx context.Context, tx *sql.Tx, inodeID string, next []SliceRow) error {
	byChunk := map[int64][]SliceRow{}
	for _, row := range next {
		row.InodeID = inodeID
		if row.Kind == "" {
			row.Kind = SliceKindData
		}
		byChunk[row.Chunk] = append(byChunk[row.Chunk], row)
	}
	existing, err := s.listChunkBlobsTx(ctx, tx, inodeID)
	if err != nil {
		return err
	}
	keep := map[int64]struct{}{}
	for chunk, rows := range byChunk {
		keep[chunk] = struct{}{}
		if err := s.replaceChunkSlicesTx(ctx, tx, inodeID, chunk, rows); err != nil {
			return err
		}
	}
	for _, item := range existing {
		if _, ok := keep[item.chunk]; ok {
			continue
		}
		if err := s.putChunkBlobTx(ctx, tx, inodeID, item.chunk, nil); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_slices WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return fmt.Errorf("delete file_slices: %w", err)
	}
	return nil
}

func (s *Store) BlockRefCount(ctx context.Context, blockKey string) (int64, error) {
	if blockKey == "" {
		return 0, nil
	}
	var refs int64
	err := s.db.QueryRowContext(ctx, `SELECT refs FROM file_slice_refs WHERE `+s.scope.And(`block_key = ?`),
		s.scope.Args(blockKey)...).Scan(&refs)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return refs, err
}

func (s *Store) BlockRefCountTx(ctx context.Context, tx *sql.Tx, blockKey string) (int64, error) {
	if blockKey == "" {
		return 0, nil
	}
	var refs int64
	err := tx.QueryRowContext(ctx, `SELECT refs FROM file_slice_refs WHERE `+s.scope.And(`block_key = ?`)+` FOR UPDATE`,
		s.scope.Args(blockKey)...).Scan(&refs)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return refs, err
}

func (s *Store) applyRefDeltaTx(ctx context.Context, tx *sql.Tx, inodeID string, delta map[string]int64, blockLens map[string]int64, now time.Time) error {
	keys := make([]string, 0, len(delta))
	for key := range delta {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		d := delta[key]
		if d == 0 {
			continue
		}
		if d > 0 {
			size := blockLens[key]
			_, err := tx.ExecContext(ctx, `INSERT INTO file_slice_refs
				(`+s.scope.InsCols(`block_key, refs, size_bytes`)+`)
				VALUES (`+s.scope.InsVals(`?, ?, ?`)+`)
				ON DUPLICATE KEY UPDATE refs = refs + ?, size_bytes = IF(size_bytes = 0, VALUES(size_bytes), size_bytes)`,
				s.scope.Args(key, d, size, d)...)
			if err != nil {
				return fmt.Errorf("inc refs %s: %w", key, err)
			}
			continue
		}
		var refs int64
		err := tx.QueryRowContext(ctx, `SELECT refs FROM file_slice_refs WHERE `+s.scope.And(`block_key = ?`)+` FOR UPDATE`,
			s.scope.Args(key)...).Scan(&refs)
		if err != nil {
			return fmt.Errorf("lock refs %s: %w", key, err)
		}
		refs += d
		if refs < 0 {
			return fmt.Errorf("%w: negative refs for %s", ErrCompactAborted, key)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE file_slice_refs SET refs = ? WHERE `+s.scope.And(`block_key = ?`),
			append([]any{refs}, s.scope.Args(key)...)...); err != nil {
			return err
		}
		if refs == 0 {
			if err := s.enqueueBlockGCTx(ctx, tx, key, inodeID, blockLens[key], now.Add(ExtentBlockGCGrace)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) deletePendingKeysTx(ctx context.Context, tx *sql.Tx, inodeID string, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	placeholders := strings.Repeat("?,", len(keys))
	placeholders = placeholders[:len(placeholders)-1]
	args := s.scope.Args(inodeID)
	for _, k := range keys {
		args = append(args, k)
	}
	_, err := tx.ExecContext(ctx, `DELETE FROM pending_blocks WHERE `+s.scope.And(`inode_id = ? AND block_key IN (`+placeholders+`)`), args...)
	return err
}

func (s *Store) EnqueueCompactIfNeeded(ctx context.Context, inodeID string, chunks map[int64]struct{}, threshold int, now time.Time) error {
	if len(chunks) == 0 {
		return nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	return s.InTx(ctx, func(tx *sql.Tx) error {
		return s.enqueueCompactIfNeededAtTx(ctx, tx, inodeID, chunks, threshold, now)
	})
}

func (s *Store) enqueueCompactIfNeededTx(ctx context.Context, tx *sql.Tx, inodeID string, chunks map[int64]struct{}, now time.Time) error {
	return s.enqueueCompactIfNeededAtTx(ctx, tx, inodeID, chunks, ExtentCompactSoftRows, now)
}

func (s *Store) enqueueCompactChunkTx(ctx context.Context, tx *sql.Tx, inodeID string, chunk int64, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	taskID := inodeID + ":" + fmt.Sprintf("%d", chunk)
	_, err := tx.ExecContext(ctx, `INSERT INTO slice_compact_tasks
		(`+s.scope.InsCols(`task_id, inode_id, chunk, status, attempt_count, max_attempts, available_at, created_at, updated_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, 0, ?, ?, ?, ?`)+`)`,
		s.scope.Args(taskID, inodeID, chunk, SliceCompactQueued, ExtentCompactMaxAttempts, now, now, now)...)
	if err != nil && !isUniqueViolation(err) {
		return fmt.Errorf("enqueue compact: %w", err)
	}
	return nil
}

func (s *Store) enqueueCompactIfNeededAtTx(ctx context.Context, tx *sql.Tx, inodeID string, chunks map[int64]struct{}, threshold int, now time.Time) error {
	if threshold <= 0 {
		threshold = ExtentCompactSoftRows
	}
	for chunk := range chunks {
		blob, err := s.getChunkBlobTx(ctx, tx, inodeID, chunk)
		if err != nil {
			return err
		}
		n, countErr := countChunkSliceRecords(blob)
		if countErr != nil {
			return countErr
		}
		if n == 0 {
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM file_slices WHERE `+s.scope.And(`inode_id = ? AND chunk = ?`),
				s.scope.Args(inodeID, chunk)...).Scan(&n); err != nil {
				return err
			}
		}
		if n < threshold {
			continue
		}
		if err := s.enqueueCompactChunkTx(ctx, tx, inodeID, chunk, now); err != nil {
			return err
		}
	}
	return nil
}

// CommitSlicesTx applies declarative ops under inode FOR UPDATE + CAS.
func (s *Store) CommitSlicesTx(ctx context.Context, tx *sql.Tx, inodeID, opID string, expectedRevision, expectedGeneration int64, ops []SliceOp, truncateTo *int64, now time.Time) (*CommitSlicesResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if opID == "" {
		return nil, fmt.Errorf("op_id is required")
	}
	if len(ops) == 0 && truncateTo == nil {
		return nil, fmt.Errorf("commit-slices requires ops or truncate_to")
	}

	var existing SliceCommitOp
	err := tx.QueryRowContext(ctx, `SELECT op_id, inode_id, generation_after, revision_after, size_after, created_at
		FROM slice_commit_ops WHERE `+s.scope.And(`op_id = ?`), s.scope.Args(opID)...).Scan(
		&existing.OpID, &existing.InodeID, &existing.GenerationAfter, &existing.RevisionAfter, &existing.SizeAfter, &existing.CreatedAt)
	if err == nil {
		return &CommitSlicesResult{
			Revision:   existing.RevisionAfter,
			Generation: existing.GenerationAfter,
			SizeBytes:  existing.SizeAfter,
			Replayed:   true,
		}, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	st, err := s.lockExtentStateTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	if NormalizeContentLayout(st.layout) != ContentLayoutExtent {
		return nil, ErrNotExtent
	}
	if expectedRevision > 0 && (st.revision != expectedRevision || st.generation != expectedGeneration) {
		return nil, ErrRevisionConflict
	}

	if truncateTo != nil && *truncateTo < 0 {
		return nil, fmt.Errorf("truncate_to must be >= 0")
	}

	before, err := s.listSlicesForInodeTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	newGen := st.generation + 1
	// Append-only writes leave overlapping rows (JuiceFS chunk.slices).
	// Flatten before splice so truncate/hole/legacy rewrite still works.
	after, err := spliceRows(FlattenBySeq(before), ops, truncateTo, newGen)
	if err != nil {
		return nil, err
	}

	var newSize int64
	if truncateTo != nil {
		newSize = *truncateTo
	} else {
		newSize = st.sizeBytes
		for _, op := range ops {
			if op.end() > newSize {
				newSize = op.end()
			}
		}
	}
	if err := assertNonOverlapping(after); err != nil {
		return nil, err
	}
	for _, row := range after {
		if row.end() > newSize {
			return nil, fmt.Errorf("slice row %d+%d exceeds size %d", row.FileOff, row.Len, newSize)
		}
	}

	blockLens := make(map[string]int64)
	for _, row := range before {
		if row.BlockKey != "" && row.BlockLen > 0 {
			blockLens[row.BlockKey] = row.BlockLen
		}
	}
	for _, row := range after {
		if row.BlockKey != "" && row.BlockLen > 0 {
			blockLens[row.BlockKey] = row.BlockLen
		}
	}

	if err := s.replaceSlicesTx(ctx, tx, inodeID, after); err != nil {
		return nil, err
	}
	if err := s.applyRefDeltaTx(ctx, tx, inodeID, refDelta(before, after), blockLens, now); err != nil {
		return nil, err
	}

	consumed := make([]string, 0, len(ops))
	seen := map[string]struct{}{}
	for _, op := range ops {
		if op.BlockKey == "" {
			continue
		}
		if _, ok := seen[op.BlockKey]; ok {
			continue
		}
		seen[op.BlockKey] = struct{}{}
		consumed = append(consumed, op.BlockKey)
	}
	if err := s.deletePendingKeysTx(ctx, tx, inodeID, consumed); err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `UPDATE inodes SET revision = revision + 1, size_bytes = ?, mtime = ?, confirmed_at = ?, status = ?
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{newSize, now, now, StatusConfirmed}, s.scope.Args(inodeID)...)...); err != nil {
		return nil, fmt.Errorf("update inode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE contents SET slice_generation = slice_generation + 1, content_blob = NULL
		WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return nil, fmt.Errorf("update contents: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slice_commit_ops
		(`+s.scope.InsCols(`op_id, inode_id, generation_after, revision_after, size_after, created_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(opID, inodeID, newGen, st.revision+1, newSize, now)...); err != nil {
		return nil, fmt.Errorf("insert commit op: %w", err)
	}

	chunks := make(map[int64]struct{})
	for _, op := range ops {
		chunks[ChunkOf(op.FileOff)] = struct{}{}
	}
	if truncateTo != nil {
		chunks[ChunkOf(*truncateTo)] = struct{}{}
	}
	if err := s.enqueueCompactIfNeededTx(ctx, tx, inodeID, chunks, now); err != nil {
		return nil, err
	}

	chunkRows, err := s.countSlicesByChunkTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	return &CommitSlicesResult{
		Revision:   st.revision + 1,
		Generation: newGen,
		SizeBytes:  newSize,
		ChunkRows:  chunkRows,
	}, nil
}

// WriteSliceTx is JuiceFS meta.Write: append one slice to a single 64MiB
// chunk without a whole-file splice CAS.
func (s *Store) WriteSliceTx(ctx context.Context, tx *sql.Tx, inodeID, opID string, op SliceOp, now time.Time) (*CommitSlicesResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if opID == "" {
		return nil, fmt.Errorf("op_id is required")
	}
	if err := validateOp(op); err != nil {
		return nil, err
	}

	var existing SliceCommitOp
	err := tx.QueryRowContext(ctx, `SELECT op_id, inode_id, generation_after, revision_after, size_after, created_at
		FROM slice_commit_ops WHERE `+s.scope.And(`op_id = ?`), s.scope.Args(opID)...).Scan(
		&existing.OpID, &existing.InodeID, &existing.GenerationAfter, &existing.RevisionAfter, &existing.SizeAfter, &existing.CreatedAt)
	if err == nil {
		return &CommitSlicesResult{
			Revision: existing.RevisionAfter, Generation: existing.GenerationAfter,
			SizeBytes: existing.SizeAfter, Replayed: true,
		}, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	st, err := s.lockExtentStateTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	if NormalizeContentLayout(st.layout) != ContentLayoutExtent {
		return nil, ErrNotExtent
	}

	chunk := ChunkOf(op.FileOff)
	if ChunkOf(op.end()-1) != chunk {
		return nil, fmt.Errorf("write slice crosses chunk boundary")
	}
	kind := op.Kind
	if kind == "" {
		if op.isHole() {
			kind = SliceKindHole
		} else {
			kind = SliceKindData
		}
	}
	op.Kind = kind
	rec, err := marshalChunkSlice(op)
	if err != nil {
		return nil, err
	}
	newGen := st.generation + 1
	if err := s.appendChunkSliceTx(ctx, tx, inodeID, chunk, rec); err != nil {
		return nil, err
	}

	newSize := st.sizeBytes
	if op.end() > newSize {
		newSize = op.end()
	}
	if op.BlockKey != "" {
		if err := s.applyRefDeltaTx(ctx, tx, inodeID, map[string]int64{op.BlockKey: 1}, map[string]int64{op.BlockKey: op.BlockLen}, now); err != nil {
			return nil, err
		}
		if err := s.deletePendingKeysTx(ctx, tx, inodeID, []string{op.BlockKey}); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inodes SET revision = revision + 1, size_bytes = ?, mtime = ?, confirmed_at = ?, status = ?
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{newSize, now, now, StatusConfirmed}, s.scope.Args(inodeID)...)...); err != nil {
		return nil, fmt.Errorf("update inode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE contents SET slice_generation = slice_generation + 1, content_blob = NULL
		WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return nil, fmt.Errorf("update contents: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slice_commit_ops
		(`+s.scope.InsCols(`op_id, inode_id, generation_after, revision_after, size_after, created_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(opID, inodeID, newGen, st.revision+1, newSize, now)...); err != nil {
		return nil, fmt.Errorf("insert commit op: %w", err)
	}
	// JuiceFS doWrite already has the blob in memory (len/sliceBytes). A
	// second SELECT+decode here made every meta.Write pay compact's cost
	// (wal-multiwrite --wait all). Compact-on-read still sees ChunkRows
	// from Plan (doRead).
	return &CommitSlicesResult{
		Revision: st.revision + 1, Generation: newGen, SizeBytes: newSize,
	}, nil
}

// TruncateSlicesTx is JuiceFS doTruncate: append hole slices over
// [min(old,new), max(old,new)) and set size. Existing overlapping writes stay.
func (s *Store) TruncateSlicesTx(ctx context.Context, tx *sql.Tx, inodeID, opID string, newSize int64, now time.Time) (*CommitSlicesResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if opID == "" {
		return nil, fmt.Errorf("op_id is required")
	}
	if newSize < 0 {
		return nil, fmt.Errorf("truncate_to must be >= 0")
	}
	var existing SliceCommitOp
	err := tx.QueryRowContext(ctx, `SELECT op_id, inode_id, generation_after, revision_after, size_after, created_at
		FROM slice_commit_ops WHERE `+s.scope.And(`op_id = ?`), s.scope.Args(opID)...).Scan(
		&existing.OpID, &existing.InodeID, &existing.GenerationAfter, &existing.RevisionAfter, &existing.SizeAfter, &existing.CreatedAt)
	if err == nil {
		return &CommitSlicesResult{
			Revision: existing.RevisionAfter, Generation: existing.GenerationAfter,
			SizeBytes: existing.SizeAfter, Replayed: true,
		}, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	st, err := s.lockExtentStateTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	if NormalizeContentLayout(st.layout) != ContentLayoutExtent {
		return nil, ErrNotExtent
	}
	if newSize == st.sizeBytes {
		chunkRows, countErr := s.countSlicesByChunkTx(ctx, tx, inodeID)
		if countErr != nil {
			return nil, countErr
		}
		return &CommitSlicesResult{
			Revision: st.revision, Generation: st.generation, SizeBytes: st.sizeBytes, ChunkRows: chunkRows,
		}, nil
	}
	holes := TruncateHoleOps(st.sizeBytes, newSize)
	newGen := st.generation + 1
	chunks := map[int64]struct{}{}
	for _, op := range holes {
		if err := validateOp(op); err != nil {
			return nil, err
		}
		op.Kind = SliceKindHole
		chunk := ChunkOf(op.FileOff)
		chunks[chunk] = struct{}{}
		rec, marshErr := marshalChunkSlice(op)
		if marshErr != nil {
			return nil, marshErr
		}
		if err := s.appendChunkSliceTx(ctx, tx, inodeID, chunk, rec); err != nil {
			return nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inodes SET revision = revision + 1, size_bytes = ?, mtime = ?, confirmed_at = ?, status = ?
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{newSize, now, now, StatusConfirmed}, s.scope.Args(inodeID)...)...); err != nil {
		return nil, fmt.Errorf("update inode: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE contents SET slice_generation = slice_generation + 1, content_blob = NULL
		WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return nil, fmt.Errorf("update contents: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slice_commit_ops
		(`+s.scope.InsCols(`op_id, inode_id, generation_after, revision_after, size_after, created_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(opID, inodeID, newGen, st.revision+1, newSize, now)...); err != nil {
		return nil, fmt.Errorf("insert commit op: %w", err)
	}
	if err := s.enqueueCompactIfNeededTx(ctx, tx, inodeID, chunks, now); err != nil {
		return nil, err
	}
	chunkRows, err := s.countSlicesByChunkTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	return &CommitSlicesResult{
		Revision: st.revision + 1, Generation: newGen, SizeBytes: newSize, ChunkRows: chunkRows,
	}, nil
}

// SetExtentLengthTx is JuiceFS doFallocate grow: raise inode length without
// appending hole slices. Truncate grow still uses TruncateSlicesTx.
func (s *Store) SetExtentLengthTx(ctx context.Context, tx *sql.Tx, inodeID, opID string, newSize int64, now time.Time) (*CommitSlicesResult, error) {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if opID == "" {
		return nil, fmt.Errorf("op_id is required")
	}
	if newSize < 0 {
		return nil, fmt.Errorf("length must be >= 0")
	}
	var existing SliceCommitOp
	err := tx.QueryRowContext(ctx, `SELECT op_id, inode_id, generation_after, revision_after, size_after, created_at
		FROM slice_commit_ops WHERE `+s.scope.And(`op_id = ?`), s.scope.Args(opID)...).Scan(
		&existing.OpID, &existing.InodeID, &existing.GenerationAfter, &existing.RevisionAfter, &existing.SizeAfter, &existing.CreatedAt)
	if err == nil {
		return &CommitSlicesResult{
			Revision: existing.RevisionAfter, Generation: existing.GenerationAfter,
			SizeBytes: existing.SizeAfter, Replayed: true,
		}, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	st, err := s.lockExtentStateTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	if NormalizeContentLayout(st.layout) != ContentLayoutExtent {
		return nil, ErrNotExtent
	}
	chunkRows, err := s.countSlicesByChunkTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	if newSize <= st.sizeBytes {
		return &CommitSlicesResult{
			Revision: st.revision, Generation: st.generation, SizeBytes: st.sizeBytes, ChunkRows: chunkRows,
		}, nil
	}
	newGen := st.generation + 1
	if _, err := tx.ExecContext(ctx, `UPDATE inodes SET revision = revision + 1, size_bytes = ?, mtime = ?, confirmed_at = ?, status = ?
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{newSize, now, now, StatusConfirmed}, s.scope.Args(inodeID)...)...); err != nil {
		return nil, fmt.Errorf("update inode length: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE contents SET slice_generation = slice_generation + 1, content_blob = NULL
		WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return nil, fmt.Errorf("update contents: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO slice_commit_ops
		(`+s.scope.InsCols(`op_id, inode_id, generation_after, revision_after, size_after, created_at`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(opID, inodeID, newGen, st.revision+1, newSize, now)...); err != nil {
		return nil, fmt.Errorf("insert commit op: %w", err)
	}
	return &CommitSlicesResult{
		Revision: st.revision + 1, Generation: newGen, SizeBytes: newSize, ChunkRows: chunkRows,
	}, nil
}

func (s *Store) countSlicesByChunkTx(ctx context.Context, tx *sql.Tx, inodeID string) ([]ChunkRowCount, error) {
	blobs, err := s.listChunkBlobsTx(ctx, tx, inodeID)
	if err != nil {
		return nil, err
	}
	if len(blobs) > 0 {
		out := make([]ChunkRowCount, 0, len(blobs))
		for _, item := range blobs {
			n, countErr := countChunkSliceRecords(item.blob)
			if countErr != nil {
				return nil, countErr
			}
			out = append(out, ChunkRowCount{Chunk: item.chunk, Rows: n})
		}
		return out, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT chunk, COUNT(*) FROM file_slices WHERE `+s.scope.And(`inode_id = ?`)+` GROUP BY chunk ORDER BY chunk`,
		s.scope.Args(inodeID)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ChunkRowCount
	for rows.Next() {
		var item ChunkRowCount
		if err := rows.Scan(&item.Chunk, &item.Rows); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// ReleaseExtentInodeTx drops slice rows, pending blocks, and refs for an inode
// that is being deleted. Must run in the same transaction as inode delete.
func (s *Store) ReleaseExtentInodeTx(ctx context.Context, tx *sql.Tx, inodeID string, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	rows, err := s.listSlicesForInodeTx(ctx, tx, inodeID)
	if err != nil {
		return err
	}
	delta := refDelta(rows, nil)
	blockLens := make(map[string]int64)
	for _, row := range rows {
		if row.BlockKey != "" {
			blockLens[row.BlockKey] = row.BlockLen
		}
	}
	if err := s.applyRefDeltaTx(ctx, tx, inodeID, delta, blockLens, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_slices WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_chunks WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_blocks WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...); err != nil {
		return err
	}
	return nil
}

// EnqueueBlockGCTx inserts a block GC task, ignoring duplicate keys.
func (s *Store) EnqueueBlockGCTx(ctx context.Context, tx *sql.Tx, blockKey, inodeID string, sizeBytes int64, availableAt time.Time) error {
	return s.enqueueBlockGCTx(ctx, tx, blockKey, inodeID, sizeBytes, availableAt)
}

// LoadChunkBlobTx is JuiceFS doRead of one chunk: the append-only slices blob.
func (s *Store) LoadChunkForCompact(ctx context.Context, inodeID string, chunk int64) ([]byte, []SliceRow, error) {
	var origin []byte
	var rows []SliceRow
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		if _, err := s.lockExtentStateTx(ctx, tx, inodeID); err != nil {
			return err
		}
		blob, err := s.getChunkBlobTx(ctx, tx, inodeID, chunk)
		if err != nil {
			return err
		}
		if len(blob) > 0 {
			decoded, decErr := decodeChunkSliceBlob(inodeID, chunk, blob)
			if decErr != nil {
				return decErr
			}
			origin = append([]byte{}, blob...)
			rows = decoded
			return nil
		}
		all, listErr := s.listSlicesForInodeTx(ctx, tx, inodeID)
		if listErr != nil {
			return listErr
		}
		rows = chunkRowsOf(all, chunk)
		origin, err = marshalChunkSlices(sliceRowsToOps(rows))
		return err
	})
	return origin, rows, err
}

// ApplyCompactTx is JuiceFS doCompactChunk: origin-prefix CAS on the chunk
// blob, then skipped head + compacted records + concurrent suffix.
func (s *Store) ApplyCompactTx(ctx context.Context, tx *sql.Tx, inodeID string, chunk int64, origin []byte, ops []SliceOp, now time.Time) error {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if _, err := s.lockExtentStateTx(ctx, tx, inodeID); err != nil {
		return err
	}
	current, err := s.getChunkBlobTx(ctx, tx, inodeID, chunk)
	if err != nil {
		return err
	}
	if len(current) == 0 {
		current = append([]byte{}, origin...)
	}
	prefix, err := decodeChunkSliceBlob(inodeID, chunk, origin)
	if err != nil {
		return err
	}
	skipped := SkipSome(prefix)
	if skipped < 0 || skipped > len(prefix) {
		skipped = 0
	}
	if skipped > 0 && CompactRangeOverlapsSkipped(prefix[:skipped], prefix[skipped:]) {
		skipped = 0
	}
	newBlob, err := CompactChunkBlob(current, origin, skipped, ops)
	if err != nil {
		return err
	}
	before, err := decodeChunkSliceBlob(inodeID, chunk, current)
	if err != nil {
		return err
	}
	after, err := decodeChunkSliceBlob(inodeID, chunk, newBlob)
	if err != nil {
		return err
	}
	if err := s.putChunkBlobTx(ctx, tx, inodeID, chunk, newBlob); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_slices WHERE `+s.scope.And(`inode_id = ? AND chunk = ?`), s.scope.Args(inodeID, chunk)...); err != nil {
		return fmt.Errorf("delete chunk slices: %w", err)
	}
	blockLens := map[string]int64{}
	for _, row := range before {
		if row.BlockKey != "" {
			blockLens[row.BlockKey] = row.BlockLen
		}
	}
	for _, row := range after {
		if row.BlockKey != "" {
			blockLens[row.BlockKey] = row.BlockLen
		}
	}
	if err := s.applyRefDeltaTx(ctx, tx, inodeID, refDelta(before, after), blockLens, now); err != nil {
		return err
	}
	keys := make([]string, 0, len(ops))
	seen := map[string]struct{}{}
	for _, op := range ops {
		if _, ok := seen[op.BlockKey]; ok {
			continue
		}
		seen[op.BlockKey] = struct{}{}
		keys = append(keys, op.BlockKey)
	}
	if err := s.deletePendingKeysTx(ctx, tx, inodeID, keys); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE contents SET slice_generation = slice_generation + 1 WHERE `+s.scope.And(`inode_id = ?`),
		s.scope.Args(inodeID)...); err != nil {
		return fmt.Errorf("bump slice_generation: %w", err)
	}
	return nil
}

func chunkRowsOf(rows []SliceRow, chunk int64) []SliceRow {
	var out []SliceRow
	for _, r := range rows {
		if r.Chunk == chunk {
			out = append(out, r)
		}
	}
	return out
}

func sliceRowMetaEqual(a, b SliceRow) bool {
	return a.FileOff == b.FileOff && a.Len == b.Len && a.Seq == b.Seq && a.BlockKey == b.BlockKey && a.BlockOff == b.BlockOff
}

// sameChunkPrefix is JuiceFS doCompactChunk: origin must still be a prefix of
// the chunk's slice list so writes that arrived during copy stay as a suffix.
func sameChunkPrefix(current, snapshot []SliceRow, chunk int64) bool {
	a := chunkRowsOf(current, chunk)
	b := chunkRowsOf(snapshot, chunk)
	if len(b) == 0 || len(a) < len(b) {
		return false
	}
	for i := range b {
		if !sliceRowMetaEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

func sameChunkSnapshot(current, snapshot []SliceRow, chunk int64) bool {
	a := chunkRowsOf(current, chunk)
	b := chunkRowsOf(snapshot, chunk)
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !sliceRowMetaEqual(a[i], b[i]) {
			return false
		}
	}
	return true
}

// PackCompactRuns groups adjacent small rows in one chunk into merge groups
// of at most ExtentMaxBlockSize. Single-row groups are skipped.
func PackCompactRuns(rows []SliceRow) [][]SliceRow {
	rawN := len(rows)
	// JuiceFS compactChunk: skip leading/trailing holes, then Compact()
	// writes interior holes as zeros in one object. Prefix CAS keeps
	// concurrent writes as a suffix, so baking cannot overlay a later
	// btree page that committed after the snapshot.
	flat := stripLeadingTrailingHoles(FlattenBySeq(rows))
	runs := packAdjacentCompactRuns(flat, 2)
	if len(runs) == 0 && rawN >= 2 && len(flat) > 0 && len(flat) < rawN {
		// Many overlapping slices collapsed to fewer visible rows (JuiceFS
		// compact of same-offset writes). Allow a 1-row replacement.
		runs = packAdjacentCompactRuns(flat, 1)
	}
	return runs
}

// SkipSome is JuiceFS meta.skipSome: leave a leading run of already-large
// identity slices out of compaction. Compacting only the tail must not
// overlap those skipped slices (baked interior holes would last-write-wins
// zeros over btree pages still covered by the head).
func SkipSome(chunk []SliceRow) int {
	skipped := 0
	total := len(chunk)
	for skipped < total {
		ss := chunk[skipped:]
		pos, size, vis := compactChunkVisible(ss)
		if size == 0 || len(ss) == 0 || len(vis) == 0 {
			break
		}
		first := ss[0]
		if first.Len < 1<<20 || first.Len*5 < size {
			break
		}
		v0 := vis[0]
		if v0.FileOff != pos || v0.BlockKey != first.BlockKey || v0.BlockOff != first.BlockOff || v0.Len != first.Len {
			break
		}
		dup := false
		for _, s := range ss[1:] {
			if s.FileOff == first.FileOff && s.BlockKey == first.BlockKey && s.BlockOff == first.BlockOff && s.Len == first.Len {
				dup = true
				break
			}
		}
		if dup {
			break
		}
		skipped++
	}
	return skipped
}

// CompactChunkVisible is JuiceFS compactChunk: strip leading/trailing holes
// from the flattened coverage and return the contiguous [pos, pos+size).
func CompactChunkVisible(ss []SliceRow) (pos, size int64, vis []SliceRow) {
	return compactChunkVisible(ss)
}

func compactChunkVisible(ss []SliceRow) (pos, size int64, vis []SliceRow) {
	vis = stripLeadingTrailingHoles(FlattenBySeq(ss))
	if len(vis) == 0 {
		return 0, 0, nil
	}
	pos = vis[0].FileOff
	end := vis[len(vis)-1].FileOff + vis[len(vis)-1].Len
	return pos, end - pos, vis
}

func CompactRangeOverlapsSkipped(head []SliceRow, compacted []SliceRow) bool {
	pos, size, _ := compactChunkVisible(compacted)
	if size <= 0 {
		return false
	}
	end := pos + size
	for _, s := range head {
		if pos < s.FileOff+s.Len && s.FileOff < end {
			return true
		}
	}
	return false
}

func stripLeadingTrailingHoles(rows []SliceRow) []SliceRow {
	i, j := 0, len(rows)
	for i < j && (rows[i].Kind == SliceKindHole || rows[i].BlockKey == "") {
		i++
	}
	for j > i && (rows[j-1].Kind == SliceKindHole || rows[j-1].BlockKey == "") {
		j--
	}
	if i >= j {
		return nil
	}
	if i == 0 && j == len(rows) {
		return rows
	}
	return append([]SliceRow(nil), rows[i:j]...)
}

func packAdjacentCompactRuns(rows []SliceRow, minRun int) [][]SliceRow {
	if minRun < 1 {
		minRun = 1
	}
	var runs [][]SliceRow
	var cur []SliceRow
	var curBytes int64
	flush := func() {
		if len(cur) >= minRun {
			runs = append(runs, cur)
		}
		cur = nil
		curBytes = 0
	}
	for i, row := range rows {
		isHole := row.Kind == SliceKindHole || row.BlockKey == ""
		if !isHole && row.Len >= ExtentMaxBlockSize {
			flush()
			continue
		}
		if isHole && len(cur) == 0 {
			continue
		}
		if len(cur) > 0 {
			prev := cur[len(cur)-1]
			if prev.FileOff+prev.Len != row.FileOff || curBytes+row.Len > ExtentMaxBlockSize {
				flush()
				if isHole {
					continue
				}
			}
		}
		if isHole && len(cur) == 0 {
			continue
		}
		cur = append(cur, row)
		curBytes += row.Len
		if i == len(rows)-1 {
			flush()
		}
	}
	return runs
}

func (s *Store) ListCompactChunksOverThreshold(ctx context.Context, inodeID string, threshold int) ([]int64, error) {
	var chunks []int64
	err := s.InTx(ctx, func(tx *sql.Tx) error {
		blobs, err := s.listChunkBlobsTx(ctx, tx, inodeID)
		if err != nil {
			return err
		}
		if len(blobs) > 0 {
			for _, item := range blobs {
				n, countErr := countChunkSliceRecords(item.blob)
				if countErr != nil {
					return countErr
				}
				if n >= threshold {
					chunks = append(chunks, item.chunk)
				}
			}
			return nil
		}
		rows, err := tx.QueryContext(ctx, `SELECT chunk FROM file_slices WHERE `+s.scope.And(`inode_id = ?`)+`
			GROUP BY chunk HAVING COUNT(*) >= ?`, append(s.scope.Args(inodeID), threshold)...)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var c int64
			if err := rows.Scan(&c); err != nil {
				return err
			}
			chunks = append(chunks, c)
		}
		return rows.Err()
	})
	return chunks, err
}
