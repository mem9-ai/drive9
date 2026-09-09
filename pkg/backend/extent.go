package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/s3client"
)

type contentLayoutCtxKey struct{}

// WithContentLayout attaches an explicit content layout to ctx for create/write.
func WithContentLayout(ctx context.Context, layout datastore.ContentLayout) context.Context {
	return context.WithValue(ctx, contentLayoutCtxKey{}, layout)
}

func contentLayoutFromContext(ctx context.Context) datastore.ContentLayout {
	v, _ := ctx.Value(contentLayoutCtxKey{}).(datastore.ContentLayout)
	return v
}

// PrepareBlock is one presigned PUT target returned by PrepareBlocks.
type PrepareBlock struct {
	FileOff   int64             `json:"file_off"`
	Len       int64             `json:"len"`
	BlockKey  string            `json:"block_key"`
	PutURL    string            `json:"put_url"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// PrepareBlocksRequest is the prepare-blocks body.
type PrepareBlocksRequest struct {
	Ranges []PrepareRange `json:"ranges"`
}

// PrepareRange is one dirty interval.
type PrepareRange struct {
	FileOff        int64  `json:"file_off"`
	Len            int64  `json:"len"`
	ChecksumSHA256 string `json:"checksum_sha256"`
}

// CommitSlicesRequest is the commit-slices body.
type CommitSlicesRequest struct {
	ExpectedRevision   int64               `json:"expected_revision"`
	ExpectedGeneration int64               `json:"expected_generation"`
	OpID               string              `json:"op_id"`
	Ops                []datastore.SliceOp `json:"ops"`
	TruncateTo         *int64              `json:"truncate_to,omitempty"`
	// GrowLength is JuiceFS doFallocate grow: raise size without hole slices.
	GrowLength bool `json:"grow_length,omitempty"`
	// Staged means objects may not be on S3 yet (writeback). Head is skipped.
	Staged bool `json:"staged,omitempty"`
	// Append is JuiceFS meta.Write: insert slice rows without whole-file CAS.
	Append bool `json:"append,omitempty"`
}

func (b *Dat9Backend) resolveLayout(ctx context.Context, path string) (datastore.ContentLayout, error) {
	return datastore.ResolveContentLayout(contentLayoutFromContext(ctx), path)
}

func (b *Dat9Backend) PrepareBlocks(ctx context.Context, path string, ranges []PrepareRange) ([]PrepareBlock, error) {
	start := time.Now()
	var err error
	defer func() { observeBackend(ctx, b.tenantID, b.tidbCloudOrgID, "extent_prepare", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	if nf.File == nil || !nf.File.IsExtent() {
		return nil, datastore.ErrNotExtent
	}
	if b.s3 == nil {
		return nil, ErrS3NotConfigured
	}

	var pending []datastore.PendingBlock
	var prepared []PrepareBlock
	encOpts, _, _ := b.s3WriteEncryption(datastore.ExtentStorageRef(nf.File.FileID))
	now := time.Now().UTC()
	for _, rng := range ranges {
		if rng.Len <= 0 || rng.FileOff < 0 {
			return nil, fmt.Errorf("invalid prepare range")
		}
		if rng.Len > datastore.ExtentMaxBlockSize {
			return nil, fmt.Errorf("prepare range exceeds %d", datastore.ExtentMaxBlockSize)
		}
		if datastore.ChunkOf(rng.FileOff) != datastore.ChunkOf(rng.FileOff+rng.Len-1) {
			return nil, fmt.Errorf("prepare range crosses chunk boundary")
		}
		if rng.ChecksumSHA256 == "" {
			return nil, fmt.Errorf("checksum_sha256 is required")
		}
		key := "blocks/" + nf.File.FileID + "/" + b.genID()
		pending = append(pending, datastore.PendingBlock{
			BlockKey:       key,
			InodeID:        nf.File.FileID,
			SizeBytes:      rng.Len,
			ChecksumSHA256: rng.ChecksumSHA256,
			ReservedBytes:  rng.Len,
			Source:         datastore.PendingBlockSourceFUSE,
		})
		checksumB64, convErr := sha256HexToBase64(rng.ChecksumSHA256)
		if convErr != nil {
			checksumB64 = rng.ChecksumSHA256
		}
		url, presignErr := b.s3.PresignPutObject(ctx, key, rng.Len, checksumB64, true, encOpts, s3client.UploadTTL)
		if presignErr != nil {
			err = presignErr
			return nil, fmt.Errorf("presign put object: %w", presignErr)
		}
		prepared = append(prepared, PrepareBlock{
			FileOff:   rng.FileOff,
			Len:       rng.Len,
			BlockKey:  key,
			PutURL:    url.URL,
			Headers:   url.Headers,
			ExpiresAt: url.ExpiresAt,
		})
	}

	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		return b.store.InsertPendingBlocksTx(tx, pending, now)
	})
	if err != nil {
		return nil, err
	}
	return prepared, nil
}

func (b *Dat9Backend) CommitSlices(ctx context.Context, path string, req CommitSlicesRequest) (*datastore.CommitSlicesResult, error) {
	start := time.Now()
	var err error
	defer func() { observeBackend(ctx, b.tenantID, b.tidbCloudOrgID, "extent_commit", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	if nf.File == nil || !nf.File.IsExtent() {
		return nil, datastore.ErrNotExtent
	}
	if b.s3 == nil {
		return nil, ErrS3NotConfigured
	}
	if len(req.Ops) == 0 && req.TruncateTo == nil {
		return nil, fmt.Errorf("commit-slices requires ops or truncate_to")
	}
	if req.OpID != "" {
		if existing, getErr := b.store.GetSliceCommitOp(ctx, req.OpID); getErr == nil && existing != nil {
			return &datastore.CommitSlicesResult{
				Revision:   existing.RevisionAfter,
				Generation: existing.GenerationAfter,
				SizeBytes:  existing.SizeAfter,
				Replayed:   true,
			}, nil
		} else if getErr != nil && !errors.Is(getErr, datastore.ErrNotFound) {
			return nil, getErr
		}
	}

	keys := make([]string, 0, len(req.Ops))
	for _, op := range req.Ops {
		if op.Kind == datastore.SliceKindHole || op.BlockKey == "" {
			continue
		}
		keys = append(keys, op.BlockKey)
	}
	pending, err := b.store.GetPendingBlocksForInode(ctx, nf.File.FileID, keys)
	if err != nil {
		return nil, err
	}
	ops := make([]datastore.SliceOp, 0, len(req.Ops))
	for _, op := range req.Ops {
		if op.Kind == datastore.SliceKindHole || op.BlockKey == "" {
			op.Kind = datastore.SliceKindHole
			ops = append(ops, op)
			continue
		}
		pb, ok := pending[op.BlockKey]
		if !ok {
			if !req.Staged {
				return nil, fmt.Errorf("%w: %s", datastore.ErrPendingExpired, op.BlockKey)
			}
			// JuiceFS writeback meta.Write records slices before object PUT.
			// Compact skips until HeadObject succeeds.
			filled := op
			if filled.BlockLen == 0 {
				filled.BlockLen = filled.Len
			}
			ops = append(ops, filled)
			continue
		}
		if time.Since(pb.CreatedAt) > datastore.ExtentPendingTTL {
			return nil, datastore.ErrPendingExpired
		}
		if !req.Staged {
			head, headErr := b.s3.HeadObject(ctx, op.BlockKey)
			if headErr != nil {
				return nil, fmt.Errorf("head object %s: %w", op.BlockKey, headErr)
			}
			if head == nil || !head.Exists || head.Size != pb.SizeBytes {
				return nil, datastore.ErrBlockNotLanded
			}
			if pb.ChecksumSHA256 != "" && head.ChecksumSHA256 != "" && !checksumEqual(pb.ChecksumSHA256, head.ChecksumSHA256) {
				return nil, datastore.ErrBlockNotLanded
			}
		}
		filled := op
		filled.BlockLen = pb.SizeBytes
		if filled.ChecksumSHA256 == "" {
			filled.ChecksumSHA256 = pb.ChecksumSHA256
		}
		ops = append(ops, filled)
	}

	var result *datastore.CommitSlicesResult
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UTC()
		// JuiceFS meta.Write always concatenates slices. The old whole-file
		// flatten-replace dropped covered tails and decremented refs on the
		// write path (negative refs / EIO under concurrent sqlite).
		if len(ops) == 0 && req.TruncateTo != nil {
			var out *datastore.CommitSlicesResult
			var truncErr error
			if req.GrowLength {
				out, truncErr = b.store.SetExtentLengthTx(ctx, tx, nf.File.FileID, req.OpID, *req.TruncateTo, now)
			} else {
				out, truncErr = b.store.TruncateSlicesTx(ctx, tx, nf.File.FileID, req.OpID, *req.TruncateTo, now)
			}
			if truncErr != nil {
				return truncErr
			}
			result = out
			return nil
		}
		var out *datastore.CommitSlicesResult
		var writeErr error
		// JuiceFS VFS.Truncate then meta.Write: holes first so payload seq wins.
		if req.TruncateTo != nil {
			out, writeErr = b.store.TruncateSlicesTx(ctx, tx, nf.File.FileID, req.OpID+":trunc", *req.TruncateTo, now)
			if writeErr != nil {
				return writeErr
			}
		}
		for i, op := range ops {
			opID := req.OpID
			if i > 0 {
				opID = fmt.Sprintf("%s:%d", req.OpID, i)
			}
			out, writeErr = b.store.WriteSliceTx(ctx, tx, nf.File.FileID, opID, op, now)
			if writeErr != nil {
				return writeErr
			}
		}
		result = out
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !result.Replayed {
		var putBytes int64
		for _, op := range ops {
			putBytes += op.Len
		}
		metrics.RecordExtentPutBytes(putBytes)
		_ = b.store.DeleteOldSliceCommitOps(ctx, nf.File.FileID, datastore.ExtentCommitOpsRetain, time.Now().UTC().Add(-datastore.ExtentCommitOpsMaxAge))
		b.notifyWorkEnqueued(BackendWorkFileGC)
	}
	return result, nil
}

func (b *Dat9Backend) AssembleExtent(ctx context.Context, inodeID string, start, end int64, w io.Writer) error {
	if end <= start {
		return nil
	}
	rows, err := b.store.ListSlicesOverlapping(ctx, inodeID, start, end)
	if err != nil {
		return err
	}
	rows = datastore.FlattenBySeq(rows)
	if b.s3 == nil {
		return ErrS3NotConfigured
	}
	pos := start
	for _, row := range rows {
		r0, r1 := row.FileOff, row.FileOff+row.Len
		if r1 <= start || r0 >= end {
			continue
		}
		if r0 > pos {
			if err := writeExtentZeros(w, r0-pos); err != nil {
				return err
			}
			pos = r0
		}
		if row.Kind == datastore.SliceKindHole || row.BlockKey == "" {
			from := start
			if r0 > from {
				from = r0
			}
			to := end
			if r1 < to {
				to = r1
			}
			if to > from {
				if err := writeExtentZeros(w, to-from); err != nil {
					return err
				}
			}
			pos = to
			continue
		}
		from := start
		if r0 > from {
			from = r0
		}
		to := end
		if r1 < to {
			to = r1
		}
		delta := from - r0
		blockStart := row.BlockOff + delta
		blockEnd := blockStart + (to - from) - 1
		rc, err := b.getObjectRangeWriteback(ctx, row.BlockKey, blockStart, blockEnd)
		if err != nil {
			return fmt.Errorf("get object range %s: %w", row.BlockKey, err)
		}
		_, copyErr := io.Copy(w, rc)
		_ = rc.Close()
		if copyErr != nil {
			return copyErr
		}
		pos = to
	}
	if pos < end {
		if err := writeExtentZeros(w, end-pos); err != nil {
			return err
		}
	}
	return nil
}

func (b *Dat9Backend) getObjectRangeWriteback(ctx context.Context, key string, start, end int64) (io.ReadCloser, error) {
	// JuiceFS chunkstore reads writeback cache. Drive9 HTTP assemble has no
	// FUSE cache; writeback commits meta before PUT, so retry NoSuchKey.
	var last error
	backoff := 20 * time.Millisecond
	for i := 0; i < 8; i++ {
		rc, err := b.s3.GetObjectRange(ctx, key, start, end)
		if err == nil {
			return rc, nil
		}
		last = err
		msg := err.Error()
		if !strings.Contains(msg, "NoSuchKey") && !strings.Contains(msg, "404") && !strings.Contains(msg, "not found") {
			return nil, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
	return nil, last
}

const extentZeroBufSize = 1 << 20

var extentZeroBuf = make([]byte, extentZeroBufSize)

func writeExtentZeros(w io.Writer, n int64) error {
	for n > 0 {
		chunk := int64(len(extentZeroBuf))
		if chunk > n {
			chunk = n
		}
		if _, err := w.Write(extentZeroBuf[:chunk]); err != nil {
			return err
		}
		n -= chunk
	}
	return nil
}

func (b *Dat9Backend) GetSlices(ctx context.Context, path string) ([]datastore.SliceRow, int64, int64, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, 0, 0, err
	}
	st, err := b.store.LoadExtentReadState(ctx, path)
	if err != nil {
		return nil, 0, 0, err
	}
	return st.Rows, st.Revision, st.Generation, nil
}

// ExtentReadPart is one presigned ranged GET covering a slice row (or the
// intersection of a row with the requested file range).
type ExtentReadPart struct {
	FileOff   int64             `json:"file_off"`
	Len       int64             `json:"len"`
	BlockKey  string            `json:"block_key"`
	BlockOff  int64             `json:"block_off"`
	GetURL    string            `json:"get_url"`
	Headers   map[string]string `json:"headers,omitempty"`
	ExpiresAt time.Time         `json:"expires_at"`
}

// ExtentReadPlan is the client-side read recipe: metadata plus presigned GETs.
// Bytes are fetched from object storage, not from drive9-server.
type ExtentReadPlan struct {
	Revision   int64            `json:"revision"`
	Generation int64            `json:"generation"`
	SizeBytes  int64            `json:"size_bytes"`
	Parts      []ExtentReadPart `json:"parts"`
	// ChunkRows is JuiceFS doRead's raw slice count per chunk (len(ss)).
	ChunkRows []datastore.ChunkRowCount `json:"chunk_rows,omitempty"`
	// Layout is JuiceFS buildSlice of the loaded chunks, cached in FUSE
	// open-file (of.CacheChunk) until Write/compact InvalidateChunk.
	Layout []datastore.SliceRow `json:"layout,omitempty"`
}

// CompactSlicesRequest is a client-driven compaction swap. New blocks must
// already be PUT (via prepare-blocks) and listed in Ops.
type CompactSlicesRequest struct {
	Chunk    int64                `json:"chunk"`
	Snapshot []datastore.SliceRow `json:"snapshot"`
	Ops      []datastore.SliceOp  `json:"ops"`
}

// PlanExtentRead returns presigned ranged GETs for the slice rows that cover
// [start, end). Each part is the full intersecting row so clients can cache
// the immutable block window. end==start returns an empty plan. If end<=0 the
// file size is used.
func (b *Dat9Backend) PlanExtentRead(ctx context.Context, path string, start, end int64) (*ExtentReadPlan, error) {
	startT := time.Now()
	var err error
	defer func() { observeBackend(ctx, b.tenantID, b.tidbCloudOrgID, "extent_read_plan", err, startT) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	if b.s3 == nil {
		return nil, ErrS3NotConfigured
	}
	// JuiceFS meta.Read: doRead is one chunk blob plus inode length. Stat
	// then ListSlices in two txns can pair a post-VACUUM size with the
	// pre-rewrite slice list (btreeInitPage zeros).
	st, err := b.store.LoadExtentReadState(ctx, path)
	if err != nil {
		return nil, err
	}
	size := st.SizeBytes
	if start < 0 {
		start = 0
	}
	switch {
	case end < 0:
		end = size
	case end == 0:
		end = start
	case end > size:
		end = size
	}
	if start >= end || size == 0 {
		return &ExtentReadPlan{
			Revision: st.Revision, Generation: st.Generation, SizeBytes: size,
			ChunkRows: countPlanChunkRows(st.Rows),
			Layout:    datastore.FlattenBySeq(st.Rows),
		}, nil
	}
	// JuiceFS meta.Read doRead: the whole 64MiB chunk's slice list, then
	// buildSlice. Overlapping-only SELECT can drop a later longer write
	// that does not itself overlap the request but whose flatten remnant does.
	touched := map[int64]struct{}{}
	for off := start; off < end; {
		touched[datastore.ChunkOf(off)] = struct{}{}
		next := (off/datastore.ExtentChunkSize + 1) * datastore.ExtentChunkSize
		if next <= off {
			break
		}
		off = next
	}
	all := st.Rows
	var rows []datastore.SliceRow
	for _, row := range all {
		if _, ok := touched[row.Chunk]; ok {
			rows = append(rows, row)
		}
	}
	rows = datastore.FlattenBySeq(rows)
	plan := &ExtentReadPlan{
		Revision:   st.Revision,
		Generation: st.Generation,
		SizeBytes:  size,
		Parts:      make([]ExtentReadPart, 0, len(rows)+1),
		ChunkRows:  countPlanChunkRows(st.Rows),
		Layout:     datastore.FlattenBySeq(st.Rows),
	}
	pos := start
	covered := false
	appendHole := func(from, to int64) {
		if to <= from {
			return
		}
		plan.Parts = append(plan.Parts, ExtentReadPart{FileOff: from, Len: to - from})
	}
	for _, row := range rows {
		r0, r1 := row.FileOff, row.FileOff+row.Len
		if r1 <= start || r0 >= end {
			continue
		}
		covered = true
		if r0 > pos {
			// JuiceFS buildSlice inserts Id=0 for gaps.
			appendHole(pos, r0)
			pos = r0
		}
		if row.Kind == datastore.SliceKindHole || row.BlockKey == "" {
			to := r1
			if to > end {
				to = end
			}
			if to > pos {
				appendHole(pos, to)
				pos = to
			}
			continue
		}
		from := start
		if r0 > from {
			from = r0
		}
		to := end
		if r1 < to {
			to = r1
		}
		delta := from - r0
		blockStart := row.BlockOff + delta
		blockEnd := blockStart + (to - from) - 1
		// JuiceFS chunk.ReadAt: range is part of the slice object. Presign
		// the range (Drive9-must: no client AWS creds) instead of a full GET
		// plus an unsigned Range that S3 may ignore (200 full body).
		url, presignErr := b.s3.PresignGetObjectRange(ctx, row.BlockKey, blockStart, blockEnd, s3client.DownloadTTL)
		if presignErr != nil {
			err = presignErr
			return nil, fmt.Errorf("presign get %s: %w", row.BlockKey, presignErr)
		}
		plan.Parts = append(plan.Parts, ExtentReadPart{
			FileOff:   from,
			Len:       to - from,
			BlockKey:  row.BlockKey,
			BlockOff:  blockStart,
			GetURL:    url,
			Headers:   map[string]string{"Range": fmt.Sprintf("bytes=%d-%d", blockStart, blockEnd)},
			ExpiresAt: time.Now().UTC().Add(s3client.DownloadTTL),
		})
		if to > pos {
			pos = to
		}
	}
	if covered && pos < end {
		appendHole(pos, end)
	}
	// JuiceFS meta.Read: doRead returns slices, then go compactChunk.
	inodeID := st.InodeID
	go func() {
		if enqErr := b.store.EnqueueCompactIfNeeded(context.Background(), inodeID, touched, datastore.ExtentCompactReadRows, time.Now().UTC()); enqErr == nil {
			b.notifyWorkEnqueued(BackendWorkFileGC)
		}
	}()
	return plan, nil
}

func countPlanChunkRows(rows []datastore.SliceRow) []datastore.ChunkRowCount {
	if len(rows) == 0 {
		return nil
	}
	n := map[int64]int{}
	for _, row := range rows {
		n[row.Chunk]++
	}
	out := make([]datastore.ChunkRowCount, 0, len(n))
	for chunk, rows := range n {
		out = append(out, datastore.ChunkRowCount{Chunk: chunk, Rows: rows})
	}
	return out
}

// CompactSlices applies a client-prepared compaction. The server only heads
// new objects and swaps metadata; it does not copy bytes.
func (b *Dat9Backend) CompactSlices(ctx context.Context, path string, req CompactSlicesRequest) error {
	start := time.Now()
	var err error
	defer func() { observeBackend(ctx, b.tenantID, b.tidbCloudOrgID, "extent_compact", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return err
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		return err
	}
	if nf.File == nil || !nf.File.IsExtent() {
		return datastore.ErrNotExtent
	}
	if b.s3 == nil {
		return ErrS3NotConfigured
	}
	if len(req.Ops) == 0 {
		return nil
	}
	keys := make([]string, 0, len(req.Ops))
	for _, op := range req.Ops {
		keys = append(keys, op.BlockKey)
	}
	pending, err := b.store.GetPendingBlocksForInode(ctx, nf.File.FileID, keys)
	if err != nil {
		return err
	}
	ops := make([]datastore.SliceOp, 0, len(req.Ops))
	for _, op := range req.Ops {
		pb, ok := pending[op.BlockKey]
		if !ok {
			return fmt.Errorf("%w: %s", datastore.ErrPendingExpired, op.BlockKey)
		}
		head, headErr := b.s3.HeadObject(ctx, op.BlockKey)
		if headErr != nil {
			return fmt.Errorf("head object %s: %w", op.BlockKey, headErr)
		}
		if head == nil || !head.Exists || head.Size != pb.SizeBytes {
			return datastore.ErrBlockNotLanded
		}
		filled := op
		filled.BlockLen = pb.SizeBytes
		if filled.ChecksumSHA256 == "" {
			filled.ChecksumSHA256 = pb.ChecksumSHA256
		}
		ops = append(ops, filled)
	}
	origin, encErr := datastore.EncodeSliceRows(req.Snapshot)
	if encErr != nil {
		return encErr
	}
	now := time.Now().UTC()
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		return b.store.ApplyCompactTx(ctx, tx, nf.File.FileID, req.Chunk, origin, ops, now)
	})
	return err
}

func (b *Dat9Backend) createExtentFile(ctx context.Context, path string) (inodeID string, err error) {
	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return "", err
	}
	if err := rejectRootFileNodePath(path); err != nil {
		return "", err
	}
	if b.s3 == nil {
		return "", ErrS3NotConfigured
	}
	fileID := b.genID()
	now := time.Now()
	encOpts, encMode, encKeyID := b.s3WriteEncryption(datastore.ExtentStorageRef(fileID))
	_ = encOpts
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.ensureFileCountQuotaServer(ctx, tx, 1); err != nil {
			return err
		}
		if err := b.store.InsertFileTx(tx, &datastore.File{
			FileID:                 fileID,
			StorageType:            datastore.StorageS3,
			StorageRef:             datastore.ExtentStorageRef(fileID),
			StorageEncryptionMode:  encMode,
			StorageEncryptionKeyID: encKeyID,
			SizeBytes:              0,
			Revision:               1,
			Status:                 datastore.StatusConfirmed,
			CreatedAt:              now,
			ConfirmedAt:            &now,
			ContentLayout:          datastore.ContentLayoutExtent,
			SliceGeneration:        0,
		}); err != nil {
			return err
		}
		if err := b.store.EnsureParentDirsTx(tx, path, b.genID); err != nil {
			return err
		}
		return b.store.InsertNodeTx(tx, &datastore.FileNode{
			NodeID: b.genID(), Path: path, ParentPath: pathutil.ParentPath(path),
			Name: pathutil.BaseName(path), FileID: fileID, CreatedAt: now,
		})
	})
	if err != nil {
		return "", err
	}
	return fileID, nil
}

func (b *Dat9Backend) IngestExtentBytes(ctx context.Context, path string, data []byte) (revision int64, generation int64, err error) {
	_, revision, generation, err = b.IngestExtentReader(ctx, path, bytes.NewReader(data), int64(len(data)), -1)
	return revision, generation, err
}

// ShouldIngestExtent reports whether a PUT/create should write blocks/ +
// file_slices instead of a single blob. Existing single files are never
// converted.
func (b *Dat9Backend) ShouldIngestExtent(ctx context.Context, path string) bool {
	nf, err := b.store.Stat(ctx, path)
	if err == nil && nf != nil && nf.File != nil && nf.File.IsExtent() {
		return true
	}
	if errors.Is(err, datastore.ErrNotFound) {
		layout, lerr := b.resolveLayout(ctx, path)
		return lerr == nil && layout == datastore.ContentLayoutExtent
	}
	return false
}

// IngestExtentReader fully replaces an extent file's logical bytes from r.
func (b *Dat9Backend) IngestExtentReader(ctx context.Context, path string, r io.Reader, size, expectedRevision int64) (written, revision, generation int64, err error) {
	if err := b.ensureUploadSizeAllowed(size); err != nil {
		return 0, 0, 0, err
	}
	if size > 0 {
		if err := b.ensureFileSizeQuota(ctx, size); err != nil {
			return 0, 0, 0, err
		}
	}
	layout, err := b.resolveLayout(ctx, path)
	if err != nil {
		return 0, 0, 0, err
	}
	existing, statErr := b.store.Stat(ctx, path)
	var inodeID string
	var expectedRev, expectedGen int64
	switch {
	case errors.Is(statErr, datastore.ErrNotFound):
		if layout != datastore.ContentLayoutExtent {
			return 0, 0, 0, datastore.ErrNotExtent
		}
		if expectedRevision > 0 {
			return 0, 0, 0, datastore.ErrRevisionConflict
		}
		inodeID, err = b.createExtentFile(ctx, path)
		if err != nil {
			return 0, 0, 0, err
		}
		expectedRev, expectedGen = 1, 0
	case statErr != nil:
		return 0, 0, 0, statErr
	default:
		if existing.File == nil {
			return 0, 0, 0, datastore.ErrNotFound
		}
		if !existing.File.IsExtent() {
			return 0, 0, 0, datastore.ErrExtentUseCommit
		}
		if expectedRevision == 0 {
			return 0, 0, 0, datastore.ErrRevisionConflict
		}
		if expectedRevision > 0 && existing.File.Revision != expectedRevision {
			return 0, 0, 0, datastore.ErrRevisionConflict
		}
		inodeID = existing.File.FileID
		expectedRev = existing.File.Revision
		expectedGen = existing.File.SliceGeneration
		if expectedRevision < 0 {
			expectedRev = existing.File.Revision
		}
	}
	if size == 0 {
		if existing == nil || existing.File == nil || existing.File.SizeBytes == 0 {
			return 0, expectedRev, expectedGen, nil
		}
		to := int64(0)
		if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
			out, commitErr := b.store.CommitSlicesTx(ctx, tx, inodeID, "ingest:"+b.genID(), expectedRev, expectedGen, nil, &to, time.Now().UTC())
			if commitErr != nil {
				return commitErr
			}
			revision = out.Revision
			generation = out.Generation
			return nil
		}); err != nil {
			return 0, 0, 0, err
		}
		return 0, revision, generation, nil
	}
	if b.s3 == nil {
		return 0, 0, 0, ErrS3NotConfigured
	}
	encOpts, _, _ := b.s3WriteEncryption(datastore.ExtentStorageRef(inodeID))
	parts := datastore.SplitPrepareRange(0, size)
	var ops []datastore.SliceOp
	var pending []datastore.PendingBlock
	now := time.Now().UTC()
	buf := make([]byte, 0, datastore.ExtentMaxBlockSize)
	for _, part := range parts {
		if cap(buf) < int(part.Len) {
			buf = make([]byte, part.Len)
		} else {
			buf = buf[:part.Len]
		}
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, 0, 0, fmt.Errorf("read extent ingest at %d: %w", part.FileOff, err)
		}
		sum := sha256.Sum256(buf)
		hexSum := hex.EncodeToString(sum[:])
		key := "blocks/" + inodeID + "/" + b.genID()
		chunk := append([]byte(nil), buf...)
		if err := b.s3.PutObject(ctx, key, bytes.NewReader(chunk), part.Len, encOpts); err != nil {
			return 0, 0, 0, fmt.Errorf("put extent block: %w", err)
		}
		pending = append(pending, datastore.PendingBlock{
			BlockKey:       key,
			InodeID:        inodeID,
			SizeBytes:      part.Len,
			ChecksumSHA256: hexSum,
			ReservedBytes:  part.Len,
			Source:         datastore.PendingBlockSourceUpload,
		})
		ops = append(ops, datastore.SliceOp{
			FileOff:        part.FileOff,
			Len:            part.Len,
			BlockKey:       key,
			BlockOff:       0,
			BlockLen:       part.Len,
			ChecksumSHA256: hexSum,
		})
	}
	var truncateTo *int64
	remoteSize := int64(0)
	if existing != nil && existing.File != nil {
		remoteSize = existing.File.SizeBytes
	}
	if size < remoteSize {
		to := size
		truncateTo = &to
	}
	if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.store.InsertPendingBlocksTx(tx, pending, now); err != nil {
			return err
		}
		out, err := b.store.CommitSlicesTx(ctx, tx, inodeID, "ingest:"+b.genID(), expectedRev, expectedGen, ops, truncateTo, now)
		if err != nil {
			return err
		}
		revision = out.Revision
		generation = out.Generation
		return nil
	}); err != nil {
		return 0, 0, 0, err
	}
	metrics.RecordExtentPutBytes(size)
	return size, revision, generation, nil
}

// CopyFileRange copies src[off:off+length) onto dst[dstOff:dstOff+length) by
// sharing block keys (JuiceFS copy_file_range / clone). Holes stay holes.
func (b *Dat9Backend) PresignPutBlock(ctx context.Context, path, blockKey string, size int64, checksumSHA256 string) (*PrepareBlock, error) {
	path, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	if nf.File == nil || !nf.File.IsExtent() {
		return nil, datastore.ErrNotExtent
	}
	if b.s3 == nil {
		return nil, ErrS3NotConfigured
	}
	if blockKey == "" || !strings.HasPrefix(blockKey, "blocks/"+nf.File.FileID+"/") {
		return nil, fmt.Errorf("block_key is not owned by this inode")
	}
	encOpts, _, _ := b.s3WriteEncryption(datastore.ExtentStorageRef(nf.File.FileID))
	checksumB64 := checksumSHA256
	if decoded, convErr := sha256HexToBase64(checksumSHA256); convErr == nil {
		checksumB64 = decoded
	}
	url, presignErr := b.s3.PresignPutObject(ctx, blockKey, size, checksumB64, false, encOpts, s3client.UploadTTL)
	if presignErr != nil {
		return nil, presignErr
	}
	return &PrepareBlock{
		Len: size, BlockKey: blockKey, PutURL: url.URL, Headers: url.Headers, ExpiresAt: url.ExpiresAt,
	}, nil
}

func (b *Dat9Backend) CopyFileRange(ctx context.Context, srcPath, dstPath string, srcOff, dstOff, length, expectedRevision, expectedGeneration int64) (*datastore.CommitSlicesResult, error) {
	srcPath, err := pathutil.Canonicalize(srcPath)
	if err != nil {
		return nil, err
	}
	dstPath, err = pathutil.Canonicalize(dstPath)
	if err != nil {
		return nil, err
	}
	src, err := b.store.Stat(ctx, srcPath)
	if err != nil {
		return nil, err
	}
	if src.File == nil || !src.File.IsExtent() {
		return nil, datastore.ErrNotExtent
	}
	if length <= 0 {
		return &datastore.CommitSlicesResult{Revision: expectedRevision, Generation: expectedGeneration, SizeBytes: 0}, nil
	}
	rows, err := b.store.ListSlicesOverlapping(ctx, src.File.FileID, srcOff, srcOff+length)
	if err != nil {
		return nil, err
	}
	var ops []datastore.SliceOp
	pos := srcOff
	end := srcOff + length
	for _, row := range rows {
		r0, r1 := row.FileOff, row.FileOff+row.Len
		if r1 <= srcOff || r0 >= end {
			continue
		}
		if r0 > pos {
			gap := r0 - pos
			ops = append(ops, splitHoleOps(dstOff+(pos-srcOff), gap)...)
			pos = r0
		}
		from := srcOff
		if r0 > from {
			from = r0
		}
		to := end
		if r1 < to {
			to = r1
		}
		delta := from - r0
		op := datastore.SliceOp{
			FileOff: dstOff + (from - srcOff), Len: to - from,
			BlockKey: row.BlockKey, BlockOff: row.BlockOff + delta, BlockLen: row.BlockLen,
			ChecksumSHA256: row.ChecksumSHA256, Kind: row.Kind,
		}
		if row.Kind == datastore.SliceKindHole || row.BlockKey == "" {
			op.Kind = datastore.SliceKindHole
			op.BlockKey = ""
		}
		ops = append(ops, splitCopyOps(op)...)
		pos = to
	}
	if pos < end {
		ops = append(ops, splitHoleOps(dstOff+(pos-srcOff), end-pos)...)
	}
	return b.CommitSlices(ctx, dstPath, CommitSlicesRequest{
		ExpectedRevision: expectedRevision, ExpectedGeneration: expectedGeneration,
		OpID: "clone:" + b.genID(), Ops: ops, Staged: true,
	})
}

func splitHoleOps(fileOff, length int64) []datastore.SliceOp {
	return datastore.SplitHoleRange(fileOff, length)
}

func splitCopyOps(op datastore.SliceOp) []datastore.SliceOp {
	if op.Kind == datastore.SliceKindHole || op.BlockKey == "" {
		return splitHoleOps(op.FileOff, op.Len)
	}
	var out []datastore.SliceOp
	for _, part := range datastore.SplitPrepareRange(op.FileOff, op.Len) {
		delta := part.FileOff - op.FileOff
		part.BlockKey = op.BlockKey
		part.BlockOff = op.BlockOff + delta
		part.BlockLen = op.BlockLen
		part.ChecksumSHA256 = op.ChecksumSHA256
		out = append(out, part)
	}
	return out
}

func (b *Dat9Backend) SetContentLayout(ctx context.Context, path string, layout datastore.ContentLayout) error {
	layout, err := datastore.ParseContentLayout(string(layout))
	if err != nil {
		return err
	}
	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return err
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		return err
	}
	if nf.File == nil {
		return datastore.ErrNotFound
	}
	if nf.File.Layout() == layout {
		return nil
	}
	if nf.File.SizeBytes != 0 || nf.File.Layout() != datastore.ContentLayoutSingle {
		return fmt.Errorf("content_layout can only be set on empty single files")
	}
	if layout != datastore.ContentLayoutExtent {
		return fmt.Errorf("converting away from extent is not supported")
	}
	now := time.Now().UTC()
	return b.store.InTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE contents SET content_layout = ?, storage_type = ?, storage_ref = ?, storage_ref_hash = ?, content_blob = NULL, slice_generation = 0
			WHERE inode_id = ?`,
			string(datastore.ContentLayoutExtent), datastore.StorageS3, datastore.ExtentStorageRef(nf.File.FileID),
			datastore.StorageRefHash(datastore.ExtentStorageRef(nf.File.FileID)), nf.File.FileID)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE inodes SET mtime = ? WHERE inode_id = ?`, now, nf.File.FileID)
		return err
	})
}

func sha256HexToBase64(hexSum string) (string, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(hexSum))
	if err != nil {
		return "", err
	}
	if len(raw) != sha256.Size {
		return "", fmt.Errorf("checksum is not sha256")
	}
	return base64.StdEncoding.EncodeToString(raw), nil
}

func checksumEqual(a, b string) bool {
	a = strings.TrimSpace(strings.ToLower(a))
	b = strings.TrimSpace(strings.ToLower(b))
	if a == b {
		return true
	}
	if dec, err := base64.StdEncoding.DecodeString(a); err == nil {
		a = hex.EncodeToString(dec)
	}
	if dec, err := base64.StdEncoding.DecodeString(b); err == nil {
		b = hex.EncodeToString(dec)
	}
	return a == b
}
