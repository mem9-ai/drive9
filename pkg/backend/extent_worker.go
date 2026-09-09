package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"go.uber.org/zap"
)

func (b *Dat9Backend) ProcessOneBlockGCTask(ctx context.Context) (bool, error) {
	if _, err := b.store.RecoverExpiredBlockGCTasks(ctx, time.Now().UTC(), 100); err != nil {
		logger.Warn(ctx, "block_gc_recover_failed", zap.Error(err))
	}
	task, found, err := b.store.ClaimBlockGCTask(ctx, time.Now().UTC(), 5*time.Minute)
	if err != nil || !found {
		return found, err
	}
	var refs int64
	if txErr := b.store.InTx(ctx, func(tx *sql.Tx) error {
		var err error
		refs, err = b.store.BlockRefCountTx(ctx, tx, task.BlockKey)
		return err
	}); txErr != nil {
		_ = b.store.RetryBlockGCTask(ctx, task.TaskID, task.Receipt, time.Now().UTC().Add(2*time.Second), txErr.Error())
		return true, txErr
	}
	if refs > 0 {
		if err := b.store.AckBlockGCTask(ctx, task.TaskID, task.Receipt); err != nil {
			return true, err
		}
		return true, nil
	}
	handled, enqErr := b.enqueueObjectGCCandidateCtx(ctx, task.BlockKey, meta.ObjectGCReasonFileDelete, task.InodeID)
	if enqErr != nil {
		_ = b.store.RetryBlockGCTask(ctx, task.TaskID, task.Receipt, time.Now().UTC().Add(2*time.Second), enqErr.Error())
		return true, enqErr
	}
	if !handled {
		if txErr := b.store.InTx(ctx, func(tx *sql.Tx) error {
			live, err := b.store.BlockRefCountTx(ctx, tx, task.BlockKey)
			if err != nil {
				return err
			}
			refs = live
			return nil
		}); txErr != nil {
			_ = b.store.RetryBlockGCTask(ctx, task.TaskID, task.Receipt, time.Now().UTC().Add(2*time.Second), txErr.Error())
			return true, txErr
		}
		if refs > 0 {
			if err := b.store.AckBlockGCTask(ctx, task.TaskID, task.Receipt); err != nil {
				return true, err
			}
			return true, nil
		}
		if err := b.DeleteS3ObjectForGC(ctx, task.BlockKey); err != nil {
			_ = b.store.RetryBlockGCTask(ctx, task.TaskID, task.Receipt, time.Now().UTC().Add(2*time.Second), err.Error())
			return true, err
		}
	}
	if err := b.store.AckBlockGCTask(ctx, task.TaskID, task.Receipt); err != nil {
		return true, err
	}
	return true, nil
}

func (b *Dat9Backend) ProcessOnePendingBlockGC(ctx context.Context) (bool, error) {
	cutoff := time.Now().UTC().Add(-datastore.ExtentPendingTTL)
	blocks, err := b.store.ListExpiredPendingBlocks(ctx, cutoff, 1)
	if err != nil || len(blocks) == 0 {
		return false, err
	}
	pb := blocks[0]
	head, err := b.s3.HeadObject(ctx, pb.BlockKey)
	if err != nil {
		return true, err
	}
	if head != nil && head.Exists {
		if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
			return b.store.EnqueueBlockGCTx(ctx, tx, pb.BlockKey, pb.InodeID, pb.SizeBytes, time.Now().UTC().Add(datastore.ExtentBlockGCGrace))
		}); err != nil {
			return true, err
		}
	}
	if err := b.store.DeletePendingBlock(ctx, pb.BlockKey); err != nil {
		return true, err
	}
	return true, nil
}

func (b *Dat9Backend) ProcessOneSliceCompact(ctx context.Context) (bool, error) {
	if _, err := b.store.RecoverExpiredSliceCompactTasks(ctx, time.Now().UTC(), 100); err != nil {
		logger.Warn(ctx, "slice_compact_recover_failed", zap.Error(err))
	}
	task, found, err := b.store.ClaimSliceCompactTask(ctx, time.Now().UTC(), 5*time.Minute)
	if err != nil || !found {
		return found, err
	}
	if err := b.compactChunk(ctx, task); err != nil {
		if strings.Contains(err.Error(), "pending blocks") || strings.Contains(err.Error(), "not landed") {
			_ = b.store.DelaySliceCompactTask(ctx, task.TaskID, task.Receipt, time.Now().UTC().Add(5*time.Second), err.Error())
			return true, err
		}
		if task.MaxAttempts > 0 && task.AttemptCount+1 >= task.MaxAttempts {
			_ = b.store.DeleteSliceCompactTask(ctx, task.TaskID, task.Receipt)
			return true, err
		}
		_ = b.store.RetrySliceCompactTask(ctx, task.TaskID, task.Receipt, time.Now().UTC().Add(5*time.Second), err.Error())
		return true, err
	}
	if err := b.store.DeleteSliceCompactTask(ctx, task.TaskID, task.Receipt); err != nil {
		return true, err
	}
	_ = b.store.DeleteOldSliceCommitOps(ctx, task.InodeID, datastore.ExtentCommitOpsRetain, time.Now().UTC().Add(-datastore.ExtentCommitOpsMaxAge))
	return true, nil
}

func (b *Dat9Backend) compactChunk(ctx context.Context, task *datastore.SliceCompactTask) error {
	hasPending, err := b.store.HasPendingBlocks(ctx, task.InodeID)
	if err != nil {
		return err
	}
	if hasPending {
		return fmt.Errorf("skip compact: pending blocks exist")
	}
	origin, chunkRows, err := b.store.LoadChunkForCompact(ctx, task.InodeID, task.Chunk)
	if err != nil {
		return err
	}
	if len(chunkRows) < 2 {
		return nil
	}
	skipped := datastore.SkipSome(chunkRows)
	compacted := chunkRows
	if skipped > 0 && skipped < len(chunkRows) && !datastore.CompactRangeOverlapsSkipped(chunkRows[:skipped], chunkRows[skipped:]) {
		compacted = chunkRows[skipped:]
	}
	if len(compacted) < 2 {
		return nil
	}
	// JuiceFS compactChunk: one new slice covering the visible [pos, pos+size).
	// PackCompactRuns last-write-wins overlayed btree overflow pages.
	pos, size, vis := datastore.CompactChunkVisible(compacted)
	if size <= 0 || len(vis) == 0 {
		return nil
	}
	encOpts, _, _ := b.s3WriteEncryption(datastore.ExtentStorageRef(task.InodeID))
	now := time.Now().UTC()
	buf := make([]byte, size)
	for _, row := range vis {
		if row.Kind == datastore.SliceKindHole || row.BlockKey == "" {
			continue
		}
		off := row.FileOff - pos
		if off < 0 || off+row.Len > size {
			return fmt.Errorf("compact vis off=%d len=%d size=%d", off, row.Len, size)
		}
		if head, headErr := b.s3.HeadObject(ctx, row.BlockKey); headErr != nil || head == nil || !head.Exists {
			return fmt.Errorf("skip compact: block not landed")
		}
		rc, err := b.s3.GetObjectRange(ctx, row.BlockKey, row.BlockOff, row.BlockOff+row.Len-1)
		if err != nil {
			return err
		}
		n, copyErr := io.ReadFull(rc, buf[off:off+row.Len])
		_ = rc.Close()
		if copyErr != nil {
			return copyErr
		}
		if int64(n) != row.Len {
			return fmt.Errorf("compact copy %s off=%d: got %d want %d", row.BlockKey, row.BlockOff, n, row.Len)
		}
	}
	h := sha256.Sum256(buf)
	sum := hex.EncodeToString(h[:])
	key := "blocks/" + task.InodeID + "/" + b.genID()
	if err := b.s3.PutObject(ctx, key, bytes.NewReader(buf), size, encOpts); err != nil {
		return err
	}
	pending := []datastore.PendingBlock{{
		BlockKey: key, InodeID: task.InodeID, SizeBytes: size,
		ChecksumSHA256: sum, ReservedBytes: size, Source: datastore.PendingBlockSourceCompact,
	}}
	op := datastore.SliceOp{
		FileOff: pos, Len: size, BlockKey: key,
		BlockOff: 0, BlockLen: size, ChecksumSHA256: sum,
	}
	return b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.store.InsertPendingBlocksTx(tx, pending, now); err != nil {
			return err
		}
		return b.store.ApplyCompactTx(ctx, tx, task.InodeID, task.Chunk, origin, []datastore.SliceOp{op}, now)
	})
}
