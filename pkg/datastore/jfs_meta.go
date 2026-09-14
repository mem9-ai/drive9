// This file is the extent meta engine's RPC surface: RunExtentMetaOp runs one
// JuiceFS meta op inside a tenant transaction, dispatchExtentOp maps an op name
// onto the engine method that implements it, and dispatchExtentRead serves the
// read-only ops without the write transaction. The engine methods themselves
// live in the jfs_* files listed in jfs.go.
package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

func jfsLockBlockFromRaw(op string, raw json.RawMessage) (block bool, writeLock bool) {
	if op != "setlk" && op != "flock" {
		return false, false
	}
	var peek struct {
		Block bool   `json:"block"`
		Ltype uint32 `json:"ltype"`
	}
	if json.Unmarshal(raw, &peek) != nil {
		return false, false
	}
	return peek.Block, jfsIsWriteLock(peek.Ltype)
}

// dispatchExtentRead serves the read-only ops on a plain connection, the way
// upstream JuiceFS serves them through simpleTxn (autocommit) or roTxn (a
// read-only snapshot). Running them inside the tenant write transaction costs
// two extra TiDB round trips each, on the ops a sqlite workload issues
// thousands of times.
func (s *Store) dispatchExtentRead(ctx context.Context, db execer, op string, raw json.RawMessage) (any, int, error) {
	switch op {
	case "get_counter":
		var in struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		v, err := s.jfsGetCounterTx(db, in.Name)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "value": v}, 0, nil
	case "lookup":
		var in struct {
			Parent uint64 `json:"parent"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ino, attr, eno, err := s.jfsLookupTx(db, in.Parent, in.Name)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": ino, "attr": attr}, eno, nil
	case "getattr":
		var in struct {
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		attr, eno, err := s.jfsGetAttrTx(db, in.Inode)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": in.Inode, "attr": attr}, eno, nil
	case "readlink":
		var in struct {
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		target, eno, err := s.jfsReadlinkTx(db, in.Inode)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "target": target}, eno, nil
	case "readdir":
		var in struct {
			Inode uint64 `json:"inode"`
			Plus  uint8  `json:"plus"`
			Limit int    `json:"limit"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		entries, err := s.jfsReaddirTx(db, in.Inode, in.Limit)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "entries": entries}, 0, nil
	case "read":
		var in struct {
			Inode uint64 `json:"inode"`
			Indx  uint32 `json:"indx"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		buf, err := s.jfsReadTx(db, in.Inode, in.Indx)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "slices": buf}, 0, nil
	}
	return nil, int(syscall.EINVAL), fmt.Errorf("extent op %q is not read-only", op)
}

// extentReadOnlyOp reports whether an op only reads the tenant schema.
func extentReadOnlyOp(op string) bool {
	switch op {
	case "lookup", "getattr", "readlink", "readdir", "read", "get_counter":
		return true
	}
	return false
}

// ExtentQuotaLimit is the soft admission snapshot the extent write path
// enforces inside its transaction. It mirrors what the classic write path
// checks (tenant max file size and tenant storage quota) with the tenant's
// extent bytes already included in UsedBytes; the snapshot is taken per
// request from the same caches the classic path reads, so an extent mount can
// no longer write past either limit. Values <= 0 mean "not configured".
type ExtentQuotaLimit struct {
	MaxFileSizeBytes int64
	MaxStorageBytes  int64
	UsedBytes        int64
}

// admit reports an errno when growing an extent file from oldLen to newLen
// would break the tenant's file-size or storage limit.
func (q *ExtentQuotaLimit) admit(oldLen, newLen uint64) int {
	if q == nil || newLen <= oldLen {
		return 0
	}
	if q.MaxFileSizeBytes > 0 && int64(newLen) > q.MaxFileSizeBytes {
		return int(syscall.EFBIG)
	}
	if q.MaxStorageBytes > 0 {
		growth := int64(newLen) - int64(oldLen)
		if q.UsedBytes+growth > q.MaxStorageBytes {
			return int(syscall.EDQUOT)
		}
	}
	return 0
}
func (s *Store) RunExtentMetaOp(ctx context.Context, op string, raw json.RawMessage, quota *ExtentQuotaLimit) (json.RawMessage, int, error) {
	opStart := time.Now()
	defer func() { observeStoreOp(ctx, "extent_"+op, opStart, nil) }()
	var (
		out   any
		errno int
		opErr error
	)
	block, writeLock := jfsLockBlockFromRaw(op, raw)
	const maxAttempts = 8
	var err error
	attempts := 0
	if op == "compact" {
		return s.runCompactOp(ctx, raw)
	}
	// Read-only ops do not need the tenant write transaction. Upstream JuiceFS
	// serves them through simpleTxn (autocommit) or roTxn (a read-only
	// snapshot), and on this engine a read-write BEGIN + COMMIT is two extra
	// TiDB round trips per lookup, getattr, readlink, readdir and chunk read —
	// the ops a sqlite workload issues thousands of times.
	if extentReadOnlyOp(op) && s.jfsInited.Load() {
		out, errno, opErr = s.dispatchExtentRead(ctx, s.db, op, raw)
		if opErr == nil {
			body, mErr := json.Marshal(out)
			if mErr != nil {
				return nil, int(syscall.EIO), mErr
			}
			return body, errno, nil
		}
		// Anything else keeps the transactional path and its retry loop.
		out, errno, opErr = nil, 0, nil
	}
	for {
		out, errno, opErr = nil, 0, nil
		err = s.InTx(ctx, func(tx *sql.Tx) error {
			if err := s.jfsEnsureInitTx(tx); err != nil {
				return err
			}
			out, errno, opErr = s.dispatchExtentOp(ctx, tx, op, raw, quota)
			if opErr != nil {
				return opErr
			}
			if errno != 0 {
				return nil
			}
			return nil
		})
		if err == nil && block && errno == int(syscall.EAGAIN) {
			sleep := 10 * time.Millisecond
			if writeLock {
				sleep = time.Millisecond
			}
			select {
			case <-ctx.Done():
				out = map[string]any{"errno": int(syscall.EINTR)}
				errno = int(syscall.EINTR)
				err = nil
			case <-time.After(sleep):
				continue
			}
		}
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			out = map[string]any{"errno": int(syscall.EINTR)}
			errno = int(syscall.EINTR)
			break
		}
		attempts++
		if attempts < maxAttempts && (isUniqueViolation(err) || isDeadlock(err)) {
			continue
		}
		if errno == 0 {
			return nil, int(syscall.EIO), err
		}
		break
	}
	body, mErr := json.Marshal(out)
	if mErr != nil {
		return nil, int(syscall.EIO), mErr
	}
	// Deleting a file only records jfs_delfile inside the transaction (a full
	// slice walk there made delete-journal COMMIT take seconds). Reclaim its
	// blocks right after the commit; anything this misses is retried by the
	// tenant worker, and the delfile row survives until the walk finishes.
	if errno == 0 && (op == "unlink" || op == "delete_sustained") {
		_, _ = s.DrainPendingDeletedFiles(ctx, 4)
	}
	// A chunk that reached the compact threshold queues its own task, after the
	// write transaction committed: the write txn must not take
	// slice_compact_tasks locks, and the slice count is already in the reply.
	if errno == 0 && op == "write" {
		s.enqueueCompactAfterWrite(ctx, raw, out)
	}
	// A truncate (or a setattr that changed the size) can push a chunk past the
	// same threshold by appending zero slices; the dispatch reported which ones.
	if errno == 0 && (op == "truncate" || op == "setattr") {
		s.enqueueCompactAfterTruncate(ctx, raw, out)
	}
	return body, errno, nil
}

// enqueueCompactAfterTruncate queues compaction for the chunks a length change
// pushed past the threshold. Without it a chunk grown only by repeated
// truncates would never be compacted, and its slice list — which every read
// walks — would keep growing with no pressure valve.
func (s *Store) enqueueCompactAfterTruncate(ctx context.Context, raw json.RawMessage, resp any) {
	m, ok := resp.(map[string]any)
	if !ok {
		return
	}
	chunks, ok := m["compact_chunks"].([]uint32)
	if !ok || len(chunks) == 0 {
		return
	}
	var in struct {
		Inode uint64 `json:"inode"`
	}
	if err := json.Unmarshal(raw, &in); err != nil || in.Inode == 0 {
		return
	}
	for _, indx := range chunks {
		taskID := strings.ToLower(ulid.Make().String())
		if _, err := s.db.ExecContext(ctx, `INSERT INTO slice_compact_tasks (task_id, extent_ino, chunk, status, max_attempts, available_at)
			VALUES (?, ?, ?, 'PENDING', 8, CURRENT_TIMESTAMP(3))
			ON DUPLICATE KEY UPDATE status = IF(status IN ('COMPLETED','FAILED'), 'PENDING', status)`,
			taskID, in.Inode, indx); err != nil && ctx.Err() == nil {
			logger.Warn(ctx, "extent compact enqueue after truncate failed",
				zap.Uint64("inode", in.Inode), zap.Uint32("chunk", indx), zap.Error(err))
		}
	}
}

// enqueueCompactAfterWrite queues a compact task for a write that carried its
// chunk across the compact threshold, which is the only way a chunk grows
// (truncate and compact shrink it, delete removes it). This replaces the
// self-feeding scan of jfs_chunk: `LENGTH(slices)` has no index, so every mount
// paid a full table scan every two seconds, and the upsert that followed it
// overwrote a lease another executor already held.
//
// Only the crossing write queues, so a chunk that stays above the threshold
// does not pay a round trip on every write while compaction is in flight. It is
// best effort: the client triggers compact itself when its own slice count
// crosses maxSlices, and a failed enqueue only delays compaction.
func (s *Store) enqueueCompactAfterWrite(ctx context.Context, raw json.RawMessage, resp any) {
	m, ok := resp.(map[string]any)
	if !ok {
		return
	}
	nslice, ok := m["num_slices"].(int)
	if !ok {
		return
	}
	var in struct {
		Inode uint64 `json:"inode"`
		Indx  uint32 `json:"indx"`
		Slice *struct {
			Id uint64 `json:"id"`
		} `json:"slice"`
		Parts []struct {
			Off uint32 `json:"off"`
		} `json:"parts"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return
	}
	added := len(in.Parts)
	if added == 0 && in.Slice != nil {
		added = 1
	}
	if added == 0 || nslice < jfsCompactThreshold || nslice-added >= jfsCompactThreshold {
		return
	}
	// Revive a finished task, never touch a queued or leased one: the same
	// (inode, chunk) row is reused, so a live lease stays with its owner.
	taskID := strings.ToLower(ulid.Make().String())
	_, err := s.db.ExecContext(ctx, `INSERT INTO slice_compact_tasks (task_id, extent_ino, chunk, status, max_attempts, available_at)
		VALUES (?, ?, ?, 'PENDING', 8, CURRENT_TIMESTAMP(3))
		ON DUPLICATE KEY UPDATE status = IF(status IN ('COMPLETED','FAILED'), 'PENDING', status)`,
		taskID, in.Inode, in.Indx)
	if err != nil && ctx.Err() == nil {
		logger.Warn(ctx, "extent compact enqueue failed",
			zap.Uint64("inode", in.Inode), zap.Uint32("indx", in.Indx), zap.Int("slices", nslice), zap.Error(err))
	}
}

// runCompactOp is JuiceFS dbMeta.doCompactChunk: CAS in one txn, then
// deleteSlice after that txn. GC INSERT under the chunk FOR UPDATE held
// sqlite Write/Flush on the same inode for seconds (crash01 --wait all).
func (s *Store) runCompactOp(ctx context.Context, raw json.RawMessage) (json.RawMessage, int, error) {
	var in struct {
		Inode   uint64 `json:"inode"`
		Indx    uint32 `json:"indx"`
		Origin  []byte `json:"origin"`
		Skipped int    `json:"skipped"`
		Pos     uint32 `json:"pos"`
		Id      uint64 `json:"id"`
		Size    uint32 `json:"size"`
	}
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, int(syscall.EINVAL), err
	}
	var (
		eno  int
		olds []extentSliceRef
	)
	const maxAttempts = 8
	var err error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		olds = nil
		eno = 0
		err = s.InTx(ctx, func(tx *sql.Tx) error {
			if err := s.jfsEnsureInitTx(tx); err != nil {
				return err
			}
			var err error
			eno, olds, err = s.jfsCompactTx(tx, in.Inode, in.Indx, in.Origin, in.Skipped, in.Pos, in.Id, in.Size)
			return err
		})
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return nil, int(syscall.EINTR), ctx.Err()
		}
		if isUniqueViolation(err) || isDeadlock(err) {
			continue
		}
		return nil, int(syscall.EIO), err
	}
	if err != nil {
		return nil, int(syscall.EIO), err
	}
	if eno == 0 && len(olds) > 0 {
		// The CAS above already removed these slices from the chunk blob, so this
		// record is the only way the drain can still find their blocks. It must
		// therefore survive the caller going away: run it on a context detached
		// from the request (the RPC handler runs on r.Context(), so a client
		// disconnect or unmount would otherwise discard it) and never drop the
		// error silently.
		enqueueCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), deadSliceGCEnqueueTimeout)
		defer cancel()
		if gerr := s.InTx(enqueueCtx, func(tx *sql.Tx) error {
			return s.jfsEnqueueDeadSliceGCTx(tx, in.Inode, olds)
		}); gerr != nil {
			logger.Warn(enqueueCtx, "extent_dead_slice_gc_enqueue_failed",
				zap.Uint64("inode", in.Inode), zap.Int("slices", len(olds)), zap.Error(gerr))
		}
	}
	body, mErr := json.Marshal(map[string]any{"errno": eno})
	if mErr != nil {
		return nil, int(syscall.EIO), mErr
	}
	return body, eno, nil
}

// deadSliceGCEnqueueTimeout bounds the post-commit block-GC record write of a
// compaction. It is short because the transaction is a handful of INSERTs, and
// it exists so a wedged database cannot hold an RPC handler open for ever.
const deadSliceGCEnqueueTimeout = 10 * time.Second

func (s *Store) dispatchExtentOp(ctx context.Context, tx *sql.Tx, op string, raw json.RawMessage, quota *ExtentQuotaLimit) (any, int, error) {
	switch op {
	case "get_counter":
		var in struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		v, err := s.jfsGetCounterTx(tx, in.Name)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "value": v}, 0, nil
	case "incr_counter":
		var in struct {
			Name  string `json:"name"`
			Value int64  `json:"value"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		v, err := s.jfsIncrCounterTx(tx, in.Name, in.Value)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "value": v}, 0, nil
	case "set_if_small":
		var in struct {
			Name  string `json:"name"`
			Value int64  `json:"value"`
			Diff  int64  `json:"diff"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ok, err := s.jfsSetIfSmallTx(tx, in.Name, in.Value, in.Diff)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "ok": ok}, 0, nil
	case "load":
		b, err := s.jfsLoadFormatTx(tx)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "format": b}, 0, nil
	case "init":
		var in struct {
			Format json.RawMessage `json:"format"`
			Force  bool            `json:"force"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsStoreFormatTx(tx, in.Format, in.Force); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	case "new_session":
		var in struct {
			Sid    uint64 `json:"sid"`
			Expire int64  `json:"expire"`
			Info   []byte `json:"info"`
			Update bool   `json:"update"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsUpsertSessionTx(tx, in.Sid, in.Expire, in.Info, in.Update); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	case "refresh_session":
		var in struct {
			Sid    uint64 `json:"sid"`
			Expire int64  `json:"expire"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsRefreshSessionTx(tx, in.Sid, in.Expire); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	case "lookup":
		var in struct {
			Parent uint64 `json:"parent"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ino, attr, eno, err := s.jfsLookupTx(tx, in.Parent, in.Name)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": ino, "attr": attr}, eno, nil
	case "getattr":
		var in struct {
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		attr, eno, err := s.jfsGetAttrTx(tx, in.Inode)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": in.Inode, "attr": attr}, eno, nil
	case "setattr":
		var in struct {
			Inode uint64     `json:"inode"`
			Set   uint16     `json:"set"`
			Attr  ExtentAttr `json:"attr"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		var compactChunks []uint32
		attr, eno, err := s.jfsSetAttrTx(tx, in.Inode, in.Set, in.Attr, quota, &compactChunks)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{
			"errno": eno, "inode": in.Inode, "attr": attr,
			// A size change can arrive through setattr (a direct meta call, or
			// the VFS SetAttr that follows Truncate), so the reply has to carry
			// the chunks the post-commit enqueue should look at.
			"compact_chunks": compactChunks,
		}, eno, nil
	case "mknod":
		var in struct {
			Parent   uint64     `json:"parent"`
			Name     string     `json:"name"`
			Type     uint8      `json:"type"`
			Mode     uint16     `json:"mode"`
			Cumask   uint16     `json:"cumask"`
			Path     string     `json:"path"`
			Inode    uint64     `json:"inode"`
			Attr     ExtentAttr `json:"attr"`
			ProjPath string     `json:"proj_path"`
			Uid      uint32     `json:"uid"`
			Gid      uint32     `json:"gid"`
			Indx     uint32     `json:"indx"`
			Parts    []struct {
				Off   uint32      `json:"off"`
				Slice ExtentSlice `json:"slice"`
			} `json:"parts"`
			Mtime time.Time `json:"mtime"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ino, attr, eno, err := s.jfsMknodTx(ctx, tx, in.Parent, in.Name, in.Type, in.Mode, in.Cumask, in.Inode, in.Attr, in.ProjPath, in.Uid, in.Gid, in.Path)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		if eno == 0 && len(in.Parts) > 0 {
			parts := make([]extentWritePart, 0, len(in.Parts))
			for _, p := range in.Parts {
				parts = append(parts, extentWritePart{Off: p.Off, Slice: p.Slice})
			}
			_, wattr, _, _, weno, werr := s.jfsWritePartsTx(tx, ino, in.Indx, parts, in.Mtime, quota)
			if werr != nil {
				return nil, int(syscall.EIO), werr
			}
			if weno != 0 {
				eno = weno
			}
			if wattr != nil {
				attr = wattr
			}
		}
		return map[string]any{"errno": eno, "inode": ino, "attr": attr}, eno, nil
	case "readlink":
		var in struct {
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		target, eno, err := s.jfsReadlinkTx(tx, in.Inode)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "target": target}, eno, nil
	case "unlink":
		var in struct {
			Parent    uint64 `json:"parent"`
			Name      string `json:"name"`
			SkipTrash bool   `json:"skip_trash"`
			ProjPath  string `json:"proj_path"`
			Sid       uint64 `json:"sid"`
			Opened    bool   `json:"opened"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		attr, eno, err := s.jfsUnlinkTx(ctx, tx, in.Parent, in.Name, in.ProjPath, in.Sid, in.Opened)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "attr": attr}, eno, nil
	case "rmdir":
		var in struct {
			Parent   uint64 `json:"parent"`
			Name     string `json:"name"`
			ProjPath string `json:"proj_path"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ino, attr, eno, err := s.jfsRmdirTx(tx, in.Parent, in.Name, in.ProjPath)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": ino, "attr": attr}, eno, nil
	case "rename":
		var in struct {
			SrcParent uint64 `json:"src_parent"`
			SrcName   string `json:"src_name"`
			DstParent uint64 `json:"dst_parent"`
			DstName   string `json:"dst_name"`
			Flags     uint32 `json:"flags"`
			SrcPath   string `json:"src_path"`
			DstPath   string `json:"dst_path"`
			// DstOpened/Sid describe the REPLACED destination, not the source:
			// the FUSE frontend sets them when this mount still holds the
			// destination open, and the server then keeps its node and blocks
			// until the last close (jfs_sustained) instead of reclaiming them
			// with the rename. Same contract as the unlink op's Opened/Sid.
			DstOpened bool   `json:"dst_opened"`
			Sid       uint64 `json:"sid"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ino, tinode, attr, tattr, eno, err := s.jfsRenameTx(ctx, tx, in.SrcParent, in.SrcName, in.DstParent, in.DstName, in.SrcPath, in.DstPath, in.Flags, in.DstOpened, in.Sid)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": ino, "tinode": tinode, "attr": attr, "tattr": tattr}, eno, nil
	case "readdir":
		var in struct {
			Inode uint64 `json:"inode"`
			Plus  uint8  `json:"plus"`
			Limit int    `json:"limit"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		entries, err := s.jfsReaddirTx(tx, in.Inode, in.Limit)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "entries": entries}, 0, nil
	case "read":
		var in struct {
			Inode uint64 `json:"inode"`
			Indx  uint32 `json:"indx"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		buf, err := s.jfsReadTx(tx, in.Inode, in.Indx)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "slices": buf}, 0, nil
	case "write":
		var in struct {
			Inode uint64      `json:"inode"`
			Indx  uint32      `json:"indx"`
			Off   uint32      `json:"off"`
			Slice ExtentSlice `json:"slice"`
			Parts []struct {
				Off   uint32      `json:"off"`
				Slice ExtentSlice `json:"slice"`
			} `json:"parts"`
			Mtime time.Time `json:"mtime"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		parts := make([]extentWritePart, 0, len(in.Parts)+1)
		for _, p := range in.Parts {
			parts = append(parts, extentWritePart{Off: p.Off, Slice: p.Slice})
		}
		if len(parts) == 0 {
			parts = append(parts, extentWritePart{Off: in.Off, Slice: in.Slice})
		}
		nslice, attr, dlen, dspace, eno, err := s.jfsWritePartsTx(tx, in.Inode, in.Indx, parts, in.Mtime, quota)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		// Hand the chunk's resulting mapping back with the write. The client
		// caches it instead of dropping its cached mapping and asking again:
		// one chunk covers 64 MB, so a database of that size is a single chunk
		// and every commit would otherwise invalidate the whole file's read
		// mapping, at one HTTP metadata query per read.
		var slices []byte
		if eno == 0 {
			slices, err = s.jfsReadTx(tx, in.Inode, in.Indx)
			if err != nil {
				return nil, int(syscall.EIO), err
			}
		}
		return map[string]any{
			"errno": eno, "num_slices": nslice, "attr": attr,
			"delta_length": dlen, "delta_space": dspace, "slices": slices,
		}, eno, nil
	case "delete_slice":
		var in struct {
			Id   uint64 `json:"id"`
			Size uint32 `json:"size"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsEnqueueSliceGCTx(tx, in.Id, in.Size, 0); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	case "truncate":
		var in struct {
			Inode         uint64 `json:"inode"`
			Flags         uint8  `json:"flags"`
			Length        uint64 `json:"length"`
			SkipPermCheck bool   `json:"skip_perm_check"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		var compactChunks []uint32
		attr, dlen, dspace, eno, err := s.jfsTruncateTx(tx, in.Inode, in.Length, quota, &compactChunks)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{
			"errno": eno, "attr": attr, "delta_length": dlen, "delta_space": dspace,
			"compact_chunks": compactChunks,
		}, eno, nil
	case "delete_sustained":
		var in struct {
			Sid   uint64 `json:"sid"`
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsDeleteSustainedTx(tx, in.Sid, in.Inode); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	case "flock":
		var in struct {
			Inode uint64 `json:"inode"`
			Sid   uint64 `json:"sid"`
			Owner uint64 `json:"owner"`
			Ltype uint32 `json:"ltype"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		eno, err := s.jfsFlockTx(tx, in.Inode, in.Sid, in.Owner, in.Ltype)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno}, eno, nil
	case "getlk":
		var in struct {
			Inode uint64 `json:"inode"`
			Sid   uint64 `json:"sid"`
			Owner uint64 `json:"owner"`
			Ltype uint32 `json:"ltype"`
			Start uint64 `json:"start"`
			End   uint64 `json:"end"`
			Pid   uint32 `json:"pid"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ltype, start, end, pid, eno, err := s.jfsGetlkTx(tx, in.Inode, in.Sid, in.Owner, in.Ltype, in.Start, in.End)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "ltype": ltype, "start": start, "end": end, "pid": pid}, eno, nil
	case "setlk":
		var in struct {
			Inode uint64 `json:"inode"`
			Sid   uint64 `json:"sid"`
			Owner uint64 `json:"owner"`
			Ltype uint32 `json:"ltype"`
			Start uint64 `json:"start"`
			End   uint64 `json:"end"`
			Pid   uint32 `json:"pid"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		eno, err := s.jfsSetlkTx(tx, in.Inode, in.Sid, in.Owner, in.Ltype, in.Start, in.End, in.Pid)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno}, eno, nil
	case "find_stale_sessions":
		var in struct {
			Limit int `json:"limit"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		sids, err := s.jfsFindStaleSessionsTx(tx, in.Limit)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "sids": sids}, 0, nil
	case "clean_stale_session":
		var in struct {
			Sid uint64 `json:"sid"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsCleanStaleSessionTx(tx, in.Sid); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	case "get_session":
		return map[string]any{"errno": 0}, 0, nil
	case "claim_compact":
		ino, indx, taskID, err := s.jfsClaimCompactTx(tx)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return map[string]any{"errno": 0, "inode": 0, "indx": 0, "task_id": ""}, 0, nil
			}
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0, "inode": ino, "indx": indx, "task_id": taskID}, 0, nil
	case "requeue_compact":
		var in struct {
			TaskID string `json:"task_id"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		var cause error
		if in.Error != "" {
			cause = errors.New(in.Error)
		}
		if err := s.jfsRequeueCompactTx(tx, in.TaskID, cause); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	case "complete_compact":
		var in struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsCompleteCompactTx(tx, in.TaskID); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	default:
		return map[string]any{"errno": int(syscall.ENOSYS)}, int(syscall.ENOSYS), nil
	}
}
