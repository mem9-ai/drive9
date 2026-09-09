package datastore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/oklog/ulid/v2"
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

// overlayExtentStat copies jfs_node.length onto File.SizeBytes.
// JuiceFS writes update jfs_node only; inodes.size_bytes stays 0 until
// truncate. Remote drive9 fs stat reads inodes.size_bytes unless overlaid.
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
	var length sql.NullInt64
	err := db.QueryRowContext(ctx, `SELECT j.length FROM file_nodes fn
		INNER JOIN jfs_node j ON j.inode = fn.extent_ino
		WHERE `+s.scope.AndAs("fn", `fn.path_hash = ? AND fn.path = ?`)+`
		AND fn.extent_ino IS NOT NULL AND fn.extent_ino <> 0`,
		s.scope.Args(fileNodePathHash(path), path)...).Scan(&length)
	if err != nil || !length.Valid {
		return
	}
	nf.File.SizeBytes = length.Int64
}

func (s *Store) withExtentStat(ctx context.Context, db execer, nf *NodeWithFile, err error) (*NodeWithFile, error) {
	if err != nil || nf == nil {
		return nf, err
	}
	s.overlayExtentStat(ctx, db, nf)
	return nf, nil
}

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

// RunExtentMetaOp executes one JuiceFS-engine RPC inside a tenant transaction
// and dual-writes the file_nodes projection for namespace ops.
func (s *Store) RunExtentMetaOp(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
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
	for {
		out, errno, opErr = nil, 0, nil
		err = s.InTx(ctx, func(tx *sql.Tx) error {
			if err := s.jfsEnsureInitTx(tx); err != nil {
				return err
			}
			out, errno, opErr = s.dispatchExtentOp(ctx, tx, op, raw)
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
	return body, errno, nil
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
		_ = s.InTx(ctx, func(tx *sql.Tx) error {
			return s.jfsEnqueueDeadSliceGCTx(tx, in.Inode, olds)
		})
	}
	body, mErr := json.Marshal(map[string]any{"errno": eno})
	if mErr != nil {
		return nil, int(syscall.EIO), mErr
	}
	return body, eno, nil
}

func (s *Store) dispatchExtentOp(ctx context.Context, tx *sql.Tx, op string, raw json.RawMessage) (any, int, error) {
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
		attr, eno, err := s.jfsSetAttrTx(tx, in.Inode, in.Set, in.Attr)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": in.Inode, "attr": attr}, eno, nil
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
			_, wattr, _, _, weno, werr := s.jfsWritePartsTx(tx, ino, in.Indx, parts, in.Mtime)
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
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ino, tinode, attr, tattr, eno, err := s.jfsRenameTx(tx, in.SrcParent, in.SrcName, in.DstParent, in.DstName, in.SrcPath, in.DstPath, in.Flags)
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
		nslice, attr, dlen, dspace, eno, err := s.jfsWritePartsTx(tx, in.Inode, in.Indx, parts, in.Mtime)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{
			"errno": eno, "num_slices": nslice, "attr": attr,
			"delta_length": dlen, "delta_space": dspace,
		}, eno, nil
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
		attr, dlen, dspace, eno, err := s.jfsTruncateTx(tx, in.Inode, in.Length)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{
			"errno": eno, "attr": attr, "delta_length": dlen, "delta_space": dspace,
		}, eno, nil
	case "compact":
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
		eno, _, err := s.jfsCompactTx(tx, in.Inode, in.Indx, in.Origin, in.Skipped, in.Pos, in.Id, in.Size)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno}, eno, nil
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
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		if err := s.jfsRequeueCompactTx(tx, in.TaskID); err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": 0}, 0, nil
	default:
		return map[string]any{"errno": int(syscall.ENOSYS)}, int(syscall.ENOSYS), nil
	}
}

func (s *Store) jfsEnsureInitTx(tx *sql.Tx) error {
	if s != nil && s.jfsInited.Load() {
		return nil
	}
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, jfsRootIno).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		if s != nil {
			s.jfsInited.Store(true)
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
	}
	return nil
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

func (s *Store) jfsGetCounterTx(tx *sql.Tx, name string) (int64, error) {
	var v int64
	err := tx.QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, name).Scan(&v)
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

func (s *Store) jfsLookupTx(tx *sql.Tx, parent uint64, name string) (uint64, *ExtentAttr, int, error) {
	var ino uint64
	err := tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&ino)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return 0, nil, 0, err
	}
	attr, eno, err := s.jfsGetAttrTx(tx, ino)
	return ino, attr, eno, err
}

func (s *Store) jfsGetAttrTx(tx *sql.Tx, ino uint64) (*ExtentAttr, int, error) {
	var typ uint8
	var mode uint16
	var uid, gid uint32
	var atime, mtime, ctime int64
	var asec, msec, csec int16
	var nlink uint32
	var length, parent uint64
	var rdev uint32
	err := tx.QueryRow(`SELECT type, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, parent, rdev
		FROM jfs_node WHERE inode = ?`, ino).Scan(
		&typ, &mode, &uid, &gid, &atime, &mtime, &ctime, &asec, &msec, &csec, &nlink, &length, &parent, &rdev)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return nil, 0, err
	}
	attr := jfsAttrFromNode(typ, mode, uid, gid, atime, mtime, ctime, asec, msec, csec, nlink, length, parent, rdev)
	return &attr, 0, nil
}

func (s *Store) jfsSetAttrTx(tx *sql.Tx, ino uint64, set uint16, patch ExtentAttr) (*ExtentAttr, int, error) {
	attr, eno, err := s.jfsGetAttrTx(tx, ino)
	if err != nil || eno != 0 {
		return attr, eno, err
	}
	const (
		setMode     = 1 << 0
		setUID      = 1 << 1
		setGID      = 1 << 2
		setSize     = 1 << 3
		setAtime    = 1 << 4
		setMtime    = 1 << 5
		setAtimeNow = 1 << 7
		setMtimeNow = 1 << 8
	)
	if set&setMode != 0 {
		attr.Mode = patch.Mode
	}
	if set&setUID != 0 {
		attr.Uid = patch.Uid
	}
	if set&setGID != 0 {
		attr.Gid = patch.Gid
	}
	if set&setSize != 0 {
		attr.Length = patch.Length
	}
	nowt := time.Now()
	if set&setAtimeNow != 0 {
		attr.Atime = nowt.Unix()
		attr.Atimensec = uint32(nowt.Nanosecond())
	} else if set&setAtime != 0 {
		attr.Atime = patch.Atime
		attr.Atimensec = patch.Atimensec
	}
	if set&setMtimeNow != 0 {
		attr.Mtime = nowt.Unix()
		attr.Mtimensec = uint32(nowt.Nanosecond())
	} else if set&setMtime != 0 {
		attr.Mtime = patch.Mtime
		attr.Mtimensec = patch.Mtimensec
	}
	now := nowt.UnixNano()
	atime, asec := jfsSplitTime(attr.Atime*1e9 + int64(attr.Atimensec))
	mtime, msec := jfsSplitTime(attr.Mtime*1e9 + int64(attr.Mtimensec))
	ctime, csec := jfsSplitTime(now)
	if _, err := tx.Exec(`UPDATE jfs_node SET mode=?, uid=?, gid=?, length=?, atime=?, mtime=?, ctime=?, atimensec=?, mtimensec=?, ctimensec=? WHERE inode=?`,
		attr.Mode, attr.Uid, attr.Gid, attr.Length, atime, mtime, ctime, asec, msec, csec, ino); err != nil {
		return nil, 0, err
	}
	return s.jfsGetAttrTx(tx, ino)
}

func (s *Store) jfsMknodTx(ctx context.Context, tx *sql.Tx, parent uint64, name string, typ uint8, mode, cumask uint16, inode uint64, attr ExtentAttr, projPath string, uid, gid uint32, symlinkTarget string) (uint64, *ExtentAttr, int, error) {
	if parent == 0 {
		parent = jfsRootIno
	}
	if _, eno, err := s.jfsGetAttrTx(tx, parent); err != nil {
		return 0, nil, 0, err
	} else if eno == int(syscall.ENOENT) && projPath != "" {
		var err error
		parent, err = s.jfsEnsureParentsTx(tx, projPath, uid, gid)
		if err != nil {
			return 0, nil, 0, err
		}
	} else if eno != 0 {
		return 0, nil, eno, nil
	}
	var exist uint64
	err := tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&exist)
	if err == nil {
		a, _, gerr := s.jfsGetAttrTx(tx, exist)
		return exist, a, int(syscall.EEXIST), gerr
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, nil, 0, err
	}
	if inode == 0 {
		v, err := s.jfsIncrCounterTx(tx, "nextInode", 1)
		if err != nil {
			return 0, nil, 0, err
		}
		inode = uint64(v)
	}
	mode = mode &^ cumask
	nlink := uint32(1)
	length := uint64(0)
	switch typ {
	case jfsTypeDir:
		nlink = 2
		length = 4096
	case jfsTypeSymlink:
		length = uint64(len(symlinkTarget))
		if attr.Length > 0 {
			length = attr.Length
		}
	}
	now := time.Now().UnixNano()
	atime, asec := jfsSplitTime(now)
	if attr.Uid == 0 {
		attr.Uid = uid
	}
	if attr.Gid == 0 {
		attr.Gid = gid
	}
	if _, err := tx.Exec(`INSERT INTO jfs_node
		(inode, type, flags, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, rdev, parent)
		VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		inode, typ, mode, attr.Uid, attr.Gid, atime, atime, atime, asec, asec, asec, nlink, length, attr.Rdev, parent); err != nil {
		return 0, nil, 0, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		parent, []byte(name), inode, typ); err != nil {
		return 0, nil, 0, err
	}
	if typ == jfsTypeSymlink {
		if _, err := tx.Exec(`INSERT INTO jfs_symlink (inode, target) VALUES (?, ?)`, inode, []byte(symlinkTarget)); err != nil {
			return 0, nil, 0, err
		}
	}
	if typ != jfsTypeDir && projPath != "" {
		if jfsTestFailBeforeProjection != nil {
			if err := jfsTestFailBeforeProjection(); err != nil {
				return 0, nil, 0, err
			}
		}
		if err := s.insertExtentProjectionTx(tx, projPath, inode, mode); err != nil {
			return 0, nil, 0, err
		}
	}
	if typ == jfsTypeDir && projPath != "" {
		dirPath := projPath
		if !strings.HasSuffix(dirPath, "/") {
			dirPath += "/"
		}
		_, _ = tx.Exec(`UPDATE file_nodes SET extent_ino = ? WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			append([]any{inode}, s.scope.Args(fileNodePathHash(dirPath), dirPath)...)...)
	}
	out := jfsAttrFromNode(typ, mode, attr.Uid, attr.Gid, atime, atime, atime, asec, asec, asec, nlink, length, parent, attr.Rdev)
	return inode, &out, 0, nil
}

func (s *Store) jfsEnsureParentsTx(tx *sql.Tx, filePath string, uid, gid uint32) (uint64, error) {
	parentPath := pathutil.ParentPath(filePath)
	parent := uint64(jfsRootIno)
	if parentPath == "/" {
		return parent, nil
	}
	rel := strings.Trim(parentPath, "/")
	parts := strings.Split(rel, "/")
	acc := ""
	for _, p := range parts {
		acc += "/" + p
		dirPath := acc + "/"
		var exist uint64
		err := tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(p)).Scan(&exist)
		if err == nil {
			parent = exist
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		v, err := s.jfsIncrCounterTx(tx, "nextInode", 1)
		if err != nil {
			return 0, err
		}
		ino := uint64(v)
		now := time.Now().UnixNano()
		atime, asec := jfsSplitTime(now)
		if _, err := tx.Exec(`INSERT INTO jfs_node
			(inode, type, flags, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, rdev, parent)
			VALUES (?, ?, 0, 0755, ?, ?, ?, ?, ?, ?, ?, ?, 2, 4096, 0, ?)`,
			ino, jfsTypeDir, uid, gid, atime, atime, atime, asec, asec, asec, parent); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
			parent, []byte(p), ino, jfsTypeDir); err != nil {
			return 0, err
		}
		_, _ = tx.Exec(`UPDATE file_nodes SET extent_ino = ? WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			append([]any{ino}, s.scope.Args(fileNodePathHash(dirPath), dirPath)...)...)
		parent = ino
	}
	return parent, nil
}

func (s *Store) insertExtentProjectionTx(tx *sql.Tx, path string, extentIno uint64, mode uint16) error {
	now := time.Now().UTC()
	inodeID := strings.ToLower(ulid.Make().String())
	nodeID := strings.ToLower(ulid.Make().String())
	parent := pathutil.ParentPath(path)
	name := pathutil.BaseName(path)
	if _, err := tx.Exec(`INSERT INTO inodes (`+s.scope.InsCols(`inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at`)+`)
		VALUES (`+s.scope.InsVals(`?, 0, 1, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(inodeID, uint32(mode), StatusConfirmed, now, now, now)...); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO contents (`+s.scope.InsCols(`inode_id, storage_type, storage_ref, storage_ref_hash, storage_encryption_mode, storage_encryption_key_id, content_layout`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, '', '', 'none', '', ?`)+`)`,
		s.scope.Args(inodeID, StorageExtent, string(ContentLayoutExtent))...); err != nil {
		return fmt.Errorf("insert extent contents: %w", err)
	}
	_, err := tx.Exec(`INSERT INTO file_nodes (`+s.scope.InsCols(`node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory, file_id, inode_id, created_at, content_layout, extent_ino`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(nodeID, path, fileNodePathHash(path), parent, fileNodePathHash(parent),
			name, inodeID, inodeID, now, string(ContentLayoutExtent), extentIno)...)
	if isUniqueViolation(err) {
		return fmt.Errorf("%w", ErrPathConflict)
	}
	return err
}

func (s *Store) jfsParentInoForPathTx(tx *sql.Tx, filePath string) (uint64, error) {
	parentPath := pathutil.ParentPath(filePath)
	if parentPath == "/" {
		return jfsRootIno, nil
	}
	dirPath := parentPath
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}
	var ino sql.NullInt64
	err := tx.QueryRow(`SELECT extent_ino FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
		s.scope.Args(fileNodePathHash(dirPath), dirPath)...).Scan(&ino)
	if err == nil && ino.Valid && ino.Int64 > 0 {
		return uint64(ino.Int64), nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return s.jfsEnsureParentsTx(tx, filePath, 0, 0)
}

func (s *Store) jfsLinkEdgeTx(tx *sql.Tx, dstPath string, ino uint64) error {
	parent, err := s.jfsParentInoForPathTx(tx, dstPath)
	if err != nil {
		return err
	}
	name := pathutil.BaseName(dstPath)
	var exist uint64
	err = tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&exist)
	if err == nil {
		return ErrPathConflict
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var typ uint8
	if err := tx.QueryRow(`SELECT type FROM jfs_node WHERE inode = ?`, ino).Scan(&typ); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		parent, []byte(name), ino, typ); err != nil {
		return err
	}
	now := time.Now().UnixNano()
	ctime, csec := jfsSplitTime(now)
	_, err = tx.Exec(`UPDATE jfs_node SET nlink = nlink + 1, ctime = ?, ctimensec = ? WHERE inode = ?`, ctime, csec, ino)
	return err
}

func (s *Store) jfsUnlinkTx(ctx context.Context, tx *sql.Tx, parent uint64, name, projPath string, sid uint64, opened bool) (*ExtentAttr, int, error) {
	var ino uint64
	var typ uint8
	err := tx.QueryRow(`SELECT inode, type FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&ino, &typ)
	if errors.Is(err, sql.ErrNoRows) {
		if projPath != "" {
			_, _ = tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
				s.scope.Args(fileNodePathHash(projPath), projPath)...)
		}
		return nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return nil, 0, err
	}
	if typ == jfsTypeDir {
		return nil, int(syscall.EPERM), nil
	}
	attr, eno, err := s.jfsGetAttrTx(tx, ino)
	if err != nil || eno != 0 {
		return attr, eno, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)); err != nil {
		return nil, 0, err
	}
	path := projPath
	if path == "" {
		path, _ = s.jfsProjectionPathTx(tx, parent, name)
	}
	if path != "" {
		if _, err := tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			s.scope.Args(fileNodePathHash(path), path)...); err != nil {
			return nil, 0, err
		}
	}
	attr.Nlink--
	now := time.Now().UnixNano()
	ctime, csec := jfsSplitTime(now)
	if attr.Nlink > 0 {
		_, err = tx.Exec(`UPDATE jfs_node SET nlink = ?, ctime = ?, ctimensec = ? WHERE inode = ?`, attr.Nlink, ctime, csec, ino)
		return attr, 0, err
	}
	if opened {
		if _, err := tx.Exec(`INSERT IGNORE INTO jfs_sustained (sid, inode) VALUES (?, ?)`, sid, ino); err != nil {
			return nil, 0, err
		}
		_, err = tx.Exec(`UPDATE jfs_node SET nlink = 0, ctime = ?, ctimensec = ? WHERE inode = ?`, ctime, csec, ino)
		return attr, 0, err
	}
	if err := s.jfsEnqueueFileGCTx(tx, ino, attr.Length); err != nil {
		return nil, 0, err
	}
	if attr.Typ == jfsTypeSymlink {
		if _, err := tx.Exec(`DELETE FROM jfs_symlink WHERE inode = ?`, ino); err != nil {
			return nil, 0, err
		}
	}
	if _, err := tx.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino); err != nil {
		return nil, 0, err
	}
	return attr, 0, nil
}

func (s *Store) jfsReadlinkTx(tx *sql.Tx, ino uint64) ([]byte, int, error) {
	var target []byte
	err := tx.QueryRow(`SELECT target FROM jfs_symlink WHERE inode = ?`, ino).Scan(&target)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return nil, 0, err
	}
	return target, 0, nil
}

func (s *Store) jfsProjectionPathTx(tx *sql.Tx, parent uint64, name string) (string, error) {
	if parent == jfsRootIno {
		return "/" + name, nil
	}
	var p sql.NullString
	err := tx.QueryRow(`SELECT path FROM file_nodes WHERE extent_ino = ? AND is_directory = 1 LIMIT 1`, parent).Scan(&p)
	if err != nil || !p.Valid {
		return "", err
	}
	return strings.TrimSuffix(p.String, "/") + "/" + name, nil
}

func (s *Store) jfsRmdirTx(tx *sql.Tx, parent uint64, name, projPath string) (uint64, *ExtentAttr, int, error) {
	var ino uint64
	var typ uint8
	err := tx.QueryRow(`SELECT inode, type FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&ino, &typ)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return 0, nil, 0, err
	}
	if typ != jfsTypeDir {
		return 0, nil, int(syscall.ENOTDIR), nil
	}
	var children int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = ?`, ino).Scan(&children); err != nil {
		return 0, nil, 0, err
	}
	if children > 0 {
		return 0, nil, int(syscall.ENOTEMPTY), nil
	}
	attr, _, err := s.jfsGetAttrTx(tx, ino)
	if err != nil {
		return 0, nil, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)); err != nil {
		return 0, nil, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino); err != nil {
		return 0, nil, 0, err
	}
	return ino, attr, 0, nil
}

const (
	jfsRenameNoReplace = uint32(1 << 0)
	jfsRenameExchange  = uint32(1 << 1)
)

func (s *Store) jfsRenameTx(tx *sql.Tx, srcParent uint64, srcName string, dstParent uint64, dstName, srcPath, dstPath string, flags uint32) (uint64, uint64, *ExtentAttr, *ExtentAttr, int, error) {
	ino, attr, eno, err := s.jfsLookupTx(tx, srcParent, srcName)
	if err != nil || eno != 0 {
		return 0, 0, nil, nil, eno, err
	}
	if flags&jfsRenameExchange != 0 {
		return 0, 0, attr, nil, int(syscall.ENOTSUP), nil
	}
	var exist uint64
	err = tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, dstParent, []byte(dstName)).Scan(&exist)
	if err == nil {
		if exist == ino {
			return ino, exist, attr, attr, 0, nil
		}
		if flags&jfsRenameNoReplace != 0 {
			return 0, exist, attr, nil, int(syscall.EEXIST), nil
		}
		dstAttr, denos, derr := s.jfsGetAttrTx(tx, exist)
		if derr != nil || denos != 0 {
			return 0, exist, attr, dstAttr, denos, derr
		}
		if attr.Typ == jfsTypeDir && dstAttr.Typ != jfsTypeDir {
			return 0, exist, attr, dstAttr, int(syscall.ENOTDIR), nil
		}
		if attr.Typ != jfsTypeDir && dstAttr.Typ == jfsTypeDir {
			return 0, exist, attr, dstAttr, int(syscall.EISDIR), nil
		}
		if dstAttr.Typ == jfsTypeDir {
			var children int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = ?`, exist).Scan(&children); err != nil {
				return 0, 0, nil, nil, 0, err
			}
			if children > 0 {
				return 0, exist, attr, dstAttr, int(syscall.ENOTEMPTY), nil
			}
		}
		if _, ueno, uerr := s.jfsUnlinkTx(context.Background(), tx, dstParent, dstName, dstPath, 0, false); uerr != nil || ueno != 0 {
			if dstAttr.Typ == jfsTypeDir && ueno == int(syscall.EPERM) {
				if _, _, reno, rerr := s.jfsRmdirTx(tx, dstParent, dstName, dstPath); rerr != nil || reno != 0 {
					return 0, exist, attr, dstAttr, reno, rerr
				}
			} else {
				return 0, exist, attr, dstAttr, ueno, uerr
			}
		}
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil, nil, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, srcParent, []byte(srcName)); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ? AND inode <> ?`, dstParent, []byte(dstName), ino); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		dstParent, []byte(dstName), ino, attr.Typ); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if _, err := tx.Exec(`UPDATE jfs_node SET parent = ? WHERE inode = ?`, dstParent, ino); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if dstPath != "" {
		if err := s.jfsMoveProjectionTx(tx, ino, srcPath, srcName, dstPath); err != nil {
			return 0, 0, nil, nil, 0, err
		}
	}
	return ino, 0, attr, nil, 0, nil
}

func (s *Store) jfsMoveProjectionTx(tx *sql.Tx, extentIno uint64, srcPath, srcName, dstPath string) error {
	dstParent := pathutil.ParentPath(dstPath)
	dstName := pathutil.BaseName(dstPath)
	setArgs := []any{dstPath, fileNodePathHash(dstPath), dstParent, fileNodePathHash(dstParent), dstName}
	try := func(where string, whereArgs []any) (int64, error) {
		q := `UPDATE file_nodes SET path = ?, path_hash = ?, parent_path = ?, parent_path_hash = ?, name = ? WHERE ` + s.scope.And(where)
		args := append(append([]any{}, setArgs...), s.scope.Args(whereArgs...)...)
		res, err := tx.Exec(q, args...)
		if isUniqueViolation(err) {
			_, _ = tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
				s.scope.Args(fileNodePathHash(dstPath), dstPath)...)
			res, err = tx.Exec(q, args...)
		}
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		return n, nil
	}
	if srcPath != "" {
		n, err := try(`path_hash = ? AND path = ?`, []any{fileNodePathHash(srcPath), srcPath})
		if err != nil {
			return err
		}
		if n > 0 {
			return nil
		}
	}
	if extentIno == 0 || srcName == "" {
		return nil
	}
	_, err := try(`extent_ino = ? AND name = ?`, []any{extentIno, srcName})
	return err
}

func (s *Store) jfsReaddirTx(tx *sql.Tx, ino uint64, limit int) ([]map[string]any, error) {
	q := `SELECT name, inode, type FROM jfs_edge WHERE parent = ? ORDER BY name`
	args := []any{ino}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []map[string]any
	for rows.Next() {
		var name []byte
		var child uint64
		var typ uint8
		if err := rows.Scan(&name, &child, &typ); err != nil {
			return nil, err
		}
		attr, _, err := s.jfsGetAttrTx(tx, child)
		if err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"Inode": child, "Name": name, "Attr": attr})
	}
	return out, rows.Err()
}

func (s *Store) jfsReadTx(tx *sql.Tx, ino uint64, indx uint32) ([]byte, error) {
	var buf []byte
	err := tx.QueryRow(`SELECT slices FROM jfs_chunk WHERE inode = ? AND indx = ?`, ino, indx).Scan(&buf)
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

func (s *Store) jfsWritePartsTx(tx *sql.Tx, ino uint64, indx uint32, parts []extentWritePart, mtime time.Time) (int, *ExtentAttr, int64, int64, int, error) {
	// JuiceFS doWrite: ForUpdate Get node, upsert slice, insert sliceRef,
	// update node. Slice-count SELECT only when the chunk already existed
	// (compact decision). We skip that SELECT and compact INSERT: compact
	// is claim_compact scanning jfs_chunk, not the sqlite exclusive write.
	var typ uint8
	var length, parent uint64
	var uid, gid uint32
	err := tx.QueryRow(`SELECT type, length, parent, uid, gid FROM jfs_node WHERE inode = ?`, ino).
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
	// JuiceFS doWrite returns len(slices)/sliceBytes from the in-memory blob.
	// LENGTH() is the SQL equivalent without pulling the blob (CONCAT write).
	var nbytes int
	if err := tx.QueryRow(`SELECT LENGTH(slices) FROM jfs_chunk WHERE inode = ? AND indx = ?`, ino, indx).Scan(&nbytes); err != nil {
		return 0, nil, 0, 0, 0, err
	}
	attr := ExtentAttr{Typ: typ, Length: length, Parent: parent, Uid: uid, Gid: gid, Full: true}
	return nbytes / extentSliceBytes, &attr, dlen, dspace, 0, nil
}

func (s *Store) jfsTruncateTx(tx *sql.Tx, ino, length uint64) (*ExtentAttr, int64, int64, int, error) {
	attr, eno, err := s.jfsGetAttrTx(tx, ino)
	if err != nil || eno != 0 {
		return attr, 0, 0, eno, err
	}
	if attr.Length == length {
		return attr, 0, 0, 0, nil
	}
	dlen := int64(length) - int64(attr.Length)
	left, right := attr.Length, length
	if left > right {
		left, right = right, left
	}
	l := uint32(right - left)
	if right > (left/ExtentChunkSize+1)*ExtentChunkSize {
		l = ExtentChunkSize - uint32(left%ExtentChunkSize)
	}
	zero := marshalExtentSlice(uint32(left%ExtentChunkSize), 0, 0, 0, l)
	if _, err := tx.Exec(`INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE slices = CONCAT(slices, VALUES(slices))`, ino, uint32(left/ExtentChunkSize), zero); err != nil {
		return nil, 0, 0, 0, err
	}
	now := time.Now().UnixNano()
	mt, msec := jfsSplitTime(now)
	if _, err := tx.Exec(`UPDATE jfs_node SET length=?, mtime=?, ctime=?, mtimensec=?, ctimensec=? WHERE inode=?`,
		length, mt, mt, msec, msec, ino); err != nil {
		return nil, 0, 0, 0, err
	}
	_, _ = tx.Exec(`UPDATE inodes SET size_bytes = ?, mtime = ? WHERE inode_id IN (
		SELECT inode_id FROM file_nodes WHERE extent_ino = ?)`, length, time.Now().UTC(), ino)
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
	_, _ = tx.Exec(`UPDATE slice_compact_tasks SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(3)
		WHERE extent_ino = ? AND chunk = ? AND status IN ('PENDING','LEASED')`, ino, indx)
	return 0, olds, nil
}

func (s *Store) jfsEnqueueSliceGCTx(tx *sql.Tx, id uint64, size uint32, ino uint64) error {
	nblocks := int((size + ExtentBlockSize - 1) / ExtentBlockSize)
	if nblocks < 1 {
		nblocks = 1
	}
	for i := 0; i < nblocks; i++ {
		key := fmt.Sprintf("chunks/%d/%d/%d_%d_%d", id/1000/1000, id/1000, id, i, blockLen(size, i))
		taskID := strings.ToLower(ulid.Make().String())
		if _, err := tx.Exec(`INSERT IGNORE INTO block_gc_tasks (task_id, block_key, extent_ino, size_bytes, status, max_attempts)
			VALUES (?, ?, ?, ?, 'PENDING', 8)`, taskID, key, nullUint64(ino), blockLen(size, i)); err != nil {
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
		_, err := tx.Exec(`INSERT IGNORE INTO block_gc_tasks (task_id, block_key, extent_ino, size_bytes, status, max_attempts) VALUES `+
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
			ins = append(ins, "(?, ?, ?, ?, 'PENDING', 8)")
			insArgs = append(insArgs, taskID, key, nullUint64(ino), blockLen(o.size, i))
			if len(ins) >= batch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
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
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_sustained WHERE inode = ?`, ino).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
		return nil
	}
	attr, eno, err := s.jfsGetAttrTx(tx, ino)
	if err != nil || eno != 0 {
		return err
	}
	if attr != nil && attr.Nlink == 0 {
		return s.jfsEnqueueFileGCTx(tx, ino, attr.Length)
	}
	return nil
}

func jfsIsUnlock(ltype uint32) bool {
	return ltype == uint32(syscall.F_UNLCK) || ltype == 'U' || ltype == 2
}

func jfsIsWriteLock(ltype uint32) bool {
	return ltype == uint32(syscall.F_WRLCK) || ltype == 'W' || ltype == 3
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
	return uint32(syscall.F_UNLCK), start, end, 0, 0, nil
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
		return s.unlinkExtentPathTx(tx, path, opened)
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
	rows, err := s.db.QueryContext(ctx, `SELECT path FROM file_nodes WHERE `+
		s.scope.And(`is_directory = 0 AND content_layout = ? AND (path = ? OR path LIKE ?)`),
		s.scope.Args(string(ContentLayoutExtent), dirPath, prefix+"%")...)
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

// jfsEdgeOfTx returns the (parent, name) of the edge pointing at ino.
func (s *Store) jfsEdgeOfTx(tx *sql.Tx, ino uint64) (uint64, []byte, bool, error) {
	var parent uint64
	var name []byte
	err := tx.QueryRow(`SELECT parent, name FROM jfs_edge WHERE inode = ? LIMIT 1`, ino).Scan(&parent, &name)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	return parent, name, true, nil
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
		err := tx.QueryRow(`SELECT type, length, nlink FROM jfs_node WHERE inode = ?`, node).Scan(&typ, &length, &nlink)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// Edge without a node (already half-deleted): drop the edge below.
		case err != nil:
			return err
		case typ == jfsTypeFile && nlink > 0:
			if _, err := tx.Exec(`INSERT IGNORE INTO jfs_delfile (inode, length, expire) VALUES (?, ?, ?)`,
				node, length, now); err != nil {
				return err
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

// jfsRenameDirEdgeTx moves a directory's edge to newParent/newName. Children
// are keyed by parent inode, so one edge update moves the whole subtree.
func (s *Store) jfsRenameDirEdgeTx(tx *sql.Tx, ino, newParent uint64, newName string) error {
	if ino == 0 || ino == jfsRootIno || newName == "" {
		return nil
	}
	_, _, ok, err := s.jfsEdgeOfTx(tx, ino)
	if err != nil || !ok {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, newParent, []byte(newName)); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE jfs_edge SET parent = ?, name = ? WHERE inode = ?`, newParent, []byte(newName), ino)
	return err
}

func (s *Store) unlinkExtentPathTx(tx *sql.Tx, path string, opened bool) error {
	var ino sql.NullInt64
	var layout sql.NullString
	err := tx.QueryRow(`SELECT content_layout, extent_ino FROM file_nodes WHERE `+
		s.scope.And(`path_hash = ? AND path = ?`),
		s.scope.Args(fileNodePathHash(path), path)...).Scan(&layout, &ino)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if layout.String != string(ContentLayoutExtent) || !ino.Valid || ino.Int64 == 0 {
		return ErrNotFound
	}
	attr, eno, err := s.jfsGetAttrTx(tx, uint64(ino.Int64))
	if err != nil {
		return err
	}
	if eno != 0 || attr == nil {
		_, err = tx.Exec(`DELETE FROM file_nodes WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			s.scope.Args(fileNodePathHash(path), path)...)
		return err
	}
	name := pathutil.BaseName(path)
	_, eno, err = s.jfsUnlinkTx(context.Background(), tx, attr.Parent, name, path, 0, opened)
	if err != nil {
		return err
	}
	if eno == int(syscall.ENOENT) {
		return ErrNotFound
	}
	if eno != 0 {
		return fmt.Errorf("extent unlink: %w", syscall.Errno(eno))
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

func (s *Store) jfsClaimCompactTx(tx *sql.Tx) (ino uint64, chunk uint32, taskID string, err error) {
	err = tx.QueryRow(`SELECT task_id, extent_ino, chunk FROM slice_compact_tasks
		WHERE status = 'PENDING' OR (status = 'LEASED' AND lease_until < CURRENT_TIMESTAMP(3))
		ORDER BY created_at LIMIT 1 FOR UPDATE`).Scan(&taskID, &ino, &chunk)
	if err == nil {
		_, err = tx.Exec(`UPDATE slice_compact_tasks SET status = 'LEASED', leased_at = CURRENT_TIMESTAMP(3),
			lease_until = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 5 MINUTE) WHERE task_id = ?`, taskID)
		return ino, chunk, taskID, err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, "", err
	}
	// JuiceFS compact is not an INSERT on the write txn; doWrite returns
	// numSlices and baseMeta.Write launches compactChunk. Discover fat
	// chunks here as a backup. Skip runaway blobs (LENGTH ≫ maxSlices):
	// a 500MB leftover row made compact SELECT+UPDATE saturate TiDB and
	// stall sqlite --finish on other inodes. Write-path compact keeps
	// live chunks ≤ maxSlices (60KiB).
	minBytes := jfsCompactThreshold * extentSliceBytes
	// 4× JuiceFS maxSlices (240KiB). Live Write compact keeps chunks near
	// 60KiB; 500MiB leftovers from before that trigger are skipped so
	// compact SELECT+UPDATE cannot saturate TiDB.
	maxBytes := extentCompactSlices * extentSliceBytes * 4
	err = tx.QueryRow(`SELECT inode, indx FROM jfs_chunk WHERE LENGTH(slices) >= ? AND LENGTH(slices) <= ? LIMIT 1`, minBytes, maxBytes).
		Scan(&ino, &chunk)
	if err != nil {
		return 0, 0, "", err
	}
	taskID = strings.ToLower(ulid.Make().String())
	_, err = tx.Exec(`INSERT INTO slice_compact_tasks (task_id, extent_ino, chunk, status, max_attempts, leased_at, lease_until)
		VALUES (?, ?, ?, 'LEASED', 8, CURRENT_TIMESTAMP(3), DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 5 MINUTE))
		ON DUPLICATE KEY UPDATE status = 'LEASED', task_id = VALUES(task_id),
			leased_at = CURRENT_TIMESTAMP(3),
			lease_until = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 5 MINUTE)`,
		taskID, ino, chunk)
	return ino, chunk, taskID, err
}

func (s *Store) jfsRequeueCompactTx(tx *sql.Tx, taskID string) error {
	_, err := tx.Exec(`UPDATE slice_compact_tasks SET status = 'PENDING', leased_at = NULL, lease_until = NULL
		WHERE task_id = ? AND status = 'LEASED'`, taskID)
	return err
}

// ListPendingBlockGC returns block keys ready for deletion.
func (s *Store) ListPendingBlockGC(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT block_key FROM block_gc_tasks
		WHERE status = 'PENDING' ORDER BY created_at LIMIT ?`, limit)
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
