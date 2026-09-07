package datastore

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/oklog/ulid/v2"
)

const (
	jfsRootIno   = uint64(1)
	jfsTypeFile  = uint8(1)
	jfsTypeDir   = uint8(2)
	jfsCompactN  = 350
)

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
	Path         string
	ContentLayout ContentLayout
	ExtentIno    uint64
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

// RunExtentMetaOp executes one JuiceFS-engine RPC inside a tenant transaction
// and dual-writes the file_nodes projection for namespace ops.
func (s *Store) RunExtentMetaOp(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
	var (
		out    any
		errno  int
		opErr  error
	)
	err := s.InTx(ctx, func(tx *sql.Tx) error {
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
	if err != nil && errno == 0 {
		return nil, int(syscall.EIO), err
	}
	body, mErr := json.Marshal(out)
	if mErr != nil {
		return nil, int(syscall.EIO), mErr
	}
	return body, errno, nil
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
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		ino, attr, eno, err := s.jfsMknodTx(ctx, tx, in.Parent, in.Name, in.Type, in.Mode, in.Cumask, in.Inode, in.Attr, in.ProjPath, in.Uid, in.Gid)
		if err != nil {
			return nil, int(syscall.EIO), err
		}
		return map[string]any{"errno": eno, "inode": ino, "attr": attr}, eno, nil
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
		ino, tinode, attr, tattr, eno, err := s.jfsRenameTx(tx, in.SrcParent, in.SrcName, in.DstParent, in.DstName, in.SrcPath, in.DstPath)
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
			Mtime time.Time   `json:"mtime"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, int(syscall.EINVAL), err
		}
		nslice, attr, dlen, dspace, eno, err := s.jfsWriteTx(tx, in.Inode, in.Indx, in.Off, in.Slice, in.Mtime)
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
		eno, err := s.jfsCompactTx(tx, in.Inode, in.Indx, in.Origin, in.Skipped, in.Pos, in.Id, in.Size)
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
		return map[string]any{"errno": 0, "ltype": uint32(syscall.F_UNLCK), "start": 0, "end": 0, "pid": 0}, 0, nil
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
		return map[string]any{"errno": 0, "sids": []uint64{}}, 0, nil
	case "clean_stale_session":
		return map[string]any{"errno": 0}, 0, nil
	case "get_session":
		return map[string]any{"errno": 0}, 0, nil
	default:
		return map[string]any{"errno": int(syscall.ENOSYS)}, int(syscall.ENOSYS), nil
	}
}

func (s *Store) jfsEnsureInitTx(tx *sql.Tx) error {
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, jfsRootIno).Scan(&n); err != nil {
		return err
	}
	if n > 0 {
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
	return nil
}

func jfsSplitTime(ns int64) (int64, int16) {
	return ns / 1e3, int16(ns % 1e3)
}

func jfsAttrFromNode(typ uint8, mode uint16, uid, gid uint32, atime, mtime, ctime int64, asec, msec, csec int16, nlink uint32, length uint64, parent uint64) ExtentAttr {
	return ExtentAttr{
		Typ:       typ,
		Mode:      mode,
		Uid:       uid,
		Gid:       gid,
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
	err := tx.QueryRow(`SELECT type, mode, uid, gid, atime, mtime, ctime, atimensec, mtimensec, ctimensec, nlink, length, parent
		FROM jfs_node WHERE inode = ?`, ino).Scan(
		&typ, &mode, &uid, &gid, &atime, &mtime, &ctime, &asec, &msec, &csec, &nlink, &length, &parent)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, int(syscall.ENOENT), nil
	}
	if err != nil {
		return nil, 0, err
	}
	attr := jfsAttrFromNode(typ, mode, uid, gid, atime, mtime, ctime, asec, msec, csec, nlink, length, parent)
	return &attr, 0, nil
}

func (s *Store) jfsSetAttrTx(tx *sql.Tx, ino uint64, set uint16, patch ExtentAttr) (*ExtentAttr, int, error) {
	attr, eno, err := s.jfsGetAttrTx(tx, ino)
	if err != nil || eno != 0 {
		return attr, eno, err
	}
	const (
		setMode = 1 << 0
		setUID  = 1 << 1
		setGID  = 1 << 2
		setSize = 1 << 3
		setAtime = 1 << 4
		setMtime = 1 << 5
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
	now := time.Now().UnixNano()
	atime, asec := jfsSplitTime(attr.Atime*1e9 + int64(attr.Atimensec))
	mtime, msec := jfsSplitTime(attr.Mtime*1e9 + int64(attr.Mtimensec))
	ctime, csec := jfsSplitTime(now)
	if _, err := tx.Exec(`UPDATE jfs_node SET mode=?, uid=?, gid=?, length=?, atime=?, mtime=?, ctime=?, atimensec=?, mtimensec=?, ctimensec=? WHERE inode=?`,
		attr.Mode, attr.Uid, attr.Gid, attr.Length, atime, mtime, ctime, asec, msec, csec, ino); err != nil {
		return nil, 0, err
	}
	return s.jfsGetAttrTx(tx, ino)
}

func (s *Store) jfsMknodTx(ctx context.Context, tx *sql.Tx, parent uint64, name string, typ uint8, mode, cumask uint16, inode uint64, attr ExtentAttr, projPath string, uid, gid uint32) (uint64, *ExtentAttr, int, error) {
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
	if typ == jfsTypeDir {
		nlink = 2
		length = 4096
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
		VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)`,
		inode, typ, mode, attr.Uid, attr.Gid, atime, atime, atime, asec, asec, asec, nlink, length, parent); err != nil {
		return 0, nil, 0, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		parent, []byte(name), inode, typ); err != nil {
		return 0, nil, 0, err
	}
	if typ == jfsTypeFile && projPath != "" {
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
	out := jfsAttrFromNode(typ, mode, attr.Uid, attr.Gid, atime, atime, atime, asec, asec, asec, nlink, length, parent)
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

func (s *Store) jfsUnlinkTx(ctx context.Context, tx *sql.Tx, parent uint64, name, projPath string, sid uint64, opened bool) (*ExtentAttr, int, error) {
	var ino uint64
	var typ uint8
	err := tx.QueryRow(`SELECT inode, type FROM jfs_edge WHERE parent = ? AND name = ?`, parent, []byte(name)).Scan(&ino, &typ)
	if errors.Is(err, sql.ErrNoRows) {
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
	if _, err := tx.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino); err != nil {
		return nil, 0, err
	}
	return attr, 0, nil
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

func (s *Store) jfsRenameTx(tx *sql.Tx, srcParent uint64, srcName string, dstParent uint64, dstName, srcPath, dstPath string) (uint64, uint64, *ExtentAttr, *ExtentAttr, int, error) {
	ino, attr, eno, err := s.jfsLookupTx(tx, srcParent, srcName)
	if err != nil || eno != 0 {
		return 0, 0, nil, nil, eno, err
	}
	var exist uint64
	err = tx.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, dstParent, []byte(dstName)).Scan(&exist)
	if err == nil {
		return 0, exist, attr, nil, int(syscall.EEXIST), nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil, nil, 0, err
	}
	if _, err := tx.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, srcParent, []byte(srcName)); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_edge (parent, name, inode, type) VALUES (?, ?, ?, ?)`,
		dstParent, []byte(dstName), ino, attr.Typ); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if _, err := tx.Exec(`UPDATE jfs_node SET parent = ? WHERE inode = ?`, dstParent, ino); err != nil {
		return 0, 0, nil, nil, 0, err
	}
	if srcPath != "" && dstPath != "" {
		parent := pathutil.ParentPath(dstPath)
		name := pathutil.BaseName(dstPath)
		if _, err := tx.Exec(`UPDATE file_nodes SET path = ?, path_hash = ?, parent_path = ?, parent_path_hash = ?, name = ?
			WHERE `+s.scope.And(`path_hash = ? AND path = ?`),
			append([]any{dstPath, fileNodePathHash(dstPath), parent, fileNodePathHash(parent), name},
				s.scope.Args(fileNodePathHash(srcPath), srcPath)...)...); err != nil {
			return 0, 0, nil, nil, 0, err
		}
	}
	return ino, 0, attr, nil, 0, nil
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

func (s *Store) jfsWriteTx(tx *sql.Tx, ino uint64, indx, off uint32, sl ExtentSlice, mtime time.Time) (int, *ExtentAttr, int64, int64, int, error) {
	attr, eno, err := s.jfsGetAttrTx(tx, ino)
	if err != nil || eno != 0 {
		return 0, attr, 0, 0, eno, err
	}
	if attr.Typ != jfsTypeFile {
		return 0, attr, 0, 0, int(syscall.EPERM), nil
	}
	newlen := uint64(indx)*ExtentChunkSize + uint64(off) + uint64(sl.Len)
	var dlen, dspace int64
	if newlen > attr.Length {
		dlen = int64(newlen - attr.Length)
		dspace = dlen
		attr.Length = newlen
	}
	buf := marshalExtentSlice(off, sl.Id, sl.Size, sl.Off, sl.Len)
	if _, err := tx.Exec(`INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE slices = CONCAT(slices, VALUES(slices))`, ino, indx, buf); err != nil {
		return 0, nil, 0, 0, 0, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_chunk_ref (chunkid, size, refs) VALUES (?, ?, 1)`, sl.Id, sl.Size); err != nil {
		return 0, nil, 0, 0, 0, err
	}
	now := time.Now().UnixNano()
	mt, msec := jfsSplitTime(mtime.UnixNano())
	ct, csec := jfsSplitTime(now)
	if _, err := tx.Exec(`UPDATE jfs_node SET length=?, mtime=?, ctime=?, mtimensec=?, ctimensec=? WHERE inode=?`,
		attr.Length, mt, ct, msec, csec, ino); err != nil {
		return 0, nil, 0, 0, 0, err
	}
	if _, err := tx.Exec(`UPDATE inodes SET size_bytes = ?, mtime = ? WHERE inode_id IN (
		SELECT inode_id FROM file_nodes WHERE extent_ino = ?)`, attr.Length, mtime.UTC(), ino); err != nil {
		// projection size is best-effort if the subquery is empty
		_ = err
	}
	var slices []byte
	_ = tx.QueryRow(`SELECT slices FROM jfs_chunk WHERE inode = ? AND indx = ?`, ino, indx).Scan(&slices)
	nslice := len(slices) / extentSliceBytes
	if nslice >= jfsCompactN {
		taskID := strings.ToLower(ulid.Make().String())
		_, _ = tx.Exec(`INSERT IGNORE INTO slice_compact_tasks (task_id, extent_ino, chunk, status, max_attempts)
			VALUES (?, ?, ?, 'PENDING', 8)`, taskID, ino, indx)
	}
	out, _, _ := s.jfsGetAttrTx(tx, ino)
	return nslice, out, dlen, dspace, 0, nil
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

func (s *Store) jfsCompactTx(tx *sql.Tx, ino uint64, indx uint32, origin []byte, skipped int, pos uint32, id uint64, size uint32) (int, error) {
	var cur []byte
	err := tx.QueryRow(`SELECT slices FROM jfs_chunk WHERE inode = ? AND indx = ? FOR UPDATE`, ino, indx).Scan(&cur)
	if errors.Is(err, sql.ErrNoRows) {
		return int(syscall.EINVAL), nil
	}
	if err != nil {
		return 0, err
	}
	if len(cur) < len(origin) || string(cur[:len(origin)]) != string(origin) {
		return int(syscall.EINVAL), nil
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
		return 0, err
	}
	if _, err := tx.Exec(`INSERT INTO jfs_chunk_ref (chunkid, size, refs) VALUES (?, ?, 1)`, id, size); err != nil {
		return 0, err
	}
	for i := skipBytes; i+extentSliceBytes <= len(origin); i += extentSliceBytes {
		oldID := binary.BigEndian.Uint64(origin[i+4 : i+12])
		oldSize := binary.BigEndian.Uint32(origin[i+12 : i+16])
		if oldID == 0 {
			continue
		}
		if _, err := tx.Exec(`UPDATE jfs_chunk_ref SET refs = refs - 1 WHERE chunkid = ? AND size = ?`, oldID, oldSize); err != nil {
			return 0, err
		}
		var refs int
		if err := tx.QueryRow(`SELECT refs FROM jfs_chunk_ref WHERE chunkid = ?`, oldID).Scan(&refs); err == nil && refs <= 0 {
			_ = s.jfsEnqueueSliceGCTx(tx, oldID, oldSize, ino)
		}
	}
	_, _ = tx.Exec(`UPDATE slice_compact_tasks SET status = 'COMPLETED', completed_at = CURRENT_TIMESTAMP(3)
		WHERE extent_ino = ? AND chunk = ? AND status IN ('PENDING','LEASED')`, ino, indx)
	return 0, nil
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
	rows, err := tx.Query(`SELECT slices FROM jfs_chunk WHERE inode = ?`, ino)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var buf []byte
		if err := rows.Scan(&buf); err != nil {
			return err
		}
		for i := 0; i+extentSliceBytes <= len(buf); i += extentSliceBytes {
			id := binary.BigEndian.Uint64(buf[i+4 : i+12])
			sz := binary.BigEndian.Uint32(buf[i+12 : i+16])
			if id == 0 {
				continue
			}
			if _, err := tx.Exec(`UPDATE jfs_chunk_ref SET refs = refs - 1 WHERE chunkid = ?`, id); err != nil {
				return err
			}
			var refs int
			if err := tx.QueryRow(`SELECT refs FROM jfs_chunk_ref WHERE chunkid = ?`, id).Scan(&refs); err == nil && refs <= 0 {
				if err := s.jfsEnqueueSliceGCTx(tx, id, sz, ino); err != nil {
					return err
				}
			}
		}
	}
	if _, err := tx.Exec(`DELETE FROM jfs_chunk WHERE inode = ?`, ino); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT IGNORE INTO jfs_delfile (inode, length, expire) VALUES (?, ?, ?)`, ino, length, time.Now().Unix())
	return err
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

func (s *Store) jfsFlockTx(tx *sql.Tx, ino, sid, owner uint64, ltype uint32) (int, error) {
	const (
		fUnlck = 1
		fWrlck = 3
	)
	if ltype == uint32(syscall.F_UNLCK) || ltype == fUnlck {
		_, err := tx.Exec(`DELETE FROM jfs_flock WHERE inode = ? AND sid = ? AND owner = ?`, ino, sid, int64(owner))
		return 0, err
	}
	var n int
	wantWrite := ltype == uint32(syscall.F_WRLCK) || ltype == fWrlck
	q := `SELECT COUNT(*) FROM jfs_flock WHERE inode = ? AND NOT (sid = ? AND owner = ?)`
	if !wantWrite {
		q = `SELECT COUNT(*) FROM jfs_flock WHERE inode = ? AND NOT (sid = ? AND owner = ?) AND ltype = 87`
	}
	if err := tx.QueryRow(q, ino, sid, int64(owner)).Scan(&n); err != nil {
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

func (s *Store) jfsSetlkTx(tx *sql.Tx, ino, sid, owner uint64, ltype uint32, start, end uint64, pid uint32) (int, error) {
	// POSIX record locks: store as a single blob per (inode,sid,owner).
	if ltype == uint32(syscall.F_UNLCK) {
		_, err := tx.Exec(`DELETE FROM jfs_plock WHERE inode = ? AND sid = ? AND owner = ?`, ino, sid, int64(owner))
		return 0, err
	}
	rec := fmt.Sprintf("%d:%d:%d:%d", ltype, start, end, pid)
	_, err := tx.Exec(`INSERT INTO jfs_plock (inode, sid, owner, records) VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE records = VALUES(records)`, ino, sid, int64(owner), []byte(rec))
	return 0, err
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

func (s *Store) ClaimCompactTask(ctx context.Context) (ino uint64, chunk uint32, taskID string, err error) {
	err = s.InTx(ctx, func(tx *sql.Tx) error {
		err := tx.QueryRow(`SELECT task_id, extent_ino, chunk FROM slice_compact_tasks
			WHERE status = 'PENDING' ORDER BY created_at LIMIT 1 FOR UPDATE`).Scan(&taskID, &ino, &chunk)
		if errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE slice_compact_tasks SET status = 'LEASED', leased_at = CURRENT_TIMESTAMP(3),
			lease_until = DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 5 MINUTE) WHERE task_id = ?`, taskID)
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, "", nil
	}
	return ino, chunk, taskID, err
}
