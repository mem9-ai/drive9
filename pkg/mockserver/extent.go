package mockserver

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ExtentAttr mirrors pkg/datastore.ExtentAttr field-for-field (JSON tags
// included) so the real extent client can decode mock replies.
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
}

// errno values used here (Linux syscall numbers).
const (
	enoOK        = 0
	enoINVAL     = 22
	enoEIO       = 5
	enoENOENT    = 2
	enoEEXIST    = 17
	enoENOTDIR   = 20
	enoEISDIR    = 21
	enoENOTEMPTY = 39
	enoENOSYS    = 38
)

// setattr set-bits understood by the mock (JuiceFS FATTR_* subset).
const (
	fattrMode   = 0x1
	fattrMtime  = 0x10
	fattrLength = 0x40
)

// handleExtentMeta implements POST /v1/extent/meta (spec §7.2): op selector
// via X-Drive9-Extent-Op or ?op=, JSON body ≤4MiB, HTTP 200 with the op's
// JSON ({"errno":N} + payload), HTTP 500 iff errno==EIO, ENOSYS for unknown
// ops. The op registry mirrors pkg/datastore/jfs_meta.go; ops marked TODO
// below still need their JuiceFS semantics ported before the real extent
// client can run against the mock.
func (s *Server) handleExtentMeta(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tdb := tenantFrom(r)
	op := strings.TrimSpace(r.Header.Get("X-Drive9-Extent-Op"))
	if op == "" {
		op = strings.TrimSpace(r.URL.Query().Get("op"))
	}
	if op == "" {
		errJSON(w, http.StatusBadRequest, "missing extent op")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body")
		return
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	body, httpStatus := tdb.runExtentOp(op, raw)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_, _ = w.Write(body)
}

func errnoBody(errno int, extra map[string]any) ([]byte, int) {
	m := map[string]any{"errno": errno}
	for k, v := range extra {
		m[k] = v
	}
	b, _ := json.Marshal(m)
	if errno == enoEIO {
		return b, http.StatusInternalServerError
	}
	return b, http.StatusOK
}

func jsonBody(v any) ([]byte, int) {
	b, _ := json.Marshal(v)
	return b, http.StatusOK
}

func (t *tenantDB) runExtentOp(op string, raw []byte) ([]byte, int) {
	switch op {
	// ---- counters / format / sessions ----
	case "get_counter":
		var in struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		var v int64
		err := t.db.QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, in.Name).Scan(&v)
		if err != nil {
			return errnoBody(enoEIO, nil)
		}
		return jsonBody(map[string]any{"errno": enoOK, "value": v})
	case "incr_counter":
		var in struct {
			Name  string `json:"name"`
			Value int64  `json:"value"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		if _, err := t.db.Exec(
			`INSERT INTO jfs_counter (name, value) VALUES (?, ?)
			 ON CONFLICT(name) DO UPDATE SET value = value + excluded.value`,
			in.Name, in.Value); err != nil {
			return errnoBody(enoEIO, nil)
		}
		var v int64
		_ = t.db.QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, in.Name).Scan(&v)
		return jsonBody(map[string]any{"errno": enoOK, "value": v})
	case "load":
		var format []byte
		_ = t.db.QueryRow(`SELECT value FROM jfs_counter WHERE name = 'format'`).Scan(&format)
		return jsonBody(map[string]any{"errno": enoOK, "format": json.RawMessage(orDefaultJSON(format))})
	case "init":
		var in struct {
			Format json.RawMessage `json:"format"`
			Force  bool            `json:"force"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		if _, err := t.db.Exec(
			`INSERT INTO jfs_counter (name, value) VALUES ('format', 0)
			 ON CONFLICT(name) DO UPDATE SET value = 0`); err != nil {
			return errnoBody(enoEIO, nil)
		}
		return jsonBody(map[string]any{"errno": enoOK})
	case "new_session":
		var in struct {
			Sid    uint64 `json:"sid"`
			Expire int64  `json:"expire"`
			Info   []byte `json:"info"`
			Update bool   `json:"update"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		if _, err := t.db.Exec(
			`INSERT INTO jfs_session (sid, created_at) VALUES (?, ?)
			 ON CONFLICT(sid) DO UPDATE SET created_at = excluded.created_at`,
			in.Sid, time.Now().Unix()); err != nil {
			return errnoBody(enoEIO, nil)
		}
		return jsonBody(map[string]any{"errno": enoOK, "directory_projection": true})
	case "refresh_session":
		var in struct {
			Sid    uint64 `json:"sid"`
			Expire int64  `json:"expire"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		if _, err := t.db.Exec(`UPDATE jfs_session SET created_at = ? WHERE sid = ?`,
			time.Now().Unix(), in.Sid); err != nil {
			return errnoBody(enoEIO, nil)
		}
		return jsonBody(map[string]any{"errno": enoOK})

	// ---- namespace ----
	case "lookup":
		var in struct {
			Parent uint64 `json:"parent"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		ino, attr, eno := t.jfsLookup(in.Parent, in.Name)
		return jsonBody(map[string]any{"errno": eno, "inode": ino, "attr": attr})
	case "getattr":
		var in struct {
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		attr, eno := t.jfsGetAttr(in.Inode)
		return jsonBody(map[string]any{"errno": eno, "inode": in.Inode, "attr": attr})
	case "setattr":
		var in struct {
			Inode uint64     `json:"inode"`
			Set   uint16     `json:"set"`
			Attr  ExtentAttr `json:"attr"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		attr, eno := t.jfsSetAttr(in.Inode, in.Set, in.Attr)
		return jsonBody(map[string]any{"errno": eno, "inode": in.Inode, "attr": attr, "compactChunks": []uint32{}})
	case "mknod":
		var in struct {
			Parent uint64     `json:"parent"`
			Name   string     `json:"name"`
			Type   uint8      `json:"type"`
			Mode   uint16     `json:"mode"`
			Inode  uint64     `json:"inode"`
			Attr   ExtentAttr `json:"attr"`
			Uid    uint32     `json:"uid"`
			Gid    uint32     `json:"gid"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		ino, attr, eno := t.jfsMknod(in.Parent, in.Name, in.Type, in.Mode, in.Inode, in.Uid, in.Gid)
		return jsonBody(map[string]any{"errno": eno, "inode": ino, "attr": attr})
	case "unlink":
		var in struct {
			Parent uint64 `json:"parent"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		eno := t.jfsUnlink(in.Parent, in.Name, false)
		return jsonBody(map[string]any{"errno": eno})
	case "rmdir":
		var in struct {
			Parent uint64 `json:"parent"`
			Name   string `json:"name"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		eno := t.jfsUnlink(in.Parent, in.Name, true)
		return jsonBody(map[string]any{"errno": eno})
	case "rename":
		var in struct {
			ParentSrc uint64 `json:"parentSrc"`
			NameSrc   string `json:"nameSrc"`
			ParentDst uint64 `json:"parentDst"`
			NameDst   string `json:"nameDst"`
			Inode     uint64 `json:"inode"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		eno := t.jfsRename(in.ParentSrc, in.NameSrc, in.ParentDst, in.NameDst)
		return jsonBody(map[string]any{"errno": eno})
	case "readdir":
		var in struct {
			Inode uint64 `json:"inode"`
			Plus  uint8  `json:"plus"`
			Limit int    `json:"limit"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		entries, eno := t.jfsReaddir(in.Inode)
		return jsonBody(map[string]any{"errno": eno, "entries": entries})

	// ---- content (simplified fidelity: slices are echoed, not interpreted
	// — full JuiceFS block semantics are the fidelity TODO below) ----
	case "read":
		var in struct {
			Inode uint64 `json:"inode"`
			Indx  uint32 `json:"indx"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		var slices json.RawMessage
		err := t.db.QueryRow(`SELECT slices FROM jfs_chunk WHERE inode = ? AND chunk = ?`,
			in.Inode, in.Indx).Scan(&slices)
		if err != nil {
			return jsonBody(map[string]any{"errno": enoOK, "slices": []any{}})
		}
		return jsonBody(map[string]any{"errno": enoOK, "slices": slices})
	case "write":
		var in struct {
			Inode uint64          `json:"inode"`
			Indx  uint32          `json:"indx"`
			Parts json.RawMessage `json:"parts"`
			Mtime time.Time       `json:"mtime"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		attr, eno := t.jfsWriteParts(in.Inode, in.Indx, in.Parts)
		return jsonBody(map[string]any{"errno": eno, "inode": in.Inode, "attr": attr})
	case "truncate":
		var in struct {
			Inode  uint64 `json:"inode"`
			Length uint64 `json:"length"`
			Mtime  int64  `json:"mtime"`
		}
		if err := json.Unmarshal(raw, &in); err != nil {
			return errnoBody(enoINVAL, nil)
		}
		attr, eno := t.jfsTruncate(in.Inode, in.Length)
		return jsonBody(map[string]any{"errno": eno, "inode": in.Inode, "attr": attr})

	default:
		// Remaining registry ops TODO (return the same shape the real
		// server's default case does): set_if_small, readlink, symlink via
		// mknod type, delete_slice, open_ref, delete_sustained, flock,
		// getlk, setlk, find_stale_sessions, clean_stale_session,
		// get_session, claim/requeue/complete_compact, compact.
		return jsonBody(map[string]any{"errno": enoENOSYS})
	}
}

func orDefaultJSON(b []byte) string {
	if len(b) == 0 {
		return "{}"
	}
	return string(b)
}

// ------------------------------------------------------------ jfs helpers

type jfsNodeRow struct {
	Inode uint64
	Type  uint8
	Size  uint64
	Mode  uint16
	Atime, Mtime, Ctime int64
}

func (n jfsNodeRow) attr() ExtentAttr {
	return ExtentAttr{
		Typ:    n.Type,
		Mode:   n.Mode,
		Nlink:  1,
		Length: n.Size,
		Atime:  n.Atime, Mtime: n.Mtime, Ctime: n.Ctime,
	}
}

func (t *tenantDB) jfsNode(ino uint64) (*jfsNodeRow, int) {
	var n jfsNodeRow
	var typ, mode int64
	err := t.db.QueryRow(
		`SELECT inode, type, size, mode, atime, mtime, ctime FROM jfs_node WHERE inode = ?`, ino).
		Scan(&n.Inode, &typ, &n.Size, &mode, &n.Atime, &n.Mtime, &n.Ctime)
	if err != nil {
		return nil, enoENOENT
	}
	n.Type, n.Mode = uint8(typ), uint16(mode)
	return &n, enoOK
}

func (t *tenantDB) jfsLookup(parent uint64, name string) (uint64, *ExtentAttr, int) {
	if name == "" {
		n, eno := t.jfsNode(parent)
		if eno != enoOK {
			return 0, nil, eno
		}
		a := n.attr()
		return n.Inode, &a, enoOK
	}
	var ino uint64
	err := t.db.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, name).Scan(&ino)
	if err != nil {
		return 0, nil, enoENOENT
	}
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return 0, nil, eno
	}
	a := n.attr()
	return ino, &a, enoOK
}

func (t *tenantDB) jfsGetAttr(ino uint64) (*ExtentAttr, int) {
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return nil, eno
	}
	a := n.attr()
	return &a, enoOK
}

func (t *tenantDB) jfsSetAttr(ino uint64, set uint16, attr ExtentAttr) (*ExtentAttr, int) {
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return nil, eno
	}
	now := time.Now().UnixNano()
	if set&fattrMode != 0 {
		_, _ = t.db.Exec(`UPDATE jfs_node SET mode = ? WHERE inode = ?`, attr.Mode, ino)
	}
	if set&fattrMtime != 0 {
		_, _ = t.db.Exec(`UPDATE jfs_node SET mtime = ? WHERE inode = ?`, attr.Mtime, ino)
	}
	if set&fattrLength != 0 {
		if n.Type != 2 {
			return nil, enoEISDIR
		}
		_, _ = t.db.Exec(`UPDATE jfs_node SET size = ? WHERE inode = ?`, attr.Length, ino)
		if attr.Length == 0 {
			_, _ = t.db.Exec(`DELETE FROM jfs_chunk WHERE inode = ?`, ino)
		}
	}
	_, _ = t.db.Exec(`UPDATE jfs_node SET ctime = ? WHERE inode = ?`, now, ino)
	out, _ := t.jfsGetAttr(ino)
	return out, enoOK
}

func (t *tenantDB) jfsMknod(parent uint64, name string, typ uint8, mode uint16, ino uint64, uid, gid uint32) (uint64, *ExtentAttr, int) {
	if _, eno := t.jfsNode(parent); eno != enoOK {
		return 0, nil, eno
	}
	pn, _ := t.jfsNode(parent)
	if pn != nil && pn.Type != 1 {
		return 0, nil, enoENOTDIR
	}
	var existing uint64
	err := t.db.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, name).Scan(&existing)
	if err == nil {
		return 0, nil, enoEEXIST
	}
	if ino == 0 {
		var next int64
		_ = t.db.QueryRow(`SELECT value FROM jfs_counter WHERE name = 'nextInode'`).Scan(&next)
		ino = uint64(next)
		_, _ = t.db.Exec(`UPDATE jfs_counter SET value = value + 1 WHERE name = 'nextInode'`)
	}
	now := time.Now().UnixNano()
	if _, err := t.db.Exec(
		`INSERT INTO jfs_node (inode, type, size, mode, atime, mtime, ctime, parent) VALUES (?, ?, 0, ?, ?, ?, ?, ?)`,
		ino, typ, mode, now, now, now, parent); err != nil {
		return 0, nil, enoEIO
	}
	if _, err := t.db.Exec(`INSERT INTO jfs_edge (parent, name, inode) VALUES (?, ?, ?)`, parent, name, ino); err != nil {
		return 0, nil, enoEIO
	}
	n, _ := t.jfsNode(ino)
	a := n.attr()
	a.Uid, a.Gid = uid, gid
	return ino, &a, enoOK
}

func (t *tenantDB) jfsUnlink(parent uint64, name string, isDir bool) int {
	var ino uint64
	err := t.db.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parent, name).Scan(&ino)
	if err != nil {
		return enoENOENT
	}
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return eno
	}
	if isDir && n.Type != 1 {
		return enoENOTDIR
	}
	if !isDir && n.Type == 1 {
		return enoEISDIR
	}
	if isDir {
		var children int
		_ = t.db.QueryRow(`SELECT COUNT(*) FROM jfs_edge WHERE parent = ?`, ino).Scan(&children)
		if children > 0 {
			return enoENOTEMPTY
		}
	}
	if _, err := t.db.Exec(`DELETE FROM jfs_edge WHERE parent = ? AND name = ?`, parent, name); err != nil {
		return enoEIO
	}
	_, _ = t.db.Exec(`DELETE FROM jfs_node WHERE inode = ?`, ino)
	_, _ = t.db.Exec(`DELETE FROM jfs_chunk WHERE inode = ?`, ino)
	return enoOK
}

func (t *tenantDB) jfsRename(parentSrc uint64, nameSrc string, parentDst uint64, nameDst string) int {
	var ino uint64
	err := t.db.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parentSrc, nameSrc).Scan(&ino)
	if err != nil {
		return enoENOENT
	}
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return eno
	}
	if parentSrc != parentDst {
		if _, eno := t.jfsNode(parentDst); eno != enoOK {
			return eno
		}
		dn, _ := t.jfsNode(parentDst)
		if dn != nil && dn.Type != 1 {
			return enoENOTDIR
		}
	}
	var exists uint64
	if err := t.db.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = ? AND name = ?`, parentDst, nameDst).Scan(&exists); err == nil {
		return enoEEXIST
	}
	now := time.Now().UnixNano()
	if _, err := t.db.Exec(`UPDATE jfs_edge SET parent = ?, name = ? WHERE parent = ? AND name = ?`,
		parentDst, nameDst, parentSrc, nameSrc); err != nil {
		return enoEIO
	}
	if n.Type == 1 && parentSrc != parentDst {
		_, _ = t.db.Exec(`UPDATE jfs_node SET parent = ?, ctime = ? WHERE inode = ?`, parentDst, now, ino)
	}
	return enoOK
}

func (t *tenantDB) jfsReaddir(ino uint64) ([]map[string]any, int) {
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return nil, eno
	}
	if n.Type != 1 {
		return nil, enoENOTDIR
	}
	rows, err := t.db.Query(`SELECT name, inode FROM jfs_edge WHERE parent = ? ORDER BY name`, ino)
	if err != nil {
		return nil, enoEIO
	}
	defer rows.Close()
	entries := []map[string]any{}
	for rows.Next() {
		var name string
		var child uint64
		if err := rows.Scan(&name, &child); err != nil {
			return nil, enoEIO
		}
		e := map[string]any{"inode": child, "name": name}
		if cn, en := t.jfsNode(child); en == enoOK {
			a := cn.attr()
			e["attr"] = &a
		}
		entries = append(entries, e)
	}
	return entries, enoOK
}

func (t *tenantDB) jfsWriteParts(ino uint64, indx uint32, parts json.RawMessage) (*ExtentAttr, int) {
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return nil, eno
	}
	if n.Type == 1 {
		return nil, enoEISDIR
	}
	if _, err := t.db.Exec(
		`INSERT INTO jfs_chunk (inode, chunk, slices) VALUES (?, ?, ?)
		 ON CONFLICT(inode, chunk) DO UPDATE SET slices = excluded.slices`,
		ino, indx, string(parts)); err != nil {
		return nil, enoEIO
	}
	now := time.Now().UnixNano()
	_, _ = t.db.Exec(`UPDATE jfs_node SET mtime = ?, ctime = ? WHERE inode = ?`, now, now, ino)
	out, _ := t.jfsGetAttr(ino)
	return out, enoOK
}

func (t *tenantDB) jfsTruncate(ino uint64, length uint64) (*ExtentAttr, int) {
	n, eno := t.jfsNode(ino)
	if eno != enoOK {
		return nil, eno
	}
	if n.Type == 1 {
		return nil, enoEISDIR
	}
	now := time.Now().UnixNano()
	if _, err := t.db.Exec(`UPDATE jfs_node SET size = ?, mtime = ?, ctime = ? WHERE inode = ?`,
		length, now, now, ino); err != nil {
		return nil, enoEIO
	}
	if length == 0 {
		_, _ = t.db.Exec(`DELETE FROM jfs_chunk WHERE inode = ?`, ino)
	}
	out, _ := t.jfsGetAttr(ino)
	return out, enoOK
}

var _ = fmt.Sprintf
