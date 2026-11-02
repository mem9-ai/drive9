package mockserver

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

type ctxKey struct{}

func withTenant(r *http.Request, t *tenantDB) context.Context {
	return context.WithValue(r.Context(), ctxKey{}, t)
}

func tenantFrom(r *http.Request) *tenantDB {
	t, _ := r.Context().Value(ctxKey{}).(*tenantDB)
	return t
}

// handleBusiness dispatches /v1/fs[...]: batch selectors on the exact paths,
// the fs domain under /v1/fs/.
func (s *Server) handleBusiness(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/fs:batch-stat", "/v1/fs:batch-read-small":
		// the mount daemon probes these during startup; empty SUCCESS for a
		// fresh tenant is a valid answer and keeps the daemon from wedging
		// on a 501 (a Tier 1 fidelity TODO fills in real batching)
		writeJSON(w, http.StatusOK, map[string]any{"results": []any{}})
		return
	case "/v1/fs:batch-write", "/v1/fs:batch-upload":
		errJSONCode(w, http.StatusNotImplemented, "batch_not_implemented",
			"batch endpoints are mock Tier 1")
		return
	}
	if !s.authMaybe(w, r) {
		return
	}
	s.handleFS(w, r)
}

// authMaybe authenticates and injects the tenant; reports failure via w.
func (s *Server) authMaybe(w http.ResponseWriter, r *http.Request) bool {
	tdb, ok := s.authenticate(w, r)
	if !ok {
		return false
	}
	*r = *r.WithContext(withTenant(r, tdb))
	return true
}

// fsPath canonicalizes the URL path into a drive9 path (dirs keep the
// trailing slash, files never do).
func fsPath(raw string, isDirHint bool) (string, error) {
	p := "/" + strings.TrimPrefix(raw, "/v1/fs/")
	if p == "/v1/fs" {
		p = "/"
	}
	if isDirHint || strings.HasSuffix(raw, "/") {
		return pathutil.CanonicalizeDir(p)
	}
	return pathutil.Canonicalize(p)
}

type nodeRow struct {
	Path        string
	Kind        string
	Size        int64
	Revision    int64
	Mode        int64
	MtimeNS     int64
	BtimeNS     int64
	Tags        string
	Description string
	Target      *string
}

func (n nodeRow) isDir() bool { return n.Kind == "dir" }

func (t *tenantDB) getNode(p string) (*nodeRow, error) {
	row := t.db.QueryRow(
		`SELECT path, kind, size, revision, mode, mtime_ns, btime_ns, tags, description, target
		 FROM file_nodes WHERE path = ?`, p)
	var n nodeRow
	err := row.Scan(&n.Path, &n.Kind, &n.Size, &n.Revision, &n.Mode, &n.MtimeNS, &n.BtimeNS, &n.Tags, &n.Description, &n.Target)
	if err != nil {
		return nil, ErrNotFound
	}
	return &n, nil
}

func (t *tenantDB) readFile(p string) ([]byte, *nodeRow, error) {
	n, err := t.getNode(p)
	if err != nil {
		return nil, nil, err
	}
	if n.isDir() {
		return nil, nil, ErrNotFound
	}
	var data []byte
	err = t.db.QueryRow(`SELECT data FROM file_nodes WHERE path = ?`, p).Scan(&data)
	if err != nil {
		return nil, nil, err
	}
	return data, n, nil
}

func (t *tenantDB) ensureParent(p string) error {
	parent := pathutil.ParentPath(p)
	if parent == p {
		return nil
	}
	if _, err := t.getNode(parent); err != nil {
		return ErrNotFound
	}
	return nil
}

// ensureParentDirs mirrors the production backend's EnsureParentDirs: every
// missing ancestor of p is created (mkdir -p semantics — the CLI and the
// mount rely on it for first writes into fresh trees).
func (t *tenantDB) ensureParentDirs(p string) error {
	parent := pathutil.ParentPath(p)
	if parent == p {
		return nil
	}
	if err := t.ensureParentDirs(parent); err != nil {
		return err
	}
	if _, err := t.getNode(parent); err == ErrNotFound {
		now := time.Now().UnixNano()
		if _, err2 := t.db.Exec(
			`INSERT INTO file_nodes (path, kind, size, revision, mode, mtime_ns, btime_ns)
			 VALUES (?, 'dir', 0, 1, 493, ?, ?)`, parent, now, now); err2 != nil {
			return err2
		}
	}
	return nil
}

func (t *tenantDB) putFile(p string, data []byte, expectedRevision int64) (int64, error) {
	if err := t.ensureParentDirs(p); err != nil {
		return 0, err
	}
	now := time.Now().UnixNano()
	existing, err := t.getNode(p)
	switch {
	case err == nil:
		if expectedRevision >= 0 && existing.Revision != expectedRevision {
			return 0, ErrPathConflict
		}
		if existing.isDir() {
			return 0, ErrPathConflict
		}
		rev := existing.Revision + 1
		_, err := t.db.Exec(`UPDATE file_nodes SET size=?, revision=?, data=?, mtime_ns=? WHERE path=?`,
			len(data), rev, data, now, p)
		return rev, err
	case err == ErrNotFound:
		if expectedRevision >= 0 && expectedRevision != 0 {
			return 0, ErrNotFound
		}
		rev := int64(1)
		_, err := t.db.Exec(
			`INSERT INTO file_nodes (path, kind, size, revision, data, mode, mtime_ns, btime_ns)
			 VALUES (?, 'file', ?, ?, ?, 420, ?, ?)`,
			p, len(data), rev, data, now, now)
		if err != nil {
			return 0, err
		}
		return rev, nil
	default:
		return 0, err
	}
}

func (t *tenantDB) mkdir(p string, mode int64) error {
	if err := t.ensureParentDirs(p); err != nil {
		return err
	}
	if _, err := t.getNode(p); err == nil {
		return ErrPathConflict
	}
	now := time.Now().UnixNano()
	_, err := t.db.Exec(
		`INSERT INTO file_nodes (path, kind, size, revision, mode, mtime_ns, btime_ns)
		 VALUES (?, 'dir', 0, 1, ?, ?, ?)`, p, mode, now, now)
	return err
}

func (t *tenantDB) listDir(p string) ([]nodeRow, error) {
	prefix := p // canonical dir path ends with '/'
	rows, err := t.db.Query(
		`SELECT path, kind, size, revision, mode, mtime_ns, btime_ns, tags, description, target
		 FROM file_nodes WHERE path LIKE ? ESCAPE '\'`, likePrefix(prefix))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []nodeRow
	for rows.Next() {
		var n nodeRow
		if err := rows.Scan(&n.Path, &n.Kind, &n.Size, &n.Revision, &n.Mode, &n.MtimeNS, &n.BtimeNS, &n.Tags, &n.Description, &n.Target); err != nil {
			return nil, err
		}
		rest := strings.TrimPrefix(n.Path, prefix)
		if rest == "" || strings.Contains(strings.TrimSuffix(rest, "/"), "/") {
			continue // only direct children
		}
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, rows.Err()
}

func likePrefix(p string) string {
	return strings.ReplaceAll(p, `\`, `\\`) + "%"
}

func (t *tenantDB) deleteTree(p string) error {
	if strings.HasSuffix(p, "/") {
		_, err := t.db.Exec(`DELETE FROM file_nodes WHERE path = ? OR path LIKE ? ESCAPE '\'`, p, likePrefix(p))
		return err
	}
	_, err := t.db.Exec(`DELETE FROM file_nodes WHERE path = ?`, p)
	return err
}

// ---------------------------------------------------------------- handler

func (s *Server) handleFS(w http.ResponseWriter, r *http.Request) {
	tdb := tenantFrom(r)
	rawPath := r.URL.Path
	query := r.URL.Query()

	if r.Method == http.MethodGet && query.Has("stat") {
		s.fsStat(w, r, tdb, rawPath)
		return
	}
	if r.Method == http.MethodGet && query.Has("list") {
		s.fsList(w, r, tdb, rawPath)
		return
	}
	if query.Get("grep") != "" || query.Has("find") || query.Has("tasks") {
		// Tier 1 selectors: return the documented empty shapes.
		switch {
		case query.Has("tasks"):
			writeJSON(w, http.StatusOK, map[string]any{"tasks": []any{}})
		case query.Has("find"):
			writeJSON(w, http.StatusOK, map[string]any{"results": []any{}})
		default:
			writeJSON(w, http.StatusOK, map[string]any{"matches": []any{}})
		}
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.fsRead(w, r, tdb, rawPath)
	case http.MethodHead:
		s.fsHead(w, r, tdb, rawPath)
	case http.MethodPut:
		s.fsPut(w, r, tdb, rawPath)
	case http.MethodDelete:
		s.fsDelete(w, r, tdb, rawPath)
	case http.MethodPost:
		s.fsPost(w, r, tdb, rawPath, query)
	case http.MethodPatch:
		errJSONCode(w, http.StatusNotImplemented, "patch_not_implemented",
			"PATCH plans are mock Tier 1")
	default:
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

func (s *Server) fsRead(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string) {
	p, err := fsPath(raw, false)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid path")
		return
	}
	data, n, err := t.readFile(p)
	if err != nil {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	total := int64(len(data))
	if rng := r.Header.Get("Range"); rng != "" {
		start, end, ok := parseContentRange(rng, total)
		if !ok {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", total))
			errJSONCode(w, http.StatusRequestedRangeNotSatisfiable, "invalid_range", "range not satisfiable")
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Dat9-Revision", fmt.Sprint(n.Revision))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(data[start : end+1])
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf(`"%d-%d"`, n.Revision, total))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("X-Dat9-Resource-ID", p)
	w.Header().Set("X-Dat9-Revision", fmt.Sprint(n.Revision))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) fsHead(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string) {
	p, err := fsPath(raw, strings.HasSuffix(raw, "/"))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid path")
		return
	}
	n, err := t.getNode(p)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("X-Dat9-IsDir", fmt.Sprint(n.isDir()))
	w.Header().Set("X-Dat9-Size", fmt.Sprint(n.Size))
	w.Header().Set("X-Dat9-Revision", fmt.Sprint(n.Revision))
	w.Header().Set("X-Dat9-Mtime", fmt.Sprint(n.MtimeNS))
	w.Header().Set("X-Dat9-Btime", fmt.Sprint(n.BtimeNS))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
}

func (s *Server) fsStat(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string) {
	p, err := fsPath(raw, strings.HasSuffix(raw, "/"))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid path")
		return
	}
	n, err := t.getNode(p)
	if err != nil {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	w.Header().Set("X-Dat9-Mtime", fmt.Sprint(n.MtimeNS))
	w.Header().Set("X-Dat9-Btime", fmt.Sprint(n.BtimeNS))
	writeJSON(w, http.StatusOK, map[string]any{
		"size":        n.Size,
		"isdir":       n.isDir(),
		"resource_id": n.Path,
		"nlink":       1,
		"revision":    n.Revision,
		"mtime":       n.MtimeNS,
		"btime":       n.BtimeNS,
		"tags":        parseTags(n.Tags),
		"description": n.Description,
	})
}

func parseTags(s string) map[string]string {
	out := map[string]string{}
	if s == "" {
		return out
	}
	for _, kv := range strings.Split(s, ",") {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	return out
}

func (s *Server) fsList(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string) {
	p, err := fsPath(raw, true)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid path")
		return
	}
	n, err := t.getNode(p)
	if err != nil {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	if !n.isDir() {
		errJSON(w, http.StatusBadRequest, "not a directory")
		return
	}
	children, err := t.listDir(p)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "list failed")
		return
	}
	entries := make([]map[string]any, 0, len(children))
	for _, c := range children {
		entries = append(entries, map[string]any{
			"name":        pathutil.BaseName(c.Path),
			"size":        c.Size,
			"isDir":       c.isDir(),
			"mtime":       c.MtimeNS,
			"revision":    c.Revision,
			"mode":        c.Mode,
			"hasMode":     true,
			"resource_id": c.Path,
			"nlink":       1,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries})
}

func (s *Server) fsPut(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string) {
	p, err := fsPath(raw, false)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid path")
		return
	}
	if r.ContentLength > s.cfg.MaxUploadBytes {
		errJSON(w, http.StatusRequestEntityTooLarge, "payload too large")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, s.cfg.MaxUploadBytes+1))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body")
		return
	}
	expected := int64(-1)
	if v := r.Header.Get("X-Dat9-Expected-Revision"); v != "" {
		if _, err := fmt.Sscan(v, &expected); err != nil {
			expected = -1
		}
	}
	rev, err := t.putFile(p, body, expected)
	if err == ErrPathConflict {
		errJSONCode(w, http.StatusConflict, "revision_conflict", "path conflict")
		return
	}
	if err == ErrNotFound {
		errJSON(w, http.StatusNotFound, "parent not found")
		return
	}
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "write failed")
		return
	}
	if desc := r.Header.Get("X-Dat9-Description"); desc != "" {
		_, _ = t.db.Exec(`UPDATE file_nodes SET description = ? WHERE path = ?`, desc, p)
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revision": rev})
}

func (s *Server) fsDelete(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string) {
	p, err := fsPath(raw, strings.HasSuffix(raw, "/"))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid path")
		return
	}
	n, err := t.getNode(p)
	if err != nil {
		errJSON(w, http.StatusNotFound, "not found")
		return
	}
	recursive := r.URL.Query().Get("recursive") == "1"
	if n.isDir() && !recursive {
		children, err := t.listDir(p)
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "list failed")
			return
		}
		if len(children) > 0 {
			errJSONCode(w, http.StatusConflict, "directory_not_empty", "directory not empty")
			return
		}
	}
	if err := t.deleteTree(p); err != nil {
		errJSON(w, http.StatusInternalServerError, "delete failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) fsPost(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string, q map[string][]string) {
	get := func(name string) string {
		if v, ok := q[name]; ok && len(v) > 0 {
			return v[0]
		}
		return ""
	}
	// The real client sends bare selectors (`?rename`, `?mkdir`); the spec
	// documents the `=1` form. Presence of the key means the selector is on.
	has := func(name string) bool {
		_, ok := q[name]
		return ok
	}
	// headerOrQuery resolves the source path header with optional base64url
	// encoding (spec §1 / §4.9).
	headerOrQuery := func(name string) string {
		v := r.Header.Get(name)
		if v == "" {
			return ""
		}
		if r.Header.Get("X-Dat9-Path-Encoding") == "base64url" {
			if dec, err := base64.RawURLEncoding.DecodeString(v); err == nil {
				return string(dec)
			}
		}
		return v
	}
	switch {
	case has("mkdir"):
		p, err := fsPath(raw, true)
		if err != nil {
			errJSON(w, http.StatusBadRequest, "invalid path")
			return
		}
		mode := int64(0o755)
		if v := get("mode"); v != "" {
			_, _ = fmt.Sscan(v, &mode)
		}
		if err := t.mkdir(p, mode); err == ErrPathConflict {
			errJSONCode(w, http.StatusConflict, "path_conflict", "path conflict")
			return
		} else if err == ErrNotFound {
			errJSON(w, http.StatusNotFound, "parent not found")
			return
		} else if err != nil {
			errJSON(w, http.StatusInternalServerError, "mkdir failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case has("rename"):
		dst, err := fsPath(raw, false)
		if err != nil {
			errJSON(w, http.StatusBadRequest, "invalid path")
			return
		}
		src := headerOrQuery("X-Dat9-Rename-Source")
		if src == "" {
			errJSON(w, http.StatusBadRequest, "missing X-Dat9-Rename-Source")
			return
		}
		rev, err := t.rename(src, dst)
		if err == ErrNotFound {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		if err == ErrPathConflict {
			errJSONCode(w, http.StatusConflict, "path_conflict", "path conflict")
			return
		}
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "rename failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revision": rev})
	case has("copy"):
		dst, err := fsPath(raw, false)
		if err != nil {
			errJSON(w, http.StatusBadRequest, "invalid path")
			return
		}
		src := headerOrQuery("X-Dat9-Copy-Source")
		if src == "" {
			errJSON(w, http.StatusBadRequest, "missing X-Dat9-Copy-Source")
			return
		}
		rev, err := t.copy(src, dst)
		if err == ErrNotFound {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		if err != nil {
			errJSON(w, http.StatusInternalServerError, "copy failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revision": rev})
	case has("append-log"):
		s.fsAppendLog(w, r, t, raw)
	case has("append"), has("setmeta"), has("chmod"),
		has("create"), has("symlink"), has("hardlink"):
		tier1 := []string{}
		for _, sel := range []string{"append", "setmeta", "chmod", "create", "symlink", "hardlink"} {
			if has(sel) {
				tier1 = append(tier1, sel)
			}
		}
		errJSONCode(w, http.StatusNotImplemented, "selector_not_implemented",
			"fs POST selector is mock Tier 1: "+strings.Join(tier1, ","))
	default:
		errJSON(w, http.StatusBadRequest, "unknown selector")
	}
}

func (s *Server) fsAppendLog(w http.ResponseWriter, r *http.Request, t *tenantDB, raw string) {
	p, err := fsPath(raw, false)
	if err != nil {
		errJSON(w, http.StatusBadRequest, "invalid path")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, s.cfg.MaxUploadBytes+1))
	if err != nil {
		errJSONCode(w, http.StatusBadRequest, "append_log_invalid_request", "read body")
		return
	}
	expectedSize := r.Header.Get("X-Dat9-Expected-Size")
	expectedRev := r.Header.Get("X-Dat9-Expected-Revision")
	if expectedSize == "" || expectedRev == "" {
		errJSONCode(w, http.StatusBadRequest, "append_log_invalid_request",
			"append-log requires X-Dat9-Expected-Revision and X-Dat9-Expected-Size")
		return
	}
	var wantSize int64
	if _, err := fmt.Sscan(expectedSize, &wantSize); err != nil {
		errJSONCode(w, http.StatusBadRequest, "append_log_invalid_request", "bad X-Dat9-Expected-Size")
		return
	}
	cur, n, err := t.readFile(p)
	var base []byte
	if err == nil {
		var wantRev int64
		_, _ = fmt.Sscan(expectedRev, &wantRev)
		if wantRev >= 0 && n.Revision != wantRev {
			errJSONCode(w, http.StatusConflict, "append_log_conflict", "revision conflict")
			return
		}
		if int64(len(cur)) != wantSize {
			errJSONCode(w, http.StatusConflict, "append_log_conflict",
				fmt.Sprintf("expected size %d, have %d", wantSize, len(cur)))
			return
		}
		base = cur
	}
	rev, err := t.putFile(p, append(base, body...), -1)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "append failed")
		return
	}
	w.Header().Set("X-Dat9-Revision", fmt.Sprint(rev))
	writeJSON(w, http.StatusOK, map[string]any{"revision": rev, "size_bytes": wantSize + int64(len(body))})
}

func (t *tenantDB) rename(src, dst string) (int64, error) {
	sn, err := t.getNode(src)
	if err != nil {
		return 0, ErrNotFound
	}
	if _, err := t.getNode(dst); err == nil {
		return 0, ErrPathConflict
	}
	if err := t.ensureParentDirs(dst); err != nil {
		return 0, err
	}
	now := time.Now().UnixNano()
	if sn.isDir() {
		rows, err := t.db.Query(`SELECT path FROM file_nodes WHERE path LIKE ? ESCAPE '\'`, likePrefix(src))
		if err != nil {
			return 0, err
		}
		defer rows.Close()
		type move struct{ from, to string }
		var moves []move
		for rows.Next() {
			var p string
			if err := rows.Scan(&p); err != nil {
				return 0, err
			}
			moves = append(moves, move{p, dst + strings.TrimPrefix(p, src)})
		}
		if err := rows.Err(); err != nil {
			return 0, err
		}
		for _, m := range moves {
			if _, err := t.db.Exec(`UPDATE file_nodes SET path = ?, mtime_ns = ? WHERE path = ?`, m.to, now, m.from); err != nil {
				return 0, err
			}
		}
		return sn.Revision, nil
	}
	rev := sn.Revision + 1
	if _, err := t.db.Exec(`UPDATE file_nodes SET path = ?, revision = ?, mtime_ns = ? WHERE path = ?`, dst, rev, now, src); err != nil {
		return 0, err
	}
	return rev, nil
}

func (t *tenantDB) copy(src, dst string) (int64, error) {
	data, _, err := t.readFile(src)
	if err != nil {
		return 0, ErrNotFound
	}
	return t.putFile(dst, append([]byte(nil), data...), -1)
}
