package mockserver

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// handleUploads implements the V1 multipart protocol (spec §6.1). The mock
// keeps parts in the tenantdb and assembles the file on complete; part URLs
// point back at this server (`/v1/uploads/{id}/part/{n}` — an extension the
// production protocol satisfies via presigned S3 URLs; the client PUTs bytes
// to whatever URL the plan carries, so the shape holds).
func (s *Server) handleUploads(w http.ResponseWriter, r *http.Request) {
	tdb := tenantFrom(r)
	rest := strings.TrimPrefix(r.URL.Path, "/v1/uploads")
	rest = strings.TrimPrefix(rest, "/")

	switch {
	case rest == "" || rest == "initiate":
		if r.Method != http.MethodPost {
			errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.uploadInitiate(w, r, tdb)
	case rest == "" && r.Method == http.MethodGet:
		s.uploadList(w, r, tdb)
	default:
		segs := strings.Split(rest, "/")
		if len(segs) < 2 {
			errJSON(w, http.StatusNotFound, "not found")
			return
		}
		id := segs[0]
		action := segs[1]
		// PUT /v1/uploads/{id}/part/{n}: upload one part's bytes
		if action == "part" && len(segs) == 3 && r.Method == http.MethodPut {
			s.uploadPutPart(w, r, tdb, id, segs[2])
			return
		}
		switch action {
		case "complete":
			s.uploadComplete(w, r, tdb, id)
		case "resume":
			s.uploadResume(w, r, tdb, id)
		case "abort":
			s.uploadAbort(w, r, tdb, id)
		default:
			errJSON(w, http.StatusNotFound, "not found")
		}
	}
}

func (s *Server) uploadInitiate(w http.ResponseWriter, r *http.Request, t *tenantDB) {
	var in struct {
		Path             string   `json:"path"`
		TotalSize        int64    `json:"total_size"`
		PartChecksums    []string `json:"part_checksums"`
		ExpectedRevision int64    `json:"expected_revision"`
		Description      string   `json:"description"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in); err != nil {
		errJSON(w, http.StatusBadRequest, "decode body")
		return
	}
	p, err := canonicalUploadPath(in.Path)
	if err != nil || in.TotalSize <= 0 {
		errJSON(w, http.StatusBadRequest, "invalid path or total_size")
		return
	}
	if in.TotalSize > s.cfg.MaxUploadBytes {
		errJSON(w, http.StatusRequestEntityTooLarge, "upload too large")
		return
	}
	if len(in.PartChecksums) == 0 {
		errJSON(w, http.StatusBadRequest, "part_checksums required")
		return
	}
	exp := in.ExpectedRevision
	if exp == 0 {
		exp = -1
	}
	id := hex.EncodeToString(randomBytes(12))
	partSize := int64(8 << 20)
	if in.TotalSize < partSize {
		partSize = in.TotalSize
	}
	totalParts := (in.TotalSize + partSize - 1) / partSize
	now := time.Now()
	if _, err := t.db.Exec(
		`INSERT INTO uploads (upload_id, path, total_size, part_size, expected_revision, status, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, 'active', ?, ?)`,
		id, p, in.TotalSize, partSize, exp, now.Unix(), now.Add(24*time.Hour).Unix()); err != nil {
		errJSON(w, http.StatusInternalServerError, "create upload")
		return
	}
	parts := make([]map[string]any, 0, totalParts)
	for i := int64(1); i <= totalParts; i++ {
		parts = append(parts, map[string]any{
			"number": i,
			"url":    fmt.Sprintf("%s/v1/uploads/%s/part/%d", s.publicBase(r), id, i),
			"size":   min64(partSize, in.TotalSize-(i-1)*partSize),
		})
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"upload_id": id,
		"key":       p,
		"part_size": partSize,
		"parts":     parts,
	})
}

func (s *Server) publicBase(r *http.Request) string {
	return "http://" + r.Host
}

func canonicalUploadPath(raw string) (string, error) {
	p := "/" + strings.TrimPrefix(raw, "/")
	return fsPath("/v1/fs"+p, strings.HasSuffix(p, "/"))
}

func (s *Server) uploadPutPart(w http.ResponseWriter, r *http.Request, t *tenantDB, id, numRaw string) {
	num, err := strconv.ParseInt(numRaw, 10, 64)
	if err != nil || num < 1 {
		errJSON(w, http.StatusBadRequest, "bad part number")
		return
	}
	var partSize int64
	var status string
	err = t.db.QueryRow(`SELECT part_size, status FROM uploads WHERE upload_id = ?`, id).Scan(&partSize, &status)
	if err != nil {
		errJSON(w, http.StatusNotFound, "upload not found")
		return
	}
	if status != "active" {
		errJSON(w, http.StatusGone, "upload expired")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, partSize+1))
	if err != nil || int64(len(body)) > partSize {
		errJSON(w, http.StatusBadRequest, "part size mismatch")
		return
	}
	if _, err := t.db.Exec(
		`INSERT INTO upload_parts (upload_id, number, size, data) VALUES (?, ?, ?, ?)
		 ON CONFLICT(upload_id, number) DO UPDATE SET size = excluded.size, data = excluded.data`,
		id, num, len(body), body); err != nil {
		errJSON(w, http.StatusInternalServerError, "store part")
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%d-%d"`, num, len(body)))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) uploadComplete(w http.ResponseWriter, r *http.Request, t *tenantDB, id string) {
	var in struct {
		Tags          map[string]string `json:"tags"`
		ChecksumSHA256 string           `json:"checksum_sha256"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&in)
	var path string
	var total, partSize, expected int64
	var status string
	err := t.db.QueryRow(
		`SELECT path, total_size, part_size, expected_revision, status FROM uploads WHERE upload_id = ?`, id).
		Scan(&path, &total, &partSize, &expected, &status)
	if err != nil {
		errJSON(w, http.StatusNotFound, "upload not found")
		return
	}
	if status != "active" {
		errJSON(w, http.StatusGone, "upload expired")
		return
	}
	rows, err := t.db.Query(`SELECT data FROM upload_parts WHERE upload_id = ? ORDER BY number`, id)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "read parts")
		return
	}
	defer rows.Close()
	var assembled []byte
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			errJSON(w, http.StatusInternalServerError, "scan part")
			return
		}
		assembled = append(assembled, data...)
	}
	if int64(len(assembled)) != total {
		errJSON(w, http.StatusBadRequest, fmt.Sprintf("assembled %d of %d bytes", len(assembled), total))
		return
	}
	rev, err := t.putFile(path, assembled, expected)
	if err == ErrPathConflict {
		errJSONCode(w, http.StatusConflict, "revision_conflict", "path conflict")
		return
	}
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "commit upload")
		return
	}
	if _, err := t.db.Exec(`UPDATE uploads SET status = 'completed' WHERE upload_id = ?`, id); err != nil {
		errJSON(w, http.StatusInternalServerError, "finalize upload")
		return
	}
	_, _ = t.db.Exec(`DELETE FROM upload_parts WHERE upload_id = ?`, id)
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "revision": rev})
}

func (s *Server) uploadResume(w http.ResponseWriter, r *http.Request, t *tenantDB, id string) {
	var path string
	var total, partSize, expected int64
	var status string
	var expires int64
	err := t.db.QueryRow(
		`SELECT path, total_size, part_size, expected_revision, status, expires_at FROM uploads WHERE upload_id = ?`, id).
		Scan(&path, &total, &partSize, &expected, &status, &expires)
	if err != nil {
		errJSON(w, http.StatusNotFound, "upload not found")
		return
	}
	if status != "active" || time.Now().Unix() > expires {
		errJSON(w, http.StatusGone, "upload expired")
		return
	}
	totalParts := (total + partSize - 1) / partSize
	parts := make([]map[string]any, 0, totalParts)
	for i := int64(1); i <= totalParts; i++ {
		var sz int64
		sq := t.db.QueryRow(`SELECT size FROM upload_parts WHERE upload_id = ? AND number = ?`, id, i).Scan(&sz)
		if sq != nil {
			sz = 0
		}
		parts = append(parts, map[string]any{
			"number": i,
			"url":    fmt.Sprintf("%s/v1/uploads/%s/part/%d", s.publicBase(r), id, i),
			"size":   min64(partSize, total-(i-1)*partSize),
			"have":   sz,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"upload_id": id,
		"key":       path,
		"part_size": partSize,
		"parts":     parts,
	})
}

func (s *Server) uploadAbort(w http.ResponseWriter, r *http.Request, t *tenantDB, id string) {
	res, err := t.db.Exec(`UPDATE uploads SET status = 'aborted' WHERE upload_id = ? AND status = 'active'`, id)
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "abort upload")
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		errJSON(w, http.StatusNotFound, "upload not found")
		return
	}
	_, _ = t.db.Exec(`DELETE FROM upload_parts WHERE upload_id = ?`, id)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) uploadList(w http.ResponseWriter, r *http.Request, t *tenantDB) {
	writeJSON(w, http.StatusOK, map[string]any{"uploads": []any{}})
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
