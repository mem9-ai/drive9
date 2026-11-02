package mockserver

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	_ "modernc.org/sqlite"
)

// handleSQL implements POST /v1/sql (spec §8): the body's single query runs
// READ-ONLY against the tenant's own SQLite file, so black-box backend
// checks can inspect mock state directly (jfs_* tables included). Read-only
// is enforced by opening a separate ro handle; writes fail at prepare time.
func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		errJSON(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	tdb := tenantFrom(r)
	var in struct {
		Query string `json:"query"`
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		errJSON(w, http.StatusBadRequest, "read body")
		return
	}
	if err := json.Unmarshal(body, &in); err != nil || strings.TrimSpace(in.Query) == "" {
		errJSON(w, http.StatusBadRequest, "body must be {\"query\": ...}")
		return
	}
	db, err := tdb.readOnlyHandle()
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "open tenant db")
		return
	}
	defer db.Close()
	rows, err := db.Query(in.Query)
	if err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		errJSON(w, http.StatusInternalServerError, "columns")
		return
	}
	out := []map[string]any{}
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			errJSON(w, http.StatusInternalServerError, "scan")
			return
		}
		row := map[string]any{}
		for i, c := range cols {
			row[c] = sqliteValue(vals[i])
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		errJSON(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func sqliteValue(v any) any {
	switch b := v.(type) {
	case []byte:
		return string(b)
	default:
		return v
	}
}
