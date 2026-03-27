package datastore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

type SearchResult struct {
	Path      string   `json:"path"`
	Name      string   `json:"name"`
	SizeBytes int64    `json:"size_bytes"`
	Score     *float64 `json:"score,omitempty"`
}

type FindFilter struct {
	PathPrefix string
	NameGlob   string
	TagKey     string
	TagValue   string
	After      *time.Time
	Before     *time.Time
	MinSize    int64
	MaxSize    int64
	Limit      int
}

const rrfK = 60.0

func (s *Store) Grep(ctx context.Context, query, pathPrefix string, limit int) ([]SearchResult, error) {
	if query == "" {
		return nil, fmt.Errorf("empty query")
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	fetch := limit * 3

	vecCh := make(chan []SearchResult, 1)
	ftsCh := make(chan []SearchResult, 1)

	go func() { vecCh <- s.vectorSearch(ctx, query, pathPrefix, fetch) }()
	go func() { ftsCh <- s.ftsSearch(ctx, query, pathPrefix, fetch) }()

	vec := <-vecCh
	fts := <-ftsCh

	if len(vec) == 0 && len(fts) == 0 {
		return s.keywordSearch(ctx, query, pathPrefix, limit)
	}
	return rrfMerge(fts, vec, limit), nil
}

func rrfMerge(fts, vec []SearchResult, limit int) []SearchResult {
	scores := make(map[string]float64)
	for rank, r := range fts {
		scores[r.Path] += 1.0 / (rrfK + float64(rank+1))
	}
	for rank, r := range vec {
		scores[r.Path] += 1.0 / (rrfK + float64(rank+1))
	}

	all := make(map[string]SearchResult)
	for _, r := range fts {
		all[r.Path] = r
	}
	for _, r := range vec {
		if _, ok := all[r.Path]; !ok {
			all[r.Path] = r
		}
	}

	merged := make([]SearchResult, 0, len(all))
	for _, r := range all {
		sc := scores[r.Path]
		r.Score = &sc
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool {
		return *merged[i].Score > *merged[j].Score
	})
	if len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}

func (s *Store) vectorSearch(ctx context.Context, query, pathPrefix string, limit int) []SearchResult {
	conds := []string{"f.status = 'CONFIRMED'", "f.embedding IS NOT NULL"}
	args := []any{query}

	if pathPrefix != "" && pathPrefix != "/" {
		conds = append(conds, "fn.path LIKE ?")
		args = append(args, pathPrefix+"%")
	}
	args = append(args, query, limit)

	q := `SELECT fn.path, fn.name, f.size_bytes,
		VEC_EMBED_COSINE_DISTANCE(f.embedding, ?) AS distance
		FROM file_nodes fn JOIN files f ON fn.file_id = f.file_id
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY VEC_EMBED_COSINE_DISTANCE(f.embedding, ?)
		LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []SearchResult
	for rows.Next() {
		var r SearchResult
		var dist float64
		if rows.Scan(&r.Path, &r.Name, &r.SizeBytes, &dist) != nil {
			break
		}
		sc := 1.0 - dist
		if sc < 0.3 {
			continue
		}
		r.Score = &sc
		out = append(out, r)
	}
	return out
}

func ftsSafe(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	s = strings.ReplaceAll(s, `;`, "")
	s = strings.ReplaceAll(s, `--`, "")
	s = strings.ReplaceAll(s, `/*`, "")
	s = strings.ReplaceAll(s, `*/`, "")
	s = strings.ReplaceAll(s, "\x00", "")
	return s
}

func (s *Store) ftsSearch(ctx context.Context, query, pathPrefix string, limit int) []SearchResult {
	safe := ftsSafe(query)
	ftsExpr := "fts_match_word('" + safe + "', f.content_text)"

	conds := []string{"f.status = 'CONFIRMED'", ftsExpr}
	var args []any

	if pathPrefix != "" && pathPrefix != "/" {
		conds = append(conds, "fn.path LIKE ?")
		args = append(args, pathPrefix+"%")
	}
	args = append(args, limit)

	q := `SELECT fn.path, fn.name, f.size_bytes, ` + ftsExpr + ` AS score
		FROM file_nodes fn JOIN files f ON fn.file_id = f.file_id
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY ` + ftsExpr + ` DESC
		LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil
	}
	defer rows.Close()

	var out []SearchResult
	for rows.Next() {
		var r SearchResult
		var sc float64
		if rows.Scan(&r.Path, &r.Name, &r.SizeBytes, &sc) != nil {
			break
		}
		r.Score = &sc
		out = append(out, r)
	}
	return out
}

func (s *Store) keywordSearch(ctx context.Context, query, pathPrefix string, limit int) ([]SearchResult, error) {
	conds := []string{"f.status = 'CONFIRMED'", "f.content_text LIKE CONCAT('%', ?, '%')"}
	args := []any{query}

	if pathPrefix != "" && pathPrefix != "/" {
		conds = append(conds, "fn.path LIKE ?")
		args = append(args, pathPrefix+"%")
	}
	args = append(args, limit)

	q := `SELECT fn.path, fn.name, f.size_bytes
		FROM file_nodes fn JOIN files f ON fn.file_id = f.file_id
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY f.confirmed_at DESC LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SearchResult
	for rows.Next() {
		var r SearchResult
		if err := rows.Scan(&r.Path, &r.Name, &r.SizeBytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Find(ctx context.Context, f *FindFilter) ([]SearchResult, error) {
	if f.Limit <= 0 || f.Limit > 1000 {
		f.Limit = 100
	}

	conds := []string{"f.status = 'CONFIRMED'", "fn.is_directory = 0"}
	var args []any

	if f.PathPrefix != "" && f.PathPrefix != "/" {
		conds = append(conds, "fn.path LIKE ?")
		args = append(args, f.PathPrefix+"%")
	}
	if f.NameGlob != "" {
		pattern := strings.ReplaceAll(strings.ReplaceAll(f.NameGlob, "*", "%"), "?", "_")
		conds = append(conds, "fn.name LIKE ?")
		args = append(args, pattern)
	}
	if f.TagKey != "" {
		if f.TagValue != "" {
			conds = append(conds, `EXISTS (SELECT 1 FROM file_tags t WHERE t.file_id = f.file_id AND t.tag_key = ? AND t.tag_value = ?)`)
			args = append(args, f.TagKey, f.TagValue)
		} else {
			conds = append(conds, `EXISTS (SELECT 1 FROM file_tags t WHERE t.file_id = f.file_id AND t.tag_key = ?)`)
			args = append(args, f.TagKey)
		}
	}
	if f.After != nil {
		conds = append(conds, "f.confirmed_at > ?")
		args = append(args, f.After.UTC())
	}
	if f.Before != nil {
		conds = append(conds, "f.confirmed_at < ?")
		args = append(args, f.Before.UTC())
	}
	if f.MinSize > 0 {
		conds = append(conds, "f.size_bytes >= ?")
		args = append(args, f.MinSize)
	}
	if f.MaxSize > 0 {
		conds = append(conds, "f.size_bytes <= ?")
		args = append(args, f.MaxSize)
	}
	args = append(args, f.Limit)

	q := `SELECT fn.path, fn.name, f.size_bytes
		FROM file_nodes fn JOIN files f ON fn.file_id = f.file_id
		WHERE ` + strings.Join(conds, " AND ") + `
		ORDER BY fn.path LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SearchResult
	for rows.Next() {
		var r SearchResult
		if err := rows.Scan(&r.Path, &r.Name, &r.SizeBytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
