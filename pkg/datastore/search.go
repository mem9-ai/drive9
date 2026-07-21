package datastore

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/embedding"
)

const vectorScoreThreshold = 0.3

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

// RRFMerge merges ranked FTS and vector results with reciprocal rank fusion.
func RRFMerge(fts, vec []SearchResult, limit int) []SearchResult {
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

func subtreeCond(prefix string) (string, []any) {
	if prefix != "/" {
		prefix = strings.TrimRight(prefix, "/")
	}
	return "(fn.path = ? OR fn.path LIKE ?)", []any{prefix, prefix + "/%"}
}

// scopeWhereAnd prefixes a WHERE predicate with one fs_id predicate per JOIN
// alias in shared shape and returns it unchanged in standalone shape. Every
// joined alias is filtered, not just the driving table: in shared shape
// entity ids are unique only within a tenant, so an id-only join condition
// would fan out across tenants and leak rows sideways on collision.
func scopeWhereAnd(scope Scope, pred string, aliases ...string) string {
	for i := len(aliases) - 1; i >= 0; i-- {
		pred = scope.AndAs(aliases[i], pred)
	}
	return pred
}

// scopeWhereArgs prepends one fs_id bind argument per alias, matching the
// predicate order produced by scopeWhereAnd.
func scopeWhereArgs(scope Scope, aliases int, args ...any) []any {
	if !scope.Shared() {
		return args
	}
	out := make([]any, 0, aliases+len(args))
	for i := 0; i < aliases; i++ {
		out = append(out, scope.FsID())
	}
	return append(out, args...)
}

// VectorSearch runs a vector similarity search for the supplied embedding.
func (s *Store) VectorSearch(ctx context.Context, queryEmbedding []float32, pathPrefix string, limit int) ([]SearchResult, error) {
	q, args, ok := buildVectorSearchQueryScoped(s.scope, queryEmbedding, pathPrefix, limit)
	if !ok {
		return nil, nil
	}
	return s.runVectorSearch(ctx, q, args)
}

// VectorSearchByText runs a TiDB-side text-query vector similarity search.
func (s *Store) VectorSearchByText(ctx context.Context, queryText, pathPrefix string, limit int) ([]SearchResult, error) {
	q, args, ok := buildVectorSearchByTextQueryScoped(s.scope, queryText, pathPrefix, limit)
	if !ok {
		return nil, nil
	}
	return s.runVectorSearch(ctx, q, args)
}

func (s *Store) runVectorSearch(ctx context.Context, q string, args []any) ([]SearchResult, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []SearchResult
	for rows.Next() {
		var r SearchResult
		var dist float64
		if err := rows.Scan(&r.Path, &r.Name, &r.SizeBytes, &dist); err != nil {
			return nil, err
		}
		sc := 1.0 - dist
		if sc < vectorScoreThreshold {
			continue
		}
		r.Score = &sc
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func buildVectorSearchQuery(queryEmbedding []float32, pathPrefix string, limit int) (string, []any, bool) {
	return buildVectorSearchQueryScoped(StandaloneScope(0), queryEmbedding, pathPrefix, limit)
}

// buildVectorSearchQueryScoped builds the vector search query for the given
// schema-shape scope. In shared shape every joined alias carries an fs_id
// predicate (see scopeWhereAnd).
func buildVectorSearchQueryScoped(scope Scope, queryEmbedding []float32, pathPrefix string, limit int) (string, []any, bool) {
	if len(queryEmbedding) == 0 {
		return "", nil, false
	}
	conds := []string{"i.status = 'CONFIRMED'", "s.embedding IS NOT NULL", "s.embedding_revision = i.revision"}
	vecParam := embedding.FormatVector(queryEmbedding)
	args := []any{vecParam}

	var condArgs []any
	if pathPrefix != "" && pathPrefix != "/" {
		cond, pargs := subtreeCond(pathPrefix)
		conds = append(conds, cond)
		condArgs = append(condArgs, pargs...)
	}
	// The fs_id predicates lead the WHERE clause in shared shape, so their
	// bind arguments go right after the SELECT-list distance placeholder.
	args = append(args, scopeWhereArgs(scope, 3, condArgs...)...)
	args = append(args, vecParam, limit)

	q := `SELECT fn.path, fn.name, i.size_bytes,
		VEC_EMBED_COSINE_DISTANCE(s.embedding, ?) AS distance
		FROM file_nodes fn JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id JOIN semantic s ON i.inode_id = s.inode_id
		WHERE ` + scopeWhereAnd(scope, strings.Join(conds, " AND "), "fn", "i", "s") + `
		ORDER BY VEC_EMBED_COSINE_DISTANCE(s.embedding, ?)
	LIMIT ?`
	return q, args, true
}

func buildVectorSearchByTextQuery(queryText, pathPrefix string, limit int) (string, []any, bool) {
	return buildVectorSearchByTextQueryScoped(StandaloneScope(0), queryText, pathPrefix, limit)
}

func buildVectorSearchByTextQueryScoped(scope Scope, queryText, pathPrefix string, limit int) (string, []any, bool) {
	if strings.TrimSpace(queryText) == "" {
		return "", nil, false
	}
	conds := []string{"i.status = 'CONFIRMED'", "s.embedding IS NOT NULL"}
	args := []any{queryText}

	var condArgs []any
	if pathPrefix != "" && pathPrefix != "/" {
		cond, pargs := subtreeCond(pathPrefix)
		conds = append(conds, cond)
		condArgs = append(condArgs, pargs...)
	}
	args = append(args, scopeWhereArgs(scope, 3, condArgs...)...)
	args = append(args, limit)

	q := `SELECT fn.path, fn.name, i.size_bytes,
		VEC_EMBED_COSINE_DISTANCE(s.embedding, ?) AS distance
		FROM file_nodes fn JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id JOIN semantic s ON i.inode_id = s.inode_id
		WHERE ` + scopeWhereAnd(scope, strings.Join(conds, " AND "), "fn", "i", "s") + `
		ORDER BY distance
		LIMIT ?`
	return q, args, true
}

// VectorSearchDescription runs a vector similarity search over files.description_embedding.
func (s *Store) VectorSearchDescription(ctx context.Context, queryEmbedding []float32, pathPrefix string, limit int) ([]SearchResult, error) {
	q, args, ok := buildVectorSearchDescriptionQueryScoped(s.scope, queryEmbedding, pathPrefix, limit)
	if !ok {
		return nil, nil
	}
	return s.runVectorSearch(ctx, q, args)
}

// VectorSearchDescriptionByText runs a TiDB-side text-query vector similarity search
// over files.description_embedding.
func (s *Store) VectorSearchDescriptionByText(ctx context.Context, queryText, pathPrefix string, limit int) ([]SearchResult, error) {
	q, args, ok := buildVectorSearchDescriptionByTextQueryScoped(s.scope, queryText, pathPrefix, limit)
	if !ok {
		return nil, nil
	}
	return s.runVectorSearch(ctx, q, args)
}

func buildVectorSearchDescriptionQueryScoped(scope Scope, queryEmbedding []float32, pathPrefix string, limit int) (string, []any, bool) {
	if len(queryEmbedding) == 0 {
		return "", nil, false
	}
	conds := []string{"i.status = 'CONFIRMED'", "s.description_embedding IS NOT NULL", "s.description_embedding_revision = i.revision"}
	vecParam := embedding.FormatVector(queryEmbedding)
	args := []any{vecParam}

	var condArgs []any
	if pathPrefix != "" && pathPrefix != "/" {
		cond, pargs := subtreeCond(pathPrefix)
		conds = append(conds, cond)
		condArgs = append(condArgs, pargs...)
	}
	args = append(args, scopeWhereArgs(scope, 3, condArgs...)...)
	args = append(args, vecParam, limit)

	q := `SELECT fn.path, fn.name, i.size_bytes,
		VEC_EMBED_COSINE_DISTANCE(s.description_embedding, ?) AS distance
		FROM file_nodes fn JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id JOIN semantic s ON i.inode_id = s.inode_id
		WHERE ` + scopeWhereAnd(scope, strings.Join(conds, " AND "), "fn", "i", "s") + `
		ORDER BY VEC_EMBED_COSINE_DISTANCE(s.description_embedding, ?)
	LIMIT ?`
	return q, args, true
}

func buildVectorSearchDescriptionByTextQueryScoped(scope Scope, queryText, pathPrefix string, limit int) (string, []any, bool) {
	if strings.TrimSpace(queryText) == "" {
		return "", nil, false
	}
	// Auto-embedding mode uses a generated column for description_embedding,
	// so the vector is always current and no revision gate is needed.
	conds := []string{"i.status = 'CONFIRMED'", "s.description_embedding IS NOT NULL"}
	args := []any{queryText}

	var condArgs []any
	if pathPrefix != "" && pathPrefix != "/" {
		cond, pargs := subtreeCond(pathPrefix)
		conds = append(conds, cond)
		condArgs = append(condArgs, pargs...)
	}
	args = append(args, scopeWhereArgs(scope, 3, condArgs...)...)
	args = append(args, limit)

	q := `SELECT fn.path, fn.name, i.size_bytes,
		VEC_EMBED_COSINE_DISTANCE(s.description_embedding, ?) AS distance
		FROM file_nodes fn JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id JOIN semantic s ON i.inode_id = s.inode_id
		WHERE ` + scopeWhereAnd(scope, strings.Join(conds, " AND "), "fn", "i", "s") + `
		ORDER BY distance
		LIMIT ?`
	return q, args, true
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

// FTSSearch runs a full-text search over files.content_text and files.description.
func (s *Store) FTSSearch(ctx context.Context, query, pathPrefix string, limit int) ([]SearchResult, error) {
	q, allArgs := buildFTSSearchQuery(s.scope, ftsSafe(query), pathPrefix, limit)
	rows, err := s.db.QueryContext(ctx, q, allArgs...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []SearchResult
	for rows.Next() {
		var r SearchResult
		var sc float64
		if err := rows.Scan(&r.Path, &r.Name, &r.SizeBytes, &sc); err != nil {
			return nil, err
		}
		r.Score = &sc
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// buildFTSSearchQuery assembles the full-text search query for the given
// schema-shape scope. safeQuery must already be escaped with ftsSafe.
func buildFTSSearchQuery(scope Scope, safeQuery, pathPrefix string, limit int) (string, []any) {
	contentExpr := "fts_match_word('" + safeQuery + "', content_text)"
	descExpr := "fts_match_word('" + safeQuery + "', description)"

	// In shared shape the fs_id predicate must live inside each UNION branch
	// (before the GROUP BY/LIMIT aggregation), otherwise other tenants' rows
	// compete for the shared LIMIT and dilute per-tenant recall.
	innerQ := `SELECT inode_id, MAX(score) AS score FROM (
		SELECT inode_id, ` + contentExpr + ` AS score
		FROM semantic WHERE ` + scope.And(contentExpr) + `
		UNION ALL
		SELECT inode_id, ` + descExpr + ` AS score
		FROM semantic WHERE ` + scope.And(descExpr) + `
	) fts GROUP BY inode_id ORDER BY score DESC LIMIT ?`

	args := append(scope.Args(), scope.Args()...)
	args = append(args, limit)

	var outerConds []string
	var outerArgs []any
	if pathPrefix != "" && pathPrefix != "/" {
		cond, pargs := subtreeCond(pathPrefix)
		outerConds = append(outerConds, cond)
		outerArgs = append(outerArgs, pargs...)
	}

	outerConds = append([]string{"i.status = 'CONFIRMED'"}, outerConds...)
	q := `SELECT fn.path, fn.name, i.size_bytes, fts.score
		FROM (` + innerQ + `) fts
		JOIN file_nodes fn ON COALESCE(fn.inode_id, fn.file_id) = fts.inode_id
		JOIN inodes i ON i.inode_id = fts.inode_id`
	if len(outerConds) > 0 {
		q += ` WHERE ` + scopeWhereAnd(scope, strings.Join(outerConds, " AND "), "fn", "i")
		outerArgs = scopeWhereArgs(scope, 2, outerArgs...)
	}

	return q, append(args, outerArgs...)
}

// KeywordSearch runs a LIKE-based fallback search when semantic ranking is unavailable.
func (s *Store) KeywordSearch(ctx context.Context, query, pathPrefix string, limit int) ([]SearchResult, error) {
	conds := []string{"i.status = 'CONFIRMED'", "s.content_text LIKE CONCAT('%', ?, '%')"}
	args := []any{query}

	if pathPrefix != "" && pathPrefix != "/" {
		cond, pargs := subtreeCond(pathPrefix)
		conds = append(conds, cond)
		args = append(args, pargs...)
	}
	args = append(args, limit)

	q := `SELECT fn.path, fn.name, i.size_bytes
		FROM file_nodes fn JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id JOIN semantic s ON i.inode_id = s.inode_id
		WHERE ` + scopeWhereAnd(s.scope, strings.Join(conds, " AND "), "fn", "i", "s") + `
		ORDER BY i.confirmed_at DESC LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, scopeWhereArgs(s.scope, 3, args...)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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

	conds := []string{"i.status = 'CONFIRMED'", "fn.is_directory = 0"}
	var args []any

	if f.PathPrefix != "" && f.PathPrefix != "/" {
		cond, pargs := subtreeCond(f.PathPrefix)
		conds = append(conds, cond)
		args = append(args, pargs...)
	}
	if f.NameGlob != "" {
		pattern := strings.ReplaceAll(strings.ReplaceAll(f.NameGlob, "*", "%"), "?", "_")
		conds = append(conds, "fn.name LIKE ?")
		args = append(args, pattern)
	}
	if f.TagKey != "" {
		if f.TagValue != "" {
			conds = append(conds, `EXISTS (SELECT 1 FROM file_tags t WHERE `+s.scope.AndAs("t", `t.file_id = i.inode_id AND t.tag_key = ? AND t.tag_value = ?`)+`)`)
			args = append(args, s.scope.Args(f.TagKey, f.TagValue)...)
		} else {
			conds = append(conds, `EXISTS (SELECT 1 FROM file_tags t WHERE `+s.scope.AndAs("t", `t.file_id = i.inode_id AND t.tag_key = ?`)+`)`)
			args = append(args, s.scope.Args(f.TagKey)...)
		}
	}
	if f.After != nil {
		conds = append(conds, "i.confirmed_at > ?")
		args = append(args, f.After.UTC())
	}
	if f.Before != nil {
		conds = append(conds, "i.confirmed_at < ?")
		args = append(args, f.Before.UTC())
	}
	if f.MinSize > 0 {
		conds = append(conds, "i.size_bytes >= ?")
		args = append(args, f.MinSize)
	}
	if f.MaxSize > 0 {
		conds = append(conds, "i.size_bytes <= ?")
		args = append(args, f.MaxSize)
	}
	args = append(args, f.Limit)

	q := `SELECT fn.path, fn.name, i.size_bytes
		FROM file_nodes fn JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id
		WHERE ` + scopeWhereAnd(s.scope, strings.Join(conds, " AND "), "fn", "i") + `
		ORDER BY fn.path LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, scopeWhereArgs(s.scope, 2, args...)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
