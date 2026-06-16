package db9

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
)

func repairDB9PathHashSchema(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("nil db")
	}
	for _, stmt := range []string{
		`ALTER TABLE IF EXISTS file_nodes ADD COLUMN IF NOT EXISTS path_hash VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE IF EXISTS file_nodes ADD COLUMN IF NOT EXISTS parent_path_hash VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS target_path_hash VARCHAR(64) NOT NULL DEFAULT ''`,
		`ALTER TABLE IF EXISTS uploads ADD COLUMN IF NOT EXISTS active_target_path_hash VARCHAR(64) GENERATED ALWAYS AS (CASE WHEN status = 'UPLOADING' THEN target_path_hash ELSE NULL END) STORED`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("repair db9 path-hash schema %q: %w", stmt, err)
		}
	}
	if err := backfillDB9FileNodePathHashes(ctx, db); err != nil {
		return err
	}
	if err := backfillDB9UploadPathHashes(ctx, db); err != nil {
		return err
	}
	for _, idx := range []struct {
		name string
		want []string
	}{
		{"idx_path", []string{"path_hash"}},
		{"idx_parent", []string{"parent_path_hash", "name"}},
		{"idx_upload_path", []string{"target_path_hash", "status"}},
		{"idx_uploads_active", []string{"active_target_path_hash"}},
	} {
		if err := dropDB9IndexIfMismatched(ctx, db, idx.name, idx.want); err != nil {
			return err
		}
	}
	return nil
}

func backfillDB9FileNodePathHashes(ctx context.Context, db *sql.DB) error {
	if ok, err := db9TableExists(ctx, db, "file_nodes"); err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT node_id, path, parent_path
		FROM file_nodes
		WHERE (path_hash = '' AND path <> '') OR (parent_path_hash = '' AND parent_path <> '')`)
	if err != nil {
		return fmt.Errorf("select db9 file_nodes path hashes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type row struct {
		nodeID     string
		path       string
		parentPath string
	}
	var updates []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.nodeID, &r.path, &r.parentPath); err != nil {
			return fmt.Errorf("scan db9 file_nodes path hashes: %w", err)
		}
		updates = append(updates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate db9 file_nodes path hashes: %w", err)
	}
	for _, r := range updates {
		if _, err := db.ExecContext(ctx, `UPDATE file_nodes
			SET path_hash = $1, parent_path_hash = $2
			WHERE node_id = $3`,
			db9PathHash(r.path), db9PathHash(r.parentPath), r.nodeID); err != nil {
			return fmt.Errorf("backfill db9 file_nodes path hashes: %w", err)
		}
	}
	return nil
}

func backfillDB9UploadPathHashes(ctx context.Context, db *sql.DB) error {
	if ok, err := db9TableExists(ctx, db, "uploads"); err != nil || !ok {
		return err
	}
	rows, err := db.QueryContext(ctx, `SELECT upload_id, target_path
		FROM uploads
		WHERE target_path_hash = '' AND target_path <> ''`)
	if err != nil {
		return fmt.Errorf("select db9 upload path hashes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type row struct {
		uploadID   string
		targetPath string
	}
	var updates []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.uploadID, &r.targetPath); err != nil {
			return fmt.Errorf("scan db9 upload path hashes: %w", err)
		}
		updates = append(updates, r)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate db9 upload path hashes: %w", err)
	}
	for _, r := range updates {
		if _, err := db.ExecContext(ctx, `UPDATE uploads
			SET target_path_hash = $1
			WHERE upload_id = $2`,
			db9PathHash(r.targetPath), r.uploadID); err != nil {
			return fmt.Errorf("backfill db9 upload path hashes: %w", err)
		}
	}
	return nil
}

func db9TableExists(ctx context.Context, db *sql.DB, tableName string) (bool, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, tableName).Scan(&exists); err != nil {
		return false, fmt.Errorf("check db9 table %s: %w", tableName, err)
	}
	return exists, nil
}

func dropDB9IndexIfMismatched(ctx context.Context, db *sql.DB, indexName string, want []string) error {
	got, err := db9IndexColumns(ctx, db, indexName)
	if err != nil {
		return err
	}
	if len(got) == 0 || equalStrings(got, want) {
		return nil
	}
	if _, err := db.ExecContext(ctx, `DROP INDEX IF EXISTS `+quoteDB9Identifier(indexName)); err != nil {
		return fmt.Errorf("drop db9 index %s: %w", indexName, err)
	}
	return nil
}

func db9IndexColumns(ctx context.Context, db *sql.DB, indexName string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT a.attname
		FROM pg_class idx
		JOIN pg_index i ON i.indexrelid = idx.oid
		JOIN pg_class tbl ON tbl.oid = i.indrelid
		JOIN unnest(i.indkey) WITH ORDINALITY AS cols(attnum, ord) ON true
		JOIN pg_attribute a ON a.attrelid = tbl.oid AND a.attnum = cols.attnum
		WHERE idx.relname = $1
		ORDER BY cols.ord`, indexName)
	if err != nil {
		return nil, fmt.Errorf("load db9 index %s columns: %w", indexName, err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan db9 index %s columns: %w", indexName, err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate db9 index %s columns: %w", indexName, err)
	}
	return cols, nil
}

func db9PathHash(p string) string {
	sum := sha256.Sum256([]byte(p))
	return hex.EncodeToString(sum[:])
}

func quoteDB9Identifier(identifier string) string {
	out := `"`
	for _, r := range identifier {
		if r == '"' {
			out += `""`
			continue
		}
		out += string(r)
	}
	return out + `"`
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
