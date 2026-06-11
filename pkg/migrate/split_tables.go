// Package migrate provides online migration utilities for schema transitions.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/dat9/pkg/logger"
)

// Dialect controls SQL syntax variations for different databases.
type Dialect string

const (
	DialectMySQL    Dialect = "mysql"
	DialectPostgres Dialect = "postgres"
)

// SplitTablesMigrator performs the online migration from the monolithic
// `files` table to the vertically-split `inodes`/`contents`/`semantic` tables.
type SplitTablesMigrator struct {
	db      *sql.DB
	dialect Dialect
}

// NewSplitTablesMigrator creates a migrator for the split-tables transition.
// Defaults to MySQL dialect for backwards compatibility.
func NewSplitTablesMigrator(db *sql.DB) *SplitTablesMigrator {
	return &SplitTablesMigrator{db: db, dialect: DialectMySQL}
}

// NewSplitTablesMigratorWithDialect creates a migrator with an explicit SQL dialect.
func NewSplitTablesMigratorWithDialect(db *sql.DB, dialect Dialect) *SplitTablesMigrator {
	return &SplitTablesMigrator{db: db, dialect: dialect}
}

// Result summarizes what the migration did.
type Result struct {
	InodesMigrated    int64
	ContentsMigrated  int64
	SemanticMigrated  int64
	DirInodesCreated  int64
	SharedColsUpdated int64
	Duration          time.Duration
}

// requiredTablesExist checks that the target tables for split-table migration
// are present. Missing tables mean the tenant schema has not been initialized
// for the split-table layout and migration cannot proceed.
func (m *SplitTablesMigrator) requiredTablesExist(ctx context.Context) (bool, error) {
	tables := []string{"file_nodes", "inodes", "contents", "semantic"}
	for _, table := range tables {
		var exists bool
		var q string
		switch m.dialect {
		case DialectPostgres:
			q = `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)`
		default:
			q = `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?)`
		}
		if err := m.db.QueryRowContext(ctx, q, table).Scan(&exists); err != nil {
			return false, fmt.Errorf("check table %s: %w", table, err)
		}
		if !exists {
			return false, nil
		}
	}
	return true, nil
}

// isMigrationComplete checks whether all legacy data has already been migrated
// to the split tables and all directory inodes are linked. When true, Run
// returns immediately without doing any work.
func (m *SplitTablesMigrator) isMigrationComplete(ctx context.Context) (bool, error) {
	// Check 1: any legacy files not yet migrated to inodes.
	var fileCount int64
	err := m.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM files f
		WHERE f.status != 'DELETED'
		  AND f.file_id NOT IN (SELECT inode_id FROM inodes)`).Scan(&fileCount)
	if err != nil {
		return false, fmt.Errorf("check unmigrated inodes: %w", err)
	}
	if fileCount > 0 {
		return false, nil
	}

	// Check 2: any legacy files not yet migrated to contents.
	var contentCount int64
	err = m.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM files f
		WHERE f.status != 'DELETED'
		  AND f.file_id NOT IN (SELECT inode_id FROM contents)`).Scan(&contentCount)
	if err != nil {
		return false, fmt.Errorf("check unmigrated contents: %w", err)
	}
	if contentCount > 0 {
		return false, nil
	}

	// Check 3: any legacy files not yet migrated to semantic.
	var semanticCount int64
	err = m.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM files f
		WHERE f.status != 'DELETED'
		  AND f.file_id NOT IN (SELECT inode_id FROM semantic)`).Scan(&semanticCount)
	if err != nil {
		return false, fmt.Errorf("check unmigrated semantic: %w", err)
	}
	if semanticCount > 0 {
		return false, nil
	}

	// Check 4: any directories without a linked inode_id.
	var dirCount int64
	err = m.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM file_nodes
		WHERE is_directory = 1 AND inode_id IS NULL`).Scan(&dirCount)
	if err != nil {
		return false, fmt.Errorf("check orphan directories: %w", err)
	}
	if dirCount > 0 {
		return false, nil
	}

	// Check 5: any shared-table rows still missing inode_id backfill
	// (step 5: backfillSharedInodeID covers file_nodes, file_tags, uploads,
	// file_gc_tasks). If a prior run completed steps 1-4 and then failed
	// during step 5, we must detect that and rerun.
	var sharedCount int64
	err = m.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM (
			SELECT file_id FROM file_nodes
			WHERE is_directory = 0 AND inode_id IS NULL AND file_id IS NOT NULL
			UNION ALL
			SELECT file_id FROM file_tags
			WHERE inode_id IS NULL AND file_id IS NOT NULL
			UNION ALL
			SELECT file_id FROM uploads
			WHERE inode_id IS NULL AND file_id IS NOT NULL
			UNION ALL
			SELECT file_id FROM file_gc_tasks
			WHERE inode_id IS NULL AND file_id IS NOT NULL
		) t`).Scan(&sharedCount)
	if err != nil {
		return false, fmt.Errorf("check shared backfill: %w", err)
	}
	return sharedCount == 0, nil
}

// Run executes the migration. It is idempotent — re-running is safe.
func (m *SplitTablesMigrator) Run(ctx context.Context) (*Result, error) {
	start := time.Now()
	res := &Result{}

	// Guard: migration assumes split tables exist. If they don't, the caller
	// needs to run schema initialization first.
	ok, err := m.requiredTablesExist(ctx)
	if err != nil {
		return nil, fmt.Errorf("check required tables: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("required split tables (inodes, contents, semantic) do not exist; run schema initialization first")
	}

	// Fast path: if all legacy data is already migrated, skip the heavy work.
	complete, err := m.isMigrationComplete(ctx)
	if err != nil {
		return nil, fmt.Errorf("check migration completeness: %w", err)
	}
	if complete {
		logger.Info(ctx, "migrate_split_tables_already_complete")
		return res, nil
	}

	logger.Info(ctx, "migrate_split_tables_started")

	// Step 1: migrate file inodes from files table
	n, err := m.migrateInodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate inodes: %w", err)
	}
	res.InodesMigrated = n
	logger.Info(ctx, "migrate_inodes_finished", zap.Int64("count", n))

	// Step 2: migrate file contents from files table
	n, err = m.migrateContents(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate contents: %w", err)
	}
	res.ContentsMigrated = n
	logger.Info(ctx, "migrate_contents_finished", zap.Int64("count", n))

	// Step 3: migrate semantic data from files table
	n, err = m.migrateSemantic(ctx)
	if err != nil {
		return nil, fmt.Errorf("migrate semantic: %w", err)
	}
	res.SemanticMigrated = n
	logger.Info(ctx, "migrate_semantic_finished", zap.Int64("count", n))

	// Step 4: create directory inodes for paths that have no files row
	n, err = m.createDirInodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("create directory inodes: %w", err)
	}
	res.DirInodesCreated = n
	logger.Info(ctx, "migrate_dir_inodes_finished", zap.Int64("count", n))

	// Step 5: backfill inode_id on shared tables
	n, err = m.backfillSharedInodeID(ctx)
	if err != nil {
		return nil, fmt.Errorf("backfill shared inode_id: %w", err)
	}
	res.SharedColsUpdated = n
	logger.Info(ctx, "migrate_backfill_inode_id_finished", zap.Int64("count", n))

	res.Duration = time.Since(start)
	logger.Info(ctx, "migrate_split_tables_finished",
		zap.Int64("inodes", res.InodesMigrated),
		zap.Int64("contents", res.ContentsMigrated),
		zap.Int64("semantic", res.SemanticMigrated),
		zap.Int64("dir_inodes", res.DirInodesCreated),
		zap.Int64("shared_cols", res.SharedColsUpdated),
		zap.Duration("elapsed", res.Duration))
	return res, nil
}

func (m *SplitTablesMigrator) insertIgnore(table string, columns string, selectStmt string) string {
	switch m.dialect {
	case DialectPostgres:
		return fmt.Sprintf("INSERT INTO %s (%s) %s ON CONFLICT DO NOTHING", table, columns, selectStmt)
	default:
		return fmt.Sprintf("INSERT IGNORE INTO %s (%s) %s", table, columns, selectStmt)
	}
}

// execRetryingDeadlock executes a write statement, retrying transient lock
// conflicts (MySQL Error 1213 "Deadlock found" / SQLSTATE 40001). Run is
// idempotent and is invoked on every backend open, so concurrent opens of the
// same tenant DB can race the full-table backfills into a deadlock; the
// deadlock victim must retry instead of failing the backend open.
func (m *SplitTablesMigrator) execRetryingDeadlock(ctx context.Context, query string) (sql.Result, error) {
	const maxAttempts = 5
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		res, err := m.db.ExecContext(ctx, query)
		if err == nil || !isLockConflictError(err) {
			return res, err
		}
		lastErr = err
		logger.Warn(ctx, "migrate_split_tables_lock_conflict_retry",
			zap.Int("attempt", attempt),
			zap.Error(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt) * 50 * time.Millisecond):
		}
	}
	return nil, lastErr
}

func isLockConflictError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Deadlock found") ||
		strings.Contains(msg, "Error 1213") ||
		strings.Contains(msg, "40001")
}

func (m *SplitTablesMigrator) migrateInodes(ctx context.Context) (int64, error) {
	sql := m.insertIgnore("inodes",
		"inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at, expires_at",
		fmt.Sprintf(`SELECT
			file_id, size_bytes, revision, %d, status, created_at,
			COALESCE(confirmed_at, created_at), confirmed_at, expires_at
		FROM files
		WHERE status != 'DELETED'`, 0o644))
	res, err := m.execRetryingDeadlock(ctx, sql)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (m *SplitTablesMigrator) migrateContents(ctx context.Context) (int64, error) {
	storageRefHashExpr := "COALESCE(NULLIF(storage_ref_hash, ''), LOWER(SHA2(storage_ref, 256)))"
	if m.dialect == DialectPostgres {
		storageRefHashExpr = "storage_ref_hash"
	}
	sql := m.insertIgnore("contents",
		"inode_id, storage_type, storage_ref, storage_ref_hash, storage_encryption_mode, storage_encryption_key_id, content_blob, content_type, checksum_sha256, source_id",
		fmt.Sprintf(`SELECT
			file_id, storage_type, storage_ref, %s, storage_encryption_mode, storage_encryption_key_id,
			content_blob, content_type, checksum_sha256, source_id
		FROM files
		WHERE status != 'DELETED'`, storageRefHashExpr))
	res, err := m.execRetryingDeadlock(ctx, sql)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (m *SplitTablesMigrator) migrateSemantic(ctx context.Context) (int64, error) {
	skipGenerated, err := m.semanticHasGeneratedColumns(ctx)
	if err != nil {
		return 0, fmt.Errorf("detect generated columns: %w", err)
	}

	var columns, selectCols string
	if skipGenerated {
		// TiDB auto-embedding: embedding/description_embedding are GENERATED ALWAYS.
		// Copy only non-generated columns; TiDB will recompute vectors from
		// content_text/description on insert. This avoids LLM inference cost
		// during migration. (See docs/design/metadata-schema-refactor.md §6)
		columns = "inode_id, content_text, description, embedding_revision, description_embedding_revision"
		selectCols = `file_id, content_text, description, embedding_revision, description_embedding_revision`
		logger.Info(ctx, "migrate_semantic_generated_columns_detected",
			zap.String("strategy", "skip_generated"))
	} else {
		columns = "inode_id, content_text, description, embedding, embedding_revision, description_embedding, description_embedding_revision"
		selectCols = `file_id, content_text, description, embedding, embedding_revision,
			description_embedding, description_embedding_revision`
	}

	sql := m.insertIgnore("semantic", columns,
		fmt.Sprintf(`SELECT %s FROM files WHERE status != 'DELETED'`, selectCols))
	res, err := m.execRetryingDeadlock(ctx, sql)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// semanticHasGeneratedColumns detects whether the semantic table's embedding
// column is a generated column (TiDB auto-embedding mode). When true, the
// migration must skip copying embedding/description_embedding to avoid
// triggering expensive re-computation during INSERT ... SELECT.
func (m *SplitTablesMigrator) semanticHasGeneratedColumns(ctx context.Context) (bool, error) {
	var q string
	switch m.dialect {
	case DialectPostgres:
		q = `
			SELECT generation_expression
			FROM information_schema.columns
			WHERE table_name = 'semantic' AND column_name = 'embedding'
		`
	default:
		q = `
			SELECT extra
			FROM information_schema.columns
			WHERE table_schema = DATABASE() AND table_name = 'semantic' AND column_name = 'embedding'
		`
	}

	var extra string
	if err := m.db.QueryRowContext(ctx, q).Scan(&extra); err != nil {
		if err == sql.ErrNoRows {
			// Table or column does not exist yet — no generated columns.
			return false, nil
		}
		return false, err
	}
	lower := strings.ToLower(extra)
	return lower != "" && (strings.Contains(lower, "generated") || strings.Contains(lower, "auto")), nil
}

func (m *SplitTablesMigrator) createDirInodes(ctx context.Context) (int64, error) {
	sql := m.insertIgnore("inodes",
		"inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at",
		fmt.Sprintf(`SELECT
			node_id, 0, 1, %d, 'CONFIRMED', created_at, created_at, created_at
		FROM file_nodes
		WHERE is_directory = 1`, 0o755))
	res, err := m.execRetryingDeadlock(ctx, sql)
	if err != nil {
		return 0, err
	}
	// Link migrated directory inodes back to file_nodes.
	if _, err := m.execRetryingDeadlock(ctx, `
		UPDATE file_nodes SET inode_id = node_id WHERE is_directory = 1 AND inode_id IS NULL
	`); err != nil {
		return 0, fmt.Errorf("backfill directory inode_id: %w", err)
	}
	return res.RowsAffected()
}

func (m *SplitTablesMigrator) backfillSharedInodeID(ctx context.Context) (int64, error) {
	tables := []struct {
		name string
		col  string
	}{
		{"file_nodes", "file_id"},
		{"file_tags", "file_id"},
		{"uploads", "file_id"},
		{"file_gc_tasks", "file_id"},
	}

	var total int64
	for _, tbl := range tables {
		res, err := m.execRetryingDeadlock(ctx, fmt.Sprintf(`
			UPDATE %s
			SET inode_id = %s
			WHERE inode_id IS NULL AND %s IS NOT NULL
		`, tbl.name, tbl.col, tbl.col))
		if err != nil {
			return total, fmt.Errorf("backfill %s: %w", tbl.name, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, fmt.Errorf("rows affected for %s: %w", tbl.name, err)
		}
		total += n
	}
	return total, nil
}
