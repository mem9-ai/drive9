// Package migrate provides online migration utilities for schema transitions.
package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"go.uber.org/zap"
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

// Run executes the migration. It is idempotent — re-running is safe.
func (m *SplitTablesMigrator) Run(ctx context.Context) (*Result, error) {
	start := time.Now()
	res := &Result{}

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

func (m *SplitTablesMigrator) migrateInodes(ctx context.Context) (int64, error) {
	sql := m.insertIgnore("inodes",
		"inode_id, size_bytes, revision, mode, status, created_at, mtime, confirmed_at, expires_at",
		`SELECT
			file_id, size_bytes, revision, 420, status, created_at,
			COALESCE(confirmed_at, created_at), confirmed_at, expires_at
		FROM files
		WHERE status != 'DELETED'`)
	res, err := m.db.ExecContext(ctx, sql)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (m *SplitTablesMigrator) migrateContents(ctx context.Context) (int64, error) {
	sql := m.insertIgnore("contents",
		"inode_id, storage_type, storage_ref, storage_encryption_mode, storage_encryption_key_id, content_blob, content_type, checksum_sha256, source_id",
		`SELECT
			file_id, storage_type, storage_ref, storage_encryption_mode, storage_encryption_key_id,
			content_blob, content_type, checksum_sha256, source_id
		FROM files
		WHERE status != 'DELETED'`)
	res, err := m.db.ExecContext(ctx, sql)
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
	res, err := m.db.ExecContext(ctx, sql)
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
	var arg []any
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
	if err := m.db.QueryRowContext(ctx, q, arg...).Scan(&extra); err != nil {
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
		`SELECT
			node_id, 0, 1, 493, 'CONFIRMED', created_at, created_at, created_at
		FROM file_nodes
		WHERE is_directory = 1`)
	res, err := m.db.ExecContext(ctx, sql)
	if err != nil {
		return 0, err
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
		res, err := m.db.ExecContext(ctx, fmt.Sprintf(`
			UPDATE %s
			SET inode_id = %s
			WHERE inode_id IS NULL AND %s IS NOT NULL
		`, tbl.name, tbl.col, tbl.col))
		if err != nil {
			return total, fmt.Errorf("backfill %s: %w", tbl.name, err)
		}
		n, _ := res.RowsAffected()
		total += n
	}
	return total, nil
}
