package mockserver

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// metaStore is the mock's metadb: one SQLite file holding the tenant table
// (production uses TiDB; the mock only needs key → tenant resolution).
type metaStore struct {
	db *sql.DB
}

func openMetaStore(dir string, inline, maxUpload int64) (*metaStore, error) {
	db, err := sql.Open("sqlite", dsn(filepath.Join(dir, "meta.db")))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite: single writer
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS tenants (
			tenant_id        TEXT PRIMARY KEY,
			api_key          TEXT NOT NULL UNIQUE,
			status           TEXT NOT NULL,
			inline_threshold INTEGER NOT NULL,
			max_upload_bytes INTEGER NOT NULL,
			created_at       INTEGER NOT NULL
		)`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			return nil, fmt.Errorf("meta schema: %w", err)
		}
	}
	_ = inline
	_ = maxUpload
	return &metaStore{db: db}, nil
}

func (m *metaStore) close() error { return m.db.Close() }

func (m *metaStore) createTenant(id, key string) error {
	_, err := m.db.Exec(
		`INSERT INTO tenants (tenant_id, api_key, status, inline_threshold, max_upload_bytes, created_at)
		 VALUES (?, ?, 'active', 0, 0, ?)`,
		id, key, time.Now().Unix(),
	)
	return err
}

func (m *metaStore) deleteTenant(id string) error {
	_, err := m.db.Exec(`DELETE FROM tenants WHERE tenant_id = ?`, id)
	return err
}

func (m *metaStore) tenantIDByKey(key string) (string, error) {
	var id string
	err := m.db.QueryRow(`SELECT tenant_id FROM tenants WHERE api_key = ? AND status = 'active'`, key).Scan(&id)
	if err == sql.ErrNoRows {
		return "", ErrNotFound
	}
	return id, err
}

// tenantDB is the per-tenant store (production: the tenant's TiDB schema;
// the mock: one SQLite file per tenant). It carries the fs domain tables and
// the simplified jfs_* extent tables (spec §7.2).
type tenantDB struct {
	id   string
	dir  string
	path string
	db   *sql.DB
}

func tenantDBPath(dir, id string) string {
	return filepath.Join(dir, "tenant-"+id+".db")
}

func openTenantDB(dir, id string) (*tenantDB, error) {
	path := tenantDBPath(dir, id)
	db, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	t := &tenantDB{id: id, dir: dir, path: path, db: db}
	if err := t.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return t, nil
}

func dsn(path string) string {
	return "file:" + path + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(ON)"
}

func (t *tenantDB) initSchema() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (
			path        TEXT PRIMARY KEY,
			kind        TEXT NOT NULL,             -- file | dir | symlink
			size        INTEGER NOT NULL DEFAULT 0,
			revision    INTEGER NOT NULL DEFAULT 1,
			data        BLOB,
			target      TEXT,                      -- symlink target
			mode        INTEGER NOT NULL DEFAULT 420,
			mtime_ns    INTEGER NOT NULL,
			btime_ns    INTEGER NOT NULL,
			tags        TEXT NOT NULL DEFAULT '',
			description TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE TABLE IF NOT EXISTS uploads (
			upload_id        TEXT PRIMARY KEY,
			path             TEXT NOT NULL,
			total_size       INTEGER NOT NULL,
			part_size        INTEGER NOT NULL,
			expected_revision INTEGER NOT NULL,
			status           TEXT NOT NULL,
			created_at       INTEGER NOT NULL,
			expires_at       INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS upload_parts (
			upload_id TEXT NOT NULL,
			number    INTEGER NOT NULL,
			size      INTEGER NOT NULL,
			data      BLOB NOT NULL,
			PRIMARY KEY (upload_id, number)
		)`,
		// simplified jfs_* tables (spec §7.2): enough surface for the
		// extent op registry's Tier 0 subset; full JuiceFS semantics are a
		// fidelity TODO ported from pkg/datastore/jfs_meta.go
		`CREATE TABLE IF NOT EXISTS jfs_node (
			inode  INTEGER PRIMARY KEY,
			type   INTEGER NOT NULL,             -- 1 dir, 2 file, 4 symlink …
			size   INTEGER NOT NULL DEFAULT 0,
			mode   INTEGER NOT NULL DEFAULT 420,
			atime INTEGER NOT NULL, mtime INTEGER NOT NULL, ctime INTEGER NOT NULL,
			parent INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_edge (
			parent INTEGER NOT NULL,
			name   TEXT NOT NULL,
			inode  INTEGER NOT NULL,
			PRIMARY KEY (parent, name)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_counter (
			name  TEXT PRIMARY KEY,
			value INTEGER NOT NULL
		)`,
		`INSERT OR IGNORE INTO jfs_counter (name, value) VALUES ('nextInode', 2)`,
		`INSERT OR IGNORE INTO jfs_counter (name, value) VALUES ('usedSpace', 0)`,
		`CREATE TABLE IF NOT EXISTS jfs_chunk (
			inode INTEGER NOT NULL,
			chunk INTEGER NOT NULL,
			slices BLOB NOT NULL,
			PRIMARY KEY (inode, chunk)
		)`,
		`CREATE TABLE IF NOT EXISTS jfs_session (
			sid INTEGER PRIMARY KEY,
			created_at INTEGER NOT NULL
		)`,
		// fs domain roots
		`INSERT OR IGNORE INTO file_nodes (path, kind, size, revision, mode, mtime_ns, btime_ns)
		 VALUES ('/', 'dir', 0, 1, 493, ?, ?)`,
		// extent root inode 1
		`INSERT OR IGNORE INTO jfs_node (inode, type, size, mode, atime, mtime, ctime, parent)
		 VALUES (1, 1, 0, 493, ?, ?, ?, 0)`,
		`INSERT OR IGNORE INTO jfs_edge (parent, name, inode) VALUES (0, '', 1)`,
	}
	now := time.Now().UnixNano()
	for _, q := range stmts {
		if _, err := t.db.Exec(q, now, now, now, now, now, now); err != nil {
			return fmt.Errorf("tenant schema: %w", err)
		}
	}
	return nil
}

func (t *tenantDB) close() error { return t.db.Close() }

func (t *tenantDB) dropFile() error { return nil }

// readOnlyHandle opens a fresh read-only connection for /v1/sql (spec §8).
func (t *tenantDB) readOnlyHandle() (*sql.DB, error) {
	return sql.Open("sqlite", "file:"+t.path+"?mode=ro")
}

// QueryContext/ExecContext passthroughs keep handlers terse.
func (t *tenantDB) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return t.db.QueryContext(ctx, q, args...)
}

func (t *tenantDB) ExecContext(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return t.db.ExecContext(ctx, q, args...)
}
