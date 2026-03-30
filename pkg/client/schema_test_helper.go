package client

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func initClientTenantSchema(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (node_id VARCHAR(64) PRIMARY KEY, path VARCHAR(512) NOT NULL, parent_path VARCHAR(512) NOT NULL, name VARCHAR(255) NOT NULL, is_directory BOOLEAN NOT NULL DEFAULT FALSE, file_id VARCHAR(64), created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3))`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_path ON file_nodes(path)`,
		`CREATE INDEX IF NOT EXISTS idx_parent ON file_nodes(parent_path)`,
		`CREATE INDEX IF NOT EXISTS idx_file_id ON file_nodes(file_id)`,
		`CREATE TABLE IF NOT EXISTS files (file_id VARCHAR(64) PRIMARY KEY, storage_type VARCHAR(32) NOT NULL, storage_ref TEXT NOT NULL, content_blob LONGBLOB, content_type VARCHAR(255), size_bytes BIGINT NOT NULL DEFAULT 0, checksum_sha256 VARCHAR(128), revision BIGINT NOT NULL DEFAULT 1, status VARCHAR(32) NOT NULL DEFAULT 'PENDING', source_id VARCHAR(255), content_text LONGTEXT, created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), confirmed_at DATETIME(3), expires_at DATETIME(3))`,
		`CREATE INDEX IF NOT EXISTS idx_status ON files(status, created_at)`,
		`CREATE TABLE IF NOT EXISTS file_tags (file_id VARCHAR(64) NOT NULL, tag_key VARCHAR(255) NOT NULL, tag_value VARCHAR(255) NOT NULL DEFAULT '', PRIMARY KEY (file_id, tag_key))`,
		`CREATE INDEX IF NOT EXISTS idx_kv ON file_tags(tag_key, tag_value)`,
		`CREATE TABLE IF NOT EXISTS uploads (upload_id VARCHAR(64) PRIMARY KEY, file_id VARCHAR(64) NOT NULL, target_path VARCHAR(512) NOT NULL, s3_upload_id VARCHAR(255) NOT NULL, s3_key VARCHAR(2048) NOT NULL, total_size BIGINT NOT NULL, part_size BIGINT NOT NULL, parts_total INT NOT NULL, status VARCHAR(32) NOT NULL DEFAULT 'UPLOADING', fingerprint_sha256 VARCHAR(128), idempotency_key VARCHAR(255), created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3), expires_at DATETIME(3) NOT NULL, active_target_path VARCHAR(512) AS (CASE WHEN status = 'UPLOADING' THEN target_path ELSE NULL END) STORED)`,
		`CREATE INDEX IF NOT EXISTS idx_upload_path ON uploads(target_path, status)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency ON uploads(idempotency_key)`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "syntax") && strings.Contains(stmt, "IF NOT EXISTS") {
				stmt = strings.Replace(stmt, " IF NOT EXISTS", "", 1)
				_, err = db.Exec(stmt)
			}
			if err == nil {
				continue
			}
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "duplicate key name") || strings.Contains(msg, "already exists") || strings.Contains(msg, "already exist") {
				continue
			}
			t.Fatal(err)
		}
	}
}
