// Package meta provides the inode-model metadata store for dat9.
// P0 uses SQLite as a local stand-in for db9. Two core tables:
// file_nodes (dentry/path tree) and files (inode/file entity).
package meta

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var ErrNotFound = errors.New("not found")

type StorageType string

const (
	StorageDB9 StorageType = "db9"
	StorageS3  StorageType = "s3"
)

type FileStatus string

const (
	StatusPending   FileStatus = "PENDING"
	StatusConfirmed FileStatus = "CONFIRMED"
	StatusDeleted   FileStatus = "DELETED"
)

type UploadStatus string

const (
	UploadUploading UploadStatus = "UPLOADING"
	UploadCompleted UploadStatus = "COMPLETED"
	UploadAborted   UploadStatus = "ABORTED"
	UploadExpired   UploadStatus = "EXPIRED"
)

// FileNode represents a row in the file_nodes table (dentry).
type FileNode struct {
	NodeID      string
	Path        string
	ParentPath  string
	Name        string
	IsDirectory bool
	FileID      string // empty for directories
	CreatedAt   time.Time
}

// File represents a row in the files table (inode).
type File struct {
	FileID         string
	StorageType    StorageType
	StorageRef     string
	ContentType    string
	SizeBytes      int64
	ChecksumSHA256 string
	Revision       int64
	Status         FileStatus
	SourceID       string
	ContentText    string
	CreatedAt      time.Time
	ConfirmedAt    *time.Time
	ExpiresAt      *time.Time
}

// NodeWithFile joins file_nodes and files for stat/read operations.
type NodeWithFile struct {
	Node FileNode
	File *File // nil for directories
}

// Upload represents a row in the uploads table.
type Upload struct {
	UploadID       string
	FileID         string
	TargetPath     string
	S3UploadID     string
	S3Key          string
	TotalSize      int64
	PartSize       int64
	PartsTotal     int
	Status         UploadStatus
	FingerprintSHA string
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ExpiresAt      time.Time
}

// Store is the metadata store backed by SQLite (stand-in for db9).
type Store struct {
	db *sql.DB
}

func Open(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB { return s.db }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS file_nodes (
			node_id      TEXT PRIMARY KEY,
			path         TEXT NOT NULL,
			parent_path  TEXT NOT NULL,
			name         TEXT NOT NULL,
			is_directory INTEGER NOT NULL DEFAULT 0,
			file_id      TEXT,
			created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now'))
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_path ON file_nodes(path)`,
		`CREATE INDEX IF NOT EXISTS idx_parent ON file_nodes(parent_path)`,
		`CREATE INDEX IF NOT EXISTS idx_file_id ON file_nodes(file_id)`,

		`CREATE TABLE IF NOT EXISTS files (
			file_id         TEXT PRIMARY KEY,
			storage_type    TEXT NOT NULL,
			storage_ref     TEXT NOT NULL,
			content_type    TEXT,
			size_bytes      INTEGER NOT NULL DEFAULT 0,
			checksum_sha256 TEXT,
			revision        INTEGER NOT NULL DEFAULT 1,
			status          TEXT NOT NULL DEFAULT 'PENDING',
			source_id       TEXT,
			content_text    TEXT,
			created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
			confirmed_at    TEXT,
			expires_at      TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_status ON files(status, created_at)`,

		`CREATE TABLE IF NOT EXISTS file_tags (
			file_id   TEXT NOT NULL,
			tag_key   TEXT NOT NULL,
			tag_value TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (file_id, tag_key)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_kv ON file_tags(tag_key, tag_value)`,

		`CREATE TABLE IF NOT EXISTS uploads (
			upload_id          TEXT PRIMARY KEY,
			file_id            TEXT NOT NULL,
			target_path        TEXT NOT NULL,
			s3_upload_id       TEXT NOT NULL,
			s3_key             TEXT NOT NULL,
			total_size         INTEGER NOT NULL,
			part_size          INTEGER NOT NULL,
			parts_total        INTEGER NOT NULL,
			status             TEXT NOT NULL DEFAULT 'UPLOADING',
			fingerprint_sha256 TEXT,
			idempotency_key    TEXT,
			created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
			updated_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f','now')),
			expires_at         TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_upload_path ON uploads(target_path, status)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency ON uploads(idempotency_key)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt[:60], err)
		}
	}
	return nil
}

// --- file_nodes operations ---

func (s *Store) InsertNode(n *FileNode) error {
	_, err := s.db.Exec(`INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		n.NodeID, n.Path, n.ParentPath, n.Name, n.IsDirectory, nullStr(n.FileID), timeStr(n.CreatedAt))
	return err
}

func (s *Store) GetNode(path string) (*FileNode, error) {
	row := s.db.QueryRow(`SELECT node_id, path, parent_path, name, is_directory, file_id, created_at
		FROM file_nodes WHERE path = ?`, path)
	return scanNode(row)
}

func (s *Store) ListNodes(parentPath string) ([]*FileNode, error) {
	rows, err := s.db.Query(`SELECT node_id, path, parent_path, name, is_directory, file_id, created_at
		FROM file_nodes WHERE parent_path = ? ORDER BY name`, parentPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var nodes []*FileNode
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	return nodes, rows.Err()
}

func (s *Store) DeleteNode(path string) error {
	res, err := s.db.Exec(`DELETE FROM file_nodes WHERE path = ?`, path)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteEmptyDir atomically checks a directory is empty and deletes it.
func (s *Store) DeleteEmptyDir(path string) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var count int64
	if err := tx.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE parent_path = ?`, path).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return fmt.Errorf("directory not empty: %s", path)
	}
	res, err := tx.Exec(`DELETE FROM file_nodes WHERE path = ? AND is_directory = 1`, path)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return tx.Commit()
}

func (s *Store) DeleteNodesByPrefix(prefix string) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM file_nodes WHERE path = ? OR path LIKE ?`,
		prefix, prefix+"%")
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) UpdateNodePath(oldPath, newPath, newParentPath, newName string) error {
	res, err := s.db.Exec(`UPDATE file_nodes SET path = ?, parent_path = ?, name = ?
		WHERE path = ?`, newPath, newParentPath, newName, oldPath)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) RenameDir(oldPrefix, newPrefix string) (int64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`SELECT node_id, path, parent_path, name FROM file_nodes
		WHERE path = ? OR path LIKE ? ORDER BY path`, oldPrefix, oldPrefix+"%")
	if err != nil {
		return 0, err
	}
	type update struct {
		nodeID, newPath, newParent, newName string
	}
	var updates []update
	for rows.Next() {
		var nodeID, p, pp, name string
		if err := rows.Scan(&nodeID, &p, &pp, &name); err != nil {
			rows.Close()
			return 0, err
		}
		newPath := newPrefix + strings.TrimPrefix(p, oldPrefix)
		newParent := newPrefix + strings.TrimPrefix(pp, oldPrefix)
		newName := name
		if p == oldPrefix {
			newParent = parentPath(newPrefix)
			newPath = newPrefix
			newName = baseName(newPrefix)
		}
		updates = append(updates, update{nodeID, newPath, newParent, newName})
	}
	rows.Close()

	stmt, err := tx.Prepare(`UPDATE file_nodes SET path = ?, parent_path = ?, name = ? WHERE node_id = ?`)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	for _, u := range updates {
		if _, err := stmt.Exec(u.newPath, u.newParent, u.newName, u.nodeID); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return int64(len(updates)), nil
}

func (s *Store) RefCount(fileID string) (int64, error) {
	var count int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE file_id = ?`, fileID).Scan(&count)
	return count, err
}

func (s *Store) EnsureParentDirs(path string, genID func() string) error {
	var ancestors []string
	cur := path
	for {
		parent := parentPath(cur)
		if parent == cur || parent == "/" {
			break
		}
		ancestors = append(ancestors, parent)
		cur = parent
	}

	now := time.Now()
	for i := len(ancestors) - 1; i >= 0; i-- {
		dirPath := ancestors[i]
		pp := parentPath(dirPath)
		name := baseName(dirPath)
		_, err := s.db.Exec(`INSERT OR IGNORE INTO file_nodes
			(node_id, path, parent_path, name, is_directory, created_at)
			VALUES (?, ?, ?, ?, 1, ?)`,
			genID(), dirPath, pp, name, timeStr(now))
		if err != nil && !isUniqueViolation(err) {
			return fmt.Errorf("ensure parent %s: %w", dirPath, err)
		}
	}
	return nil
}

// --- files operations ---

func (s *Store) InsertFile(f *File) error {
	_, err := s.db.Exec(`INSERT INTO files
		(file_id, storage_type, storage_ref, content_type, size_bytes, checksum_sha256,
		 revision, status, source_id, content_text, created_at, confirmed_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		f.FileID, f.StorageType, f.StorageRef, nullStr(f.ContentType),
		f.SizeBytes, nullStr(f.ChecksumSHA256), f.Revision, f.Status,
		nullStr(f.SourceID), nullStr(f.ContentText),
		timeStr(f.CreatedAt), nilTimeStr(f.ConfirmedAt), nilTimeStr(f.ExpiresAt))
	return err
}

func (s *Store) GetFile(fileID string) (*File, error) {
	row := s.db.QueryRow(`SELECT file_id, storage_type, storage_ref, content_type,
		size_bytes, checksum_sha256, revision, status, source_id, content_text,
		created_at, confirmed_at, expires_at
		FROM files WHERE file_id = ?`, fileID)
	return scanFile(row)
}

func (s *Store) UpdateFileContent(fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, size int64) error {
	res, err := s.db.Exec(`UPDATE files SET storage_type = ?, storage_ref = ?,
		content_type = ?, size_bytes = ?, checksum_sha256 = ?, content_text = ?,
		revision = revision + 1, status = 'CONFIRMED',
		confirmed_at = strftime('%Y-%m-%dT%H:%M:%f','now')
		WHERE file_id = ?`,
		storageType, storageRef, nullStr(contentType), size,
		nullStr(checksum), nullStr(contentText), fileID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ConfirmFile(fileID string) error {
	_, err := s.db.Exec(`UPDATE files SET status = 'CONFIRMED',
		confirmed_at = strftime('%Y-%m-%dT%H:%M:%f','now')
		WHERE file_id = ? AND status = 'PENDING'`, fileID)
	return err
}

func (s *Store) MarkFileDeleted(fileID string) error {
	_, err := s.db.Exec(`UPDATE files SET status = 'DELETED' WHERE file_id = ?`, fileID)
	return err
}

// --- composite operations ---

func (s *Store) Stat(path string) (*NodeWithFile, error) {
	node, err := s.GetNode(path)
	if err != nil {
		return nil, err
	}
	nf := &NodeWithFile{Node: *node}
	if !node.IsDirectory && node.FileID != "" {
		f, err := s.GetFile(node.FileID)
		if err != nil {
			return nil, err
		}
		nf.File = f
	}
	return nf, nil
}

func (s *Store) ListDir(parentPath string) ([]*NodeWithFile, error) {
	rows, err := s.db.Query(`SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		f.file_id, f.storage_type, f.storage_ref, f.content_type, f.size_bytes,
		f.checksum_sha256, f.revision, f.status, f.source_id, f.content_text,
		f.created_at, f.confirmed_at, f.expires_at
		FROM file_nodes fn
		LEFT JOIN files f ON fn.file_id = f.file_id AND f.status = 'CONFIRMED'
		WHERE fn.parent_path = ?
		ORDER BY fn.name`, parentPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*NodeWithFile
	for rows.Next() {
		nf, err := scanNodeWithFile(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, nf)
	}
	return result, rows.Err()
}

func (s *Store) DeleteFileWithRefCheck(path string) (*File, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var fileID sql.NullString
	var isDir bool
	err = tx.QueryRow(`SELECT file_id, is_directory FROM file_nodes WHERE path = ?`, path).Scan(&fileID, &isDir)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if _, err := tx.Exec(`DELETE FROM file_nodes WHERE path = ?`, path); err != nil {
		return nil, err
	}

	if isDir || !fileID.Valid || fileID.String == "" {
		return nil, tx.Commit()
	}

	var count int64
	err = tx.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE file_id = ?`, fileID.String).Scan(&count)
	if err != nil {
		return nil, err
	}

	if count > 0 {
		return nil, tx.Commit()
	}

	if _, err := tx.Exec(`UPDATE files SET status = 'DELETED' WHERE file_id = ?`, fileID.String); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`DELETE FROM file_tags WHERE file_id = ?`, fileID.String); err != nil {
		return nil, err
	}

	row := tx.QueryRow(`SELECT file_id, storage_type, storage_ref, content_type,
		size_bytes, checksum_sha256, revision, status, source_id, content_text,
		created_at, confirmed_at, expires_at
		FROM files WHERE file_id = ?`, fileID.String)
	f, err := scanFile(row)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return f, nil
}

func (s *Store) DeleteDirRecursive(dirPath string) ([]*File, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`SELECT DISTINCT file_id FROM file_nodes
		WHERE (path = ? OR path LIKE ?) AND file_id IS NOT NULL`, dirPath, dirPath+"%")
	if err != nil {
		return nil, err
	}
	var fileIDs []string
	for rows.Next() {
		var fid string
		if err := rows.Scan(&fid); err != nil {
			rows.Close()
			return nil, err
		}
		fileIDs = append(fileIDs, fid)
	}
	rows.Close()

	if _, err := tx.Exec(`DELETE FROM file_nodes WHERE path = ? OR path LIKE ?`,
		dirPath, dirPath+"%"); err != nil {
		return nil, err
	}

	var orphaned []*File
	for _, fid := range fileIDs {
		var count int64
		if err := tx.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE file_id = ?`, fid).Scan(&count); err != nil {
			return nil, err
		}
		if count > 0 {
			continue
		}
		if _, err := tx.Exec(`UPDATE files SET status = 'DELETED' WHERE file_id = ?`, fid); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`DELETE FROM file_tags WHERE file_id = ?`, fid); err != nil {
			return nil, err
		}
		row := tx.QueryRow(`SELECT file_id, storage_type, storage_ref, content_type,
			size_bytes, checksum_sha256, revision, status, source_id, content_text,
			created_at, confirmed_at, expires_at
			FROM files WHERE file_id = ?`, fid)
		f, err := scanFile(row)
		if err != nil {
			return nil, err
		}
		orphaned = append(orphaned, f)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return orphaned, nil
}

// --- uploads operations ---

func (s *Store) InsertUpload(u *Upload) error {
	_, err := s.db.Exec(`INSERT INTO uploads
		(upload_id, file_id, target_path, s3_upload_id, s3_key, total_size, part_size,
		 parts_total, status, fingerprint_sha256, idempotency_key, created_at, updated_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.UploadID, u.FileID, u.TargetPath, u.S3UploadID, u.S3Key,
		u.TotalSize, u.PartSize, u.PartsTotal, u.Status,
		nullStr(u.FingerprintSHA), nullStr(u.IdempotencyKey),
		timeStr(u.CreatedAt), timeStr(u.UpdatedAt), timeStr(u.ExpiresAt))
	return err
}

func (s *Store) GetUpload(uploadID string) (*Upload, error) {
	row := s.db.QueryRow(`SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		total_size, part_size, parts_total, status, fingerprint_sha256, idempotency_key,
		created_at, updated_at, expires_at
		FROM uploads WHERE upload_id = ?`, uploadID)
	return scanUpload(row)
}

func (s *Store) GetUploadByPath(targetPath string) (*Upload, error) {
	row := s.db.QueryRow(`SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		total_size, part_size, parts_total, status, fingerprint_sha256, idempotency_key,
		created_at, updated_at, expires_at
		FROM uploads WHERE target_path = ? AND status = 'UPLOADING'
		ORDER BY created_at DESC LIMIT 1`, targetPath)
	return scanUpload(row)
}

func (s *Store) CompleteUpload(uploadID string) error {
	_, err := s.db.Exec(`UPDATE uploads SET status = 'COMPLETED',
		updated_at = strftime('%Y-%m-%dT%H:%M:%f','now')
		WHERE upload_id = ? AND status = 'UPLOADING'`, uploadID)
	return err
}

func (s *Store) AbortUpload(uploadID string) error {
	_, err := s.db.Exec(`UPDATE uploads SET status = 'ABORTED',
		updated_at = strftime('%Y-%m-%dT%H:%M:%f','now')
		WHERE upload_id = ? AND status = 'UPLOADING'`, uploadID)
	return err
}

func (s *Store) ListUploadsByPath(targetPath string, status UploadStatus) ([]*Upload, error) {
	rows, err := s.db.Query(`SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		total_size, part_size, parts_total, status, fingerprint_sha256, idempotency_key,
		created_at, updated_at, expires_at
		FROM uploads WHERE target_path = ? AND status = ?
		ORDER BY created_at DESC`, targetPath, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var uploads []*Upload
	for rows.Next() {
		u, err := scanUpload(rows)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, u)
	}
	return uploads, rows.Err()
}

// --- scan helpers ---

type scanner interface {
	Scan(dest ...interface{}) error
}

func scanNode(s scanner) (*FileNode, error) {
	var n FileNode
	var isDir int
	var fileID sql.NullString
	var createdAt string
	err := s.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &fileID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	n.IsDirectory = isDir != 0
	n.FileID = fileID.String
	n.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	return &n, nil
}

func scanFile(s scanner) (*File, error) {
	var f File
	var contentType, checksum, sourceID, contentText sql.NullString
	var confirmedAt, expiresAt sql.NullString
	var createdAt string
	err := s.Scan(&f.FileID, &f.StorageType, &f.StorageRef, &contentType,
		&f.SizeBytes, &checksum, &f.Revision, &f.Status, &sourceID, &contentText,
		&createdAt, &confirmedAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	f.ContentType = contentType.String
	f.ChecksumSHA256 = checksum.String
	f.SourceID = sourceID.String
	f.ContentText = contentText.String
	f.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	if confirmedAt.Valid {
		t, _ := time.Parse(time.RFC3339Nano, confirmedAt.String)
		f.ConfirmedAt = &t
	}
	if expiresAt.Valid {
		t, _ := time.Parse(time.RFC3339Nano, expiresAt.String)
		f.ExpiresAt = &t
	}
	return &f, nil
}

func scanNodeWithFile(rows *sql.Rows) (*NodeWithFile, error) {
	var n FileNode
	var isDir int
	var nodeFileID sql.NullString
	var nodeCreatedAt string

	var fFileID, fStorageType, fStorageRef sql.NullString
	var fContentType, fChecksum, fSourceID, fContentText sql.NullString
	var fSizeBytes, fRevision sql.NullInt64
	var fStatus sql.NullString
	var fCreatedAt, fConfirmedAt, fExpiresAt sql.NullString

	err := rows.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
		&fFileID, &fStorageType, &fStorageRef, &fContentType, &fSizeBytes,
		&fChecksum, &fRevision, &fStatus, &fSourceID, &fContentText,
		&fCreatedAt, &fConfirmedAt, &fExpiresAt)
	if err != nil {
		return nil, err
	}

	n.IsDirectory = isDir != 0
	n.FileID = nodeFileID.String
	n.CreatedAt, _ = time.Parse(time.RFC3339Nano, nodeCreatedAt)

	nf := &NodeWithFile{Node: n}
	if fFileID.Valid {
		nf.File = &File{
			FileID:         fFileID.String,
			StorageType:    StorageType(fStorageType.String),
			StorageRef:     fStorageRef.String,
			ContentType:    fContentType.String,
			SizeBytes:      fSizeBytes.Int64,
			ChecksumSHA256: fChecksum.String,
			Revision:       fRevision.Int64,
			Status:         FileStatus(fStatus.String),
			SourceID:       fSourceID.String,
			ContentText:    fContentText.String,
		}
		if fCreatedAt.Valid {
			nf.File.CreatedAt, _ = time.Parse(time.RFC3339Nano, fCreatedAt.String)
		}
		if fConfirmedAt.Valid {
			t, _ := time.Parse(time.RFC3339Nano, fConfirmedAt.String)
			nf.File.ConfirmedAt = &t
		}
		if fExpiresAt.Valid {
			t, _ := time.Parse(time.RFC3339Nano, fExpiresAt.String)
			nf.File.ExpiresAt = &t
		}
	}
	return nf, nil
}

func scanUpload(s scanner) (*Upload, error) {
	var u Upload
	var fingerprint, idempotencyKey sql.NullString
	var createdAt, updatedAt, expiresAt string
	err := s.Scan(&u.UploadID, &u.FileID, &u.TargetPath, &u.S3UploadID, &u.S3Key,
		&u.TotalSize, &u.PartSize, &u.PartsTotal, &u.Status,
		&fingerprint, &idempotencyKey,
		&createdAt, &updatedAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	u.FingerprintSHA = fingerprint.String
	u.IdempotencyKey = idempotencyKey.String
	u.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	u.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
	u.ExpiresAt, _ = time.Parse(time.RFC3339Nano, expiresAt)
	return &u, nil
}

// --- string helpers ---

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func timeStr(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}

func nilTimeStr(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return t.Format(time.RFC3339Nano)
}

func parentPath(p string) string {
	if p == "/" {
		return "/"
	}
	p = strings.TrimSuffix(p, "/")
	idx := strings.LastIndex(p, "/")
	if idx <= 0 {
		return "/"
	}
	return p[:idx+1]
}

func baseName(p string) string {
	p = strings.TrimSuffix(p, "/")
	idx := strings.LastIndex(p, "/")
	if idx < 0 {
		return p
	}
	return p[idx+1:]
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}
