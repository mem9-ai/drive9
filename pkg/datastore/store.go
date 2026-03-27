// Package datastore provides the tenant data-plane metadata store for dat9.
// P0 uses TiDB (via MySQL protocol) as a local stand-in for db9. Two core tables:
// file_nodes (dentry/path tree) and files (inode/file entity).
package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	ErrNotFound        = errors.New("not found")
	ErrUploadNotActive = errors.New("upload is not in UPLOADING state")
	ErrUploadExpired   = errors.New("upload has expired")
	ErrPathConflict    = errors.New("path already exists")
	ErrUploadConflict  = errors.New("active upload already exists for this path")
)

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
	ContentBlob    []byte
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

// Store is the metadata store backed by TiDB/MySQL (stand-in for db9).
type Store struct {
	db             *sql.DB
	hasContentBlob bool
}

func Open(dsn string) (*Store, error) {
	lower := strings.ToLower(dsn)
	if strings.Contains(lower, "multistatements=true") || strings.Contains(lower, "multistatements=1") {
		return nil, fmt.Errorf("multiStatements is not allowed in production DSN")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping db: %w", err)
	}
	s := &Store{db: db}
	s.hasContentBlob = s.columnExists("files", "content_blob")
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }

// InTx runs fn inside a database transaction. If fn returns an error, the
// transaction is rolled back; otherwise it is committed.
func (s *Store) InTx(fn func(tx *sql.Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) columnExists(table, column string) bool {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
		table, column).Scan(&count)
	return err == nil && count > 0
}

// --- file_nodes operations ---

func (s *Store) InsertNode(n *FileNode) error {
	_, err := s.db.Exec(`INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		n.NodeID, n.Path, n.ParentPath, n.Name, n.IsDirectory, nullStr(n.FileID), n.CreatedAt.UTC())
	if isUniqueViolation(err) {
		return ErrPathConflict
	}
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
	defer func() { _ = rows.Close() }()
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
	defer func() { _ = tx.Rollback() }()

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
	defer func() { _ = tx.Rollback() }()

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
			_ = rows.Close()
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
	_ = rows.Close()

	stmt, err := tx.Prepare(`UPDATE file_nodes SET path = ?, parent_path = ?, name = ? WHERE node_id = ?`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stmt.Close() }()

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

	now := time.Now().UTC()
	for i := len(ancestors) - 1; i >= 0; i-- {
		dirPath := ancestors[i]
		pp := parentPath(dirPath)
		name := baseName(dirPath)
		_, err := s.db.Exec(`INSERT INTO file_nodes
			(node_id, path, parent_path, name, is_directory, created_at)
			VALUES (?, ?, ?, ?, 1, ?)
			ON DUPLICATE KEY UPDATE node_id = node_id`,
			genID(), dirPath, pp, name, now)
		if err != nil && !isUniqueViolation(err) {
			return fmt.Errorf("ensure parent %s: %w", dirPath, err)
		}
	}
	return nil
}

// --- files operations ---

func (s *Store) InsertFile(f *File) error {
	if s.hasContentBlob {
		_, err := s.db.Exec(`INSERT INTO files
			(file_id, storage_type, storage_ref, content_blob, content_type, size_bytes, checksum_sha256,
			 revision, status, source_id, content_text, created_at, confirmed_at, expires_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.FileID, f.StorageType, f.StorageRef, nilBytes(f.ContentBlob), nullStr(f.ContentType),
			f.SizeBytes, nullStr(f.ChecksumSHA256), f.Revision, f.Status,
			nullStr(f.SourceID), nullStr(f.ContentText),
			f.CreatedAt.UTC(), nilTime(f.ConfirmedAt), nilTime(f.ExpiresAt))
		return err
	}
	_, err := s.db.Exec(`INSERT INTO files
		(file_id, storage_type, storage_ref, content_type, size_bytes, checksum_sha256,
		 revision, status, source_id, content_text, created_at, confirmed_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		f.FileID, f.StorageType, f.StorageRef, nullStr(f.ContentType),
		f.SizeBytes, nullStr(f.ChecksumSHA256), f.Revision, f.Status,
		nullStr(f.SourceID), nullStr(f.ContentText),
		f.CreatedAt.UTC(), nilTime(f.ConfirmedAt), nilTime(f.ExpiresAt))
	return err
}

func (s *Store) GetFile(fileID string) (*File, error) {
	if s.hasContentBlob {
		row := s.db.QueryRow(`SELECT file_id, storage_type, storage_ref, content_blob, content_type,
			size_bytes, checksum_sha256, revision, status, source_id, content_text,
			created_at, confirmed_at, expires_at
			FROM files WHERE file_id = ?`, fileID)
		return scanFileWithBlob(row)
	}
	row := s.db.QueryRow(`SELECT file_id, storage_type, storage_ref, content_type,
		size_bytes, checksum_sha256, revision, status, source_id, content_text,
		created_at, confirmed_at, expires_at
		FROM files WHERE file_id = ?`, fileID)
	return scanFileNoBlob(row)
}

func (s *Store) UpdateFileContent(fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64) error {
	var (
		res sql.Result
		err error
	)
	if s.hasContentBlob {
		res, err = s.db.Exec(`UPDATE files SET storage_type = ?, storage_ref = ?,
			content_blob = ?, content_type = ?, size_bytes = ?, checksum_sha256 = ?, content_text = ?,
			revision = revision + 1, status = 'CONFIRMED',
			confirmed_at = ?
			WHERE file_id = ?`,
			storageType, storageRef, nilBytes(contentBlob), nullStr(contentType), size,
			nullStr(checksum), nullStr(contentText), time.Now().UTC(), fileID)
	} else {
		res, err = s.db.Exec(`UPDATE files SET storage_type = ?, storage_ref = ?,
			content_type = ?, size_bytes = ?, checksum_sha256 = ?, content_text = ?,
			revision = revision + 1, status = 'CONFIRMED',
			confirmed_at = ?
			WHERE file_id = ?`,
			storageType, storageRef, nullStr(contentType), size,
			nullStr(checksum), nullStr(contentText), time.Now().UTC(), fileID)
	}
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
	return s.ConfirmFileTx(s.db, fileID)
}

// execer abstracts *sql.DB and *sql.Tx for shared query execution.
type execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

func (s *Store) ConfirmFileTx(db execer, fileID string) error {
	_, err := db.Exec(`UPDATE files SET status = 'CONFIRMED',
		confirmed_at = ?
		WHERE file_id = ? AND status = 'PENDING'`, time.Now().UTC(), fileID)
	return err
}

func (s *Store) CompleteUploadTx(db execer, uploadID string) error {
	_, err := db.Exec(`UPDATE uploads SET status = 'COMPLETED',
		updated_at = ?
		WHERE upload_id = ? AND status = 'UPLOADING'`, time.Now().UTC(), uploadID)
	return err
}

func (s *Store) EnsureParentDirsTx(db execer, path string, genID func() string) error {
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
		_, err := db.Exec(`INSERT INTO file_nodes
			(node_id, path, parent_path, name, is_directory, created_at)
			VALUES (?, ?, ?, ?, 1, ?)
			ON DUPLICATE KEY UPDATE node_id = node_id`,
			genID(), dirPath, pp, name, now.UTC())
		if err != nil && !isUniqueViolation(err) {
			return fmt.Errorf("ensure parent %s: %w", dirPath, err)
		}
	}
	return nil
}

func (s *Store) InsertNodeTx(db execer, n *FileNode) error {
	_, err := db.Exec(`INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		n.NodeID, n.Path, n.ParentPath, n.Name, n.IsDirectory, nullStr(n.FileID), n.CreatedAt.UTC())
	if isUniqueViolation(err) {
		return ErrPathConflict
	}
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
	q := `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		f.file_id, f.storage_type, f.storage_ref, `
	if s.hasContentBlob {
		q += `f.content_blob, `
	}
	q += `f.content_type, f.size_bytes,
		f.checksum_sha256, f.revision, f.status, f.source_id, f.content_text,
		f.created_at, f.confirmed_at, f.expires_at
		FROM file_nodes fn
		LEFT JOIN files f ON fn.file_id = f.file_id AND f.status = 'CONFIRMED'
		WHERE fn.parent_path = ?
		ORDER BY fn.name`
	rows, err := s.db.Query(q, parentPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var result []*NodeWithFile
	for rows.Next() {
		var nf *NodeWithFile
		if s.hasContentBlob {
			nf, err = scanNodeWithFileWithBlob(rows)
		} else {
			nf, err = scanNodeWithFileNoBlob(rows)
		}
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
	defer func() { _ = tx.Rollback() }()

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

	q := `SELECT file_id, storage_type, storage_ref, `
	if s.hasContentBlob {
		q += `content_blob, `
	}
	q += `content_type, size_bytes, checksum_sha256, revision, status, source_id, content_text,
		created_at, confirmed_at, expires_at
		FROM files WHERE file_id = ?`
	row := tx.QueryRow(q, fileID.String)
	var f *File
	if s.hasContentBlob {
		f, err = scanFileWithBlob(row)
	} else {
		f, err = scanFileNoBlob(row)
	}
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
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.Query(`SELECT DISTINCT file_id FROM file_nodes
		WHERE (path = ? OR path LIKE ?) AND file_id IS NOT NULL`, dirPath, dirPath+"%")
	if err != nil {
		return nil, err
	}
	var fileIDs []string
	for rows.Next() {
		var fid string
		if err := rows.Scan(&fid); err != nil {
			_ = rows.Close()
			return nil, err
		}
		fileIDs = append(fileIDs, fid)
	}
	_ = rows.Close()

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
		q := `SELECT file_id, storage_type, storage_ref, `
		if s.hasContentBlob {
			q += `content_blob, `
		}
		q += `content_type, size_bytes, checksum_sha256, revision, status, source_id, content_text,
			created_at, confirmed_at, expires_at
			FROM files WHERE file_id = ?`
		row := tx.QueryRow(q, fid)
		var f *File
		if s.hasContentBlob {
			f, err = scanFileWithBlob(row)
		} else {
			f, err = scanFileNoBlob(row)
		}
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
		u.CreatedAt.UTC(), u.UpdatedAt.UTC(), u.ExpiresAt.UTC())
	if isUniqueViolation(err) {
		return ErrUploadConflict
	}
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
		updated_at = ?
		WHERE upload_id = ? AND status = 'UPLOADING'`, time.Now().UTC(), uploadID)
	return err
}

func (s *Store) AbortUpload(uploadID string) error {
	_, err := s.db.Exec(`UPDATE uploads SET status = 'ABORTED',
		updated_at = ?
		WHERE upload_id = ? AND status = 'UPLOADING'`, time.Now().UTC(), uploadID)
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
	defer func() { _ = rows.Close() }()
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
	var createdAt time.Time
	err := s.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &fileID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	n.IsDirectory = isDir != 0
	n.FileID = fileID.String
	n.CreatedAt = createdAt.UTC()
	return &n, nil
}

func scanFileWithBlob(s scanner) (*File, error) {
	var f File
	var contentBlob []byte
	var contentType, checksum, sourceID, contentText sql.NullString
	var confirmedAt, expiresAt sql.NullTime
	var createdAt time.Time
	err := s.Scan(&f.FileID, &f.StorageType, &f.StorageRef, &contentBlob, &contentType,
		&f.SizeBytes, &checksum, &f.Revision, &f.Status, &sourceID, &contentText,
		&createdAt, &confirmedAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	f.ContentType = contentType.String
	f.ContentBlob = append([]byte(nil), contentBlob...)
	f.ChecksumSHA256 = checksum.String
	f.SourceID = sourceID.String
	f.ContentText = contentText.String
	f.CreatedAt = createdAt.UTC()
	if confirmedAt.Valid {
		t := confirmedAt.Time.UTC()
		f.ConfirmedAt = &t
	}
	if expiresAt.Valid {
		t := expiresAt.Time.UTC()
		f.ExpiresAt = &t
	}
	return &f, nil
}

func scanFileNoBlob(s scanner) (*File, error) {
	var f File
	var contentType, checksum, sourceID, contentText sql.NullString
	var confirmedAt, expiresAt sql.NullTime
	var createdAt time.Time
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
	f.CreatedAt = createdAt.UTC()
	if confirmedAt.Valid {
		t := confirmedAt.Time.UTC()
		f.ConfirmedAt = &t
	}
	if expiresAt.Valid {
		t := expiresAt.Time.UTC()
		f.ExpiresAt = &t
	}
	return &f, nil
}

func scanNodeWithFileWithBlob(rows *sql.Rows) (*NodeWithFile, error) {
	var n FileNode
	var isDir int
	var nodeFileID sql.NullString
	var nodeCreatedAt time.Time

	var fFileID, fStorageType, fStorageRef sql.NullString
	var fContentBlob []byte
	var fContentType, fChecksum, fSourceID, fContentText sql.NullString
	var fSizeBytes, fRevision sql.NullInt64
	var fStatus sql.NullString
	var fCreatedAt, fConfirmedAt, fExpiresAt sql.NullTime

	err := rows.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
		&fFileID, &fStorageType, &fStorageRef, &fContentBlob, &fContentType, &fSizeBytes,
		&fChecksum, &fRevision, &fStatus, &fSourceID, &fContentText,
		&fCreatedAt, &fConfirmedAt, &fExpiresAt)
	if err != nil {
		return nil, err
	}

	n.IsDirectory = isDir != 0
	n.FileID = nodeFileID.String
	n.CreatedAt = nodeCreatedAt.UTC()

	nf := &NodeWithFile{Node: n}
	if fFileID.Valid {
		nf.File = &File{
			FileID:         fFileID.String,
			StorageType:    StorageType(fStorageType.String),
			StorageRef:     fStorageRef.String,
			ContentBlob:    append([]byte(nil), fContentBlob...),
			ContentType:    fContentType.String,
			SizeBytes:      fSizeBytes.Int64,
			ChecksumSHA256: fChecksum.String,
			Revision:       fRevision.Int64,
			Status:         FileStatus(fStatus.String),
			SourceID:       fSourceID.String,
			ContentText:    fContentText.String,
		}
		if fCreatedAt.Valid {
			nf.File.CreatedAt = fCreatedAt.Time.UTC()
		}
		if fConfirmedAt.Valid {
			t := fConfirmedAt.Time.UTC()
			nf.File.ConfirmedAt = &t
		}
		if fExpiresAt.Valid {
			t := fExpiresAt.Time.UTC()
			nf.File.ExpiresAt = &t
		}
	}
	return nf, nil
}

func scanNodeWithFileNoBlob(rows *sql.Rows) (*NodeWithFile, error) {
	var n FileNode
	var isDir int
	var nodeFileID sql.NullString
	var nodeCreatedAt time.Time

	var fFileID, fStorageType, fStorageRef sql.NullString
	var fContentType, fChecksum, fSourceID, fContentText sql.NullString
	var fSizeBytes, fRevision sql.NullInt64
	var fStatus sql.NullString
	var fCreatedAt, fConfirmedAt, fExpiresAt sql.NullTime

	err := rows.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
		&fFileID, &fStorageType, &fStorageRef, &fContentType, &fSizeBytes,
		&fChecksum, &fRevision, &fStatus, &fSourceID, &fContentText,
		&fCreatedAt, &fConfirmedAt, &fExpiresAt)
	if err != nil {
		return nil, err
	}

	n.IsDirectory = isDir != 0
	n.FileID = nodeFileID.String
	n.CreatedAt = nodeCreatedAt.UTC()

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
			nf.File.CreatedAt = fCreatedAt.Time.UTC()
		}
		if fConfirmedAt.Valid {
			t := fConfirmedAt.Time.UTC()
			nf.File.ConfirmedAt = &t
		}
		if fExpiresAt.Valid {
			t := fExpiresAt.Time.UTC()
			nf.File.ExpiresAt = &t
		}
	}
	return nf, nil
}

func nilBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func scanUpload(s scanner) (*Upload, error) {
	var u Upload
	var fingerprint, idempotencyKey sql.NullString
	var createdAt, updatedAt, expiresAt time.Time
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
	u.CreatedAt = createdAt.UTC()
	u.UpdatedAt = updatedAt.UTC()
	u.ExpiresAt = expiresAt.UTC()
	return &u, nil
}

// --- string helpers ---

func nullStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nilTime(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return t.UTC()
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
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Duplicate entry") || strings.Contains(msg, "UNIQUE constraint failed")
}

func (s *Store) ExecSQL(ctx context.Context, query string) ([]map[string]interface{}, error) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	isSelect := strings.HasPrefix(upper, "SELECT")
	if strings.HasPrefix(upper, "WITH") {
		hasDML := strings.Contains(upper, "INSERT") ||
			strings.Contains(upper, "UPDATE") ||
			strings.Contains(upper, "DELETE") ||
			strings.Contains(upper, "DROP") ||
			strings.Contains(upper, "ALTER") ||
			strings.Contains(upper, "TRUNCATE")
		if !hasDML {
			isSelect = true
		}
	}
	isTagWrite := strings.HasPrefix(upper, "INSERT INTO FILE_TAGS") ||
		strings.HasPrefix(upper, "UPDATE FILE_TAGS") ||
		strings.HasPrefix(upper, "DELETE FROM FILE_TAGS")

	if isTagWrite {
		if strings.HasPrefix(upper, "UPDATE") || strings.HasPrefix(upper, "DELETE") {
			for _, t := range []string{"FILE_NODES", "FILES", "UPLOADS"} {
				if strings.Contains(upper, t) {
					return nil, fmt.Errorf("DML on file_tags must not reference other tables")
				}
			}
		}
	}

	if !isSelect && !isTagWrite {
		return nil, fmt.Errorf("only SELECT queries and INSERT/UPDATE/DELETE on file_tags are allowed")
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if isTagWrite {
		res, err := s.db.ExecContext(ctx, q)
		if err != nil {
			return nil, err
		}
		affected, _ := res.RowsAffected()
		return []map[string]interface{}{{"rows_affected": affected}}, nil
	}

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	const maxRows = 1000
	var result []map[string]interface{}
	for rows.Next() {
		if len(result) >= maxRows {
			break
		}
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			v := vals[i]
			if b, ok := v.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = v
			}
		}
		result = append(result, row)
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	return result, rows.Err()
}
