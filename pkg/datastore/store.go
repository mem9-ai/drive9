// Package datastore provides the tenant data-plane metadata store for dat9.
// P0 uses TiDB (via MySQL protocol) as a local stand-in for db9. Two core tables:
// file_nodes (dentry/path tree) and files (inode/file entity).
package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
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
	UploadInitiated UploadStatus = "INITIATED"
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
	// EmbeddingRevision tracks which file revision produced the stored embedding.
	EmbeddingRevision *int64
	Status            FileStatus
	SourceID          string
	ContentText       string
	CreatedAt         time.Time
	ConfirmedAt       *time.Time
	ExpiresAt         *time.Time
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
	ChecksumAlgo   string // "SHA256" or "CRC32C"; empty defaults to "SHA256"
	FingerprintSHA string
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	ExpiresAt      time.Time
}

// Store is the metadata store backed by TiDB/MySQL (stand-in for db9).
type Store struct {
	db *sql.DB
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
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }
func (s *Store) DB() *sql.DB  { return s.db }

// InTx runs fn inside a database transaction. If fn returns an error, the
// transaction is rolled back; otherwise it is committed.
func (s *Store) InTx(ctx context.Context, fn func(tx *sql.Tx) error) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "in_tx", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	err = tx.Commit()
	return err
}

func (s *Store) columnExists(table, column string) bool {
	var count int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
		table, column).Scan(&count)
	return err == nil && count > 0
}

// --- file_nodes operations ---

func (s *Store) InsertNode(ctx context.Context, n *FileNode) error {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "insert_node", start, &opErr)

	_, err := s.db.ExecContext(ctx, `INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		n.NodeID, n.Path, n.ParentPath, n.Name, n.IsDirectory, nullStr(n.FileID), n.CreatedAt.UTC())
	if isUniqueViolation(err) {
		opErr = ErrPathConflict
		return ErrPathConflict
	}
	opErr = err
	return err
}

func (s *Store) GetNode(ctx context.Context, path string) (*FileNode, error) {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "get_node", start, &opErr)

	row := s.db.QueryRowContext(ctx, `SELECT node_id, path, parent_path, name, is_directory, file_id, created_at
		FROM file_nodes WHERE path = ?`, path)
	n, err := scanNode(row)
	opErr = err
	return n, err
}

func (s *Store) ListNodes(ctx context.Context, parentPath string) (out []*FileNode, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "list_nodes", start, &err)

	rows, err := s.db.QueryContext(ctx, `SELECT node_id, path, parent_path, name, is_directory, file_id, created_at
		FROM file_nodes WHERE parent_path = ? ORDER BY name`, parentPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	nodes := make([]*FileNode, 0)
	for rows.Next() {
		n, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, n)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out = nodes
	return out, nil
}

func (s *Store) DeleteNode(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "delete_node", start, &err)

	res, err := s.db.ExecContext(ctx, `DELETE FROM file_nodes WHERE path = ?`, path)
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
func (s *Store) DeleteEmptyDir(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "delete_empty_dir", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
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
	err = tx.Commit()
	return err
}

func (s *Store) DeleteNodesByPrefix(ctx context.Context, prefix string) (n int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "delete_nodes_by_prefix", start, &err)

	res, err := s.db.ExecContext(ctx, `DELETE FROM file_nodes WHERE path = ? OR path LIKE ?`,
		prefix, prefix+"%")
	if err != nil {
		return 0, err
	}
	n, err = res.RowsAffected()
	return n, err
}

func (s *Store) UpdateNodePath(ctx context.Context, oldPath, newPath, newParentPath, newName string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_node_path", start, &err)

	res, err := s.db.ExecContext(ctx, `UPDATE file_nodes SET path = ?, parent_path = ?, name = ?
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

func (s *Store) RenameDir(ctx context.Context, oldPrefix, newPrefix string) (count int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "rename_dir", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
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
	count = int64(len(updates))
	return count, nil
}

func (s *Store) RefCount(ctx context.Context, fileID string) (count int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ref_count", start, &err)

	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM file_nodes WHERE file_id = ?`, fileID).Scan(&count)
	return count, err
}

func (s *Store) EnsureParentDirs(ctx context.Context, path string, genID func() string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "ensure_parent_dirs", start, &err)

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
		_, err := s.db.ExecContext(ctx, `INSERT INTO file_nodes
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

func (s *Store) InsertFile(ctx context.Context, f *File) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "insert_file", start, &err)

	_, err = s.db.ExecContext(ctx, `INSERT INTO files
		(file_id, storage_type, storage_ref, content_blob, content_type, size_bytes, checksum_sha256,
		 revision, status, source_id, content_text, created_at, confirmed_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		f.FileID, f.StorageType, f.StorageRef, nilBytes(f.ContentBlob), nullStr(f.ContentType),
		f.SizeBytes, nullStr(f.ChecksumSHA256), f.Revision, f.Status,
		nullStr(f.SourceID), nullStr(f.ContentText),
		f.CreatedAt.UTC(), nilTime(f.ConfirmedAt), nilTime(f.ExpiresAt))
	return err
}

func (s *Store) GetFile(ctx context.Context, fileID string) (out *File, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "get_file", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT file_id, storage_type, storage_ref, content_blob, content_type,
		size_bytes, checksum_sha256, revision, embedding_revision, status, source_id, content_text,
		created_at, confirmed_at, expires_at
		FROM files WHERE file_id = ?`, fileID)
	out, err = scanFileWithBlob(row)
	return out, err
}

func (s *Store) UpdateFileContent(ctx context.Context, fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64) (newRevision int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_file_content", start, &err)

	res, err := s.db.ExecContext(ctx, `UPDATE files SET storage_type = ?, storage_ref = ?,
		content_blob = ?, content_type = ?, size_bytes = ?, checksum_sha256 = ?, content_text = ?,
		revision = revision + 1, status = 'CONFIRMED',
		confirmed_at = ?
		WHERE file_id = ?`,
		storageType, storageRef, nilBytes(contentBlob), nullStr(contentType), size,
		nullStr(checksum), nullStr(contentText), time.Now().UTC(), fileID)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return 0, ErrNotFound
	}
	var rev int64
	if err := s.db.QueryRowContext(ctx, `SELECT revision FROM files WHERE file_id = ?`, fileID).Scan(&rev); err != nil {
		return 0, fmt.Errorf("read revision after update: %w", err)
	}
	return rev, nil
}

// UpdateFileSearchText updates files.content_text for search enrichment.
// When expectedRevision is positive, the update applies only to that revision.
// This prevents stale async tasks from overwriting a newer file revision.
func (s *Store) UpdateFileSearchText(ctx context.Context, fileID string, expectedRevision int64, contentText string) (updated bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_file_search_text", start, &err)

	res, err := s.updateFileSearchTextExec(s.db, fileID, expectedRevision, contentText)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// UpdateFileSearchTextTx updates files.content_text inside an existing
// transaction while preserving the revision gate used by async image tasks.
func (s *Store) UpdateFileSearchTextTx(db execer, fileID string, expectedRevision int64, contentText string) (bool, error) {
	res, err := s.updateFileSearchTextExec(db, fileID, expectedRevision, contentText)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) updateFileSearchTextExec(db execer, fileID string, expectedRevision int64, contentText string) (sql.Result, error) {
	if expectedRevision > 0 {
		return db.Exec(`UPDATE files SET content_text = ?
			WHERE file_id = ? AND status = 'CONFIRMED' AND revision = ?`,
			nullStr(contentText), fileID, expectedRevision)
	}
	return db.Exec(`UPDATE files SET content_text = ?
		WHERE file_id = ? AND status = 'CONFIRMED'`,
		nullStr(contentText), fileID)
}

func (s *Store) ConfirmFile(ctx context.Context, fileID string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "confirm_file", start, &err)

	_, err = s.db.ExecContext(ctx, `UPDATE files SET status = 'CONFIRMED',
		confirmed_at = ?
		WHERE file_id = ? AND status = 'PENDING'`, time.Now().UTC(), fileID)
	return err
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

func (s *Store) MarkFileDeleted(ctx context.Context, fileID string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "mark_file_deleted", start, &err)

	_, err = s.db.ExecContext(ctx, `UPDATE files SET status = 'DELETED' WHERE file_id = ?`, fileID)
	return err
}

// ConfirmedStorageBytesTx returns the total bytes occupied by confirmed file
// entities in the current tenant database.
func (s *Store) ConfirmedStorageBytesTx(db execer) (int64, error) {
	var total sql.NullInt64
	if err := db.QueryRow(`SELECT COALESCE(SUM(size_bytes), 0) FROM files WHERE status = 'CONFIRMED'`).Scan(&total); err != nil {
		return 0, err
	}
	return total.Int64, nil
}

// ActiveUploadReservedBytesTx returns the additional bytes reserved by active
// multipart uploads beyond what is already counted by the confirmed file set.
func (s *Store) ActiveUploadReservedBytesTx(db execer) (int64, error) {
	var total sql.NullInt64
	// TODO: This aggregation joins uploads -> file_nodes -> files on every quota
	// check. If upload concurrency grows, re-evaluate the access path and add a
	// more targeted uploads status/index strategy or a pre-aggregated quota state.
	err := db.QueryRow(`SELECT COALESCE(SUM(
		CASE
			WHEN u.total_size > COALESCE(f.size_bytes, 0) THEN u.total_size - COALESCE(f.size_bytes, 0)
			ELSE 0
		END
	), 0)
		FROM uploads u
		LEFT JOIN file_nodes fn ON fn.path = u.target_path
		LEFT JOIN files f ON f.file_id = fn.file_id AND f.status = 'CONFIRMED'
		WHERE u.status IN ('INITIATED', 'UPLOADING') AND u.expires_at > ?`, time.Now().UTC()).Scan(&total)
	if err != nil {
		return 0, err
	}
	return total.Int64, nil
}

// ConfirmedFileSizeByPathTx returns the current confirmed file size at path.
// Missing paths and directories report zero bytes.
func (s *Store) ConfirmedFileSizeByPathTx(db execer, path string) (int64, error) {
	var size sql.NullInt64
	err := db.QueryRow(`SELECT f.size_bytes
		FROM file_nodes fn
		JOIN files f ON f.file_id = fn.file_id
		WHERE fn.path = ? AND fn.is_directory = 0 AND f.status = 'CONFIRMED'
		LIMIT 1`, path).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return size.Int64, nil
}

// --- composite operations ---

func (s *Store) Stat(ctx context.Context, path string) (out *NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "stat", start, &err)

	node, err := s.GetNode(ctx, path)
	if err != nil {
		return nil, err
	}
	nf := &NodeWithFile{Node: *node}
	if !node.IsDirectory && node.FileID != "" {
		f, err := s.GetFile(ctx, node.FileID)
		if err != nil {
			return nil, err
		}
		nf.File = f
	}
	out = nf
	return out, nil
}

func (s *Store) ListDir(ctx context.Context, parentPath string) (out []*NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "list_dir", start, &err)

	// TODO(#110): ReadDir only needs lightweight file metadata. Split this into a
	// metadata-only listing path so directory scans do not fetch or copy content_blob.
	q := `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		f.file_id, f.storage_type, f.storage_ref, f.content_blob, f.content_type, f.size_bytes,
		f.checksum_sha256, f.revision, f.embedding_revision, f.status, f.source_id, f.content_text,
		f.created_at, f.confirmed_at, f.expires_at
		FROM file_nodes fn
		LEFT JOIN files f ON fn.file_id = f.file_id AND f.status = 'CONFIRMED'
		WHERE fn.parent_path = ?
		ORDER BY fn.name`
	rows, err := s.db.QueryContext(ctx, q, parentPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	result := make([]*NodeWithFile, 0)
	for rows.Next() {
		nf, err := scanNodeWithFileWithBlob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, nf)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out = result
	return out, nil
}

func (s *Store) DeleteFileWithRefCheck(ctx context.Context, path string) (out *File, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "delete_file_with_ref_check", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
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

	row := tx.QueryRow(`SELECT file_id, storage_type, storage_ref, content_blob, content_type,
		size_bytes, checksum_sha256, revision, embedding_revision, status, source_id, content_text,
		created_at, confirmed_at, expires_at
		FROM files WHERE file_id = ?`, fileID.String)
	f, err := scanFileWithBlob(row)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	out = f
	return out, nil
}

func (s *Store) DeleteDirRecursive(ctx context.Context, dirPath string) (out []*File, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "delete_dir_recursive", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
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
		row := tx.QueryRow(`SELECT file_id, storage_type, storage_ref, content_blob, content_type,
			size_bytes, checksum_sha256, revision, embedding_revision, status, source_id, content_text,
			created_at, confirmed_at, expires_at
			FROM files WHERE file_id = ?`, fid)
		f, err := scanFileWithBlob(row)
		if err != nil {
			return nil, err
		}
		orphaned = append(orphaned, f)
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	out = orphaned
	return out, nil
}

// --- uploads operations ---

func (s *Store) InsertUpload(ctx context.Context, u *Upload) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "insert_upload", start, &err)

	err = s.InsertUploadTx(s.db, u)
	return err
}

func (s *Store) InsertUploadTx(db execer, u *Upload) error {
	algo := u.ChecksumAlgo
	if algo == "" {
		algo = "SHA256"
	}
	_, err := db.Exec(`INSERT INTO uploads
		(upload_id, file_id, target_path, s3_upload_id, s3_key, total_size, part_size,
		 parts_total, status, checksum_algo, fingerprint_sha256, idempotency_key, created_at, updated_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.UploadID, u.FileID, u.TargetPath, u.S3UploadID, u.S3Key,
		u.TotalSize, u.PartSize, u.PartsTotal, u.Status, algo,
		nullStr(u.FingerprintSHA), nullStr(u.IdempotencyKey),
		u.CreatedAt.UTC(), u.UpdatedAt.UTC(), u.ExpiresAt.UTC())
	if isUniqueViolation(err) {
		return ErrUploadConflict
	}
	return err
}

func (s *Store) GetUpload(ctx context.Context, uploadID string) (out *Upload, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "get_upload", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		total_size, part_size, parts_total, status, checksum_algo, fingerprint_sha256, idempotency_key,
		created_at, updated_at, expires_at
		FROM uploads WHERE upload_id = ?`, uploadID)
	out, err = scanUpload(row)
	return out, err
}

func (s *Store) GetUploadByPath(ctx context.Context, targetPath string) (out *Upload, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "get_upload_by_path", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		total_size, part_size, parts_total, status, checksum_algo, fingerprint_sha256, idempotency_key,
		created_at, updated_at, expires_at
		FROM uploads WHERE target_path = ? AND status IN ('INITIATED', 'UPLOADING') AND expires_at > ?
		ORDER BY created_at DESC LIMIT 1`, targetPath, time.Now().UTC())
	out, err = scanUpload(row)
	return out, err
}

func (s *Store) CompleteUpload(ctx context.Context, uploadID string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "complete_upload", start, &err)

	_, err = s.db.ExecContext(ctx, `UPDATE uploads SET status = 'COMPLETED',
		updated_at = ?
		WHERE upload_id = ? AND status = 'UPLOADING'`, time.Now().UTC(), uploadID)
	return err
}

func (s *Store) AbortUpload(ctx context.Context, uploadID string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "abort_upload", start, &err)

	_, err = s.db.ExecContext(ctx, `UPDATE uploads SET status = 'ABORTED',
		updated_at = ?
		WHERE upload_id = ? AND status = 'UPLOADING'`, time.Now().UTC(), uploadID)
	return err
}

// AbortUploadV2 sets an upload to ABORTED from either INITIATED or UPLOADING.
// Returns nil (idempotent) if the upload is already aborted or not found.
func (s *Store) AbortUploadV2(ctx context.Context, uploadID string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "abort_upload_v2", start, &err)

	_, err = s.db.ExecContext(ctx, `UPDATE uploads SET status = 'ABORTED',
		updated_at = ?
		WHERE upload_id = ? AND status IN ('INITIATED', 'UPLOADING')`, time.Now().UTC(), uploadID)
	return err
}

// UpdateUploadStatus transitions an upload to a new status.
func (s *Store) UpdateUploadStatus(ctx context.Context, uploadID string, status UploadStatus) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_upload_status", start, &err)

	_, err = s.db.ExecContext(ctx, `UPDATE uploads SET status = ?,
		updated_at = ?
		WHERE upload_id = ?`, string(status), time.Now().UTC(), uploadID)
	return err
}

// TransitionUploadStatus atomically transitions an upload from expectedStatus to newStatus.
// Returns ErrUploadNotActive if the current status doesn't match expectedStatus.
func (s *Store) TransitionUploadStatus(ctx context.Context, uploadID string, expectedStatus, newStatus UploadStatus) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "transition_upload_status", start, &err)

	res, err := s.db.ExecContext(ctx, `UPDATE uploads SET status = ?,
		updated_at = ?
		WHERE upload_id = ? AND status = ?`, string(newStatus), time.Now().UTC(), uploadID, string(expectedStatus))
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrUploadNotActive
	}
	return nil
}

func (s *Store) ListUploadsByPath(ctx context.Context, targetPath string, status UploadStatus) (out []*Upload, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "list_uploads_by_path", start, &err)

	rows, err := s.db.QueryContext(ctx, `SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		total_size, part_size, parts_total, status, checksum_algo, fingerprint_sha256, idempotency_key,
		created_at, updated_at, expires_at
		FROM uploads WHERE target_path = ? AND status = ?
		ORDER BY created_at DESC`, targetPath, status)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	uploads := make([]*Upload, 0)
	for rows.Next() {
		u, err := scanUpload(rows)
		if err != nil {
			return nil, err
		}
		uploads = append(uploads, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out = uploads
	return out, nil
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
	var embeddingRevision sql.NullInt64
	var confirmedAt, expiresAt sql.NullTime
	var createdAt time.Time
	err := s.Scan(&f.FileID, &f.StorageType, &f.StorageRef, &contentBlob, &contentType,
		&f.SizeBytes, &checksum, &f.Revision, &embeddingRevision, &f.Status, &sourceID, &contentText,
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
	if embeddingRevision.Valid {
		rev := embeddingRevision.Int64
		f.EmbeddingRevision = &rev
	}
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
	var fSizeBytes, fRevision, fEmbeddingRevision sql.NullInt64
	var fStatus sql.NullString
	var fCreatedAt, fConfirmedAt, fExpiresAt sql.NullTime

	err := rows.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
		&fFileID, &fStorageType, &fStorageRef, &fContentBlob, &fContentType, &fSizeBytes,
		&fChecksum, &fRevision, &fEmbeddingRevision, &fStatus, &fSourceID, &fContentText,
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
		if fEmbeddingRevision.Valid {
			rev := fEmbeddingRevision.Int64
			nf.File.EmbeddingRevision = &rev
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
	var checksumAlgo sql.NullString
	var fingerprint, idempotencyKey sql.NullString
	var createdAt, updatedAt, expiresAt time.Time
	err := s.Scan(&u.UploadID, &u.FileID, &u.TargetPath, &u.S3UploadID, &u.S3Key,
		&u.TotalSize, &u.PartSize, &u.PartsTotal, &u.Status,
		&checksumAlgo, &fingerprint, &idempotencyKey,
		&createdAt, &updatedAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	u.ChecksumAlgo = checksumAlgo.String
	if u.ChecksumAlgo == "" {
		u.ChecksumAlgo = "SHA256"
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

var wsNorm = regexp.MustCompile(`\s+`)

func normalizeSQL(s string) string {
	return wsNorm.ReplaceAllString(strings.TrimSpace(s), " ")
}

func observeStoreOp(ctx context.Context, op string, start time.Time, errp *error) {
	result := "ok"
	if errp != nil && *errp != nil {
		switch {
		case errors.Is(*errp, ErrNotFound):
			result = "not_found"
		case errors.Is(*errp, ErrPathConflict), errors.Is(*errp, ErrUploadConflict):
			result = "conflict"
		default:
			result = "error"
		}
		logger.Error(ctx, "datastore_op_failed", zap.String("operation", op), zap.String("result", result), zap.Error(*errp))
	}
	metrics.RecordOperation("datastore", op, result, time.Since(start))
}

func (s *Store) ExecSQL(ctx context.Context, query string) (out []map[string]interface{}, err error) {
	start := time.Now()
	defer func() {
		result := "ok"
		if err != nil {
			result = "error"
		}
		metrics.RecordOperation("datastore", "exec_sql", result, time.Since(start))
	}()

	q := strings.TrimSpace(query)
	norm := strings.ToUpper(normalizeSQL(q))

	isSelect := strings.HasPrefix(norm, "SELECT")
	if strings.HasPrefix(norm, "WITH") {
		hasDML := strings.Contains(norm, "INSERT") ||
			strings.Contains(norm, "UPDATE") ||
			strings.Contains(norm, "DELETE") ||
			strings.Contains(norm, "DROP") ||
			strings.Contains(norm, "ALTER") ||
			strings.Contains(norm, "TRUNCATE")
		if !hasDML {
			isSelect = true
		}
	}
	isTagWrite := strings.HasPrefix(norm, "INSERT INTO FILE_TAGS") ||
		strings.HasPrefix(norm, "UPDATE FILE_TAGS") ||
		strings.HasPrefix(norm, "DELETE FROM FILE_TAGS")

	if isTagWrite {
		if strings.HasPrefix(norm, "UPDATE") || strings.HasPrefix(norm, "DELETE") {
			if strings.Contains(norm, " JOIN ") || strings.Contains(norm, " USING ") {
				return nil, fmt.Errorf("multi-table DML not allowed; single-table statements on file_tags only")
			}
			if strings.HasPrefix(norm, "UPDATE") {
				setIdx := strings.Index(norm, " SET ")
				if setIdx > 0 && strings.Contains(norm[:setIdx], ",") {
					return nil, fmt.Errorf("multi-table DML not allowed; single-table statements on file_tags only")
				}
			}
			if strings.HasPrefix(norm, "DELETE") {
				fromIdx := strings.Index(norm, " FROM ")
				if fromIdx > 0 {
					rest := norm[fromIdx+6:]
					endIdx := strings.IndexAny(rest, " ;")
					if endIdx < 0 {
						endIdx = len(rest)
					}
					tablePart := rest[:endIdx]
					if strings.Contains(tablePart, ",") {
						return nil, fmt.Errorf("multi-table DML not allowed; single-table statements on file_tags only")
					}
				}
			}
		}
	}

	if !isSelect && !isTagWrite {
		err = fmt.Errorf("only SELECT queries and INSERT/UPDATE/DELETE on file_tags are allowed")
		logger.Warn(ctx, "datastore_exec_sql_rejected", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	if s == nil || s.db == nil {
		err = fmt.Errorf("database is closed")
		logger.Error(ctx, "datastore_exec_sql_closed", zap.Error(err))
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if isTagWrite {
		res, err := s.db.ExecContext(ctx, q)
		if err != nil {
			logger.Error(ctx, "datastore_exec_sql_tag_write_failed", zap.Int("query_len", len(q)), zap.Error(err))
			return nil, err
		}
		affected, _ := res.RowsAffected()
		return []map[string]interface{}{{"rows_affected": affected}}, nil
	}

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		logger.Error(ctx, "datastore_exec_sql_query_failed", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	const maxRows = 1000
	result := make([]map[string]interface{}, 0)
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
	if err := rows.Err(); err != nil {
		logger.Error(ctx, "datastore_exec_sql_scan_failed", zap.Int("query_len", len(q)), zap.Error(err))
		return nil, err
	}
	return result, nil
}
