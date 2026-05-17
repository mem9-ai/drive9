// Package datastore provides the tenant data-plane metadata store for dat9.
// Core metadata is split across file_nodes (dentry/path tree), inodes,
// contents, and semantic. The legacy files table is still supported for
// existing tenants during the split-table transition.
package datastore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/mysqlutil"
	"go.uber.org/zap"
)

var (
	ErrNotFound                = errors.New("not found")
	ErrUploadNotActive         = errors.New("upload is not in UPLOADING state")
	ErrUploadExpired           = errors.New("upload has expired")
	ErrPathConflict            = errors.New("path already exists")
	ErrUploadConflict          = errors.New("active upload already exists for this path")
	ErrIdempotencyConflict     = errors.New("duplicate idempotency key")
	ErrJournalConflict         = errors.New("journal create conflict")
	ErrJournalClosed           = errors.New("journal is closed")
	ErrJournalValidation       = errors.New("journal validation failed")
	ErrJournalPayloadTooLarge  = errors.New("journal payload too large")
	ErrRevisionConflict        = errors.New("revision conflict")
	ErrFileGCTaskLeaseMismatch = errors.New("file gc task lease mismatch")
)

type StorageType string

const (
	StorageDB9 StorageType = "db9"
	StorageS3  StorageType = "s3"
)

type StorageEncryptionMode string

const (
	StorageEncryptionLegacy  StorageEncryptionMode = "legacy"
	StorageEncryptionNone    StorageEncryptionMode = "none"
	StorageEncryptionSSES3   StorageEncryptionMode = "sse-s3"
	StorageEncryptionSSEKMS  StorageEncryptionMode = "sse-kms"
	StorageEncryptionDSSEKMS StorageEncryptionMode = "dsse-kms"
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
	InodeID     string // links to inodes.inode_id; for directories, this is the directory's inode
	CreatedAt   time.Time
}

// File represents a row in the files table (inode).
type File struct {
	FileID                 string
	StorageType            StorageType
	StorageRef             string
	StorageEncryptionMode  StorageEncryptionMode
	StorageEncryptionKeyID string
	ContentBlob            []byte
	ContentType            string
	SizeBytes              int64
	ChecksumSHA256         string
	Revision               int64
	Mode                   uint32
	// EmbeddingRevision tracks which file revision produced the stored embedding.
	EmbeddingRevision *int64
	Status            FileStatus
	SourceID          string
	ContentText       string
	Description       string
	// DescriptionEmbeddingRevision tracks which file revision produced the stored description_embedding.
	DescriptionEmbeddingRevision *int64
	CreatedAt                    time.Time
	ConfirmedAt                  *time.Time
	ExpiresAt                    *time.Time
}

// NodeWithFile joins file_nodes and files for stat/read operations.
type NodeWithFile struct {
	Node    FileNode
	File    *File // nil for directories
	Mode    uint32
	HasMode bool
}

// Upload represents a row in the uploads table.
type Upload struct {
	UploadID               string
	FileID                 string
	TargetPath             string
	S3UploadID             string
	S3Key                  string
	StorageEncryptionMode  StorageEncryptionMode
	StorageEncryptionKeyID string
	TotalSize              int64
	PartSize               int64
	PartsTotal             int
	ExpectedRevision       *int64
	Status                 UploadStatus
	FingerprintSHA         string
	IdempotencyKey         string
	Description            string
	CreatedAt              time.Time
	UpdatedAt              time.Time
	ExpiresAt              time.Time
}

// Store is the metadata store backed by TiDB/MySQL (stand-in for db9).
type Store struct {
	db             *sql.DB
	useLegacyFiles bool // true when the legacy `files` table exists and needs dual-write
}

func Open(dsn string) (*Store, error) {
	lower := strings.ToLower(dsn)
	if strings.Contains(lower, "multistatements=true") || strings.Contains(lower, "multistatements=1") {
		return nil, fmt.Errorf("multiStatements is not allowed in production DSN")
	}
	db, err := mysqlutil.OpenInstrumented(context.Background(), dsn, mysqlutil.RoleUser)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	s := &Store{db: db}
	hasLegacy, err := s.detectLegacyFiles(context.Background())
	if err != nil {
		_ = mysqlutil.CloseInstrumented(db)
		return nil, fmt.Errorf("detect legacy files table: %w", err)
	}
	s.useLegacyFiles = hasLegacy
	return s, nil
}

func (s *Store) Close() error { return mysqlutil.CloseInstrumented(s.db) }
func (s *Store) DB() *sql.DB  { return s.db }

// HasLegacyFiles reports whether the legacy `files` table exists in this
// tenant database. When false, all writes skip the `files` table entirely
// and only target the split tables (inodes / contents / semantic).
func (s *Store) HasLegacyFiles() bool { return s.useLegacyFiles }

func (s *Store) detectLegacyFiles(ctx context.Context) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables
		 WHERE table_schema = DATABASE() AND table_name = 'files'`).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

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
		`SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?`,
		table, column).Scan(&count)
	return err == nil && count > 0
}

// --- file_nodes operations ---

func (s *Store) InsertNode(ctx context.Context, n *FileNode) error {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "insert_node", start, &opErr)

	_, err := s.db.ExecContext(ctx, `INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, inode_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		n.NodeID, n.Path, n.ParentPath, n.Name, n.IsDirectory, nullStr(n.FileID), nullStr(n.InodeID), n.CreatedAt.UTC())
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

	row := s.db.QueryRowContext(ctx, `SELECT node_id, path, parent_path, name, is_directory, file_id, inode_id, created_at
		FROM file_nodes WHERE path = ?`, path)
	n, err := scanNode(row)
	opErr = err
	return n, err
}

func (s *Store) ListNodes(ctx context.Context, parentPath string) (out []*FileNode, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "list_nodes", start, &err)

	rows, err := s.db.QueryContext(ctx, `SELECT node_id, path, parent_path, name, is_directory, file_id, inode_id, created_at
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

	hasChildren, err := dirHasChildrenTx(ctx, tx, path)
	if err != nil {
		return err
	}
	if hasChildren {
		return fmt.Errorf("directory not empty: %s", path)
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM file_nodes WHERE path = ? AND is_directory = 1`, path)
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

	where, args := pathPrefixPredicate("path", prefix)
	res, err := s.db.ExecContext(ctx, `DELETE FROM file_nodes WHERE `+where, args...)
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

// RenameFileReplacingTarget atomically renames a file node. If newPath already
// names a file, that target dentry is replaced and its file is marked deleted
// when no other nodes reference it.
func (s *Store) RenameFileReplacingTarget(ctx context.Context, oldPath, newPath, newParentPath, newName string) (out *File, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "rename_file_replacing_target", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	type nodeRef struct {
		nodeID string
		fileID sql.NullString
		isDir  bool
	}
	var old nodeRef
	err = tx.QueryRow(`SELECT node_id, file_id, is_directory FROM file_nodes WHERE path = ? FOR UPDATE`, oldPath).
		Scan(&old.nodeID, &old.fileID, &old.isDir)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	if old.isDir {
		return nil, fmt.Errorf("source is a directory: %s", oldPath)
	}
	if oldPath == newPath {
		return nil, tx.Commit()
	}

	var dst nodeRef
	hasDst := false
	err = tx.QueryRow(`SELECT node_id, file_id, is_directory FROM file_nodes WHERE path = ? FOR UPDATE`, newPath).
		Scan(&dst.nodeID, &dst.fileID, &dst.isDir)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	} else {
		hasDst = true
		if dst.isDir {
			return nil, ErrPathConflict
		}
		if dst.nodeID == old.nodeID {
			return nil, tx.Commit()
		}
		if _, err := tx.Exec(`DELETE FROM file_nodes WHERE node_id = ?`, dst.nodeID); err != nil {
			return nil, err
		}
	}

	res, err := tx.Exec(`UPDATE file_nodes SET path = ?, parent_path = ?, name = ? WHERE node_id = ?`,
		newPath, newParentPath, newName, old.nodeID)
	if isUniqueViolation(err) {
		return nil, ErrPathConflict
	}
	if err != nil {
		return nil, err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return nil, ErrNotFound
	}

	if hasDst && dst.fileID.Valid && dst.fileID.String != "" {
		var count int64
		if err := tx.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE file_id = ? FOR UPDATE`, dst.fileID.String).Scan(&count); err != nil {
			return nil, err
		}
		if count == 0 {
			if s.useLegacyFiles {
				if _, err := tx.Exec(`UPDATE files SET status = 'DELETED' WHERE file_id = ?`, dst.fileID.String); err != nil {
					return nil, err
				}
			}
			if _, err := tx.Exec(`UPDATE inodes SET status = 'DELETED' WHERE inode_id = ?`, dst.fileID.String); err != nil {
				return nil, err
			}
			if _, err := tx.Exec(`DELETE FROM file_tags WHERE file_id = ?`, dst.fileID.String); err != nil {
				return nil, err
			}
			f, err := s.scanFileForGCTx(tx, dst.fileID.String)
			if err != nil {
				return nil, err
			}
			task, err := NewFileGCTaskFromFile(f, time.Now().UTC())
			if err != nil {
				return nil, err
			}
			if _, err := s.EnqueueFileGCTaskTx(tx, task); err != nil {
				return nil, err
			}
			out = f
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
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

func (s *Store) EnsureParentDirs(ctx context.Context, path string, genID func() string) error {
	start := time.Now()
	var err error
	defer observeStoreOp(ctx, "ensure_parent_dirs", start, &err)

	// Run inside a transaction so the sequence of per-parent inserts is
	// atomic. Deadlocks can still happen when many goroutines contend for
	// the same unique-key lock on file_nodes.path; retry a bounded number
	// of times.
	const maxAttempts = 3
	for attempt := 0; attempt < maxAttempts; attempt++ {
		var tx *sql.Tx
		tx, err = s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		err = ensureParentDirsWithExecer(ctx, tx, path, genID)
		if err != nil {
			_ = tx.Rollback()
			if isDeadlock(err) && attempt < maxAttempts-1 {
				continue
			}
			return err
		}
		err = tx.Commit()
		if err != nil {
			_ = tx.Rollback()
			if isDeadlock(err) && attempt < maxAttempts-1 {
				continue
			}
			return err
		}
		return nil
	}
	return err
}

// --- files operations ---

func (s *Store) InsertFile(ctx context.Context, f *File) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "insert_file", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if s.useLegacyFiles {
		mode := fileStorageEncryptionModeForWrite(f.StorageEncryptionMode)
		_, err = tx.Exec(`INSERT INTO files
			(file_id, storage_type, storage_ref, storage_ref_hash, storage_encryption_mode, storage_encryption_key_id,
			 content_blob, content_type, size_bytes, checksum_sha256,
			 revision, status, source_id, content_text, description, created_at, confirmed_at, expires_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.FileID, f.StorageType, f.StorageRef, StorageRefHash(f.StorageRef), mode,
			storageEncryptionKeyIDForWrite(mode, f.StorageEncryptionKeyID), nilBytes(f.ContentBlob), nullStr(f.ContentType),
			f.SizeBytes, nullStr(f.ChecksumSHA256), f.Revision, f.Status,
			nullStr(f.SourceID), nullStr(f.ContentText), nullStr(f.Description),
			f.CreatedAt.UTC(), nilTime(f.ConfirmedAt), nilTime(f.ExpiresAt))
		if err != nil {
			return err
		}
	}

	// Dual-write to split tables
	if err := s.insertSplitTablesTx(tx, f); err != nil {
		return err
	}

	return tx.Commit()
}

// insertSplitTablesTx inserts corresponding rows into inodes, contents, and semantic.
func (s *Store) insertSplitTablesTx(tx execer, f *File) error {
	now := time.Now().UTC()
	mode := f.Mode
	if mode == 0 {
		mode = 0o644
	}
	inode := &Inode{
		InodeID:     f.FileID,
		SizeBytes:   f.SizeBytes,
		Revision:    f.Revision,
		Mode:        mode,
		Status:      f.Status,
		CreatedAt:   f.CreatedAt,
		Mtime:       coalesceTime(f.ConfirmedAt, now),
		ConfirmedAt: f.ConfirmedAt,
		ExpiresAt:   f.ExpiresAt,
	}
	if err := s.InsertInodeTx(tx, inode); err != nil {
		return fmt.Errorf("insert inode: %w", err)
	}
	content := &Content{
		InodeID:                f.FileID,
		StorageType:            f.StorageType,
		StorageRef:             f.StorageRef,
		StorageEncryptionMode:  f.StorageEncryptionMode,
		StorageEncryptionKeyID: f.StorageEncryptionKeyID,
		ContentBlob:            f.ContentBlob,
		ContentType:            f.ContentType,
		ChecksumSHA256:         f.ChecksumSHA256,
		SourceID:               f.SourceID,
	}
	if err := s.InsertContentTx(tx, content); err != nil {
		return fmt.Errorf("insert content: %w", err)
	}
	semantic := &Semantic{
		InodeID:                      f.FileID,
		ContentText:                  f.ContentText,
		Description:                  f.Description,
		EmbeddingRevision:            f.EmbeddingRevision,
		DescriptionEmbeddingRevision: f.DescriptionEmbeddingRevision,
	}
	if err := s.InsertSemanticTx(tx, semantic); err != nil {
		return fmt.Errorf("insert semantic: %w", err)
	}
	return nil
}

func (s *Store) GetFile(ctx context.Context, fileID string) (out *File, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "get_file", start, &err)

	inode, err := s.GetInode(ctx, fileID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	content, contentErr := s.GetContent(ctx, fileID)
	if contentErr != nil && !errors.Is(contentErr, sql.ErrNoRows) {
		return nil, fmt.Errorf("get content: %w", contentErr)
	}
	semantic, semanticErr := s.GetSemantic(ctx, fileID)
	if semanticErr != nil && !errors.Is(semanticErr, sql.ErrNoRows) {
		return nil, fmt.Errorf("get semantic: %w", semanticErr)
	}
	out = assembleFile(inode, content, semantic)
	return out, nil
}

// HasConfirmedS3StorageRef reports whether a confirmed S3 file still points at
// the exact storage ref. Callers pass the hash so the query can use
// idx_contents_storage_ref_hash and then compare the full ref to avoid trusting
// the hash as the identity.
func (s *Store) HasConfirmedS3StorageRef(ctx context.Context, storageRefHash, storageRef string) (out bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "has_confirmed_s3_storage_ref", start, &err)
	if storageRefHash == "" || storageRef == "" {
		return false, nil
	}
	row := s.db.QueryRowContext(ctx, `SELECT 1
		FROM contents c JOIN inodes i ON i.inode_id = c.inode_id
		WHERE c.storage_type = ? AND i.status = ? AND c.storage_ref_hash = ? AND c.storage_ref = ?
		LIMIT 1`, StorageS3, StatusConfirmed, storageRefHash, storageRef)
	var one int
	if err := row.Scan(&one); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *Store) UpdateFileContent(ctx context.Context, fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64, description string) (newRevision int64, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_file_content", start, &err)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC()
	var rev int64
	if s.useLegacyFiles {
		query := `UPDATE files SET storage_type = ?, storage_ref = ?, storage_ref_hash = ?,
			content_blob = ?, content_type = ?, size_bytes = ?, checksum_sha256 = ?, content_text = ?`
		args := []any{storageType, storageRef, StorageRefHash(storageRef), nilBytes(contentBlob), nullStr(contentType), size,
			nullStr(checksum), nullStr(contentText)}
		if description != "" {
			query += `, description = ?, description_embedding = NULL, description_embedding_revision = NULL`
			args = append(args, description)
		} else {
			query += `, description_embedding_revision = CASE
				WHEN description_embedding IS NOT NULL THEN revision + 1
				ELSE description_embedding_revision
				END`
		}
		query += `, revision = revision + 1, status = 'CONFIRMED', confirmed_at = ? WHERE file_id = ?`
		args = append(args, now, fileID)
		res, err := tx.Exec(query, args...)
		if err != nil {
			return 0, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return 0, ErrNotFound
		}
		if err := tx.QueryRow(`SELECT revision FROM files WHERE file_id = ?`, fileID).Scan(&rev); err != nil {
			return 0, fmt.Errorf("read revision after update: %w", err)
		}
	} else {
		var currentRev int64
		if err := tx.QueryRow(`SELECT revision FROM inodes WHERE inode_id = ? AND status = 'CONFIRMED' FOR UPDATE`, fileID).Scan(&currentRev); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return 0, ErrNotFound
			}
			return 0, fmt.Errorf("read current revision: %w", err)
		}
		rev = currentRev + 1
	}

	// Dual-write to split tables
	if err := s.UpdateInodeContentTx(tx, fileID, size, rev, StatusConfirmed, now); err != nil {
		return 0, fmt.Errorf("update inode: %w", err)
	}
	var encryptionMode StorageEncryptionMode
	if err := tx.QueryRow(`SELECT storage_encryption_mode FROM contents WHERE inode_id = ?`, fileID).Scan(&encryptionMode); err != nil {
		return 0, fmt.Errorf("read encryption mode: %w", err)
	}
	if err := s.UpdateContentTx(tx, fileID, storageType, storageRef, contentType, checksum, contentBlob, encryptionMode); err != nil {
		return 0, fmt.Errorf("update content: %w", err)
	}
	if err := s.UpdateSemanticTx(tx, fileID, contentText, description); err != nil {
		return 0, fmt.Errorf("update semantic: %w", err)
	}

	return rev, tx.Commit()
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
	if s.useLegacyFiles {
		var res sql.Result
		var err error
		if expectedRevision > 0 {
			res, err = db.Exec(`UPDATE files SET content_text = ?
				WHERE file_id = ? AND status = 'CONFIRMED' AND revision = ?`,
				nullStr(contentText), fileID, expectedRevision)
		} else {
			res, err = db.Exec(`UPDATE files SET content_text = ?
				WHERE file_id = ? AND status = 'CONFIRMED'`,
				nullStr(contentText), fileID)
		}
		if err != nil {
			return nil, err
		}
		// Dual-write to split tables
		if expectedRevision > 0 {
			if _, err := db.Exec(`UPDATE semantic SET content_text = ?
				WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND status = 'CONFIRMED' AND revision = ?)`,
				nullStr(contentText), fileID, fileID, expectedRevision); err != nil {
				return nil, fmt.Errorf("update semantic content_text: %w", err)
			}
			return res, nil
		}
		if _, err := db.Exec(`UPDATE semantic SET content_text = ?
			WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND status = 'CONFIRMED')`,
			nullStr(contentText), fileID, fileID); err != nil {
			return nil, fmt.Errorf("update semantic content_text: %w", err)
		}
		return res, nil
	}
	// New tenant without legacy files: write directly to semantic.
	if expectedRevision > 0 {
		return db.Exec(`UPDATE semantic SET content_text = ?
			WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND status = 'CONFIRMED' AND revision = ?)`,
			nullStr(contentText), fileID, fileID, expectedRevision)
	}
	return db.Exec(`UPDATE semantic SET content_text = ?
		WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND status = 'CONFIRMED')`,
		nullStr(contentText), fileID, fileID)
}

// ReplaceFileTagsTx replaces all tags for fileID inside an existing transaction.
//
// Callers should only invoke this when they intend to replace the tag set for
// the current write revision. Passing an empty map clears all existing tags.
// Callers that intend to preserve the current tag set must skip this call.
func (s *Store) ReplaceFileTagsTx(db execer, fileID string, tags map[string]string) error {
	if _, err := db.Exec(`DELETE FROM file_tags WHERE file_id = ?`, fileID); err != nil {
		return err
	}
	if len(tags) == 0 {
		return nil
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if _, err := db.Exec(`INSERT INTO file_tags (file_id, tag_key, tag_value) VALUES (?, ?, ?)`, fileID, k, tags[k]); err != nil {
			return err
		}
	}
	return nil
}

// ReplaceFileTagsByPrefixTx replaces only tags whose key starts with prefix
// inside an existing transaction.
//
// This is intended for system-owned tag namespaces. Passing an empty map clears
// that namespace while preserving all other file tags.
func (s *Store) ReplaceFileTagsByPrefixTx(db execer, fileID string, prefix string, tags map[string]string) error {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return fmt.Errorf("tag prefix is required")
	}
	pattern := likeLiteralPrefixPattern(prefix)
	if _, err := db.Exec(`DELETE FROM file_tags WHERE file_id = ? AND tag_key LIKE ? ESCAPE '\\'`, fileID, pattern); err != nil {
		return err
	}
	if len(tags) == 0 {
		return nil
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		if !strings.HasPrefix(k, prefix) {
			return fmt.Errorf("tag key %q does not match prefix %q", k, prefix)
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		if _, err := db.Exec(`INSERT INTO file_tags (file_id, tag_key, tag_value) VALUES (?, ?, ?)`, fileID, k, tags[k]); err != nil {
			return err
		}
	}
	return nil
}

func likeLiteralPrefixPattern(prefix string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(prefix) + "%"
}

// GetFileTags returns all tags for fileID.
func (s *Store) GetFileTags(ctx context.Context, fileID string) (out map[string]string, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "get_file_tags", start, &err)

	rows, err := s.db.QueryContext(ctx, `SELECT tag_key, tag_value FROM file_tags WHERE file_id = ? ORDER BY tag_key`, fileID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	tags := make(map[string]string)
	for rows.Next() {
		var k string
		var v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		tags[k] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out = tags
	return out, nil
}

// execer abstracts *sql.DB and *sql.Tx for shared query execution.
type execer interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// FileStorageMeta holds the lightweight storage metadata needed by upload
// overwrite logic, fetched with FOR UPDATE row locking.
type FileStorageMeta struct {
	StorageType StorageType
	StorageRef  string
	Revision    int64
	SizeBytes   int64
	ContentType string
}

// GetFileStorageMetaForUpdateTx returns confirmed file storage metadata with
// row-level locking inside an existing transaction.
func (s *Store) GetFileStorageMetaForUpdateTx(db execer, fileID string) (*FileStorageMeta, error) {
	var m FileStorageMeta
	var contentType sql.NullString
	err := db.QueryRow(`SELECT c.storage_type, c.storage_ref, i.revision, i.size_bytes, COALESCE(c.content_type, '')
		FROM inodes i JOIN contents c ON i.inode_id = c.inode_id
		WHERE i.inode_id = ? AND i.status = 'CONFIRMED' FOR UPDATE`, fileID).Scan(
		&m.StorageType, &m.StorageRef, &m.Revision, &m.SizeBytes, &contentType)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	m.ContentType = contentType.String
	return &m, nil
}

func (s *Store) CompleteUploadTx(db execer, uploadID string) error {
	_, err := db.Exec(`UPDATE uploads SET status = 'COMPLETED',
		updated_at = ?
		WHERE upload_id = ? AND status = 'UPLOADING'`, time.Now().UTC(), uploadID)
	return err
}

// Chmod updates the permission bits (mode) of the file or directory at path.
// It returns ErrNotFound if the path does not exist or the node has no
// associated inode record (e.g. an old directory created before the
// split-table migration).
func (s *Store) Chmod(ctx context.Context, path string, mode uint32) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "chmod", start, &err)

	node, err := s.GetNode(ctx, path)
	if err != nil {
		return err
	}
	inodeID := node.InodeID
	if inodeID == "" && !node.IsDirectory {
		inodeID = node.FileID
	}
	if inodeID == "" {
		return ErrNotFound
	}

	mode = mode & 0o777
	res, err := s.db.ExecContext(ctx, `UPDATE inodes SET mode = ? WHERE inode_id = ? AND status = 'CONFIRMED'`, mode, inodeID)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) EnsureParentDirsTx(db execer, path string, genID func() string) error {
	return ensureParentDirsWithExecer(context.Background(), db, path, genID)
}

// deterministicNodeID returns a stable 64-char hex ID derived from the path.
// Using a deterministic ID means concurrent creators for the same missing
// parent path will collide on the same inode_id; INSERT IGNORE swallows the
// duplicate, so no orphan inode is ever created. This works on TiDB as well
// as MySQL because it does not rely on gap locking.
func deterministicNodeID(path string) string {
	h := sha256.Sum256([]byte(path))
	return hex.EncodeToString(h[:])
}

// ensureParentDirsWithExecer is the shared implementation used by both
// EnsureParentDirs and EnsureParentDirsTx. It uses a deterministic inode_id
// derived from the path so concurrent attempts to create the same missing
// parent cannot leak orphan inodes, even on TiDB which does not support gap
// locking for SELECT ... FOR UPDATE on non-existing rows.
func ensureParentDirsWithExecer(ctx context.Context, db execer, path string, genID func() string) error {
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

		// Check if the directory already exists and has an inode_id.
		var existingInodeID sql.NullString
		selectErr := db.QueryRowContext(ctx,
			`SELECT inode_id FROM file_nodes WHERE path = ? AND is_directory = 1`,
			dirPath).Scan(&existingInodeID)
		if selectErr == nil && existingInodeID.Valid {
			// Already exists and has an inode_id — nothing to do.
			continue
		}
		if selectErr != nil && !errors.Is(selectErr, sql.ErrNoRows) {
			return fmt.Errorf("check parent dir %s: %w", dirPath, selectErr)
		}

		// Use a deterministic ID so concurrent creators for the same missing
		// parent path collide on the same inode_id. INSERT IGNORE swallows the
		// duplicate, so no orphan inode is ever created. This works on TiDB
		// as well as MySQL because it does not rely on gap locking.
		nodeID := deterministicNodeID(dirPath)
		_, err := db.ExecContext(ctx, `INSERT IGNORE INTO inodes
			(inode_id, size_bytes, revision, mode, status, created_at, mtime)
			VALUES (?, 0, 1, ?, 'CONFIRMED', ?, ?)`,
			nodeID, 0o755, now, now)
		if err != nil {
			return fmt.Errorf("ensure parent inode %s: %w", dirPath, err)
		}

		if selectErr == nil && !existingInodeID.Valid {
			// Directory row exists from pre-migration (inode_id was NULL).
			// Backfill the inode_id.
			_, err = db.ExecContext(ctx,
				`UPDATE file_nodes SET inode_id = ? WHERE path = ? AND is_directory = 1`,
				nodeID, dirPath)
			if err != nil {
				return fmt.Errorf("backfill parent inode_id %s: %w", dirPath, err)
			}
			continue
		}

		// Directory does not exist — insert the dentry.
		_, err = db.ExecContext(ctx, `INSERT INTO file_nodes
			(node_id, path, parent_path, name, is_directory, inode_id, created_at)
			VALUES (?, ?, ?, ?, 1, ?, ?)
			ON DUPLICATE KEY UPDATE node_id = node_id`,
			nodeID, dirPath, pp, name, nodeID, now)
		if err != nil && !isUniqueViolation(err) {
			return fmt.Errorf("ensure parent %s: %w", dirPath, err)
		}
	}
	return nil
}

func (s *Store) InsertNodeTx(db execer, n *FileNode) error {
	_, err := db.Exec(`INSERT INTO file_nodes (node_id, path, parent_path, name, is_directory, file_id, inode_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		n.NodeID, n.Path, n.ParentPath, n.Name, n.IsDirectory, nullStr(n.FileID), nullStr(n.InodeID), n.CreatedAt.UTC())
	if isUniqueViolation(err) {
		return ErrPathConflict
	}
	return err
}

func (s *Store) MarkFileDeleted(ctx context.Context, fileID string) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "mark_file_deleted", start, &err)

	if s.useLegacyFiles {
		_, err = s.db.ExecContext(ctx, `UPDATE files SET status = 'DELETED' WHERE file_id = ?`, fileID)
		if err != nil {
			return err
		}
	}
	_, err = s.db.ExecContext(ctx, `UPDATE inodes SET status = 'DELETED' WHERE inode_id = ?`, fileID)
	return err
}

// ConfirmedStorageBytesTx returns the total bytes occupied by confirmed file
// entities in the current tenant database.
func (s *Store) ConfirmedStorageBytesTx(db execer) (int64, error) {
	var total sql.NullInt64
	if err := db.QueryRow(`SELECT COALESCE(SUM(size_bytes), 0) FROM inodes WHERE status = 'CONFIRMED'`).Scan(&total); err != nil {
		return 0, err
	}
	return total.Int64, nil
}

// ConfirmedMediaFileCountTx returns the number of confirmed files whose
// content_type starts with "image/" or "audio/". This count drives the
// per-tenant media LLM budget gate: files that exceed the quota are still
// stored but their LLM extraction tasks (img_extract_text, audio_extract_text)
// are not enqueued.
func (s *Store) ConfirmedMediaFileCountTx(db execer) (int64, error) {
	var count sql.NullInt64
	if err := db.QueryRow(`SELECT COUNT(*) FROM inodes i JOIN contents c ON i.inode_id = c.inode_id WHERE i.status = 'CONFIRMED' AND (c.content_type LIKE 'image/%' OR c.content_type LIKE 'audio/%')`).Scan(&count); err != nil {
		return 0, err
	}
	return count.Int64, nil
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
			WHEN u.total_size > COALESCE(i.size_bytes, 0) THEN u.total_size - COALESCE(i.size_bytes, 0)
			ELSE 0
		END
	), 0)
		FROM uploads u
		LEFT JOIN file_nodes fn ON fn.path = u.target_path
		LEFT JOIN inodes i ON i.inode_id = COALESCE(fn.inode_id, fn.file_id) AND i.status = 'CONFIRMED'
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
	err := db.QueryRow(`SELECT i.size_bytes
		FROM file_nodes fn
		JOIN inodes i ON i.inode_id = COALESCE(fn.inode_id, fn.file_id)
		WHERE fn.path = ? AND fn.is_directory = 0 AND i.status = 'CONFIRMED'
		LIMIT 1`, path).Scan(&size)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return size.Int64, nil
}

// ConfirmedFileSummary holds the minimal per-file info needed for quota backfill.
type ConfirmedFileSummary struct {
	FileID      string
	SizeBytes   int64
	ContentType string
}

// ListConfirmedFileSummaries returns confirmed files in cursor-paginated batches,
// ordered by file_id. Pass cursor="" for the first page. Returns an empty cursor
// when there are no more rows.
func (s *Store) ListConfirmedFileSummaries(ctx context.Context, cursor string, limit int) ([]ConfirmedFileSummary, string, error) {
	if limit <= 0 {
		limit = 500
	}
	var rows *sql.Rows
	var err error
	if cursor == "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT i.inode_id, i.size_bytes, COALESCE(c.content_type, '')
			 FROM inodes i JOIN contents c ON i.inode_id = c.inode_id
			 WHERE i.status = 'CONFIRMED'
			 ORDER BY i.inode_id ASC LIMIT ?`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT i.inode_id, i.size_bytes, COALESCE(c.content_type, '')
			 FROM inodes i JOIN contents c ON i.inode_id = c.inode_id
			 WHERE i.status = 'CONFIRMED' AND i.inode_id > ?
			 ORDER BY i.inode_id ASC LIMIT ?`, cursor, limit)
	}
	if err != nil {
		return nil, "", fmt.Errorf("query confirmed files: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ConfirmedFileSummary
	for rows.Next() {
		var f ConfirmedFileSummary
		if err := rows.Scan(&f.FileID, &f.SizeBytes, &f.ContentType); err != nil {
			return nil, "", fmt.Errorf("scan confirmed file: %w", err)
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(out) == limit {
		nextCursor = out[len(out)-1].FileID
	}
	return out, nextCursor, nil
}

type ConfirmedS3Ref struct {
	StorageRef     string
	StorageRefHash string
}

func (s *Store) ListConfirmedS3Refs(ctx context.Context, cursor string, limit int) ([]ConfirmedS3Ref, string, error) {
	if limit <= 0 {
		limit = 500
	}
	var rows *sql.Rows
	var err error
	if cursor == "" {
		rows, err = s.db.QueryContext(ctx,
			`SELECT c.storage_ref, c.storage_ref_hash
			 FROM contents c JOIN inodes i ON i.inode_id = c.inode_id
			 WHERE i.status = 'CONFIRMED' AND c.storage_type = 's3' AND c.storage_ref <> ''
			 ORDER BY c.storage_ref ASC LIMIT ?`, limit)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT c.storage_ref, c.storage_ref_hash
			 FROM contents c JOIN inodes i ON i.inode_id = c.inode_id
			 WHERE i.status = 'CONFIRMED' AND c.storage_type = 's3' AND c.storage_ref <> '' AND c.storage_ref > ?
			 ORDER BY c.storage_ref ASC LIMIT ?`, cursor, limit)
	}
	if err != nil {
		return nil, "", fmt.Errorf("query confirmed s3 refs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ConfirmedS3Ref, 0)
	for rows.Next() {
		var ref ConfirmedS3Ref
		if err := rows.Scan(&ref.StorageRef, &ref.StorageRefHash); err != nil {
			return nil, "", fmt.Errorf("scan confirmed s3 ref: %w", err)
		}
		if ref.StorageRefHash == "" {
			ref.StorageRefHash = StorageRefHash(ref.StorageRef)
		}
		out = append(out, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(out) == limit {
		nextCursor = out[len(out)-1].StorageRef
	}
	return out, nextCursor, nil
}

func (s *Store) HasActiveUploads(ctx context.Context) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM uploads
		 WHERE status IN ('INITIATED', 'UPLOADING') AND expires_at > ?
		 LIMIT 1`, time.Now().UTC()).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) SanitizeForkRuntimeState(ctx context.Context) error {
	return s.InTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE fn FROM file_nodes fn JOIN inodes i ON i.inode_id = COALESCE(fn.inode_id, fn.file_id) WHERE i.status <> 'CONFIRMED'`); err != nil {
			return err
		}
		now := time.Now().UTC()
		if _, err := tx.ExecContext(ctx, `UPDATE contents c JOIN inodes i ON i.inode_id = c.inode_id SET c.storage_ref = '', c.storage_ref_hash = '', c.content_blob = NULL WHERE i.status <> 'CONFIRMED'`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE inodes SET status = 'DELETED', expires_at = ? WHERE status <> 'CONFIRMED'`, now); err != nil {
			return err
		}
		if s.useLegacyFiles {
			if _, err := tx.ExecContext(ctx, `UPDATE files SET status = 'DELETED', storage_ref = '', storage_ref_hash = '', content_blob = NULL, expires_at = ? WHERE status <> 'CONFIRMED'`, now); err != nil {
				return err
			}
		}
		for _, stmt := range []string{
			`DELETE FROM uploads`,
			`DELETE FROM file_gc_tasks`,
			`DELETE FROM semantic_tasks`,
			`DELETE FROM llm_usage`,
			`DELETE FROM vault_audit_log`,
			`DELETE FROM vault_grants`,
			`DELETE FROM vault_tokens`,
			`DELETE FROM vault_secret_fields`,
			`DELETE FROM vault_secrets`,
			`DELETE FROM vault_policies`,
			`DELETE FROM vault_deks`,
		} {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				if isMissingTableError(err) {
					continue
				}
				return err
			}
		}
		return nil
	})
}

func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "error 1146") ||
		strings.Contains(msg, "table") && strings.Contains(msg, "doesn't exist")
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

func (s *Store) StatPathFallback(ctx context.Context, primaryPath, fallbackPath string) (out *NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "stat_path_fallback", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, c.storage_type, c.storage_ref, c.storage_encryption_mode, c.storage_encryption_key_id,
		c.content_blob, c.content_type, i.size_bytes,
		c.checksum_sha256, i.revision, i.mode, s.embedding_revision, i.status, c.source_id, s.content_text,
		s.description, s.description_embedding_revision, i.created_at, i.confirmed_at, i.expires_at
		FROM file_nodes fn
		LEFT JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED'
		LEFT JOIN contents c ON i.inode_id = c.inode_id
		LEFT JOIN semantic s ON i.inode_id = s.inode_id
		WHERE fn.path = ? OR fn.path = ?
		ORDER BY CASE WHEN fn.path = ? THEN 0 ELSE 1 END
		LIMIT 1`, primaryPath, fallbackPath, primaryPath)
	out, err = scanNodeWithFileWithBlob(row)
	return out, err
}

// StatPathFallbackLite is like StatPathFallback but only fetches lightweight
// metadata fields needed for FUSE stat/HEAD operations. It skips content_blob,
// content_text, description, embedding columns, checksum, and source_id.
func (s *Store) StatPathFallbackLite(ctx context.Context, primaryPath, fallbackPath string) (out *NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "stat_path_fallback_lite", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, i.size_bytes, i.revision, i.mode, i.status, i.created_at, i.confirmed_at
		FROM file_nodes fn
		LEFT JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED'
		WHERE fn.path = ? OR fn.path = ?
		ORDER BY CASE WHEN fn.path = ? THEN 0 ELSE 1 END
		LIMIT 1`, primaryPath, fallbackPath, primaryPath)
	out, err = scanNodeWithFileLite(row)
	return out, err
}

// StatLite is like Stat but only fetches lightweight metadata fields.
func (s *Store) StatLite(ctx context.Context, path string) (out *NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "stat_lite", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, i.size_bytes, i.revision, i.mode, i.status, i.created_at, i.confirmed_at
		FROM file_nodes fn
		LEFT JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED'
		WHERE fn.path = ?
		LIMIT 1`, path)
	out, err = scanNodeWithFileLite(row)
	return out, err
}

// StatForRead fetches the minimal fields needed to serve a GET request in a
// single pass: storage_type, storage_ref, content_blob (for db9), size_bytes,
// revision, and confirmed_at. This avoids the two-stat pattern of
// PresignGetObject-then-ReadCtx.
func (s *Store) StatForRead(ctx context.Context, path string) (out *NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "stat_for_read", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, c.storage_type, c.storage_ref, c.content_blob, i.size_bytes, i.revision, i.mode, i.status, i.created_at, i.confirmed_at
		FROM file_nodes fn
		LEFT JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED'
		LEFT JOIN contents c ON i.inode_id = c.inode_id
		WHERE fn.path = ?
		LIMIT 1`, path)
	out, err = scanNodeWithFileForRead(row)
	return out, err
}

// StatPathFallbackForRead is like StatForRead but with primary/fallback path resolution.
func (s *Store) StatPathFallbackForRead(ctx context.Context, primaryPath, fallbackPath string) (out *NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "stat_path_fallback_for_read", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, c.storage_type, c.storage_ref, c.content_blob, i.size_bytes, i.revision, i.mode, i.status, i.created_at, i.confirmed_at
		FROM file_nodes fn
		LEFT JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED'
		LEFT JOIN contents c ON i.inode_id = c.inode_id
		WHERE fn.path = ? OR fn.path = ?
		ORDER BY CASE WHEN fn.path = ? THEN 0 ELSE 1 END
		LIMIT 1`, primaryPath, fallbackPath, primaryPath)
	out, err = scanNodeWithFileForRead(row)
	return out, err
}

func (s *Store) ListDir(ctx context.Context, parentPath string) (out []*NodeWithFile, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "list_dir", start, &err)

	// TODO(#110): ReadDir only needs lightweight file metadata. Split this into a
	// metadata-only listing path so directory scans do not fetch or copy content_blob.
	q := `SELECT fn.node_id, fn.path, fn.parent_path, fn.name, fn.is_directory, fn.file_id, fn.created_at,
		i.inode_id, i.size_bytes, i.revision, i.mode, i.status, i.created_at, i.confirmed_at,
		s.embedding_revision
		FROM file_nodes fn
		LEFT JOIN inodes i ON COALESCE(fn.inode_id, fn.file_id) = i.inode_id AND i.status = 'CONFIRMED'
		LEFT JOIN semantic s ON i.inode_id = s.inode_id
		WHERE fn.parent_path = ?
		ORDER BY fn.name`
	rows, err := s.db.QueryContext(ctx, q, parentPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	result := make([]*NodeWithFile, 0)
	for rows.Next() {
		var n FileNode
		var isDir int
		var nodeFileID sql.NullString
		var nodeCreatedAt time.Time
		var fFileID sql.NullString
		var fSizeBytes, fRevision sql.NullInt64
		var fMode sql.NullInt64
		var fStatus sql.NullString
		var fCreatedAt, fConfirmedAt sql.NullTime
		var fEmbeddingRevision sql.NullInt64
		if err := rows.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
			&fFileID, &fSizeBytes, &fRevision, &fMode, &fStatus, &fCreatedAt, &fConfirmedAt, &fEmbeddingRevision); err != nil {
			return nil, err
		}
		n.IsDirectory = isDir != 0
		n.FileID = nodeFileID.String
		n.CreatedAt = nodeCreatedAt.UTC()
		nf := &NodeWithFile{Node: n}
		if fFileID.Valid {
			nf.File = &File{
				FileID:    fFileID.String,
				SizeBytes: fSizeBytes.Int64,
				Revision:  fRevision.Int64,
				Mode:      uint32(fMode.Int64),
				Status:    FileStatus(fStatus.String),
			}
			if fCreatedAt.Valid {
				nf.File.CreatedAt = fCreatedAt.Time.UTC()
			}
			if fConfirmedAt.Valid {
				t := fConfirmedAt.Time.UTC()
				nf.File.ConfirmedAt = &t
			}
			if fEmbeddingRevision.Valid {
				rev := fEmbeddingRevision.Int64
				nf.File.EmbeddingRevision = &rev
			}
		}
		if fMode.Valid {
			nf.Mode = uint32(fMode.Int64)
			nf.HasMode = true
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
	err = tx.QueryRow(`SELECT file_id, is_directory FROM file_nodes WHERE path = ? FOR UPDATE`, path).Scan(&fileID, &isDir)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if isDir {
		return nil, ErrNotFound
	}

	if _, err := tx.Exec(`DELETE FROM file_nodes WHERE path = ?`, path); err != nil {
		return nil, err
	}

	if !fileID.Valid || fileID.String == "" {
		return nil, tx.Commit()
	}

	var count int64
	err = tx.QueryRow(`SELECT COUNT(*) FROM file_nodes WHERE file_id = ? FOR UPDATE`, fileID.String).Scan(&count)
	if err != nil {
		return nil, err
	}

	if count > 0 {
		return nil, tx.Commit()
	}

	if s.useLegacyFiles {
		if _, err := tx.Exec(`UPDATE files SET status = 'DELETED' WHERE file_id = ?`, fileID.String); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(`UPDATE inodes SET status = 'DELETED' WHERE inode_id = ?`, fileID.String); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`DELETE FROM file_tags WHERE file_id = ?`, fileID.String); err != nil {
		return nil, err
	}

	f, err := s.scanFileForGCTx(tx, fileID.String)
	if err != nil {
		return nil, err
	}
	task, err := NewFileGCTaskFromFile(f, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if _, err := s.EnqueueFileGCTaskTx(tx, task); err != nil {
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
		if s.useLegacyFiles {
			if _, err := tx.Exec(`UPDATE files SET status = 'DELETED' WHERE file_id = ?`, fid); err != nil {
				return nil, err
			}
		}
		if _, err := tx.Exec(`UPDATE inodes SET status = 'DELETED' WHERE inode_id = ?`, fid); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`DELETE FROM file_tags WHERE file_id = ?`, fid); err != nil {
			return nil, err
		}
		f, err := s.scanFileForGCTx(tx, fid)
		if err != nil {
			return nil, err
		}
		task, err := NewFileGCTaskFromFile(f, time.Now().UTC())
		if err != nil {
			return nil, err
		}
		if _, err := s.EnqueueFileGCTaskTx(tx, task); err != nil {
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

func dirHasChildrenTx(ctx context.Context, tx *sql.Tx, path string) (bool, error) {
	var one int
	err := tx.QueryRowContext(ctx, `SELECT 1 FROM file_nodes WHERE parent_path = ? LIMIT 1 FOR UPDATE`, path).Scan(&one)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func pathPrefixPredicate(column, prefix string) (string, []any) {
	return "BINARY " + column + " LIKE BINARY ? ESCAPE '\\\\'", []any{likeLiteralPrefixPattern(prefix)}
}

// --- uploads operations ---

func (s *Store) InsertUpload(ctx context.Context, u *Upload) (err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "insert_upload", start, &err)

	err = s.InsertUploadTx(s.db, u)
	return err
}

func (s *Store) InsertUploadTx(db execer, u *Upload) error {
	mode := uploadStorageEncryptionModeForWrite(u.StorageEncryptionMode)
	_, err := db.Exec(`INSERT INTO uploads
		(upload_id, file_id, target_path, s3_upload_id, s3_key, storage_encryption_mode,
		 storage_encryption_key_id, total_size, part_size,
		 parts_total, expected_revision, status, fingerprint_sha256, idempotency_key, description, created_at, updated_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		u.UploadID, u.FileID, u.TargetPath, u.S3UploadID, u.S3Key,
		mode, storageEncryptionKeyIDForWrite(mode, u.StorageEncryptionKeyID),
		u.TotalSize, u.PartSize, u.PartsTotal, nullInt64Ptr(u.ExpectedRevision), u.Status,
		nullStr(u.FingerprintSHA), nullStr(u.IdempotencyKey), nullStr(u.Description),
		u.CreatedAt.UTC(), u.UpdatedAt.UTC(), u.ExpiresAt.UTC())
	if isUniqueViolation(err) {
		// Distinguish constraint: idx_idempotency (duplicate key) vs idx_uploads_active (concurrent path).
		// MySQL embeds the constraint name in the error message; no structured alternative exists.
		// If the constraint is renamed, update this string and corresponding tests.
		if strings.Contains(err.Error(), "idx_idempotency") {
			return ErrIdempotencyConflict
		}
		return ErrUploadConflict
	}
	return err
}

func (s *Store) GetUpload(ctx context.Context, uploadID string) (out *Upload, err error) {
	start := time.Now()
	var opErr error
	defer observeStoreOp(ctx, "get_upload", start, &opErr)

	queryStart := time.Now()
	rows, err := s.db.QueryContext(ctx, `SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		storage_encryption_mode, storage_encryption_key_id,
		total_size, part_size, parts_total, expected_revision, status, fingerprint_sha256, idempotency_key,
		description, created_at, updated_at, expires_at
		FROM uploads WHERE upload_id = ?`, uploadID)
	queryContextDurationMs := datastorePhaseMs(queryStart)
	if err != nil {
		opErr = err
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	firstRowStart := time.Now()
	if !rows.Next() {
		firstRowDurationMs := datastorePhaseMs(firstRowStart)
		if err := rows.Err(); err != nil {
			opErr = err
			return nil, err
		}
		logger.InfoBenchTiming(ctx, "datastore_get_upload_timing",
			zap.String("upload_id", uploadID),
			zap.Float64("query_context_ms", queryContextDurationMs),
			zap.Float64("first_row_ms", firstRowDurationMs),
			zap.Float64("scan_ms", 0),
			zap.Float64("hydrate_ms", 0),
			zap.String("result", "not_found"),
			zap.Float64("total_ms", datastorePhaseMs(start)),
		)
		opErr = ErrNotFound
		return nil, ErrNotFound
	}
	firstRowDurationMs := datastorePhaseMs(firstRowStart)

	var u Upload
	var fingerprint, idempotencyKey sql.NullString
	var expectedRevision sql.NullInt64
	var createdAt, updatedAt, expiresAt time.Time
	scanStart := time.Now()
	var description sql.NullString
	err = rows.Scan(&u.UploadID, &u.FileID, &u.TargetPath, &u.S3UploadID, &u.S3Key,
		&u.StorageEncryptionMode, &u.StorageEncryptionKeyID,
		&u.TotalSize, &u.PartSize, &u.PartsTotal, &expectedRevision, &u.Status,
		&fingerprint, &idempotencyKey, &description,
		&createdAt, &updatedAt, &expiresAt)
	if description.Valid {
		u.Description = description.String
	}
	scanDurationMs := datastorePhaseMs(scanStart)
	if err != nil {
		opErr = err
		return nil, err
	}
	if err := rows.Err(); err != nil {
		opErr = err
		return nil, err
	}

	hydrateStart := time.Now()
	u.FingerprintSHA = fingerprint.String
	u.IdempotencyKey = idempotencyKey.String
	if expectedRevision.Valid {
		rev := expectedRevision.Int64
		u.ExpectedRevision = &rev
	}
	u.CreatedAt = createdAt.UTC()
	u.UpdatedAt = updatedAt.UTC()
	u.ExpiresAt = expiresAt.UTC()
	hydrateDurationMs := datastorePhaseMs(hydrateStart)

	logger.InfoBenchTiming(ctx, "datastore_get_upload_timing",
		zap.String("upload_id", uploadID),
		zap.Int("parts_total", u.PartsTotal),
		zap.String("status", string(u.Status)),
		zap.Float64("query_context_ms", queryContextDurationMs),
		zap.Float64("first_row_ms", firstRowDurationMs),
		zap.Float64("scan_ms", scanDurationMs),
		zap.Float64("hydrate_ms", hydrateDurationMs),
		zap.String("result", "ok"),
		zap.Float64("total_ms", datastorePhaseMs(start)),
	)

	out = &u
	return out, nil
}

func (s *Store) GetUploadByPath(ctx context.Context, targetPath string) (out *Upload, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "get_upload_by_path", start, &err)

	row := s.db.QueryRowContext(ctx, `SELECT upload_id, file_id, target_path, s3_upload_id, s3_key,
		storage_encryption_mode, storage_encryption_key_id,
		total_size, part_size, parts_total, expected_revision, status, fingerprint_sha256, idempotency_key,
		description, created_at, updated_at, expires_at
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
		storage_encryption_mode, storage_encryption_key_id,
		total_size, part_size, parts_total, expected_revision, status, fingerprint_sha256, idempotency_key,
		description, created_at, updated_at, expires_at
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
	var fileID, inodeID sql.NullString
	var createdAt time.Time
	err := s.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &fileID, &inodeID, &createdAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	n.IsDirectory = isDir != 0
	n.FileID = fileID.String
	n.InodeID = inodeID.String
	n.CreatedAt = createdAt.UTC()
	return &n, nil
}

func assembleFile(inode *Inode, content *Content, semantic *Semantic) *File {
	f := &File{
		FileID:      inode.InodeID,
		SizeBytes:   inode.SizeBytes,
		Revision:    inode.Revision,
		Mode:        inode.Mode,
		Status:      inode.Status,
		CreatedAt:   inode.CreatedAt,
		ConfirmedAt: inode.ConfirmedAt,
		ExpiresAt:   inode.ExpiresAt,
	}
	if content != nil {
		f.StorageType = content.StorageType
		f.StorageRef = content.StorageRef
		f.StorageEncryptionMode = content.StorageEncryptionMode
		f.StorageEncryptionKeyID = content.StorageEncryptionKeyID
		f.ContentBlob = append([]byte(nil), content.ContentBlob...)
		f.ContentType = content.ContentType
		f.ChecksumSHA256 = content.ChecksumSHA256
		f.SourceID = content.SourceID
	}
	if semantic != nil {
		f.ContentText = semantic.ContentText
		f.Description = semantic.Description
		f.EmbeddingRevision = semantic.EmbeddingRevision
		f.DescriptionEmbeddingRevision = semantic.DescriptionEmbeddingRevision
	}
	return f
}

func scanFileForGC(s scanner) (*File, error) {
	var f File
	var contentType, checksum, sourceID sql.NullString
	var embeddingRevision sql.NullInt64
	var confirmedAt, expiresAt sql.NullTime
	var createdAt time.Time
	err := s.Scan(&f.FileID, &f.StorageType, &f.StorageRef, &f.StorageEncryptionMode,
		&f.StorageEncryptionKeyID, &contentType, &f.SizeBytes, &checksum,
		&f.Revision, &embeddingRevision, &f.Status, &sourceID, &createdAt, &confirmedAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	f.ContentType = contentType.String
	f.ChecksumSHA256 = checksum.String
	f.SourceID = sourceID.String
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

// scanFileForGCTx reads the minimal file fields needed for GC task creation.
// For legacy tenants it queries the files table; for new tenants it queries
// the split tables (inodes/contents/semantic).
func (s *Store) scanFileForGCTx(db execer, fileID string) (*File, error) {
	if s.useLegacyFiles {
		row := db.QueryRow(`SELECT file_id, storage_type, storage_ref, storage_encryption_mode,
			storage_encryption_key_id, content_type, size_bytes, checksum_sha256,
			revision, embedding_revision, status, source_id, created_at, confirmed_at, expires_at
			FROM files WHERE file_id = ?`, fileID)
		return scanFileForGC(row)
	}
	var f File
	f.FileID = fileID
	f.Status = StatusDeleted
	var contentType sql.NullString
	var embRev sql.NullInt64
	err := db.QueryRow(`SELECT c.storage_type, c.storage_ref, i.size_bytes, COALESCE(c.content_type, ''),
		s.embedding_revision
		FROM inodes i
		JOIN contents c ON i.inode_id = c.inode_id
		LEFT JOIN semantic s ON i.inode_id = s.inode_id
		WHERE i.inode_id = ?`, fileID).Scan(
		&f.StorageType, &f.StorageRef, &f.SizeBytes, &contentType, &embRev)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	f.ContentType = contentType.String
	if embRev.Valid {
		f.EmbeddingRevision = &embRev.Int64
	}
	return &f, nil
}

func scanNodeWithFileWithBlob(s scanner) (*NodeWithFile, error) {
	var n FileNode
	var isDir int
	var nodeFileID sql.NullString
	var nodeCreatedAt time.Time

	var fFileID, fStorageType, fStorageRef, fStorageEncryptionMode, fStorageEncryptionKeyID sql.NullString
	var fContentBlob []byte
	var fContentType, fChecksum, fSourceID, fContentText, fDescription sql.NullString
	var fSizeBytes, fRevision, fMode, fEmbeddingRevision, fDescriptionEmbeddingRevision sql.NullInt64
	var fStatus sql.NullString
	var fCreatedAt, fConfirmedAt, fExpiresAt sql.NullTime

	err := s.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
		&fFileID, &fStorageType, &fStorageRef, &fStorageEncryptionMode, &fStorageEncryptionKeyID,
		&fContentBlob, &fContentType, &fSizeBytes,
		&fChecksum, &fRevision, &fMode, &fEmbeddingRevision, &fStatus, &fSourceID, &fContentText,
		&fDescription, &fDescriptionEmbeddingRevision, &fCreatedAt, &fConfirmedAt, &fExpiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	n.IsDirectory = isDir != 0
	n.FileID = nodeFileID.String
	n.CreatedAt = nodeCreatedAt.UTC()

	nf := &NodeWithFile{Node: n}
	if fFileID.Valid {
		nf.File = &File{
			FileID:                 fFileID.String,
			StorageType:            StorageType(fStorageType.String),
			StorageRef:             fStorageRef.String,
			StorageEncryptionMode:  StorageEncryptionMode(fStorageEncryptionMode.String),
			StorageEncryptionKeyID: fStorageEncryptionKeyID.String,
			ContentBlob:            append([]byte(nil), fContentBlob...),
			ContentType:            fContentType.String,
			SizeBytes:              fSizeBytes.Int64,
			ChecksumSHA256:         fChecksum.String,
			Revision:               fRevision.Int64,
			Mode:                   uint32(fMode.Int64),
			Status:                 FileStatus(fStatus.String),
			SourceID:               fSourceID.String,
			ContentText:            fContentText.String,
			Description:            fDescription.String,
		}
		if fEmbeddingRevision.Valid {
			rev := fEmbeddingRevision.Int64
			nf.File.EmbeddingRevision = &rev
		}
		if fDescriptionEmbeddingRevision.Valid {
			rev := fDescriptionEmbeddingRevision.Int64
			nf.File.DescriptionEmbeddingRevision = &rev
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
	if fMode.Valid {
		nf.Mode = uint32(fMode.Int64)
		nf.HasMode = true
	}
	return nf, nil
}

// scanNodeWithFileForRead scans the result set for ReadPlan: node fields +
// file_id, storage_type, storage_ref, content_blob, size_bytes, revision,
// status, created_at, confirmed_at. Includes blob for db9 inline reads but
// excludes content_text, description, embedding columns.
func scanNodeWithFileForRead(s scanner) (*NodeWithFile, error) {
	var n FileNode
	var isDir int
	var nodeFileID sql.NullString
	var nodeCreatedAt time.Time

	var fFileID, fStorageType, fStorageRef sql.NullString
	var fContentBlob []byte
	var fSizeBytes, fRevision sql.NullInt64
	var fMode sql.NullInt64
	var fStatus sql.NullString
	var fCreatedAt, fConfirmedAt sql.NullTime

	err := s.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
		&fFileID, &fStorageType, &fStorageRef, &fContentBlob, &fSizeBytes, &fRevision, &fMode, &fStatus, &fCreatedAt, &fConfirmedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	n.IsDirectory = isDir != 0
	n.FileID = nodeFileID.String
	n.CreatedAt = nodeCreatedAt.UTC()

	nf := &NodeWithFile{Node: n}
	if fFileID.Valid {
		nf.File = &File{
			FileID:      fFileID.String,
			StorageType: StorageType(fStorageType.String),
			StorageRef:  fStorageRef.String,
			ContentBlob: append([]byte(nil), fContentBlob...),
			SizeBytes:   fSizeBytes.Int64,
			Revision:    fRevision.Int64,
			Status:      FileStatus(fStatus.String),
		}
		if fCreatedAt.Valid {
			nf.File.CreatedAt = fCreatedAt.Time.UTC()
		}
		if fConfirmedAt.Valid {
			t := fConfirmedAt.Time.UTC()
			nf.File.ConfirmedAt = &t
		}
	}
	if fMode.Valid {
		nf.Mode = uint32(fMode.Int64)
		nf.HasMode = true
	}
	return nf, nil
}

// scanNodeWithFileLite scans a lite result set that only includes metadata
// fields needed for stat/HEAD: node fields + file_id, size_bytes, revision,
// status, created_at, confirmed_at. No blob/text/description/embedding columns.
func scanNodeWithFileLite(s scanner) (*NodeWithFile, error) {
	var n FileNode
	var isDir int
	var nodeFileID sql.NullString
	var nodeCreatedAt time.Time

	var fFileID sql.NullString
	var fSizeBytes, fRevision sql.NullInt64
	var fMode sql.NullInt64
	var fStatus sql.NullString
	var fCreatedAt, fConfirmedAt sql.NullTime

	err := s.Scan(&n.NodeID, &n.Path, &n.ParentPath, &n.Name, &isDir, &nodeFileID, &nodeCreatedAt,
		&fFileID, &fSizeBytes, &fRevision, &fMode, &fStatus, &fCreatedAt, &fConfirmedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	n.IsDirectory = isDir != 0
	n.FileID = nodeFileID.String
	n.CreatedAt = nodeCreatedAt.UTC()

	nf := &NodeWithFile{Node: n}
	if fFileID.Valid {
		nf.File = &File{
			FileID:    fFileID.String,
			SizeBytes: fSizeBytes.Int64,
			Revision:  fRevision.Int64,
			Mode:      uint32(fMode.Int64),
			Status:    FileStatus(fStatus.String),
		}
		if fCreatedAt.Valid {
			nf.File.CreatedAt = fCreatedAt.Time.UTC()
		}
		if fConfirmedAt.Valid {
			t := fConfirmedAt.Time.UTC()
			nf.File.ConfirmedAt = &t
		}
	}
	if fMode.Valid {
		nf.Mode = uint32(fMode.Int64)
		nf.HasMode = true
	}
	return nf, nil
}

func nilBytes(b []byte) any {
	if len(b) == 0 {
		return nil
	}
	return b
}

func fileStorageEncryptionModeForWrite(mode StorageEncryptionMode) StorageEncryptionMode {
	if mode == "" {
		return StorageEncryptionLegacy
	}
	return mode
}

func uploadStorageEncryptionModeForWrite(mode StorageEncryptionMode) StorageEncryptionMode {
	if mode == "" {
		return StorageEncryptionNone
	}
	return mode
}

func storageEncryptionKeyIDForWrite(mode StorageEncryptionMode, keyID string) string {
	switch mode {
	case StorageEncryptionSSEKMS, StorageEncryptionDSSEKMS:
		return keyID
	default:
		return ""
	}
}

func scanUpload(s scanner) (*Upload, error) {
	var u Upload
	var fingerprint, idempotencyKey, description sql.NullString
	var expectedRevision sql.NullInt64
	var createdAt, updatedAt, expiresAt time.Time
	err := s.Scan(&u.UploadID, &u.FileID, &u.TargetPath, &u.S3UploadID, &u.S3Key,
		&u.StorageEncryptionMode, &u.StorageEncryptionKeyID,
		&u.TotalSize, &u.PartSize, &u.PartsTotal, &expectedRevision, &u.Status,
		&fingerprint, &idempotencyKey, &description,
		&createdAt, &updatedAt, &expiresAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	u.FingerprintSHA = fingerprint.String
	u.IdempotencyKey = idempotencyKey.String
	u.Description = description.String
	if expectedRevision.Valid {
		rev := expectedRevision.Int64
		u.ExpectedRevision = &rev
	}
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

func nullInt64Ptr(v *int64) interface{} {
	if v == nil {
		return nil
	}
	return *v
}

func nilTime(t *time.Time) interface{} {
	if t == nil {
		return nil
	}
	return t.UTC()
}

func coalesceTime(t *time.Time, fallback time.Time) time.Time {
	if t != nil {
		return *t
	}
	return fallback
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

func isDeadlock(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "Deadlock found") || strings.Contains(msg, "deadlock")
}

var wsNorm = regexp.MustCompile(`\s+`)

func normalizeSQL(s string) string {
	return wsNorm.ReplaceAllString(strings.TrimSpace(s), " ")
}

func datastorePhaseMs(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func observeStoreOp(ctx context.Context, op string, start time.Time, errp *error) {
	elapsed := time.Since(start)
	result := "ok"
	if errp != nil && *errp != nil {
		switch {
		case errors.Is(*errp, ErrNotFound):
			result = "not_found"
		case errors.Is(*errp, ErrPathConflict), errors.Is(*errp, ErrUploadConflict), errors.Is(*errp, ErrIdempotencyConflict):
			result = "conflict"
		default:
			result = "error"
		}
		logger.Error(ctx, "datastore_op_failed", zap.String("operation", op), zap.String("result", result), zap.Error(*errp))
	}
	logger.InfoBenchTiming(ctx, "datastore_op_timing",
		zap.String("operation", op),
		zap.String("result", result),
		zap.Float64("duration_ms", float64(elapsed.Microseconds())/1000.0))
	metrics.RecordOperation("datastore", op, result, elapsed)
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
