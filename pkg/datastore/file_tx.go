package datastore

import (
	"fmt"
	"time"
)

// CreateConfirmedEmptyFileParams defines the metadata needed to create a
// confirmed empty file entity and its namespace entry in one transaction.
type CreateConfirmedEmptyFileParams struct {
	Path      string
	FileID    string
	NodeID    string
	CreatedAt time.Time
}

// CreateConfirmedEmptyFileTx creates a confirmed empty file row plus its file
// node inside an existing transaction.
func (s *Store) CreateConfirmedEmptyFileTx(db execer, params CreateConfirmedEmptyFileParams, genID func() string) error {
	createdAt := params.CreatedAt.UTC()
	confirmedAt := createdAt

	if err := s.InsertFileTx(db, &File{
		FileID:      params.FileID,
		StorageType: StorageDB9,
		StorageRef:  "inline",
		ContentBlob: []byte{},
		SizeBytes:   0,
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   createdAt,
		ConfirmedAt: &confirmedAt,
	}); err != nil {
		return err
	}
	if err := s.EnsureParentDirsTx(db, params.Path, genID); err != nil {
		return err
	}
	return s.InsertNodeTx(db, &FileNode{
		NodeID:     params.NodeID,
		Path:       params.Path,
		ParentPath: parentPath(params.Path),
		Name:       baseName(params.Path),
		FileID:     params.FileID,
		CreatedAt:  createdAt,
	})
}

// InsertFileTx inserts a file row inside an existing transaction.
func (s *Store) InsertFileTx(db execer, f *File) error {
	_, err := db.Exec(`INSERT INTO files
		(file_id, storage_type, storage_ref, content_blob, content_type, size_bytes, checksum_sha256,
		 revision, status, source_id, content_text, created_at, confirmed_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		f.FileID, f.StorageType, f.StorageRef, nilBytes(f.ContentBlob), nullStr(f.ContentType),
		f.SizeBytes, nullStr(f.ChecksumSHA256), f.Revision, f.Status,
		nullStr(f.SourceID), nullStr(f.ContentText),
		f.CreatedAt.UTC(), nilTime(f.ConfirmedAt), nilTime(f.ExpiresAt))
	return err
}

// UpdateFileContentTx updates file bytes/metadata inside an existing transaction.
// The helper also clears embedding state so later semantic processing recomputes
// vectors for the new revision.
func (s *Store) UpdateFileContentTx(db execer, fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64) (int64, error) {
	return s.updateFileContentTx(db, fileID, 0, false, storageType, storageRef, contentType, checksum, contentText, contentBlob, size)
}

// UpdateFileContentIfRevisionTx updates file bytes/metadata only if the
// current revision exactly matches expectedRevision.
func (s *Store) UpdateFileContentIfRevisionTx(db execer, fileID string, expectedRevision int64, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64) (int64, error) {
	return s.updateFileContentTx(db, fileID, expectedRevision, false, storageType, storageRef, contentType, checksum, contentText, contentBlob, size)
}

func (s *Store) updateFileContentTx(db execer, fileID string, expectedRevision int64, preserveEmbedding bool, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64) (int64, error) {
	now := time.Now().UTC()
	query := `UPDATE files SET storage_type = ?, storage_ref = ?,
		content_blob = ?, content_type = ?, size_bytes = ?, checksum_sha256 = ?, content_text = ?,`
	if preserveEmbedding {
		query += `
		revision = revision + 1, status = 'CONFIRMED', confirmed_at = ?
		WHERE file_id = ?`
	} else {
		query += `
		embedding = NULL, embedding_revision = NULL,
		revision = revision + 1, status = 'CONFIRMED', confirmed_at = ?
		WHERE file_id = ?`
	}
	args := []any{
		storageType, storageRef, nilBytes(contentBlob), nullStr(contentType), size,
		nullStr(checksum), nullStr(contentText), now, fileID,
	}
	if expectedRevision > 0 {
		query += ` AND revision = ? AND status = 'CONFIRMED'`
		args = append(args, expectedRevision)
	}
	res, err := db.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}
	if rowsAffected == 0 {
		if expectedRevision > 0 {
			return 0, ErrRevisionConflict
		}
		return 0, ErrNotFound
	}
	var rev int64
	if err := db.QueryRow(`SELECT revision FROM files WHERE file_id = ?`, fileID).Scan(&rev); err != nil {
		return 0, fmt.Errorf("read revision after update: %w", err)
	}
	return rev, nil
}

// UpdateFileContentAutoEmbeddingTx updates file bytes/metadata without touching
// embedding columns. TiDB auto-embedding mode relies on the database to derive
// vectors from content_text, so the write path must stop clearing vector state.
func (s *Store) UpdateFileContentAutoEmbeddingTx(db execer, fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64) (int64, error) {
	return s.updateFileContentTx(db, fileID, 0, true, storageType, storageRef, contentType, checksum, contentText, contentBlob, size)
}

// UpdateFileContentAutoEmbeddingIfRevisionTx updates file bytes/metadata
// without touching embedding columns and only if expectedRevision matches.
func (s *Store) UpdateFileContentAutoEmbeddingIfRevisionTx(db execer, fileID string, expectedRevision int64, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64) (int64, error) {
	return s.updateFileContentTx(db, fileID, expectedRevision, true, storageType, storageRef, contentType, checksum, contentText, contentBlob, size)
}

// ConfirmPendingFileAutoEmbeddingTx marks a pending uploaded file as confirmed
// without rewriting embedding columns. The auto-embedding mode lets TiDB own
// derived vector state after content_text becomes available.
func (s *Store) ConfirmPendingFileAutoEmbeddingTx(db execer, fileID string, storageType StorageType, storageRef, contentType string, size int64) error {
	now := time.Now().UTC()
	res, err := db.Exec(`UPDATE files SET storage_type = ?, storage_ref = ?, content_type = ?,
		size_bytes = ?, checksum_sha256 = NULL, content_text = NULL,
		status = 'CONFIRMED', confirmed_at = ?
		WHERE file_id = ? AND status = 'PENDING'`,
		storageType, storageRef, nullStr(contentType), size, now, fileID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

// ClearFileEmbeddingStateTx clears embedding columns inside an existing transaction.
func (s *Store) ClearFileEmbeddingStateTx(db execer, fileID string) error {
	res, err := db.Exec(`UPDATE files SET embedding = NULL, embedding_revision = NULL WHERE file_id = ?`, fileID)
	if err != nil {
		return err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}
