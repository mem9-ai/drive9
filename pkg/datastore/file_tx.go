package datastore

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// InsertFileTx inserts a file row inside an existing transaction.
func (s *Store) InsertFileTx(db execer, f *File) error {
	if s.useLegacyFiles {
		mode := fileStorageEncryptionModeForWrite(f.StorageEncryptionMode)
		_, err := db.Exec(`INSERT INTO files
			(file_id, storage_type, storage_ref, storage_encryption_mode, storage_encryption_key_id,
			 content_blob, content_type, size_bytes, checksum_sha256,
			 revision, status, source_id, content_text, description, description_embedding_revision,
			 created_at, confirmed_at, expires_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			f.FileID, f.StorageType, f.StorageRef, mode,
			storageEncryptionKeyIDForWrite(mode, f.StorageEncryptionKeyID), nilBytes(f.ContentBlob), nullStr(f.ContentType),
			f.SizeBytes, nullStr(f.ChecksumSHA256), f.Revision, f.Status,
			nullStr(f.SourceID), nullStr(f.ContentText), nullStr(f.Description),
			nullInt64Ptr(f.DescriptionEmbeddingRevision),
			f.CreatedAt.UTC(), nilTime(f.ConfirmedAt), nilTime(f.ExpiresAt))
		if err != nil {
			return err
		}
	}
	// Dual-write to split tables
	return s.insertSplitTablesTx(db, f)
}

// UpdateFileContentTx updates file bytes/metadata inside an existing transaction.
// The helper also clears embedding state so later semantic processing recomputes
// vectors for the new revision.
func (s *Store) UpdateFileContentTx(db execer, fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64, description string) (int64, error) {
	return s.updateFileContentTx(db, fileID, 0, false, storageType, storageRef, contentType, checksum, contentText, contentBlob, size, description)
}

// UpdateFileContentIfRevisionTx updates file bytes/metadata only if the
// current revision exactly matches expectedRevision.
func (s *Store) UpdateFileContentIfRevisionTx(db execer, fileID string, expectedRevision int64, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64, description string) (int64, error) {
	return s.updateFileContentTx(db, fileID, expectedRevision, false, storageType, storageRef, contentType, checksum, contentText, contentBlob, size, description)
}

func (s *Store) updateFileContentTx(db execer, fileID string, expectedRevision int64, preserveEmbedding bool, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64, description string) (int64, error) {
	now := time.Now().UTC()
	var rev int64
	if s.useLegacyFiles {
		query := `UPDATE files SET storage_type = ?, storage_ref = ?,
			content_blob = ?, content_type = ?, size_bytes = ?, checksum_sha256 = ?, content_text = ?,`
		args := []any{
			storageType, storageRef, nilBytes(contentBlob), nullStr(contentType), size,
			nullStr(checksum), nullStr(contentText),
		}
		if description != "" {
			var currentDesc sql.NullString
			if err := db.QueryRow(`SELECT description FROM semantic WHERE inode_id = ?`, fileID).Scan(&currentDesc); err != nil {
				return 0, fmt.Errorf("read current description: %w", err)
			}
			query += ` description = ?,`
			args = append(args, description)
			if currentDesc.String != description {
				if preserveEmbedding {
					query += ` description_embedding_revision = revision + 1,`
				} else {
					query += ` description_embedding = NULL, description_embedding_revision = NULL,`
				}
			}
		}
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
		args = append(args, now, fileID)
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
		if expectedRevision > 0 {
			rev = expectedRevision + 1
		} else {
			if err := db.QueryRow(`SELECT revision FROM files WHERE file_id = ?`, fileID).Scan(&rev); err != nil {
				return 0, fmt.Errorf("read revision after update: %w", err)
			}
		}
	} else {
		var currentRev int64
		if err := db.QueryRow(`SELECT revision FROM inodes WHERE inode_id = ? AND status = 'CONFIRMED' FOR UPDATE`, fileID).Scan(&currentRev); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if expectedRevision > 0 {
					return 0, ErrRevisionConflict
				}
				return 0, ErrNotFound
			}
			return 0, fmt.Errorf("read current revision: %w", err)
		}
		if expectedRevision > 0 && currentRev != expectedRevision {
			return 0, ErrRevisionConflict
		}
		rev = currentRev + 1
	}

	// Dual-write to split tables
	if err := s.UpdateInodeContentTx(db, fileID, size, rev, StatusConfirmed, now); err != nil {
		return 0, fmt.Errorf("update inode: %w", err)
	}
	var encryptionMode StorageEncryptionMode
	if err := db.QueryRow(`SELECT storage_encryption_mode FROM contents WHERE inode_id = ?`, fileID).Scan(&encryptionMode); err != nil {
		return 0, fmt.Errorf("read encryption mode: %w", err)
	}
	if err := s.UpdateContentTx(db, fileID, storageType, storageRef, contentType, checksum, contentBlob, encryptionMode); err != nil {
		return 0, fmt.Errorf("update content: %w", err)
	}
	if preserveEmbedding {
		if err := s.updateSemanticNoEmbedTx(db, fileID, contentText, description); err != nil {
			return 0, fmt.Errorf("update semantic: %w", err)
		}
	} else {
		if err := s.UpdateSemanticTx(db, fileID, contentText, description); err != nil {
			return 0, fmt.Errorf("update semantic: %w", err)
		}
	}

	return rev, nil
}

// UpdateFileStorageEncryptionTx updates storage encryption metadata inside an existing transaction.
func (s *Store) UpdateFileStorageEncryptionTx(db execer, fileID string, mode StorageEncryptionMode, keyID string) error {
	mode = fileStorageEncryptionModeForWrite(mode)
	if s.useLegacyFiles {
		res, err := db.Exec(`UPDATE files SET storage_encryption_mode = ?, storage_encryption_key_id = ? WHERE file_id = ?`,
			mode, storageEncryptionKeyIDForWrite(mode, keyID), fileID)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			var exists int
			if err := db.QueryRow(`SELECT 1 FROM inodes WHERE inode_id = ?`, fileID).Scan(&exists); err != nil {
				if err == sql.ErrNoRows {
					return ErrNotFound
				}
				return err
			}
		}
	} else {
		var exists int
		if err := db.QueryRow(`SELECT 1 FROM inodes WHERE inode_id = ?`, fileID).Scan(&exists); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
	}
	// Dual-write to split tables
	if _, err := db.Exec(`UPDATE contents SET storage_encryption_mode = ?, storage_encryption_key_id = ? WHERE inode_id = ?`,
		mode, storageEncryptionKeyIDForWrite(mode, keyID), fileID); err != nil {
		return fmt.Errorf("update contents encryption: %w", err)
	}
	return nil
}

// UpdateFileContentAutoEmbeddingTx updates file bytes/metadata without touching
// embedding columns. TiDB auto-embedding mode relies on the database to derive
// vectors from content_text, so the write path must stop clearing vector state.
func (s *Store) UpdateFileContentAutoEmbeddingTx(db execer, fileID string, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64, description string) (int64, error) {
	return s.updateFileContentTx(db, fileID, 0, true, storageType, storageRef, contentType, checksum, contentText, contentBlob, size, description)
}

// UpdateFileContentAutoEmbeddingIfRevisionTx updates file bytes/metadata
// without touching embedding columns and only if expectedRevision matches.
func (s *Store) UpdateFileContentAutoEmbeddingIfRevisionTx(db execer, fileID string, expectedRevision int64, storageType StorageType, storageRef, contentType, checksum, contentText string, contentBlob []byte, size int64, description string) (int64, error) {
	return s.updateFileContentTx(db, fileID, expectedRevision, true, storageType, storageRef, contentType, checksum, contentText, contentBlob, size, description)
}

// ConfirmPendingFileTx marks a pending uploaded file as confirmed inside an
// existing transaction. This is the non-auto-embedding variant: it clears
// embedding state so later semantic processing recomputes vectors.
func (s *Store) ConfirmPendingFileTx(db execer, fileID string, storageType StorageType, storageRef, contentType string, size int64, description string) error {
	now := time.Now().UTC()
	if s.useLegacyFiles {
		query := `UPDATE files SET storage_type = ?, storage_ref = ?, content_type = ?,
			size_bytes = ?, checksum_sha256 = NULL, content_text = NULL,
			embedding = NULL, embedding_revision = NULL`
		args := []any{storageType, storageRef, nullStr(contentType), size}
		if description != "" {
			query += `, description = ?`
			args = append(args, description)
		}
		query += `, status = 'CONFIRMED', confirmed_at = ? WHERE file_id = ? AND status = 'PENDING'`
		args = append(args, now, fileID)
		res, err := db.Exec(query, args...)
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
	} else {
		var status string
		if err := db.QueryRow(`SELECT status FROM inodes WHERE inode_id = ? FOR UPDATE`, fileID).Scan(&status); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if status != string(StatusPending) {
			return ErrNotFound
		}
	}
	// Dual-write to split tables
	if err := s.UpdateInodeContentTx(db, fileID, size, 1, StatusConfirmed, now); err != nil {
		return fmt.Errorf("update inode: %w", err)
	}
	if err := s.UpdateContentTx(db, fileID, storageType, storageRef, contentType, "", nil, StorageEncryptionLegacy); err != nil {
		return fmt.Errorf("update content: %w", err)
	}
	if err := s.UpdateSemanticTx(db, fileID, "", description); err != nil {
		return fmt.Errorf("update semantic: %w", err)
	}
	return nil
}

// ConfirmPendingFileAutoEmbeddingTx marks a pending uploaded file as confirmed
// without rewriting embedding columns. The auto-embedding mode lets TiDB own
// derived vector state after content_text becomes available.
func (s *Store) ConfirmPendingFileAutoEmbeddingTx(db execer, fileID string, storageType StorageType, storageRef, contentType string, size int64, description string) error {
	now := time.Now().UTC()
	if s.useLegacyFiles {
		query := `UPDATE files SET storage_type = ?, storage_ref = ?, content_type = ?,
			size_bytes = ?, checksum_sha256 = NULL, content_text = NULL`
		args := []any{storageType, storageRef, nullStr(contentType), size}
		if description != "" {
			query += `, description = ?, description_embedding_revision = revision`
			args = append(args, description)
		}
		query += `, status = 'CONFIRMED', confirmed_at = ? WHERE file_id = ? AND status = 'PENDING'`
		args = append(args, now, fileID)
		res, err := db.Exec(query, args...)
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
	} else {
		var status string
		if err := db.QueryRow(`SELECT status FROM inodes WHERE inode_id = ? FOR UPDATE`, fileID).Scan(&status); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if status != string(StatusPending) {
			return ErrNotFound
		}
	}
	// Dual-write to split tables
	if err := s.UpdateInodeContentTx(db, fileID, size, 1, StatusConfirmed, now); err != nil {
		return fmt.Errorf("update inode: %w", err)
	}
	if err := s.UpdateContentTx(db, fileID, storageType, storageRef, contentType, "", nil, StorageEncryptionLegacy); err != nil {
		return fmt.Errorf("update content: %w", err)
	}
	if description != "" {
		if err := s.updateSemanticNoEmbedTx(db, fileID, "", description); err != nil {
			return fmt.Errorf("update semantic: %w", err)
		}
	}
	return nil
}

// DeletePendingFileTx marks a pending upload file as DELETED and clears its
// storage ref inside an existing transaction. Used during overwrite when the
// new upload targets an existing path.
func (s *Store) DeletePendingFileTx(db execer, fileID string) error {
	now := time.Now().UTC()
	if s.useLegacyFiles {
		if _, err := db.Exec(`UPDATE files SET status = 'DELETED', storage_ref = '' WHERE file_id = ?`, fileID); err != nil {
			return err
		}
	}
	if _, err := db.Exec(`UPDATE inodes SET status = 'DELETED', mtime = ? WHERE inode_id = ?`, now, fileID); err != nil {
		return err
	}
	if _, err := db.Exec(`UPDATE contents SET storage_ref = '' WHERE inode_id = ?`, fileID); err != nil {
		return err
	}
	return nil
}

// ClearFileEmbeddingStateTx clears embedding columns inside an existing transaction.
func (s *Store) ClearFileEmbeddingStateTx(db execer, fileID string) error {
	if s.useLegacyFiles {
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
	} else {
		var exists int
		if err := db.QueryRow(`SELECT 1 FROM inodes WHERE inode_id = ?`, fileID).Scan(&exists); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
	}
	// Dual-write to split tables
	if _, err := db.Exec(`UPDATE semantic SET embedding = NULL, embedding_revision = NULL WHERE inode_id = ?`, fileID); err != nil {
		return fmt.Errorf("update semantic embedding: %w", err)
	}
	return nil
}
