package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Content represents a row in the contents table (storage blob metadata).
type Content struct {
	InodeID                string
	StorageType            StorageType
	StorageRef             string
	StorageRefHash         string
	StorageEncryptionMode  StorageEncryptionMode
	StorageEncryptionKeyID string
	ContentBlob            []byte
	ContentType            string
	ChecksumSHA256         string
	SourceID               string
}

// InsertContent inserts a content row.
func (s *Store) InsertContent(ctx context.Context, content *Content) error {
	mode := fileStorageEncryptionModeForWrite(content.StorageEncryptionMode)
	_, err := s.db.ExecContext(ctx, `INSERT INTO contents
		(inode_id, storage_type, storage_ref, storage_ref_hash, storage_encryption_mode, storage_encryption_key_id,
		 content_blob, content_type, checksum_sha256, source_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		content.InodeID, content.StorageType, content.StorageRef, StorageRefHash(content.StorageRef), mode,
		storageEncryptionKeyIDForWrite(mode, content.StorageEncryptionKeyID),
		nilBytes(content.ContentBlob), nullStr(content.ContentType),
		nullStr(content.ChecksumSHA256), nullStr(content.SourceID))
	return err
}

// InsertContentTx inserts a content row inside an existing transaction.
func (s *Store) InsertContentTx(db execer, content *Content) error {
	mode := fileStorageEncryptionModeForWrite(content.StorageEncryptionMode)
	_, err := db.Exec(`INSERT INTO contents
		(inode_id, storage_type, storage_ref, storage_ref_hash, storage_encryption_mode, storage_encryption_key_id,
		 content_blob, content_type, checksum_sha256, source_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		content.InodeID, content.StorageType, content.StorageRef, StorageRefHash(content.StorageRef), mode,
		storageEncryptionKeyIDForWrite(mode, content.StorageEncryptionKeyID),
		nilBytes(content.ContentBlob), nullStr(content.ContentType),
		nullStr(content.ChecksumSHA256), nullStr(content.SourceID))
	return err
}

// GetContent retrieves a content row by inode ID.
func (s *Store) GetContent(ctx context.Context, inodeID string) (*Content, error) {
	row := s.db.QueryRowContext(ctx, `SELECT inode_id, storage_type, storage_ref,
		storage_ref_hash, storage_encryption_mode, storage_encryption_key_id,
		content_blob, content_type, checksum_sha256, source_id
		FROM contents WHERE inode_id = ?`, inodeID)
	return scanContent(row)
}

// UpdateContentTx updates content metadata inside a transaction.
func (s *Store) UpdateContentTx(db execer, inodeID string, storageType StorageType, storageRef, contentType, checksum string, contentBlob []byte, storageEncryptionMode StorageEncryptionMode) error {
	mode := fileStorageEncryptionModeForWrite(storageEncryptionMode)
	_, err := db.Exec(`UPDATE contents SET
		storage_type = ?, storage_ref = ?, storage_ref_hash = ?, storage_encryption_mode = ?,
		content_blob = ?, content_type = ?, checksum_sha256 = ?
		WHERE inode_id = ?`,
		storageType, storageRef, StorageRefHash(storageRef), mode,
		nilBytes(contentBlob), nullStr(contentType), nullStr(checksum), inodeID)
	return err
}

func scanContent(row *sql.Row) (*Content, error) {
	var c Content
	var contentType, checksum, sourceID sql.NullString
	var encryptionMode string
	err := row.Scan(&c.InodeID, &c.StorageType, &c.StorageRef,
		&c.StorageRefHash, &encryptionMode, &c.StorageEncryptionKeyID,
		&c.ContentBlob, &contentType, &checksum, &sourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan content: %w", err)
	}
	c.StorageEncryptionMode = StorageEncryptionMode(encryptionMode)
	if c.StorageRefHash == "" {
		c.StorageRefHash = StorageRefHash(c.StorageRef)
	}
	c.ContentType = contentType.String
	c.ChecksumSHA256 = checksum.String
	c.SourceID = sourceID.String
	return &c, nil
}
