package datastore

import (
	"context"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/embedding"
)

// UpdateFileEmbedding conditionally writes an embedding for the current file revision.
func (s *Store) UpdateFileEmbedding(ctx context.Context, fileID string, revision int64, vector []float32) (updated bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_file_embedding", start, &err)

	if len(vector) == 0 {
		return false, fmt.Errorf("embedding vector is required")
	}
	if s.useLegacyFiles {
		res, err := s.db.ExecContext(ctx, `UPDATE files SET embedding = ?, embedding_revision = ?
			WHERE file_id = ? AND revision = ? AND status = 'CONFIRMED'`,
			embedding.FormatVector(vector), revision, fileID, revision)
		if err != nil {
			return false, err
		}
		rowsAffected, _ := res.RowsAffected()
		// Always attempt semantic update regardless of legacy rowsAffected,
		// so a retry after transient semantic failure is not skipped.
		if _, err := s.db.ExecContext(ctx, `UPDATE semantic SET embedding = ?, embedding_revision = ?
			WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND revision = ? AND status = 'CONFIRMED')`,
			embedding.FormatVector(vector), revision, fileID, fileID, revision); err != nil {
			return false, fmt.Errorf("update semantic embedding: %w", err)
		}
		return rowsAffected > 0, nil
	}
	// New tenant without legacy files: write directly to semantic.
	res, err := s.db.ExecContext(ctx, `UPDATE semantic SET embedding = ?, embedding_revision = ?
		WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND revision = ? AND status = 'CONFIRMED')`,
		embedding.FormatVector(vector), revision, fileID, fileID, revision)
	if err != nil {
		return false, fmt.Errorf("update semantic embedding: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// UpdateFileDescriptionEmbedding conditionally writes a description embedding for the current file revision.
func (s *Store) UpdateFileDescriptionEmbedding(ctx context.Context, fileID string, revision int64, vector []float32) (updated bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_file_description_embedding", start, &err)

	if len(vector) == 0 {
		return false, fmt.Errorf("embedding vector is required")
	}
	if s.useLegacyFiles {
		res, err := s.db.ExecContext(ctx, `UPDATE files SET description_embedding = ?, description_embedding_revision = ?
			WHERE file_id = ? AND revision = ? AND status = 'CONFIRMED'`,
			embedding.FormatVector(vector), revision, fileID, revision)
		if err != nil {
			return false, err
		}
		rowsAffected, _ := res.RowsAffected()
		// Always attempt semantic update regardless of legacy rowsAffected.
		if _, err := s.db.ExecContext(ctx, `UPDATE semantic SET description_embedding = ?, description_embedding_revision = ?
			WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND revision = ? AND status = 'CONFIRMED')`,
			embedding.FormatVector(vector), revision, fileID, fileID, revision); err != nil {
			return false, fmt.Errorf("update semantic description_embedding: %w", err)
		}
		return rowsAffected > 0, nil
	}
	// New tenant without legacy files: write directly to semantic.
	res, err := s.db.ExecContext(ctx, `UPDATE semantic SET description_embedding = ?, description_embedding_revision = ?
		WHERE inode_id = ? AND EXISTS (SELECT 1 FROM inodes WHERE inode_id = ? AND revision = ? AND status = 'CONFIRMED')`,
		embedding.FormatVector(vector), revision, fileID, fileID, revision)
	if err != nil {
		return false, fmt.Errorf("update semantic description_embedding: %w", err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}
