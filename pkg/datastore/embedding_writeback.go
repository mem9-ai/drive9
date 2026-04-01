package datastore

import (
	"context"
	"fmt"
	"time"

	"github.com/mem9-ai/dat9/pkg/embedding"
)

// UpdateFileEmbedding conditionally writes an embedding for the current file revision.
func (s *Store) UpdateFileEmbedding(ctx context.Context, fileID string, revision int64, vector []float32) (updated bool, err error) {
	start := time.Now()
	defer observeStoreOp(ctx, "update_file_embedding", start, &err)

	if len(vector) == 0 {
		return false, fmt.Errorf("embedding vector is required")
	}
	res, err := s.db.ExecContext(ctx, `UPDATE files SET embedding = ?, embedding_revision = ?
		WHERE file_id = ? AND revision = ? AND status = 'CONFIRMED'`,
		embedding.FormatVector(vector), revision, fileID, revision)
	if err != nil {
		return false, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rowsAffected > 0, nil
}
