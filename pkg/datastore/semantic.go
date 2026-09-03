package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Semantic represents a row in the semantic table (search & enrichment).
type Semantic struct {
	InodeID                      string
	ContentText                  string
	Description                  string
	EmbeddingRevision            *int64
	DescriptionEmbeddingRevision *int64
}

// InsertSemantic inserts a semantic row.
func (s *Store) InsertSemantic(ctx context.Context, semantic *Semantic) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO semantic
		(`+s.scope.InsCols(`inode_id, content_text, description, embedding_revision, description_embedding_revision`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(semantic.InodeID, nullStr(semantic.ContentText), nullStr(semantic.Description),
			nullInt64Ptr(semantic.EmbeddingRevision), nullInt64Ptr(semantic.DescriptionEmbeddingRevision))...)
	return err
}

// InsertSemanticTx inserts a semantic row inside an existing transaction.
func (s *Store) InsertSemanticTx(db execer, semantic *Semantic) error {
	_, err := db.Exec(`INSERT INTO semantic
		(`+s.scope.InsCols(`inode_id, content_text, description, embedding_revision, description_embedding_revision`)+`)
		VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?`)+`)`,
		s.scope.Args(semantic.InodeID, nullStr(semantic.ContentText), nullStr(semantic.Description),
			nullInt64Ptr(semantic.EmbeddingRevision), nullInt64Ptr(semantic.DescriptionEmbeddingRevision))...)
	return err
}

// GetSemantic retrieves a semantic row by inode ID.
func (s *Store) GetSemantic(ctx context.Context, inodeID string) (*Semantic, error) {
	row := s.db.QueryRowContext(ctx, `SELECT inode_id, content_text, description,
		embedding_revision, description_embedding_revision
		FROM semantic WHERE `+s.scope.And(`inode_id = ?`), s.scope.Args(inodeID)...)
	return scanSemantic(row)
}

// UpdateSemanticTx updates semantic data inside a transaction, clearing embeddings.
func (s *Store) UpdateSemanticTx(db execer, inodeID string, contentText, description string) error {
	_, err := db.Exec(`UPDATE semantic SET
		content_text = ?, description = ?,
		embedding = NULL, embedding_revision = NULL,
		description_embedding = NULL, description_embedding_revision = NULL
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{nullStr(contentText), nullStr(description)}, s.scope.Args(inodeID)...)...)
	return err
}

// updateSemanticNoEmbedTx updates semantic data without clearing embeddings.
// Used by auto-embedding mode where the database owns vector state.
func (s *Store) updateSemanticNoEmbedTx(db execer, inodeID string, contentText, description string) error {
	_, err := db.Exec(`UPDATE semantic SET
		content_text = ?, description = ?
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{nullStr(contentText), nullStr(description)}, s.scope.Args(inodeID)...)...)
	return err
}

// LockConfirmedFileRevisionTx locks the confirmed inode row for fileID inside
// an existing transaction and returns its current revision.
func (s *Store) LockConfirmedFileRevisionTx(db execer, fileID string) (int64, error) {
	var rev int64
	if err := db.QueryRow(`SELECT revision FROM inodes WHERE `+s.scope.And(`inode_id = ? AND status = 'CONFIRMED'`)+` FOR UPDATE`, s.scope.Args(fileID)...).Scan(&rev); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, fmt.Errorf("read current revision: %w", err)
	}
	return rev, nil
}

// UpdateFileDescriptionTx updates only the description of a confirmed file,
// clearing the description embedding so app-managed semantic processing
// recomputes it. Callers that serialize against concurrent content writes
// must hold the inode lock (LockConfirmedFileRevisionTx) first.
func (s *Store) UpdateFileDescriptionTx(db execer, fileID, description string) error {
	return s.updateFileDescriptionTx(db, fileID, description, false)
}

// UpdateFileDescriptionAutoEmbeddingTx updates only the description of a
// confirmed file without touching vector columns. Auto-embedding mode relies
// on the database to derive vectors from description. Callers that serialize
// against concurrent content writes must hold the inode lock
// (LockConfirmedFileRevisionTx) first.
func (s *Store) UpdateFileDescriptionAutoEmbeddingTx(db execer, fileID, description string) error {
	return s.updateFileDescriptionTx(db, fileID, description, true)
}

func (s *Store) updateFileDescriptionTx(db execer, fileID, description string, autoEmbedding bool) error {
	if s.useLegacyFiles {
		query := `UPDATE files SET description = ?`
		if !autoEmbedding {
			query += `, description_embedding = NULL, description_embedding_revision = NULL`
		}
		query += ` WHERE file_id = ?`
		if _, err := db.Exec(query, nullStr(description), fileID); err != nil {
			return err
		}
	}
	if autoEmbedding {
		if _, err := db.Exec(`UPDATE semantic SET description = ?
			WHERE `+s.scope.And(`inode_id = ?`),
			append([]any{nullStr(description)}, s.scope.Args(fileID)...)...); err != nil {
			return err
		}
		return nil
	}
	if _, err := db.Exec(`UPDATE semantic SET
		description = ?,
		description_embedding = NULL, description_embedding_revision = NULL
		WHERE `+s.scope.And(`inode_id = ?`),
		append([]any{nullStr(description)}, s.scope.Args(fileID)...)...); err != nil {
		return err
	}
	return nil
}

func scanSemantic(row *sql.Row) (*Semantic, error) {
	var sem Semantic
	var contentText, description sql.NullString
	var embRev, descEmbRev sql.NullInt64
	err := row.Scan(&sem.InodeID, &contentText, &description, &embRev, &descEmbRev)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan semantic: %w", err)
	}
	sem.ContentText = contentText.String
	sem.Description = description.String
	sem.EmbeddingRevision = nullInt64PtrValue(embRev)
	sem.DescriptionEmbeddingRevision = nullInt64PtrValue(descEmbRev)
	return &sem, nil
}

func nullInt64PtrValue(v sql.NullInt64) *int64 {
	if v.Valid {
		return &v.Int64
	}
	return nil
}
