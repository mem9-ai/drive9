package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Semantic represents a row in the semantic table (search & enrichment).
type Semantic struct {
	InodeID                        string
	ContentText                    string
	Description                    string
	EmbeddingRevision              *int64
	DescriptionEmbeddingRevision   *int64
}

// InsertSemantic inserts a semantic row.
func (s *Store) InsertSemantic(ctx context.Context, semantic *Semantic) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO semantic
		(inode_id, content_text, description, embedding_revision, description_embedding_revision)
		VALUES (?, ?, ?, ?, ?)`,
		semantic.InodeID, nullStr(semantic.ContentText), nullStr(semantic.Description),
		nullInt64Ptr(semantic.EmbeddingRevision), nullInt64Ptr(semantic.DescriptionEmbeddingRevision))
	return err
}

// InsertSemanticTx inserts a semantic row inside an existing transaction.
func (s *Store) InsertSemanticTx(db execer, semantic *Semantic) error {
	_, err := db.Exec(`INSERT INTO semantic
		(inode_id, content_text, description, embedding_revision, description_embedding_revision)
		VALUES (?, ?, ?, ?, ?)`,
		semantic.InodeID, nullStr(semantic.ContentText), nullStr(semantic.Description),
		nullInt64Ptr(semantic.EmbeddingRevision), nullInt64Ptr(semantic.DescriptionEmbeddingRevision))
	return err
}

// GetSemantic retrieves a semantic row by inode ID.
func (s *Store) GetSemantic(ctx context.Context, inodeID string) (*Semantic, error) {
	row := s.db.QueryRowContext(ctx, `SELECT inode_id, content_text, description,
		embedding_revision, description_embedding_revision
		FROM semantic WHERE inode_id = ?`, inodeID)
	return scanSemantic(row)
}

// UpdateSemanticTx updates semantic data inside a transaction, clearing embeddings.
func (s *Store) UpdateSemanticTx(db execer, inodeID string, contentText, description string) error {
	_, err := db.Exec(`UPDATE semantic SET
		content_text = ?, description = ?,
		embedding = NULL, embedding_revision = NULL,
		description_embedding = NULL, description_embedding_revision = NULL
		WHERE inode_id = ?`,
		nullStr(contentText), nullStr(description), inodeID)
	return err
}

// updateSemanticNoEmbedTx updates semantic data without clearing embeddings.
// Used by auto-embedding mode where the database owns vector state.
func (s *Store) updateSemanticNoEmbedTx(db execer, inodeID string, contentText, description string) error {
	_, err := db.Exec(`UPDATE semantic SET
		content_text = ?, description = ?
		WHERE inode_id = ?`,
		nullStr(contentText), nullStr(description), inodeID)
	return err
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
