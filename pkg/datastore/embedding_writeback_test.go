package datastore

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestUpdateFileEmbedding(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		Revision:    2,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateFileEmbedding(context.Background(), "f1", 2, []float32{0.1, 0.2, 0.3})
	if err != nil {
		t.Fatal(err)
	}
	if !updated {
		t.Fatal("expected update to succeed")
	}
	requireEmbeddingRevision(t, mustFile(t, s, "f1").EmbeddingRevision, 2)

	var raw sql.NullString
	if err := s.DB().QueryRow(`SELECT embedding FROM files WHERE file_id = ?`, "f1").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if raw.String != "[0.1,0.2,0.3]" {
		t.Fatalf("embedding=%q, want %q", raw.String, "[0.1,0.2,0.3]")
	}
}

func TestUpdateFileEmbeddingSkipsStaleRevision(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		Revision:    3,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}

	updated, err := s.UpdateFileEmbedding(context.Background(), "f1", 2, []float32{0.1, 0.2})
	if err != nil {
		t.Fatal(err)
	}
	if updated {
		t.Fatal("stale revision should not update embedding")
	}
	if got := mustFile(t, s, "f1").EmbeddingRevision; got != nil {
		t.Fatalf("embedding revision=%v, want nil", *got)
	}
}

func mustFile(t *testing.T, s *Store, fileID string) *File {
	t.Helper()
	f, err := s.GetFile(context.Background(), fileID)
	if err != nil {
		t.Fatalf("get file %s: %v", fileID, err)
	}
	return f
}
