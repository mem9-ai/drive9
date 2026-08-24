package datastore

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant/schema"
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

	vec := testEmbeddingVector(0.1, 0.2, 0.3)
	updated, err := s.UpdateFileEmbedding(context.Background(), "f1", 2, vec)
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
	got := parseStoredVector(t, raw.String)
	wantPrefix := []float32{0.1, 0.2, 0.3}
	if len(got) < len(wantPrefix) {
		t.Fatalf("embedding=%q parsed=%v, want at least %d components", raw.String, got, len(wantPrefix))
	}
	for i, want := range wantPrefix {
		if d := got[i] - want; d > 1e-4 || d < -1e-4 {
			t.Fatalf("embedding[%d]=%v from %q, want %v", i, got[i], raw.String, want)
		}
	}
}

func parseStoredVector(t *testing.T, raw string) []float32 {
	t.Helper()
	s := strings.TrimSpace(raw)
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]float32, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		f, err := strconv.ParseFloat(p, 32)
		if err != nil {
			t.Fatalf("parse vector component %q from %q: %v", p, raw, err)
		}
		out = append(out, float32(f))
	}
	return out
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

	updated, err := s.UpdateFileEmbedding(context.Background(), "f1", 2, testEmbeddingVector(0.1, 0.2))
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

func testEmbeddingVector(prefix ...float32) []float32 {
	vec := make([]float32, schema.TiDBAutoEmbeddingDimensions)
	copy(vec, prefix)
	return vec
}
