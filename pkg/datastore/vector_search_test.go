package datastore

import (
	"context"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/tenant/schema"
)

// orthogonalUnitVector returns a unit vector of schema.TiDBAutoEmbeddingDimensions
// components with 1.0 at position idx and 0 everywhere else, so any two vectors
// built with distinct idx values are orthogonal (cosine distance 1, score 0).
func orthogonalUnitVector(idx int) []float32 {
	vec := make([]float32, schema.TiDBAutoEmbeddingDimensions)
	vec[idx] = 1.0
	return vec
}

func insertSearchTestFile(t *testing.T, s *Store, fileID, path string, mutate func(*File)) {
	t.Helper()
	now := time.Now().UTC()
	f := &File{
		FileID:      fileID,
		StorageType: StorageDB9,
		StorageRef:  "/blobs/" + fileID,
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}
	if mutate != nil {
		mutate(f)
	}
	if err := s.InsertFile(context.Background(), f); err != nil {
		t.Fatalf("insert file %s: %v", fileID, err)
	}
	if err := s.InsertNode(context.Background(), &FileNode{
		NodeID:     "n-" + fileID,
		Path:       path,
		ParentPath: parentPath(path),
		Name:       baseName(path),
		FileID:     fileID,
		CreatedAt:  now,
	}); err != nil {
		t.Fatalf("insert node %s: %v", path, err)
	}
}

func TestVectorSearchReturnsAppManagedEmbedding(t *testing.T) {
	cases := []struct {
		name   string
		prefix string
		update func(s *Store, ctx context.Context, fileID string, revision int64, embedding []float32) (bool, error)
		search func(s *Store, ctx context.Context, embedding []float32, pathPrefix string, limit int) ([]SearchResult, error)
	}{
		{
			name:   "content",
			prefix: "/notes/content/",
			update: (*Store).UpdateFileEmbedding,
			search: (*Store).VectorSearch,
		},
		{
			name:   "description",
			prefix: "/notes/description/",
			update: (*Store).UpdateFileDescriptionEmbedding,
			search: (*Store).VectorSearchDescription,
		},
	}

	s := newTestStore(t)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			catID := "f-" + tc.name + "-cat"
			dogID := "f-" + tc.name + "-dog"
			nothingID := "f-" + tc.name + "-nothing"
			catPath := tc.prefix + "cat.txt"
			dogPath := tc.prefix + "dog.txt"
			nothingPath := tc.prefix + "nothing.txt"

			mutate := func(text string) func(*File) {
				return func(f *File) {
					if tc.name == "description" {
						f.Description = text
					} else {
						f.ContentText = text
					}
				}
			}

			insertSearchTestFile(t, s, catID, catPath, mutate("a cat sleeping on a mat"))
			insertSearchTestFile(t, s, dogID, dogPath, mutate("a dog barking at the mailman"))
			insertSearchTestFile(t, s, nothingID, nothingPath, mutate("no embedding for this one"))

			e1 := orthogonalUnitVector(0)
			e2 := orthogonalUnitVector(1)

			if updated, err := tc.update(s, context.Background(), catID, 1, e1); err != nil {
				t.Fatalf("update %s embedding: %v", catID, err)
			} else if !updated {
				t.Fatalf("expected %s embedding update to succeed", catID)
			}
			if updated, err := tc.update(s, context.Background(), dogID, 1, e2); err != nil {
				t.Fatalf("update %s embedding: %v", dogID, err)
			} else if !updated {
				t.Fatalf("expected %s embedding update to succeed", dogID)
			}

			results, err := tc.search(s, context.Background(), e1, tc.prefix, 10)
			if err != nil {
				t.Fatalf("search(prefix=%q): %v", tc.prefix, err)
			}
			if len(results) != 1 {
				t.Fatalf("search(prefix=%q) results=%+v, want exactly 1", tc.prefix, results)
			}
			if results[0].Path != catPath {
				t.Fatalf("search(prefix=%q) path=%q, want %q", tc.prefix, results[0].Path, catPath)
			}
			if results[0].Score == nil {
				t.Fatalf("search(prefix=%q) missing score", tc.prefix)
			}
			if d := *results[0].Score - 1.0; d > 1e-4 || d < -1e-4 {
				t.Fatalf("search(prefix=%q) score=%v, want ~1.0", tc.prefix, *results[0].Score)
			}

			dogResults, err := tc.search(s, context.Background(), e2, tc.prefix, 10)
			if err != nil {
				t.Fatalf("search(e2): %v", err)
			}
			if len(dogResults) != 1 || dogResults[0].Path != dogPath {
				t.Fatalf("search(e2) results=%+v, want exactly %q", dogResults, dogPath)
			}
		})
	}
}
