package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

type staticQueryEmbedder struct {
	vec []float32
	err error
}

func (e staticQueryEmbedder) EmbedText(context.Context, string) ([]float32, error) {
	if e.err != nil {
		return nil, e.err
	}
	return append([]float32(nil), e.vec...), nil
}

func TestGrepFallsBackToKeywordWithoutEmbedder(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/notes/a.txt", []byte("hello keyword fallback"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	results, err := b.Grep(context.Background(), "keyword", "/notes/", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != "/notes/a.txt" {
		t.Fatalf("unexpected grep results: %+v", results)
	}
}

func TestGrepFallsBackToKeywordWhenEmbeddingUnavailable(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		QueryEmbedding: QueryEmbeddingOptions{
			Client: staticQueryEmbedder{err: errors.New("embed unavailable")},
		},
	})
	if _, err := b.Write("/notes/b.txt", []byte("hello semantic fallback"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	results, err := b.Grep(context.Background(), "semantic", "/notes/", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != "/notes/b.txt" {
		t.Fatalf("unexpected grep results: %+v", results)
	}
}

func TestGrepFallsBackToKeywordWhenVectorQueryFails(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		QueryEmbedding: QueryEmbeddingOptions{
			Client: staticQueryEmbedder{vec: []float32{0.1, 0.2, 0.3}},
		},
	})
	if _, err := b.Write("/notes/c.txt", []byte("hello vector fallback"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	results, err := b.Grep(context.Background(), "vector", "/notes/", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != "/notes/c.txt" {
		t.Fatalf("unexpected grep results: %+v", results)
	}
}
