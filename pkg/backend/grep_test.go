package backend

import (
	"context"
	"errors"
	"testing"
	"time"

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

type recordingQueryEmbedder struct {
	seen chan string
	err  error
}

func (e recordingQueryEmbedder) EmbedText(_ context.Context, text string) ([]float32, error) {
	select {
	case e.seen <- text:
	default:
	}
	return nil, e.err
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

func TestGrepEmbedsQueryInApplicationLayerBeforeFallback(t *testing.T) {
	seen := make(chan string, 1)
	b := newTestBackendWithOptions(t, Options{
		QueryEmbedding: QueryEmbeddingOptions{
			Client: recordingQueryEmbedder{seen: seen, err: errors.New("embed unavailable")},
		},
	})
	if _, err := b.Write("/notes/query.txt", []byte("semantic probe from app side embed"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	results, err := b.Grep(context.Background(), "semantic probe", "/notes/", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != "/notes/query.txt" {
		t.Fatalf("unexpected grep results: %+v", results)
	}

	select {
	case got := <-seen:
		if got != "semantic probe" {
			t.Fatalf("query embedder saw %q, want %q", got, "semantic probe")
		}
	case <-time.After(time.Second):
		t.Fatal("query embedder was not called")
	}
}

func TestGrepUsesFTSResultsWhenVectorQueryFails(t *testing.T) {
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

func TestGrepFallsBackToKeywordWhenFTSIsEmptyAndVectorFails(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		QueryEmbedding: QueryEmbeddingOptions{
			Client: staticQueryEmbedder{err: errors.New("vector unavailable")},
		},
	})
	if _, err := b.Write("/notes/d.txt", []byte("connection pooling keeps latency stable"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	results, err := b.Grep(context.Background(), "pool", "/notes/", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != "/notes/d.txt" {
		t.Fatalf("unexpected grep results: %+v", results)
	}
}

func TestGrepAutoEmbeddingSkipsApplicationQueryEmbedder(t *testing.T) {
	seen := make(chan string, 1)
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		QueryEmbedding: QueryEmbeddingOptions{
			Client: recordingQueryEmbedder{seen: seen, err: errors.New("should not be called")},
		},
	})
	if _, err := b.Write("/notes/auto.txt", []byte("semantic probe from database side"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	results, err := b.Grep(context.Background(), "semantic probe", "/notes/", 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Path != "/notes/auto.txt" {
		t.Fatalf("unexpected grep results: %+v", results)
	}

	select {
	case got := <-seen:
		t.Fatalf("query embedder was called in auto mode with %q", got)
	default:
	}
}
