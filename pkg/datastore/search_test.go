package datastore

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildVectorSearchQueryIncludesCurrentRevisionFilter(t *testing.T) {
	q, args, ok := buildVectorSearchQuery([]float32{0.1, 0.2, 0.3}, "/docs/", 7)
	if !ok {
		t.Fatal("expected non-empty query embedding to build vector search SQL")
	}
	if !strings.Contains(q, "f.embedding_revision = f.revision") {
		t.Fatalf("vector search SQL missing current-revision filter: %s", q)
	}
	if !strings.Contains(q, "VEC_EMBED_COSINE_DISTANCE(f.embedding, ?)") {
		t.Fatalf("vector search SQL missing vector-distance placeholder: %s", q)
	}
	wantArgs := []any{"[0.1,0.2,0.3]", "/docs", "/docs/%", "[0.1,0.2,0.3]", 7}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("vector search args=%#v, want %#v", args, wantArgs)
	}
}

func TestBuildVectorSearchQueryWithoutPathPrefix(t *testing.T) {
	q, args, ok := buildVectorSearchQuery([]float32{0.5, 0.4}, "/", 3)
	if !ok {
		t.Fatal("expected non-empty query embedding to build vector search SQL")
	}
	if strings.Contains(q, "fn.path = ? OR fn.path LIKE ?") {
		t.Fatalf("root path should not add subtree filter: %s", q)
	}
	wantArgs := []any{"[0.5,0.4]", "[0.5,0.4]", 3}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("vector search args=%#v, want %#v", args, wantArgs)
	}
}

func TestBuildVectorSearchQuerySkipsEmptyEmbedding(t *testing.T) {
	q, args, ok := buildVectorSearchQuery(nil, "/docs", 5)
	if ok {
		t.Fatalf("expected empty query embedding to short-circuit, got query=%q args=%#v", q, args)
	}
}

func TestBuildVectorSearchByTextQueryOmitsRevisionFilter(t *testing.T) {
	q, args, ok := buildVectorSearchByTextQuery("semantic probe", "/docs/", 7)
	if !ok {
		t.Fatal("expected non-empty query text to build vector search SQL")
	}
	if strings.Contains(q, "f.embedding_revision = f.revision") {
		t.Fatalf("text-query vector SQL should not depend on embedding_revision: %s", q)
	}
	if !strings.Contains(q, "VEC_EMBED_COSINE_DISTANCE(f.embedding, ?)") {
		t.Fatalf("text-query vector SQL missing distance expression: %s", q)
	}
	wantArgs := []any{"semantic probe", "/docs", "/docs/%", "semantic probe", 7}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("text-query vector args=%#v, want %#v", args, wantArgs)
	}
}

func TestBuildVectorSearchByTextQuerySkipsEmptyQuery(t *testing.T) {
	q, args, ok := buildVectorSearchByTextQuery("   ", "/docs", 5)
	if ok {
		t.Fatalf("expected empty query text to short-circuit, got query=%q args=%#v", q, args)
	}
}
