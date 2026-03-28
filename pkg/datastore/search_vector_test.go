package datastore

import (
	"strings"
	"testing"
)

func TestBuildVectorSearchQueryIncludesTextAndImageEmbeddings(t *testing.T) {
	q, args := buildVectorSearchQuery("invoice screenshot", "/docs/", 25)

	if !strings.Contains(q, "VEC_EMBED_COSINE_DISTANCE(f.embedding,") {
		t.Fatalf("expected query to include text embedding distance: %s", q)
	}
	if !strings.Contains(q, "VEC_EMBED_COSINE_DISTANCE(f.embedding_image,") {
		t.Fatalf("expected query to include image embedding distance: %s", q)
	}
	if !strings.Contains(q, "CASE") {
		t.Fatalf("expected query to route by file type: %s", q)
	}
	if !strings.Contains(q, "f.content_type LIKE 'image/%'") {
		t.Fatalf("expected image content-type routing in query: %s", q)
	}

	if len(args) != 5 {
		t.Fatalf("expected 5 args, got %d", len(args))
	}
	if got, ok := args[0].(string); !ok || got != "invoice screenshot" {
		t.Fatalf("unexpected first arg: %#v", args[0])
	}
	if got, ok := args[1].(string); !ok || got != "invoice screenshot" {
		t.Fatalf("unexpected second query arg: %#v", args[1])
	}
	if got, ok := args[2].(string); !ok || got != "/docs" {
		t.Fatalf("unexpected path arg #2: %#v", args[2])
	}
	if got, ok := args[3].(string); !ok || got != "/docs/%" {
		t.Fatalf("unexpected path arg #3: %#v", args[3])
	}
	if got, ok := args[4].(int); !ok || got != 25 {
		t.Fatalf("unexpected limit arg: %#v", args[4])
	}
}

func TestBuildVectorSearchQueryRootPathHasNoPathArgs(t *testing.T) {
	_, args := buildVectorSearchQuery("q", "/", 10)
	if len(args) != 3 {
		t.Fatalf("expected 3 args for root path, got %d", len(args))
	}
}
