package datastore

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"
)

// insertSharedSearchFixture writes one confirmed file (inode + dentry +
// semantic row) directly with an explicit fs_id. Store write methods are not
// scope-aware yet, so shared-shape fixtures bypass them.
func insertSharedSearchFixture(t *testing.T, s *Store, fsID int64, inodeID, path, contentText string) {
	t.Helper()
	now := time.Now()
	if _, err := s.DB().Exec(`INSERT INTO inodes
		(fs_id, inode_id, size_bytes, revision, status, confirmed_at)
		VALUES (?, ?, ?, 1, 'CONFIRMED', ?)`,
		fsID, inodeID, len(contentText), now); err != nil {
		t.Fatalf("insert inode %s: %v", inodeID, err)
	}
	parent := "/"
	name := path
	if idx := strings.LastIndex(strings.TrimSuffix(path, "/"), "/"); idx >= 0 {
		parent = path[:idx+1]
		name = path[idx+1:]
	}
	if _, err := s.DB().Exec(`INSERT INTO file_nodes
		(fs_id, node_id, path, path_hash, parent_path, parent_path_hash, name, is_directory, file_id, inode_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`,
		fsID, "node-"+inodeID, path, fileNodePathHash(path), parent, fileNodePathHash(parent), name, inodeID, inodeID, now); err != nil {
		t.Fatalf("insert file_node %s: %v", path, err)
	}
	if _, err := s.DB().Exec(`INSERT INTO semantic (fs_id, inode_id, content_text) VALUES (?, ?, ?)`,
		fsID, inodeID, contentText); err != nil {
		t.Fatalf("insert semantic %s: %v", inodeID, err)
	}
}

// TestBuildVectorSearchQuerySharedShape asserts the scoped vector query
// filters every joined alias and binds the fs_ids right after the
// SELECT-list distance placeholder.
func TestBuildVectorSearchQuerySharedShape(t *testing.T) {
	const fsID int64 = 4400004
	q, args, ok := buildVectorSearchQueryScoped(SharedScope(fsID), []float32{0.1, 0.2, 0.3}, "/docs/", 7)
	if !ok {
		t.Fatal("expected non-empty query embedding to build vector search SQL")
	}
	if !strings.Contains(q, "fn.fs_id = ? AND i.fs_id = ? AND s.fs_id = ? AND i.status = 'CONFIRMED'") {
		t.Fatalf("shared vector search SQL missing leading fs_id predicates: %s", q)
	}
	wantArgs := []any{"[0.1,0.2,0.3]", fsID, fsID, fsID, "/docs", "/docs/%", "[0.1,0.2,0.3]", 7}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("shared vector search args=%#v, want %#v", args, wantArgs)
	}
}

// TestBuildFTSSearchQuerySharedShape asserts the fs_id predicate lands inside
// each UNION branch (before aggregation/LIMIT) and on each outer-join alias
// in shared shape, and that the standalone shape stays free of fs_id
// predicates.
func TestBuildFTSSearchQuerySharedShape(t *testing.T) {
	const fsID int64 = 4400005
	q, args := buildFTSSearchQuery(SharedScope(fsID), "hello", "/", 5)
	if got := strings.Count(q, "fs_id = ?"); got != 4 {
		t.Fatalf("shared FTS SQL has %d fs_id predicates, want 4 (2 UNION branches + 2 outer aliases): %s", got, q)
	}
	if !strings.Contains(q, "FROM semantic WHERE fs_id = ? AND fts_match_word('hello', content_text)") {
		t.Fatalf("content_text UNION branch missing leading fs_id predicate: %s", q)
	}
	if strings.Contains(q, "fts.fs_id") {
		t.Fatalf("fs_id predicate must not leak onto the derived table: %s", q)
	}
	wantArgs := []any{fsID, fsID, 5, fsID, fsID}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("shared FTS args=%#v, want %#v", args, wantArgs)
	}

	qStandalone, argsStandalone := buildFTSSearchQuery(StandaloneScope(0), "hello", "/", 5)
	if strings.Contains(qStandalone, "fs_id") {
		t.Fatalf("standalone FTS SQL must not reference fs_id: %s", qStandalone)
	}
	if !reflect.DeepEqual(argsStandalone, []any{5}) {
		t.Fatalf("standalone FTS args=%#v, want [5]", argsStandalone)
	}
}

// TestKeywordSearchSharedShapeParity runs the LIKE-based keyword search (the
// only search path plain MySQL supports) against the shared schema shape.
func TestKeywordSearchSharedShapeParity(t *testing.T) {
	installSharedCoreFSSchema(t)
	const fsID int64 = 4400001
	store := newSharedStore(t, fsID)
	insertSharedSearchFixture(t, store, fsID, "ino-kw1", "/docs/hello.txt", "hello semantic world")
	insertSharedSearchFixture(t, store, fsID, "ino-kw2", "/docs/other.txt", "unrelated content")

	results, err := store.KeywordSearch(context.Background(), "semantic", "/", 10)
	if err != nil {
		t.Fatalf("KeywordSearch: %v", err)
	}
	if len(results) != 1 || results[0].Path != "/docs/hello.txt" {
		t.Fatalf("results = %#v, want only /docs/hello.txt", results)
	}
	if results[0].SizeBytes != int64(len("hello semantic world")) {
		t.Fatalf("size = %d, want %d", results[0].SizeBytes, len("hello semantic world"))
	}
}

// TestKeywordSearchSharedShapeCrossTenantIsolation proves identical rows
// under two fs_ids stay invisible to each other: each store's keyword search
// returns only its own tenant's dentry.
func TestKeywordSearchSharedShapeCrossTenantIsolation(t *testing.T) {
	installSharedCoreFSSchema(t)
	const fsA, fsB int64 = 4400002, 4400003
	storeA := newSharedStore(t, fsA)
	storeB := newSharedStore(t, fsB)

	// Same inode_id and content_text under both fs_ids, distinct paths.
	insertSharedSearchFixture(t, storeA, fsA, "ino-shared", "/a/secret.txt", "cross-tenant probe token")
	insertSharedSearchFixture(t, storeB, fsB, "ino-shared", "/b/secret.txt", "cross-tenant probe token")

	resultsA, err := storeA.KeywordSearch(context.Background(), "probe token", "/", 10)
	if err != nil {
		t.Fatalf("KeywordSearch A: %v", err)
	}
	if len(resultsA) != 1 || resultsA[0].Path != "/a/secret.txt" {
		t.Fatalf("A results = %#v, want only /a/secret.txt (cross-tenant leak?)", resultsA)
	}
	resultsB, err := storeB.KeywordSearch(context.Background(), "probe token", "/", 10)
	if err != nil {
		t.Fatalf("KeywordSearch B: %v", err)
	}
	if len(resultsB) != 1 || resultsB[0].Path != "/b/secret.txt" {
		t.Fatalf("B results = %#v, want only /b/secret.txt (cross-tenant leak?)", resultsB)
	}
}
