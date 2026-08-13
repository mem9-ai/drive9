package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func TestUpsertAndRemoveGitWorkspaceIndex(t *testing.T) {
	var mu sync.Mutex
	var body []byte
	var rev int64
	exists := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Client paths are under /v1/fs/...
		path := r.URL.Path
		if path != "/v1/fs"+GitWorkspaceIndexPath && path != GitWorkspaceIndexPath && path != "/v1/fs/.drive9/git-workspaces/index.json" {
			http.NotFound(w, r)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if !exists {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if !exists {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_, _ = w.Write(body)
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			// honor create-only expected revision 0
			if exp := r.Header.Get("X-Dat9-Expected-Revision"); exp != "" {
				want, _ := strconv.ParseInt(exp, 10, 64)
				if want == 0 && exists {
					http.Error(w, "conflict", http.StatusConflict)
					return
				}
				if want > 0 && (!exists || want != rev) {
					http.Error(w, "conflict", http.StatusConflict)
					return
				}
			}
			body = data
			exists = true
			rev++
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": rev})
		case http.MethodDelete:
			exists = false
			body = nil
			rev++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "test-key")
	ctx := context.Background()
	if err := c.UpsertGitWorkspaceIndexEntry(ctx, GitWorkspaceIndexEntry{
		WorkspaceID:   "ws1",
		RootPath:      "/repo/",
		WorkspaceKind: "main",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	idx, _, err := c.ReadGitWorkspaceIndex(ctx)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !GitWorkspaceIndexHasEntries(idx) || idx.Workspaces[0].WorkspaceID != "ws1" {
		t.Fatalf("index = %+v", idx)
	}
	if err := c.RemoveGitWorkspaceIndexEntry(ctx, "ws1"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	idx, _, err = c.ReadGitWorkspaceIndex(ctx)
	if err != nil {
		t.Fatalf("Read after remove: %v", err)
	}
	if GitWorkspaceIndexHasEntries(idx) {
		t.Fatalf("want empty index after remove, got %+v", idx)
	}
}

func TestReadGitWorkspaceIndexRejectsOversizedDocument(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", strconv.Itoa(MaxGitWorkspaceIndexBytes+1))
			w.Header().Set("X-Dat9-Revision", "1")
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Error(w, "unexpected", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	c := New(srv.URL, "test-key")
	_, _, err := c.ReadGitWorkspaceIndex(context.Background())
	if !errors.Is(err, ErrGitWorkspaceIndexTooLarge) {
		t.Fatalf("Read oversized = %v, want ErrGitWorkspaceIndexTooLarge", err)
	}
}

func TestParseGitWorkspaceIndexRejectsTooManyEntries(t *testing.T) {
	entries := make([]GitWorkspaceIndexEntry, MaxGitWorkspaceIndexEntries+1)
	for i := range entries {
		entries[i] = GitWorkspaceIndexEntry{
			WorkspaceID: "ws" + strconv.Itoa(i),
			RootPath:    "/r" + strconv.Itoa(i) + "/",
		}
	}
	body, err := json.Marshal(GitWorkspaceIndex{Version: 1, Workspaces: entries})
	if err != nil {
		t.Fatal(err)
	}
	if int64(len(body)) > MaxGitWorkspaceIndexBytes {
		t.Skip("entry-count fixture exceeded byte cap")
	}
	_, err = parseGitWorkspaceIndex(body)
	if !errors.Is(err, ErrGitWorkspaceIndexTooLarge) {
		t.Fatalf("parse many entries = %v, want ErrGitWorkspaceIndexTooLarge", err)
	}
}

func TestUpsertPreservesUnknownIndexFields(t *testing.T) {
	var mu sync.Mutex
	var body []byte
	var rev int64
	exists := false

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.Method {
		case http.MethodHead:
			if !exists {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if !exists {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_, _ = w.Write(body)
		case http.MethodPut:
			data, _ := io.ReadAll(r.Body)
			body = data
			exists = true
			rev++
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", strconv.FormatInt(rev, 10))
			_ = json.NewEncoder(w).Encode(map[string]any{"revision": rev})
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(srv.Close)

	seed := []byte(`{
  "version": 1,
  "schema_note": "keep-top",
  "workspaces": [
    {"workspace_id": "ws-old", "root_path": "/old/", "alias": "keep-entry"}
  ]
}`)
	mu.Lock()
	body = seed
	exists = true
	rev = 1
	mu.Unlock()

	c := New(srv.URL, "test-key")
	if err := c.UpsertGitWorkspaceIndexEntry(context.Background(), GitWorkspaceIndexEntry{
		WorkspaceID: "ws-new",
		RootPath:    "/new/",
	}); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := c.RemoveGitWorkspaceIndexEntry(context.Background(), "ws-new"); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	idx, _, err := c.ReadGitWorkspaceIndex(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	mu.Lock()
	got := string(body)
	mu.Unlock()
	if !strings.Contains(got, `"schema_note"`) || !strings.Contains(got, `keep-top`) {
		t.Fatalf("top-level extra stripped: %s", got)
	}
	if !strings.Contains(got, `"alias"`) || !strings.Contains(got, `keep-entry`) {
		t.Fatalf("per-entry extra stripped: %s", got)
	}
	if idx == nil || len(idx.Workspaces) != 1 || idx.Workspaces[0].WorkspaceID != "ws-old" {
		t.Fatalf("index after upsert/remove = %+v", idx)
	}
}
