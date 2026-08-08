package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
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
		if path != "/v1/fs"+GitWorkspaceIndexPath && path != GitWorkspaceIndexPath {
			// also accept url-encoded or cleaned forms
			if !(len(path) > 0 && (path == "/v1/fs/.drive9/git-workspaces/index.json")) {
				http.NotFound(w, r)
				return
			}
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
