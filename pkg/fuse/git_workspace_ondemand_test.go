package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/gitcache"
)

func TestDormantNeverListsGitWorkspaces(t *testing.T) {
	var listCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/git-workspaces" && r.Method == http.MethodGet {
			listCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []any{}})
			return
		}
		// index missing
		if r.URL.Path == client.GitWorkspaceIndexPath {
			http.NotFound(w, r)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)

	// Many path resolutions while dormant.
	for i := 0; i < 20; i++ {
		if _, _, ok := fs.gitWorkspaceForPath(context.Background(), "/repo/file.txt"); ok {
			t.Fatal("dormant gitWorkspaceForPath ok=true, want false")
		}
	}
	if got := listCalls.Load(); got != 0 {
		t.Fatalf("ListGitWorkspaces calls = %d, want 0 while dormant", got)
	}
}

func TestLocalArmSignalTriggersList(t *testing.T) {
	var listCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/git-workspaces" && r.Method == http.MethodGet:
			listCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []any{}})
		case r.URL.Path == client.GitWorkspaceIndexPath:
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	localRoot := t.TempDir()
	opts := &MountOptions{LocalRoot: localRoot, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)

	if _, _, ok := fs.gitWorkspaceForPath(context.Background(), "/x"); ok {
		t.Fatal("expected miss before arm")
	}
	if listCalls.Load() != 0 {
		t.Fatalf("list calls before arm = %d", listCalls.Load())
	}

	if err := gitcache.MarkWorkspaceRegistered(context.Background(), localRoot, "ws-new"); err != nil {
		t.Fatal(err)
	}
	// Next path op should arm + list once.
	_, _, _ = fs.gitWorkspaceForPath(context.Background(), "/x")
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("list calls after local arm = %d, want 1", got)
	}
	// Second op should not re-list without force (event-driven).
	_, _, _ = fs.gitWorkspaceForPath(context.Background(), "/y")
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("list calls after second op = %d, want 1 (no empty poll)", got)
	}
}

func TestProbeIndexArmsWhenEntriesExist(t *testing.T) {
	var listCalls atomic.Int64
	idx := client.GitWorkspaceIndex{
		Version: 1,
		Workspaces: []client.GitWorkspaceIndexEntry{{
			WorkspaceID: "ws1",
			RootPath:    "/repo/",
		}},
	}
	idxBody, _ := json.Marshal(idx)
	indexFSPath := "/v1/fs" + client.GitWorkspaceIndexPath
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == indexFSPath && r.Method == http.MethodHead:
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == indexFSPath && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", "1")
			_, _ = w.Write(idxBody)
		case r.URL.Path == "/v1/git-workspaces" && r.Method == http.MethodGet:
			listCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"workspaces": []map[string]any{{
					"workspace_id": "ws1",
					"root_path":    "/repo/",
					"head_commit":  fixtureHeadCommit,
					"mode":         "fast",
					"status":       "live",
				}},
			})
		case r.URL.Path == "/v1/git-workspaces/ws1/tree":
			_ = json.NewEncoder(w).Encode(map[string]any{"nodes": []any{}})
		case r.URL.Path == "/v1/git-workspaces/ws1/overlay":
			_ = json.NewEncoder(w).Encode(map[string]any{"entries": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true, RemoteRoot: "/"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)
	if err := fs.probeGitWorkspaceIndex(context.Background()); err != nil {
		t.Fatalf("probeGitWorkspaceIndex: %v", err)
	}
	if !fs.gitWorkspacesArmed() {
		t.Fatal("expected armed after index probe")
	}
	if got := listCalls.Load(); got < 1 {
		t.Fatalf("list calls after probe = %d, want >= 1", got)
	}
}

func TestProbeIndex404StaysDormant(t *testing.T) {
	var listCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/git-workspaces" {
			listCalls.Add(1)
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)
	if err := fs.probeGitWorkspaceIndex(context.Background()); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if fs.gitWorkspacesArmed() {
		t.Fatal("should not arm on index 404")
	}
	fs.git.mu.Lock()
	confirmed := fs.git.dormantConfirmed
	fs.git.mu.Unlock()
	if !confirmed {
		t.Fatal("dormantConfirmed = false after index 404")
	}
	_, _, _ = fs.gitWorkspaceForPath(context.Background(), "/a")
	if listCalls.Load() != 0 {
		t.Fatalf("list calls = %d after dormant confirm", listCalls.Load())
	}
}

func TestGitStatePathDoesNotListWhileDormant(t *testing.T) {
	var listCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/git-workspaces" {
			listCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []any{}})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)
	// Touch .git path while dormant — must not force list.
	_, _, _ = fs.gitWorkspaceForPath(context.Background(), "/repo/.git/HEAD")
	if listCalls.Load() != 0 {
		t.Fatalf("list calls on .git while dormant = %d, want 0", listCalls.Load())
	}
}

func TestArmedLocalMarkerForcesReloadForNewWorkspace(t *testing.T) {
	// Same-mount second --fast: already armed, new refresh/<id> must force list.
	var listCalls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/git-workspaces" && r.Method == http.MethodGet {
			listCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []any{}})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	localRoot := t.TempDir()
	opts := &MountOptions{LocalRoot: localRoot, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)

	if err := gitcache.MarkWorkspaceRegistered(context.Background(), localRoot, "ws-a"); err != nil {
		t.Fatal(err)
	}
	_, _, _ = fs.gitWorkspaceForPath(context.Background(), "/a")
	first := listCalls.Load()
	if first < 1 {
		t.Fatalf("first list = %d, want >= 1", first)
	}

	// Advance local arm signal with a new workspace id (same mount, second --fast).
	time.Sleep(5 * time.Millisecond)
	if err := gitcache.MarkWorkspaceRegistered(context.Background(), localRoot, "ws-b"); err != nil {
		t.Fatal(err)
	}
	// Clear throttle so scan sees new mtime.
	fs.git.mu.Lock()
	fs.git.localArmScanAt = time.Time{}
	fs.git.mu.Unlock()
	_, _, _ = fs.gitWorkspaceForPath(context.Background(), "/b")
	if got := listCalls.Load(); got <= first {
		t.Fatalf("list calls after second local arm = %d, want > %d", got, first)
	}
}

func TestEnsureGitWorkspacesStillWorksForTests(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	if err := fs.ensureGitWorkspaces(context.Background()); err != nil {
		t.Fatalf("ensureGitWorkspaces: %v", err)
	}
	if !fs.gitWorkspacesArmed() {
		t.Fatal("ensureGitWorkspaces should arm")
	}
	rt, rel, ok := fs.loadedGitWorkspaceForPath("/repo/README.md")
	if !ok || rt == nil || rel != "README.md" {
		t.Fatalf("loaded path ok=%v rel=%q", ok, rel)
	}
	_ = time.Second
}
