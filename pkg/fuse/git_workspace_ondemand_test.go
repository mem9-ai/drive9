package fuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
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
	// Generation fingerprints marker names+bodies, so equal FS mtimes still force-list.
	if err := gitcache.MarkWorkspaceRegistered(context.Background(), localRoot, "ws-b"); err != nil {
		t.Fatal(err)
	}
	// Pin all marker mtimes to the same second to prove we do not rely on mtime alone.
	fixed := time.Unix(1_700_000_000, 0)
	for _, p := range []string{
		gitcache.WorkspaceArmedPath(localRoot),
		gitcache.WorkspaceRefreshMarkerPath(localRoot, "ws-a"),
		gitcache.WorkspaceRefreshMarkerPath(localRoot, "ws-b"),
	} {
		if err := os.Chtimes(p, fixed, fixed); err != nil {
			t.Fatal(err)
		}
	}
	// Clear throttle so scan re-reads markers.
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
}

func TestProbeIndex401DoesNotLatchDormantAndRetries(t *testing.T) {
	// First Stat returns 401 (transient); later retries must still arm when the
	// index becomes readable. Only 403 permanently latches dormantConfirmed.
	var headCalls atomic.Int64
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
			n := headCalls.Add(1)
			if n == 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			w.Header().Set("X-Dat9-Revision", "1")
			w.Header().Set("Content-Length", "10")
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == indexFSPath && r.Method == http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Dat9-Revision", "1")
			_, _ = w.Write(idxBody)
		case r.URL.Path == "/v1/git-workspaces" && r.Method == http.MethodGet:
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

	// Single probe: 401 must surface as error and must not latch dormant.
	err := fs.probeGitWorkspaceIndex(context.Background())
	if err == nil {
		t.Fatal("probeGitWorkspaceIndex: want 401 error, got nil")
	}
	if !client.IsUnauthorized(err) {
		t.Fatalf("probe err = %v, want unauthorized", err)
	}
	fs.git.mu.Lock()
	confirmed := fs.git.dormantConfirmed
	armed := fs.git.armed
	fs.git.mu.Unlock()
	if confirmed {
		t.Fatal("dormantConfirmed latched on 401; want retryable")
	}
	if armed {
		t.Fatal("armed on 401; want unarmed until readable index")
	}

	// Async probe loop retries after backoff and arms when Stat succeeds.
	done := make(chan struct{})
	go func() {
		fs.runGitWorkspaceIndexProbeLoop()
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fs.gitWorkspacesArmed() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !fs.gitWorkspacesArmed() {
		t.Fatal("expected armed after 401 then successful index Stat retry")
	}
	fs.git.mu.Lock()
	confirmed = fs.git.dormantConfirmed
	fs.git.mu.Unlock()
	if confirmed {
		t.Fatal("dormantConfirmed true after successful retry arm")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		// Loop exits on arm; allow a short grace period.
	}
	if got := headCalls.Load(); got < 2 {
		t.Fatalf("index HEAD calls = %d, want >= 2 (401 then success)", got)
	}
}

func TestProbeIndex403LatchesDormant(t *testing.T) {
	indexFSPath := "/v1/fs" + client.GitWorkspaceIndexPath
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == indexFSPath {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true, RemoteRoot: "/"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)

	done := make(chan struct{})
	go func() {
		fs.runGitWorkspaceIndexProbeLoop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("probe loop did not stop on 403")
	}
	fs.git.mu.Lock()
	confirmed := fs.git.dormantConfirmed
	armed := fs.git.armed
	fs.git.mu.Unlock()
	if !confirmed {
		t.Fatal("dormantConfirmed = false after 403; want permanent latch")
	}
	if armed {
		t.Fatal("armed after 403; want unarmed dormant")
	}
}

func TestPendingForceOrdinaryLookupsSingleList(t *testing.T) {
	// 1 external force + 2 ordinary concurrent lookups must produce exactly one
	// ListGitWorkspaces. Ordinary joins must not bump pendingForceGen (storm).
	var listCalls atomic.Int64
	listEntered := make(chan struct{}, 1)
	releaseList := make(chan struct{})
	var enteredOnce sync.Once
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/git-workspaces" && r.Method == http.MethodGet {
			n := listCalls.Add(1)
			if n == 1 {
				enteredOnce.Do(func() { listEntered <- struct{}{} })
				select {
				case <-releaseList:
				case <-time.After(5 * time.Second):
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []any{}})
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true, RemoteRoot: "/"}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient(srv.URL), opts)
	fs.markGitWorkspacesArmed()

	forceErr := make(chan error, 1)
	go func() {
		forceErr <- fs.forceRefreshGitWorkspaces(context.Background())
	}()
	select {
	case <-listEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("forced list never entered")
	}

	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _ = fs.gitWorkspaceForPath(context.Background(), "/x")
		}()
	}
	// Let waiters join the in-flight force before the leader finishes.
	time.Sleep(50 * time.Millisecond)
	close(releaseList)
	wg.Wait()
	if err := <-forceErr; err != nil {
		t.Fatalf("forceRefreshGitWorkspaces: %v", err)
	}
	// Brief settle window for any mistaken re-enter leader.
	time.Sleep(100 * time.Millisecond)
	if got := listCalls.Load(); got != 1 {
		t.Fatalf("ListGitWorkspaces calls = %d, want 1 (1 external force + 2 ordinary joins)", got)
	}
	fs.git.mu.Lock()
	pending := fs.git.pendingForce
	fs.git.mu.Unlock()
	if pending {
		t.Fatal("pendingForce still set after successful list with no mid-flight external force")
	}
}

func TestCarryGitKnownSizesIntoRuntime(t *testing.T) {
	t.Helper()
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1:0"), opts)

	const (
		wsID = "ws-carry"
		oid  = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		oid2 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	)
	prev := &gitWorkspaceRuntime{
		workspace: client.GitWorkspace{WorkspaceID: wsID, HeadCommit: "deadbeef"},
		localRoot: "/repo",
		nodes: map[string]client.GitTreeNode{
			"README.md": {
				Path: "README.md", Name: "README.md", Kind: "file", Mode: "100644",
				ObjectSHA: oid, SizeBytes: 42, CommitSHA: "deadbeef",
			},
			"other.txt": {
				Path: "other.txt", Name: "other.txt", Kind: "file", Mode: "100644",
				ObjectSHA: oid2, SizeBytes: 7, CommitSHA: "deadbeef",
			},
		},
		children: map[string][]client.GitTreeNode{},
	}
	fs.git.mu.Lock()
	fs.git.workspaces = []*gitWorkspaceRuntime{prev}
	fs.git.mu.Unlock()

	// Fresh list snapshot reintroduces SizeBytes=-1 (blobless ListGitTree shape).
	next := &gitWorkspaceRuntime{
		workspace: client.GitWorkspace{WorkspaceID: wsID, HeadCommit: "deadbeef"},
		localRoot: "/repo",
		nodes: map[string]client.GitTreeNode{
			"README.md": {
				Path: "README.md", Name: "README.md", Kind: "file", Mode: "100644",
				ObjectSHA: oid, SizeBytes: -1, CommitSHA: "deadbeef",
			},
			"other.txt": {
				Path: "other.txt", Name: "other.txt", Kind: "file", Mode: "100644",
				// SHA changed → must NOT carry old size.
				ObjectSHA: "cccccccccccccccccccccccccccccccccccccccc", SizeBytes: -1, CommitSHA: "deadbeef",
			},
			"new.txt": {
				Path: "new.txt", Name: "new.txt", Kind: "file", Mode: "100644",
				ObjectSHA: "dddddddddddddddddddddddddddddddddddddddd", SizeBytes: -1, CommitSHA: "deadbeef",
			},
		},
		children: map[string][]client.GitTreeNode{},
	}
	fs.carryGitKnownSizesIntoRuntime(next)

	if got := next.nodes["README.md"].SizeBytes; got != 42 {
		t.Fatalf("README.md size after carry = %d, want 42", got)
	}
	if got := next.nodes["other.txt"].SizeBytes; got != -1 {
		t.Fatalf("other.txt size after SHA mismatch carry = %d, want -1", got)
	}
	if got := next.nodes["new.txt"].SizeBytes; got != -1 {
		t.Fatalf("new.txt size with no prev = %d, want -1", got)
	}

	// No prev snapshot: no-op.
	lonely := &gitWorkspaceRuntime{
		workspace: client.GitWorkspace{WorkspaceID: "ws-missing"},
		nodes: map[string]client.GitTreeNode{
			"a": {Path: "a", Name: "a", Kind: "file", ObjectSHA: oid, SizeBytes: -1},
		},
	}
	fs.carryGitKnownSizesIntoRuntime(lonely)
	if got := lonely.nodes["a"].SizeBytes; got != -1 {
		t.Fatalf("lonely size = %d, want -1", got)
	}
}
