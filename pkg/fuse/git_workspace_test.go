package fuse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/gitcache"
)

const fixtureHeadCommit = "1111111111111111111111111111111111111111"

type gitWorkspaceFixture struct {
	mu              sync.Mutex
	overlay         map[string]client.GitOverlayEntry
	objectPacks     map[string]client.GitObjectPack
	state           []byte
	stateStorage    string
	mode            string
	server          *httptest.Server
	repoURL         string
	treeNodes       []client.GitTreeNode
	readmeObjectSHA string
	readmeSize      int64
}

func newGitWorkspaceFixture(t *testing.T) *gitWorkspaceFixture {
	t.Helper()
	f := &gitWorkspaceFixture{
		overlay:         make(map[string]client.GitOverlayEntry),
		objectPacks:     make(map[string]client.GitObjectPack),
		stateStorage:    "tar.gz",
		mode:            "fast",
		repoURL:         "https://example.test/repo.git",
		readmeObjectSHA: "2222222222222222222222222222222222222222",
		readmeSize:      12,
	}
	f.server = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.server.Close)
	return f
}

func (f *gitWorkspaceFixture) client() *client.Client {
	return newTestClient(f.server.URL)
}

func (f *gitWorkspaceFixture) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/git-workspaces":
		_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []client.GitWorkspace{{
			WorkspaceID: "ws1",
			RootPath:    "/repo/",
			RepoURL:     f.repoURL,
			RemoteName:  "origin",
			BranchName:  "main",
			BaseCommit:  fixtureHeadCommit,
			HeadCommit:  fixtureHeadCommit,
			Mode:        f.mode,
			Status:      "active",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/git-workspaces/ws1/tree":
		nodes := f.treeNodes
		if nodes == nil {
			nodes = []client.GitTreeNode{{
				WorkspaceID: "ws1",
				CommitSHA:   fixtureHeadCommit,
				Path:        "README.md",
				ParentPath:  "",
				Name:        "README.md",
				Kind:        "file",
				Mode:        "100644",
				ObjectSHA:   f.readmeObjectSHA,
				SizeBytes:   f.readmeSize,
			}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"nodes": nodes})
	case r.URL.Path == "/v1/git-workspaces/ws1/overlay":
		f.handleOverlay(w, r)
	case strings.HasPrefix(r.URL.Path, "/v1/git-workspaces/ws1/object-packs"):
		f.handleObjectPacks(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/git-workspaces/ws1/git-state":
		_ = json.NewEncoder(w).Encode(client.GitState{
			WorkspaceID:      "ws1",
			CheckpointCommit: "1111111111111111111111111111111111111111",
			StorageType:      f.stateStorage,
			SizeBytes:        int64(len(f.state)),
			Content:          f.state,
		})
	default:
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}
}

func (f *gitWorkspaceFixture) handleObjectPacks(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := "/v1/git-workspaces/ws1/object-packs"
	switch {
	case r.Method == http.MethodGet && r.URL.Path == prefix:
		packs := make([]client.GitObjectPack, 0, len(f.objectPacks))
		for _, pack := range f.objectPacks {
			pack.Content = nil
			packs = append(packs, pack)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"packs": packs})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, prefix+"/"):
		packID := strings.TrimPrefix(r.URL.Path, prefix+"/")
		pack, ok := f.objectPacks[packID]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
			return
		}
		_ = json.NewEncoder(w).Encode(pack)
	case r.Method == http.MethodPost && r.URL.Path == prefix:
		var req client.GitObjectPackRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(req.Content)
		packID := hex.EncodeToString(sum[:])
		pack := client.GitObjectPack{
			WorkspaceID:    "ws1",
			PackID:         packID,
			ChecksumSHA256: packID,
			SizeBytes:      int64(len(req.Content)),
			Content:        req.Content,
			CreatedAt:      time.Now(),
		}
		f.objectPacks[packID] = pack
		_ = json.NewEncoder(w).Encode(pack)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *gitWorkspaceFixture) handleOverlay(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		if p := r.URL.Query().Get("path"); p != "" {
			entry, ok := f.overlay[p]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
				return
			}
			_ = json.NewEncoder(w).Encode(entry)
			return
		}
		entries := make([]client.GitOverlayEntry, 0, len(f.overlay))
		for _, entry := range f.overlay {
			entries = append(entries, entry)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"entries": entries})
	case http.MethodPost, http.MethodPut:
		var req client.GitOverlayEntryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		entry := client.GitOverlayEntry{
			WorkspaceID:    "ws1",
			Path:           req.Path,
			Op:             req.Op,
			Kind:           req.Kind,
			Mode:           req.Mode,
			StorageType:    req.StorageType,
			StorageRef:     req.StorageRef,
			StorageRefHash: req.StorageRefHash,
			ChecksumSHA256: req.ChecksumSHA256,
			SizeBytes:      req.SizeBytes,
			BaseObjectSHA:  req.BaseObjectSHA,
			Content:        req.Content,
			CreatedAt:      time.Now(),
			UpdatedAt:      time.Now(),
		}
		if entry.Op == "" {
			entry.Op = "upsert"
		}
		if entry.Kind == "" {
			entry.Kind = "file"
		}
		if entry.SizeBytes == 0 && len(entry.Content) > 0 {
			entry.SizeBytes = int64(len(entry.Content))
		}
		f.overlay[entry.Path] = entry
		_ = json.NewEncoder(w).Encode(entry)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestGitWorkspaceOverlayPersistsAcrossFilesystemInstances(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()

	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "new.txt", &createOut); st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	content := []byte("hello overlay")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write bytes = %d, want %d", written, len(content))
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})

	fs2 := NewDat9FS(fixture.client(), opts)
	entry, handled := fs2.gitEntry("/repo/new.txt", true)
	if !handled || entry == nil {
		t.Fatalf("gitEntry handled=%t entry=%v, want persisted overlay entry", handled, entry)
	}
	got, err := fs2.readGitFile(nil, "/repo/new.txt", 0, int64(len(content)))
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("readGitFile = %q, want %q", got, content)
	}
}

func TestGitWorkspaceWriteSyncWritesOverlay(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), WritePolicy: WritePolicyWriteSync, EnableGitWorkspaces: true}
	opts.setDefaults()

	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
		Mode:     0o644,
	}, "sync.txt", &createOut); st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	content := []byte("write-sync overlay")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write bytes = %d, want %d", written, len(content))
	}

	fixture.mu.Lock()
	entry, ok := fixture.overlay["sync.txt"]
	fixture.mu.Unlock()
	if !ok {
		t.Fatalf("overlay entry missing for sync.txt")
	}
	if string(entry.Content) != string(content) {
		t.Fatalf("overlay content = %q, want %q", entry.Content, content)
	}

	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
}

func TestGitWorkspaceRestoresLocalGitStateOnLookup(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	srcGit := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcGit, "config"), []byte("[core]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	state, err := archiveLocalGitDir(srcGit)
	if err != nil {
		t.Fatal(err)
	}
	fixture.state = state

	localRoot := t.TempDir()
	opts := &MountOptions{LocalRoot: localRoot, Profile: MountProfileCodingAgent, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var out gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, ".git", &out); st != gofuse.OK {
		t.Fatalf("Lookup .git status = %v, want OK", st)
	}
	got, err := os.ReadFile(filepath.Join(localRoot, "overlay", "repo", ".git", "config"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "[core]\n" {
		t.Fatalf("restored config = %q, want %q", got, "[core]\n")
	}
}

func TestArchiveLocalGitStateDirSkipsObjectDatabases(t *testing.T) {
	gitDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte("[core]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(gitDir, "objects", "aa"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "objects", "aa", "blob"), []byte("object"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(gitDir, "modules", "sub", "objects", "bb"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "modules", "sub", "objects", "bb", "blob"), []byte("object"), 0o644); err != nil {
		t.Fatal(err)
	}

	state, err := archiveLocalGitStateDir(gitDir)
	if err != nil {
		t.Fatalf("archiveLocalGitStateDir: %v", err)
	}
	dst := t.TempDir()
	if err := extractGitArchive(state, dst); err != nil {
		t.Fatalf("extractGitArchive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "config")); err != nil {
		t.Fatalf("config missing from objectless archive: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "objects")); !os.IsNotExist(err) {
		t.Fatalf("objects restored from objectless archive, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "modules", "sub", "objects")); !os.IsNotExist(err) {
		t.Fatalf("module objects restored from objectless archive, err=%v", err)
	}
}

func TestGitWorkspaceObjectlessStateRestoresObjectsFromRemote(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	fixture := newGitWorkspaceFixture(t)
	content := []byte("hello base\n")
	repo := createGitRepoWithReadme(t, content)
	state, err := archiveLocalGitStateDir(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("archiveLocalGitStateDir: %v", err)
	}
	fixture.repoURL = repo
	fixture.state = state
	fixture.stateStorage = gitStateStorageTarGzNoObjects
	fixture.readmeObjectSHA = fuseGitOutputForTest(t, repo, "hash-object", "README.md")
	fixture.readmeSize = int64(len(content))

	localRoot := t.TempDir()
	opts := &MountOptions{LocalRoot: localRoot, Profile: MountProfileCodingAgent, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)

	got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, int64(len(content)))
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("readGitFile = %q, want %q", got, content)
	}
	if _, err := os.Stat(filepath.Join(localRoot, "overlay", "repo", ".git", "objects")); err != nil {
		t.Fatalf("restored git objects missing: %v", err)
	}
}

func TestGitWorkspaceCleanReadUsesMaterializedTreeCache(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	localRoot := t.TempDir()
	treePath := filepath.Join(gitcache.TreeRoot(localRoot, "ws1", fixtureHeadCommit), "README.md")
	if err := os.MkdirAll(filepath.Dir(treePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(treePath, []byte("cached base\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	opts := &MountOptions{LocalRoot: localRoot, EnableGitWorkspaces: true, PerfCounters: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if string(got) != "cached base\n" {
		t.Fatalf("readGitFile = %q, want cached base", got)
	}
	snap := fs.perf.snapshot()
	if got := snap.Counters["git_clean_tree_hit"]; got != 1 {
		t.Fatalf("git_clean_tree_hit = %d, want 1", got)
	}
	if got := snap.Counters["git_cat_file_count"]; got != 0 {
		t.Fatalf("git_cat_file_count = %d, want 0", got)
	}
}

func TestGitWorkspaceCleanReadUsesBlobCache(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	localRoot := t.TempDir()
	if err := gitcache.WriteBlob(localRoot, "ws1", fixtureHeadCommit, fixture.readmeObjectSHA, []byte("cached blob\n")); err != nil {
		t.Fatal(err)
	}

	opts := &MountOptions{LocalRoot: localRoot, EnableGitWorkspaces: true, PerfCounters: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if string(got) != "cached blob\n" {
		t.Fatalf("readGitFile = %q, want cached blob", got)
	}
	snap := fs.perf.snapshot()
	if got := snap.Counters["git_clean_blob_cache_hit"]; got != 1 {
		t.Fatalf("git_clean_blob_cache_hit = %d, want 1", got)
	}
	if got := snap.Counters["git_cat_file_count"]; got != 0 {
		t.Fatalf("git_cat_file_count = %d, want 0", got)
	}
}

func TestGitWorkspaceOverlayWinsOverCleanCache(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.overlay["README.md"] = client.GitOverlayEntry{
		WorkspaceID: "ws1",
		Path:        "README.md",
		Op:          "upsert",
		Kind:        "file",
		Mode:        "100644",
		Content:     []byte("overlay\n"),
		SizeBytes:   int64(len("overlay\n")),
	}
	localRoot := t.TempDir()
	treePath := filepath.Join(gitcache.TreeRoot(localRoot, "ws1", fixtureHeadCommit), "README.md")
	if err := os.MkdirAll(filepath.Dir(treePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(treePath, []byte("cached base\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	opts := &MountOptions{LocalRoot: localRoot, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if string(got) != "overlay\n" {
		t.Fatalf("readGitFile = %q, want overlay", got)
	}
}

func TestGitWorkspaceWritablePreloadFromCacheDoesNotWriteOverlay(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	localRoot := t.TempDir()
	if err := gitcache.WriteBlob(localRoot, "ws1", fixtureHeadCommit, fixture.readmeObjectSHA, []byte("hello base\n")); err != nil {
		t.Fatal(err)
	}

	opts := &MountOptions{LocalRoot: localRoot, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())
	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	var openOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_RDWR),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: openOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})

	fixture.mu.Lock()
	_, ok := fixture.overlay["README.md"]
	fixture.mu.Unlock()
	if ok {
		t.Fatalf("overlay entry written for unmodified clean file")
	}
}

func TestGitWorkspaceCleanSymlinkReadUsesTreeCache(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{{
		WorkspaceID: "ws1",
		CommitSHA:   fixtureHeadCommit,
		Path:        "link",
		ParentPath:  "",
		Name:        "link",
		Kind:        "symlink",
		Mode:        "120000",
		ObjectSHA:   "3333333333333333333333333333333333333333",
		SizeBytes:   int64(len("README.md")),
	}}
	localRoot := t.TempDir()
	treePath := filepath.Join(gitcache.TreeRoot(localRoot, "ws1", fixtureHeadCommit), "link")
	if err := os.MkdirAll(filepath.Dir(treePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("README.md", treePath); err != nil {
		t.Fatal(err)
	}

	opts := &MountOptions{LocalRoot: localRoot, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	got, err := fs.readGitFile(context.Background(), "/repo/link", 0, -1)
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if string(got) != "README.md" {
		t.Fatalf("readGitFile = %q, want README.md", got)
	}
}

func TestGitWorkspaceCleanReadSingleflightsCatFile(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	fixture := newGitWorkspaceFixture(t)
	content := []byte("hello base\n")
	repo := createGitRepoWithReadme(t, content)
	state, err := archiveLocalGitDir(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("archiveLocalGitDir: %v", err)
	}
	fixture.repoURL = repo
	fixture.state = state
	fixture.readmeObjectSHA = fuseGitOutputForTest(t, repo, "hash-object", "README.md")
	fixture.readmeSize = int64(len(content))

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true, PerfCounters: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	const readers = 20
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, content) {
				errs <- fmt.Errorf("readGitFile = %q, want %q", got, content)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	snap := fs.perf.snapshot()
	if got := snap.Counters["git_cat_file_count"]; got != 1 {
		t.Fatalf("git_cat_file_count = %d, want 1", got)
	}
}

func TestBuildLocalGitObjectPackPreservesSmallStagedBlob(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	src := createGitRepoWithReadme(t, []byte("hello base\n"))
	work := filepath.Join(t.TempDir(), "work")
	runFuseTestGit(t, "", "clone", src, work)
	rt := gitRuntimeForRepo(t, src)

	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("hello staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runFuseTestGit(t, work, "add", "README.md")

	pack, sanitize, err := buildLocalGitObjectPack(context.Background(), filepath.Join(work, ".git"), rt)
	if err != nil {
		t.Fatalf("buildLocalGitObjectPack: %v", err)
	}
	if len(pack) == 0 {
		t.Fatalf("pack is empty, want staged blob")
	}
	if len(sanitize.indexRestores) != 0 || sanitize.dropLocalRefs {
		t.Fatalf("sanitize = %+v, want no degradation", sanitize)
	}
	state, err := archiveLocalGitStateForCheckpoint(context.Background(), filepath.Join(work, ".git"), rt, sanitize)
	if err != nil {
		t.Fatalf("archiveLocalGitStateForCheckpoint: %v", err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	runFuseTestGit(t, "", "clone", "--no-checkout", src, restored)
	unpackGitPackForTest(t, filepath.Join(restored, ".git"), pack)
	if err := extractGitArchive(state, filepath.Join(restored, ".git")); err != nil {
		t.Fatalf("extractGitArchive: %v", err)
	}
	if got := fuseGitOutputForTest(t, restored, "diff", "--cached", "--name-only"); got != "README.md" {
		t.Fatalf("cached diff = %q, want README.md", got)
	}
}

func TestBuildLocalGitObjectPackDowngradesOversizedStagedBlob(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	src := createGitRepoWithReadme(t, []byte("hello base\n"))
	work := filepath.Join(t.TempDir(), "work")
	runFuseTestGit(t, "", "clone", src, work)
	rt := gitRuntimeForRepo(t, src)

	large := bytes.Repeat([]byte("x"), int(gitLocalObjectMaxBlobBytes)+1)
	if err := os.WriteFile(filepath.Join(work, "README.md"), large, 0o644); err != nil {
		t.Fatal(err)
	}
	runFuseTestGit(t, work, "add", "README.md")

	pack, sanitize, err := buildLocalGitObjectPack(context.Background(), filepath.Join(work, ".git"), rt)
	if err != nil {
		t.Fatalf("buildLocalGitObjectPack: %v", err)
	}
	if len(pack) != 0 {
		t.Fatalf("pack len = %d, want 0 for oversized staged blob", len(pack))
	}
	if len(sanitize.indexRestores) != 1 {
		t.Fatalf("sanitize.indexRestores len = %d, want 1", len(sanitize.indexRestores))
	}
	state, err := archiveLocalGitStateForCheckpoint(context.Background(), filepath.Join(work, ".git"), rt, sanitize)
	if err != nil {
		t.Fatalf("archiveLocalGitStateForCheckpoint: %v", err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	runFuseTestGit(t, "", "clone", "--no-checkout", src, restored)
	if err := extractGitArchive(state, filepath.Join(restored, ".git")); err != nil {
		t.Fatalf("extractGitArchive: %v", err)
	}
	if got := fuseGitOutputForTest(t, restored, "diff", "--cached", "--name-only"); got != "" {
		t.Fatalf("cached diff = %q, want downgraded empty index", got)
	}
}

func TestBuildLocalGitObjectPackDowngradesOnlyOversizedStagedBlob(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	src := createGitRepoWithReadme(t, []byte("hello base\n"))
	work := filepath.Join(t.TempDir(), "work")
	runFuseTestGit(t, "", "clone", src, work)
	rt := gitRuntimeForRepo(t, src)

	if err := os.WriteFile(filepath.Join(work, "staged-small.txt"), []byte("small staged\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runFuseTestGit(t, work, "add", "staged-small.txt")
	large := bytes.Repeat([]byte("x"), int(gitLocalObjectMaxBlobBytes)+1)
	if err := os.WriteFile(filepath.Join(work, "oversized-staged.bin"), large, 0o644); err != nil {
		t.Fatal(err)
	}
	runFuseTestGit(t, work, "add", "oversized-staged.bin")

	pack, sanitize, err := buildLocalGitObjectPack(context.Background(), filepath.Join(work, ".git"), rt)
	if err != nil {
		t.Fatalf("buildLocalGitObjectPack: %v", err)
	}
	if len(pack) == 0 {
		t.Fatalf("pack is empty, want small staged blob")
	}
	if len(sanitize.indexRestores) != 1 || sanitize.indexRestores[0].path != "oversized-staged.bin" {
		t.Fatalf("sanitize.indexRestores = %+v, want only oversized-staged.bin", sanitize.indexRestores)
	}
	state, err := archiveLocalGitStateForCheckpoint(context.Background(), filepath.Join(work, ".git"), rt, sanitize)
	if err != nil {
		t.Fatalf("archiveLocalGitStateForCheckpoint: %v", err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	runFuseTestGit(t, "", "clone", "--no-checkout", src, restored)
	unpackGitPackForTest(t, filepath.Join(restored, ".git"), pack)
	if err := extractGitArchive(state, filepath.Join(restored, ".git")); err != nil {
		t.Fatalf("extractGitArchive: %v", err)
	}
	if got := fuseGitOutputForTest(t, restored, "diff", "--cached", "--name-only"); got != "staged-small.txt" {
		t.Fatalf("cached diff = %q, want staged-small.txt", got)
	}
}

func TestBuildLocalGitObjectPackPreservesSmallLocalCommit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	src := createGitRepoWithReadme(t, []byte("hello base\n"))
	work := filepath.Join(t.TempDir(), "work")
	runFuseTestGit(t, "", "clone", src, work)
	runFuseTestGit(t, work, "config", "user.email", "drive9-test@example.invalid")
	runFuseTestGit(t, work, "config", "user.name", "Drive9 Test")
	rt := gitRuntimeForRepo(t, src)

	if err := os.WriteFile(filepath.Join(work, "README.md"), []byte("hello commit\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runFuseTestGit(t, work, "add", "README.md")
	runFuseTestGit(t, work, "commit", "-m", "local commit")

	pack, sanitize, err := buildLocalGitObjectPack(context.Background(), filepath.Join(work, ".git"), rt)
	if err != nil {
		t.Fatalf("buildLocalGitObjectPack: %v", err)
	}
	if len(pack) == 0 {
		t.Fatalf("pack is empty, want local commit objects")
	}
	if sanitize.dropLocalRefs {
		t.Fatalf("sanitize.dropLocalRefs = true, want false")
	}
	state, err := archiveLocalGitStateForCheckpoint(context.Background(), filepath.Join(work, ".git"), rt, sanitize)
	if err != nil {
		t.Fatalf("archiveLocalGitStateForCheckpoint: %v", err)
	}

	restored := filepath.Join(t.TempDir(), "restored")
	runFuseTestGit(t, "", "clone", "--no-checkout", src, restored)
	unpackGitPackForTest(t, filepath.Join(restored, ".git"), pack)
	if err := extractGitArchive(state, filepath.Join(restored, ".git")); err != nil {
		t.Fatalf("extractGitArchive: %v", err)
	}
	if got := fuseGitOutputForTest(t, restored, "log", "-1", "--pretty=%s"); got != "local commit" {
		t.Fatalf("restored HEAD subject = %q, want local commit", got)
	}
}

func TestGitWorkspaceTrackedLocalOnlyPathBypassesLocalOverlay(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{
		{
			WorkspaceID: "ws1",
			CommitSHA:   "1111111111111111111111111111111111111111",
			Path:        "build",
			ParentPath:  "",
			Name:        "build",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SizeBytes:   0,
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   "1111111111111111111111111111111111111111",
			Path:        "build/raw-text-plugin.mjs",
			ParentPath:  "build",
			Name:        "raw-text-plugin.mjs",
			Kind:        "file",
			Mode:        "100644",
			ObjectSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			SizeBytes:   42,
		},
	}

	opts := &MountOptions{LocalRoot: t.TempDir(), Profile: MountProfileCodingAgent, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var buildOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "build", &buildOut); st != gofuse.OK {
		t.Fatalf("Lookup build status = %v, want OK", st)
	}
	var fileOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: buildOut.NodeId}, "raw-text-plugin.mjs", &fileOut); st != gofuse.OK {
		t.Fatalf("Lookup tracked build file status = %v, want OK", st)
	}
	if fileOut.Size != 42 {
		t.Fatalf("tracked build file size = %d, want 42", fileOut.Size)
	}
}

func TestGitWorkspaceGeneratedTmpApiExtractorUsesLocalOverlay(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "packages",
			ParentPath:  "",
			Name:        "packages",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "packages/node-sdk",
			ParentPath:  "packages",
			Name:        "node-sdk",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "packages/node-sdk/src",
			ParentPath:  "packages/node-sdk",
			Name:        "src",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "cccccccccccccccccccccccccccccccccccccccc",
		},
	}

	localRoot := t.TempDir()
	opts := &MountOptions{
		LocalRoot:           localRoot,
		Profile:             MountProfileCodingAgent,
		EnableGitWorkspaces: true,
	}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	nodeSDKIno := fs.inodes.Lookup("/repo/packages/node-sdk", true, 0, time.Now())

	var dirOut gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: nodeSDKIno},
		Mode:     0o755,
	}, ".tmp-api-extractor", &dirOut); st != gofuse.OK {
		t.Fatalf("Mkdir .tmp-api-extractor status = %v, want OK", st)
	}

	var fileOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: dirOut.NodeId},
		Flags:    uint32(syscall.O_RDWR),
		Mode:     0o644,
	}, "index.d.ts", &fileOut); st != gofuse.OK {
		t.Fatalf("Create generated dts status = %v, want OK", st)
	}
	content := []byte("export {};\n")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: fileOut.NodeId},
		Fh:       fileOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write generated dts status = %v, want OK", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write generated dts bytes = %d, want %d", written, len(content))
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: fileOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush generated dts status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: fileOut.Fh})

	got, err := os.ReadFile(filepath.Join(localRoot, "overlay/repo/packages/node-sdk/.tmp-api-extractor/index.d.ts"))
	if err != nil {
		t.Fatalf("read local generated dts: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("local generated dts = %q, want %q", got, content)
	}

	entries, err := fs.listDir(context.Background(), "/repo/packages/node-sdk")
	if err != nil {
		t.Fatalf("listDir node-sdk: %v", err)
	}
	names := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		names[entry.Name] = struct{}{}
	}
	if _, ok := names[".tmp-api-extractor"]; !ok {
		t.Fatalf("listDir missing local .tmp-api-extractor entry: %#v", entries)
	}
	if _, ok := names["src"]; !ok {
		t.Fatalf("listDir missing tracked src entry: %#v", entries)
	}
}

func TestGitWorkspaceGitIgnoredGeneratedPathsUseLocalOverlay(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        ".gitignore",
			ParentPath:  "",
			Name:        ".gitignore",
			Kind:        "file",
			Mode:        "100644",
			ObjectSHA:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			SizeBytes:   96,
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "src",
			ParentPath:  "",
			Name:        "src",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "src/kimi_cli",
			ParentPath:  "src",
			Name:        "kimi_cli",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "cccccccccccccccccccccccccccccccccccccccc",
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "src/kimi_cli/vis",
			ParentPath:  "src/kimi_cli",
			Name:        "vis",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "dddddddddddddddddddddddddddddddddddddddd",
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "src/kimi_cli/web",
			ParentPath:  "src/kimi_cli",
			Name:        "web",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "src/kimi_cli/web/static",
			ParentPath:  "src/kimi_cli/web",
			Name:        "static",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "ffffffffffffffffffffffffffffffffffffffff",
		},
	}

	ignoreFile := strings.Join([]string{
		"src/kimi_cli/deps/bin",
		"src/kimi_cli/deps/tmp",
		"src/kimi_cli/_build_info.py",
		"src/kimi_cli/web/static/assets/",
		"src/kimi_cli/vis/static/",
		"",
	}, "\n")
	repo := t.TempDir()
	runFuseTestGit(t, "", "init", "-b", "main", repo)
	runFuseTestGit(t, repo, "config", "user.email", "drive9-test@example.invalid")
	runFuseTestGit(t, repo, "config", "user.name", "Drive9 Test")
	if err := os.WriteFile(filepath.Join(repo, ".gitignore"), []byte(ignoreFile), 0o644); err != nil {
		t.Fatalf("write .gitignore: %v", err)
	}
	runFuseTestGit(t, repo, "add", ".gitignore")
	runFuseTestGit(t, repo, "commit", "-m", "ignore generated outputs")
	state, err := archiveLocalGitDir(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("archiveLocalGitDir: %v", err)
	}
	fixture.state = state

	localRoot := t.TempDir()
	treeRoot := gitcache.TreeRoot(localRoot, "ws1", fixtureHeadCommit)
	if err := os.MkdirAll(treeRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(treeRoot, ".gitignore"), []byte(ignoreFile), 0o644); err != nil {
		t.Fatalf("write hydrated .gitignore: %v", err)
	}

	opts := &MountOptions{
		LocalRoot:           localRoot,
		Profile:             MountProfileCodingAgent,
		EnableGitWorkspaces: true,
		PerfCounters:        true,
	}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)

	if got := fs.observePathPolicy("/repo/src/kimi_cli/_build_info.py"); got != PathLayerLocalOnly {
		t.Fatalf("_build_info.py policy = %s, want local-only", got)
	}
	if got := fs.observePathPolicy("/repo/src/kimi_cli/web/static/assets/app.js"); got != PathLayerLocalOnly {
		t.Fatalf("web assets policy = %s, want local-only", got)
	}
	if got := fs.observePathPolicy("/repo/src/kimi_cli/web/app.py"); got != PathLayerRemotePersistent {
		t.Fatalf("source policy = %s, want remote persistent", got)
	}

	kimiCLIIno := fs.inodes.Lookup("/repo/src/kimi_cli", true, 0, time.Now())
	var buildInfoOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: kimiCLIIno},
		Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		Mode:     0o644,
	}, "_build_info.py", &buildInfoOut); st != gofuse.OK {
		t.Fatalf("Create _build_info.py status = %v, want OK", st)
	}
	content := []byte("version = 'test'\n")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: buildInfoOut.NodeId},
		Fh:       buildInfoOut.Fh,
		Size:     uint32(len(content)),
	}, content)
	if st != gofuse.OK {
		t.Fatalf("Write _build_info.py status = %v, want OK", st)
	}
	if written != uint32(len(content)) {
		t.Fatalf("Write _build_info.py bytes = %d, want %d", written, len(content))
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: buildInfoOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush _build_info.py status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: buildInfoOut.Fh})

	visIno := fs.inodes.Lookup("/repo/src/kimi_cli/vis", true, 0, time.Now())
	var staticOut gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: visIno},
		Mode:     0o755,
	}, "static", &staticOut); st != gofuse.OK {
		t.Fatalf("Mkdir vis/static status = %v, want OK", st)
	}

	if _, err := os.Stat(filepath.Join(localRoot, "overlay/repo/src/kimi_cli/vis/static")); err != nil {
		t.Fatalf("local ignored directory missing: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(localRoot, "overlay/repo/src/kimi_cli/_build_info.py"))
	if err != nil {
		t.Fatalf("read local ignored file: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("local ignored file = %q, want %q", got, content)
	}
	fixture.mu.Lock()
	_, overlayFile := fixture.overlay["src/kimi_cli/_build_info.py"]
	_, overlayDir := fixture.overlay["src/kimi_cli/vis/static"]
	fixture.mu.Unlock()
	if overlayFile || overlayDir {
		t.Fatalf("ignored generated path entered git overlay: file=%t dir=%t", overlayFile, overlayDir)
	}
}

func TestSliceReadNegativeSizeReadsToEOF(t *testing.T) {
	got := sliceRead([]byte("abcdef"), 2, -1)
	if string(got) != "cdef" {
		t.Fatalf("sliceRead = %q, want cdef", got)
	}
}

func TestGitWorkspaceUnknownSizeWritableOpenPreservesBaseContent(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	fixture := newGitWorkspaceFixture(t)
	content := []byte("hello base\n")
	repo := createGitRepoWithReadme(t, content)
	state, err := archiveLocalGitDir(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatalf("archiveLocalGitDir: %v", err)
	}
	fixture.state = state
	fixture.readmeObjectSHA = fuseGitOutputForTest(t, repo, "hash-object", "README.md")
	fixture.readmeSize = -1

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	if got := lookupOut.Size; got != 0 {
		t.Fatalf("Lookup size = %d, want 0 attr fallback for unknown size", got)
	}

	var openOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_RDWR),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	appendix := []byte("tail")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Fh:       openOut.Fh,
		Offset:   uint64(len(content)),
		Size:     uint32(len(appendix)),
	}, appendix)
	if st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	if written != uint32(len(appendix)) {
		t.Fatalf("Write bytes = %d, want %d", written, len(appendix))
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: openOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: openOut.Fh})

	fixture.mu.Lock()
	entry, ok := fixture.overlay["README.md"]
	fixture.mu.Unlock()
	if !ok {
		t.Fatalf("overlay entry missing for README.md")
	}
	want := append(append([]byte{}, content...), appendix...)
	if !bytes.Equal(entry.Content, want) {
		t.Fatalf("overlay content = %q, want %q", entry.Content, want)
	}
}

func gitRuntimeForRepo(t *testing.T, repo string) *gitWorkspaceRuntime {
	t.Helper()
	head := fuseGitOutputForTest(t, repo, "rev-parse", "HEAD")
	readmeSHA := fuseGitOutputForTest(t, repo, "hash-object", "README.md")
	return &gitWorkspaceRuntime{
		workspace: client.GitWorkspace{
			WorkspaceID: "ws1",
			RemoteName:  "origin",
			HeadCommit:  head,
		},
		nodes: map[string]client.GitTreeNode{
			"README.md": {
				Path:      "README.md",
				Kind:      "file",
				Mode:      "100644",
				ObjectSHA: readmeSHA,
				SizeBytes: 11,
			},
		},
	}
}

func unpackGitPackForTest(t *testing.T, gitDir string, pack []byte) {
	t.Helper()
	cmd := exec.Command("git", "--git-dir", gitDir, "unpack-objects", "-q")
	cmd.Stdin = bytes.NewReader(pack)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git unpack-objects: %v\n%s", err, out)
	}
}

func createGitRepoWithReadme(t *testing.T, content []byte) string {
	t.Helper()
	repo := t.TempDir()
	runFuseTestGit(t, "", "init", "-b", "main", repo)
	runFuseTestGit(t, repo, "config", "user.email", "drive9-test@example.invalid")
	runFuseTestGit(t, repo, "config", "user.name", "Drive9 Test")
	if err := os.WriteFile(filepath.Join(repo, "README.md"), content, 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runFuseTestGit(t, repo, "add", ".")
	runFuseTestGit(t, repo, "commit", "-m", "initial")
	return repo
}

func runFuseTestGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func fuseGitOutputForTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(bytes.TrimSpace(out))
}
