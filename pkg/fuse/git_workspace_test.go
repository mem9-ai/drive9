package fuse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
)

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
			BaseCommit:  "1111111111111111111111111111111111111111",
			HeadCommit:  "1111111111111111111111111111111111111111",
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
				CommitSHA:   "1111111111111111111111111111111111111111",
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
	if sanitize.resetIndex || sanitize.dropLocalRefs {
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
	if !sanitize.resetIndex {
		t.Fatalf("sanitize.resetIndex = false, want true")
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
