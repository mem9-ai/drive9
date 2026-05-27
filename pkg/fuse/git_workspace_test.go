package fuse

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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
	state           []byte
	server          *httptest.Server
	readmeObjectSHA string
	readmeSize      int64
}

func newGitWorkspaceFixture(t *testing.T) *gitWorkspaceFixture {
	t.Helper()
	f := &gitWorkspaceFixture{
		overlay:         make(map[string]client.GitOverlayEntry),
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
			RepoURL:     "https://example.test/repo.git",
			RemoteName:  "origin",
			BranchName:  "main",
			BaseCommit:  "1111111111111111111111111111111111111111",
			HeadCommit:  "1111111111111111111111111111111111111111",
			Mode:        "fast",
			Status:      "active",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/git-workspaces/ws1/tree":
		_ = json.NewEncoder(w).Encode(map[string]any{"nodes": []client.GitTreeNode{{
			WorkspaceID: "ws1",
			CommitSHA:   "1111111111111111111111111111111111111111",
			Path:        "README.md",
			ParentPath:  "",
			Name:        "README.md",
			Kind:        "file",
			Mode:        "100644",
			ObjectSHA:   f.readmeObjectSHA,
			SizeBytes:   f.readmeSize,
		}}})
	case r.URL.Path == "/v1/git-workspaces/ws1/overlay":
		f.handleOverlay(w, r)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/git-workspaces/ws1/git-state":
		_ = json.NewEncoder(w).Encode(client.GitState{
			WorkspaceID:      "ws1",
			CheckpointCommit: "1111111111111111111111111111111111111111",
			StorageType:      "tar.gz",
			SizeBytes:        int64(len(f.state)),
			Content:          f.state,
		})
	default:
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
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
