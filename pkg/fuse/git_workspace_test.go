package fuse

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
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
	overlayPuts     int
	overlayPostWait chan struct{}
	objectPacks     map[string]client.GitObjectPack
	state           []byte
	stateStorage    string
	gitStatePuts    int
	mode            string
	headCommit      string
	deleted         bool
	server          *httptest.Server
	repoURL         string
	treeNodes       []client.GitTreeNode
	readmeObjectSHA string
	readmeSize      int64
	failTree        bool
	failOverlay     bool
}

func newGitWorkspaceFixture(t *testing.T) *gitWorkspaceFixture {
	t.Helper()
	f := &gitWorkspaceFixture{
		overlay:         make(map[string]client.GitOverlayEntry),
		objectPacks:     make(map[string]client.GitObjectPack),
		stateStorage:    "tar.gz",
		mode:            "fast",
		headCommit:      fixtureHeadCommit,
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
		f.mu.Lock()
		deleted := f.deleted
		f.mu.Unlock()
		if deleted {
			_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []client.GitWorkspace{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"workspaces": []client.GitWorkspace{{
			WorkspaceID: "ws1",
			RootPath:    "/repo/",
			RepoURL:     f.repoURL,
			RemoteName:  "origin",
			BranchName:  "main",
			BaseCommit:  f.headCommit,
			HeadCommit:  f.headCommit,
			Mode:        f.mode,
			Status:      "active",
			CreatedAt:   time.Now(),
			UpdatedAt:   time.Now(),
		}}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/git-workspaces/ws1/tree":
		f.mu.Lock()
		failTree := f.failTree
		f.mu.Unlock()
		if failTree {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "tree unavailable"})
			return
		}
		nodes := f.treeNodes
		if nodes == nil {
			nodes = []client.GitTreeNode{{
				WorkspaceID: "ws1",
				CommitSHA:   f.headCommit,
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
	case r.URL.Path == "/v1/git-workspaces/ws1/git-state":
		f.handleGitState(w, r)
	case r.Method == http.MethodDelete && r.URL.Path == "/v1/git-workspaces/ws1":
		f.mu.Lock()
		f.deleted = true
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	default:
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
	}
}

func (f *gitWorkspaceFixture) handleGitState(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		_ = json.NewEncoder(w).Encode(client.GitState{
			WorkspaceID:      "ws1",
			CheckpointCommit: f.headCommit,
			StorageType:      f.stateStorage,
			SizeBytes:        int64(len(f.state)),
			Content:          f.state,
		})
	case http.MethodPost:
		var req client.GitStateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.gitStatePuts++
		f.stateStorage = req.StorageType
		f.state = append([]byte(nil), req.Content...)
		_ = json.NewEncoder(w).Encode(client.GitState{
			WorkspaceID:      "ws1",
			CheckpointCommit: req.CheckpointCommit,
			StorageType:      req.StorageType,
			SizeBytes:        int64(len(req.Content)),
			Content:          req.Content,
			CreatedAt:        time.Now(),
			UpdatedAt:        time.Now(),
		})
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
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
		pack.Content = nil
		_ = json.NewEncoder(w).Encode(pack)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (f *gitWorkspaceFixture) handleOverlay(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.failOverlay {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "overlay unavailable"})
			return
		}
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
		f.mu.Lock()
		wait := f.overlayPostWait
		f.mu.Unlock()
		if wait != nil {
			<-wait
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
		f.mu.Lock()
		defer f.mu.Unlock()
		f.overlayPuts++
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
	entry, handled := fs2.gitEntry(context.Background(), "/repo/new.txt", true)
	if !handled || entry == nil {
		t.Fatalf("gitEntry handled=%t entry=%v, want persisted overlay entry", handled, entry)
	}
	got, err := fs2.readGitFile(context.Background(), "/repo/new.txt", 0, int64(len(content)))
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("readGitFile = %q, want %q", got, content)
	}
}

func TestEnsureGitWorkspacesKeepsPreviousSnapshotOnLoadFailure(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)

	if err := fs.ensureGitWorkspaces(context.Background()); err != nil {
		t.Fatalf("initial ensureGitWorkspaces: %v", err)
	}
	fs.git.mu.Lock()
	initialLoaded := fs.git.loaded
	initialCount := len(fs.git.workspaces)
	fs.git.loadedAt = time.Now().Add(-2 * gitWorkspaceRefreshInterval)
	fs.git.mu.Unlock()
	if !initialLoaded || initialCount != 1 {
		t.Fatalf("initial workspace snapshot loaded=%t count=%d, want loaded one workspace", initialLoaded, initialCount)
	}

	fixture.mu.Lock()
	fixture.failTree = true
	fixture.mu.Unlock()
	if err := fs.ensureGitWorkspaces(context.Background()); err == nil {
		t.Fatalf("ensureGitWorkspaces error = nil, want refresh failure")
	}

	fs.git.mu.Lock()
	loaded := fs.git.loaded
	count := len(fs.git.workspaces)
	fs.git.mu.Unlock()
	if !loaded || count != initialCount {
		t.Fatalf("workspace snapshot after failed refresh loaded=%t count=%d, want loaded count %d", loaded, count, initialCount)
	}
	entry, handled := fs.gitEntry(context.Background(), "/repo/README.md", false)
	if !handled || entry == nil {
		t.Fatalf("gitEntry after failed refresh handled=%t entry=%v, want stale snapshot", handled, entry)
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

func TestGitWorkspaceWriteBackDefersOverlayRemoteAndDrains(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	wait := make(chan struct{})
	var closeWait sync.Once
	t.Cleanup(func() { closeWait.Do(func() { close(wait) }) })
	fixture.mu.Lock()
	fixture.overlayPostWait = wait
	fixture.mu.Unlock()

	opts := &MountOptions{
		LocalRoot:           t.TempDir(),
		SyncMode:            SyncInteractive,
		WritePolicy:         WritePolicyWriteBack,
		EnableGitWorkspaces: true,
		PerfCounters:        true,
	}
	opts.setDefaults()

	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT),
		Mode:     0o644,
	}, "async.txt", &createOut); st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}

	content := []byte("writeback overlay")
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Size:     uint32(len(content)),
	}, content); st != gofuse.OK {
		t.Fatalf("Write status = %v, want OK", st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("Flush status = %v, want OK", st)
	}

	fixture.mu.Lock()
	putsBeforeDrain := fixture.overlayPuts
	fixture.mu.Unlock()
	if putsBeforeDrain != 0 {
		t.Fatalf("overlay puts before drain = %d, want 0", putsBeforeDrain)
	}
	fs.git.mu.Lock()
	fs.git.loadedAt = time.Time{}
	fs.git.mu.Unlock()
	if err := fs.ensureGitWorkspaces(context.Background()); err != nil {
		t.Fatalf("ensureGitWorkspaces during pending overlay: %v", err)
	}
	got, err := fs.readGitFile(context.Background(), "/repo/async.txt", 0, -1)
	if err != nil {
		t.Fatalf("readGitFile local overlay: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("local overlay content = %q, want %q", got, content)
	}

	closeWait.Do(func() { close(wait) })
	fs.drainGitOverlayWrites()
	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})

	fixture.mu.Lock()
	entry, ok := fixture.overlay["async.txt"]
	putsAfterDrain := fixture.overlayPuts
	fixture.mu.Unlock()
	if !ok {
		t.Fatalf("overlay entry missing after drain")
	}
	if putsAfterDrain != 1 {
		t.Fatalf("overlay puts after drain = %d, want 1", putsAfterDrain)
	}
	if !bytes.Equal(entry.Content, content) {
		t.Fatalf("overlay content = %q, want %q", entry.Content, content)
	}
}

func TestGitWorkspaceOverlayReadonlyOpenKeepsKernelCacheForMmap(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()

	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}

	var cleanOpen gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_RDONLY),
	}, &cleanOpen); st != gofuse.OK {
		t.Fatalf("Open clean status = %v, want OK", st)
	}
	if cleanOpen.OpenFlags&gofuse.FOPEN_KEEP_CACHE == 0 {
		t.Fatalf("clean open flags = %d, want FOPEN_KEEP_CACHE", cleanOpen.OpenFlags)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: cleanOpen.Fh})

	content := []byte("dirty overlay\n")
	fs.applyGitOverlayEntry("ws1", client.GitOverlayEntry{
		WorkspaceID:   "ws1",
		Path:          "README.md",
		Op:            "upsert",
		Kind:          "file",
		Mode:          "100644",
		SizeBytes:     int64(len(content)),
		BaseObjectSHA: fixture.readmeObjectSHA,
		Content:       content,
	})

	var overlayOpen gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_RDONLY),
	}, &overlayOpen); st != gofuse.OK {
		t.Fatalf("Open overlay status = %v, want OK", st)
	}
	if overlayOpen.OpenFlags&gofuse.FOPEN_KEEP_CACHE == 0 {
		t.Fatalf("overlay open flags = %d, want FOPEN_KEEP_CACHE", overlayOpen.OpenFlags)
	}
	if overlayOpen.OpenFlags&gofuse.FOPEN_DIRECT_IO != 0 {
		t.Fatalf("overlay open flags = %d, do not want FOPEN_DIRECT_IO", overlayOpen.OpenFlags)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: overlayOpen.Fh})
}

func TestGitWorkspaceDirtyMirrorIsAuthoritativeForOverlayRead(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()

	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	rt, rel, ok := fs.gitWorkspaceForPath(context.Background(), "/repo/README.md")
	if !ok || rel != "README.md" {
		t.Fatalf("gitWorkspaceForPath ok=%t rel=%q, want README.md", ok, rel)
	}
	want := []byte("hello base\nappended\n")
	mirrorPath, ok := fs.gitWorkspaceDirtyMirrorPath(rt, rel)
	if !ok {
		t.Fatalf("gitWorkspaceDirtyMirrorPath missing")
	}
	if err := os.MkdirAll(filepath.Dir(mirrorPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mirrorPath, want, 0o644); err != nil {
		t.Fatal(err)
	}
	fs.applyGitOverlayEntry("ws1", client.GitOverlayEntry{
		WorkspaceID:   "ws1",
		Path:          rel,
		Op:            "upsert",
		Kind:          "file",
		Mode:          "100644",
		SizeBytes:     4,
		BaseObjectSHA: fixture.readmeObjectSHA,
		Content:       []byte("tail"),
	})

	entry, handled := fs.gitEntry(context.Background(), "/repo/README.md", false)
	if !handled || entry == nil {
		t.Fatalf("gitEntry handled=%t entry=%v, want overlay entry", handled, entry)
	}
	if entry.Size != int64(len(want)) {
		t.Fatalf("gitEntry size = %d, want dirty mirror size %d", entry.Size, len(want))
	}
	got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("readGitFile = %q, want %q", got, want)
	}
}

func TestGitWorkspaceMetadataWriteBackDefersOverlayRemoteAndDrains(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	wait := make(chan struct{})
	var closeWait sync.Once
	t.Cleanup(func() { closeWait.Do(func() { close(wait) }) })
	fixture.mu.Lock()
	fixture.overlayPostWait = wait
	fixture.mu.Unlock()

	opts := &MountOptions{
		LocalRoot:           t.TempDir(),
		SyncMode:            SyncInteractive,
		WritePolicy:         WritePolicyWriteBack,
		EnableGitWorkspaces: true,
	}
	opts.setDefaults()

	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var out gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Mode:     0o700,
	}, "private", &out); st != gofuse.OK {
		t.Fatalf("Mkdir status = %v, want OK", st)
	}
	entries, handled, err := fs.listGitDir(context.Background(), "/repo")
	if err != nil || !handled {
		t.Fatalf("listGitDir handled=%t err=%v, want handled nil", handled, err)
	}
	found := false
	for _, entry := range entries {
		if entry.Name == "private" && entry.IsDir {
			found = true
		}
	}
	if !found {
		t.Fatalf("local overlay directory private missing from listing: %+v", entries)
	}
	fixture.mu.Lock()
	putsBeforeDrain := fixture.overlayPuts
	fixture.mu.Unlock()
	if putsBeforeDrain != 0 {
		t.Fatalf("overlay puts before drain = %d, want 0", putsBeforeDrain)
	}
	fs.git.mu.Lock()
	fs.git.loadedAt = time.Time{}
	fs.git.mu.Unlock()
	if err := fs.ensureGitWorkspaces(context.Background()); err != nil {
		t.Fatalf("ensureGitWorkspaces during pending metadata: %v", err)
	}
	entries, handled, err = fs.listGitDir(context.Background(), "/repo")
	if err != nil || !handled {
		t.Fatalf("listGitDir after refresh handled=%t err=%v, want handled nil", handled, err)
	}
	found = false
	for _, entry := range entries {
		if entry.Name == "private" && entry.IsDir {
			found = true
		}
	}
	if !found {
		t.Fatalf("pending metadata disappeared after refresh: %+v", entries)
	}

	closeWait.Do(func() { close(wait) })
	fs.drainGitOverlayWrites()

	fixture.mu.Lock()
	entry, ok := fixture.overlay["private"]
	putsAfterDrain := fixture.overlayPuts
	fixture.mu.Unlock()
	if !ok {
		t.Fatalf("overlay entry missing after drain")
	}
	if putsAfterDrain != 1 {
		t.Fatalf("overlay puts after drain = %d, want 1", putsAfterDrain)
	}
	if entry.Kind != "dir" {
		t.Fatalf("overlay kind = %q, want dir", entry.Kind)
	}
}

func TestGitWorkspaceChmodDirectoryPreservesKind(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "src",
			ParentPath:  "",
			Name:        "src",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "3333333333333333333333333333333333333333",
			SizeBytes:   -1,
		},
	}
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "src", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	var attrOut gofuse.AttrOut
	if st := fs.SetAttr(nil, &gofuse.SetAttrIn{
		SetAttrInCommon: gofuse.SetAttrInCommon{
			InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
			Valid:    gofuse.FATTR_MODE,
			Mode:     0o700,
		},
	}, &attrOut); st != gofuse.OK {
		t.Fatalf("SetAttr status = %v, want OK", st)
	}
	if attrOut.Mode&uint32(syscall.S_IFMT) != uint32(syscall.S_IFDIR) || attrOut.Mode&0o777 != 0o700 {
		t.Fatalf("mode = %#o, want dir 0700", attrOut.Mode)
	}
	if _, _, err := fs.listGitDir(context.Background(), "/repo/src"); err != nil {
		t.Fatalf("listGitDir after chmod: %v", err)
	}
	fixture.mu.Lock()
	entry := fixture.overlay["src"]
	fixture.mu.Unlock()
	if entry.Kind != "dir" {
		t.Fatalf("overlay kind = %q, want dir", entry.Kind)
	}
}

func TestGitWorkspaceMkdirPreservesDirectoryMode(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var out gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Mode:     0o700,
	}, "private", &out); st != gofuse.OK {
		t.Fatalf("Mkdir status = %v, want OK", st)
	}
	fixture.mu.Lock()
	entry := fixture.overlay["private"]
	fixture.mu.Unlock()
	if entry.Kind != "dir" {
		t.Fatalf("overlay kind = %q, want dir", entry.Kind)
	}
	parsed, ok := parseGitMode(entry.Mode)
	if !ok || parsed&0o777 != 0o700 {
		t.Fatalf("overlay mode = %q parsed=%#o ok=%t, want 0700", entry.Mode, parsed, ok)
	}
	fs2 := NewDat9FS(fixture.client(), opts)
	got, handled := fs2.gitEntry(context.Background(), "/repo/private", true)
	if !handled || got == nil || !got.IsDir || got.Mode&0o777 != 0o700 {
		t.Fatalf("restored entry handled=%t entry=%+v, want dir 0700", handled, got)
	}
}

func TestGitWorkspaceReadOnlyRejectsMkdir(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true, ReadOnly: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var out gofuse.EntryOut
	if st := fs.Mkdir(nil, &gofuse.MkdirIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Mode:     0o755,
	}, "blocked", &out); st != gofuse.EROFS {
		t.Fatalf("Mkdir status = %v, want EROFS", st)
	}
	fixture.mu.Lock()
	_, ok := fixture.overlay["blocked"]
	fixture.mu.Unlock()
	if ok {
		t.Fatalf("read-only mkdir wrote overlay")
	}
}

func TestGitWorkspaceRenameRejectsNonEmptyDirectoryTarget(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{
		{WorkspaceID: "ws1", CommitSHA: fixtureHeadCommit, Path: "a", ParentPath: "", Name: "a", Kind: "dir", Mode: "040000", ObjectSHA: "3333333333333333333333333333333333333333", SizeBytes: -1},
		{WorkspaceID: "ws1", CommitSHA: fixtureHeadCommit, Path: "a/file.txt", ParentPath: "a", Name: "file.txt", Kind: "file", Mode: "100644", ObjectSHA: "4444444444444444444444444444444444444444", SizeBytes: 1},
		{WorkspaceID: "ws1", CommitSHA: fixtureHeadCommit, Path: "b", ParentPath: "", Name: "b", Kind: "dir", Mode: "040000", ObjectSHA: "5555555555555555555555555555555555555555", SizeBytes: -1},
		{WorkspaceID: "ws1", CommitSHA: fixtureHeadCommit, Path: "b/existing.txt", ParentPath: "b", Name: "existing.txt", Kind: "file", Mode: "100644", ObjectSHA: "6666666666666666666666666666666666666666", SizeBytes: 1},
	}
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Newdir:   repoIno,
	}, "a", "b")
	if st != gofuse.Status(syscall.ENOTEMPTY) {
		t.Fatalf("Rename status = %v, want ENOTEMPTY", st)
	}
}

func TestGitWorkspaceRenameDirectoryReturnsEXDEV(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "dir",
			ParentPath:  "",
			Name:        "dir",
			Kind:        "dir",
			Mode:        "040000",
			ObjectSHA:   "3333333333333333333333333333333333333333",
			SizeBytes:   -1,
		},
		{
			WorkspaceID: "ws1",
			CommitSHA:   fixtureHeadCommit,
			Path:        "dir/file.txt",
			ParentPath:  "dir",
			Name:        "file.txt",
			Kind:        "file",
			Mode:        "100644",
			ObjectSHA:   "4444444444444444444444444444444444444444",
			SizeBytes:   1,
		},
	}
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	st := fs.Rename(nil, &gofuse.RenameIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Newdir:   repoIno,
	}, "dir", "moved")
	if st != gofuse.Status(syscall.EXDEV) {
		t.Fatalf("Rename status = %v, want EXDEV", st)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.overlay) != 0 {
		t.Fatalf("overlay writes = %d, want none after directory rename fallback", len(fixture.overlay))
	}
}

func TestGitWorkspaceRootRmdirDeletesWorkspace(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.treeNodes = []client.GitTreeNode{}
	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)

	if st := fs.Rmdir(nil, &gofuse.InHeader{NodeId: 1}, "repo"); st != gofuse.OK {
		t.Fatalf("Rmdir status = %v, want OK", st)
	}
	fixture.mu.Lock()
	deleted := fixture.deleted
	fixture.mu.Unlock()
	if !deleted {
		t.Fatalf("workspace was not deleted")
	}
}

func TestGitWorkspaceRestoresLocalGitStateOnLookup(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	fixture := newGitWorkspaceFixture(t)
	repo := createGitRepoWithReadme(t, []byte("hello base\n"))
	fixture.headCommit = fuseGitOutputForTest(t, repo, "rev-parse", "HEAD")
	state, err := archiveLocalGitDir(filepath.Join(repo, ".git"))
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
	if !strings.Contains(string(got), "repositoryformatversion") {
		t.Fatalf("restored config = %q, want git config", got)
	}
}

func TestGitWorkspaceRestoreReplacesInvalidLocalGitState(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	fixture := newGitWorkspaceFixture(t)
	repo := createGitRepoWithReadme(t, []byte("hello base\n"))
	fixture.headCommit = fuseGitOutputForTest(t, repo, "rev-parse", "HEAD")
	state, err := archiveLocalGitDir(filepath.Join(repo, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	fixture.state = state

	localRoot := t.TempDir()
	opts := &MountOptions{LocalRoot: localRoot, Profile: MountProfileCodingAgent, EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	gitDir := filepath.Join(localRoot, "overlay", "repo", ".git")
	if err := os.MkdirAll(gitDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte("[broken]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(gitDir, "HEAD"), []byte(fixture.headCommit+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var out gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, ".git", &out); st != gofuse.OK {
		t.Fatalf("Lookup .git status = %v, want OK", st)
	}
	got, err := os.ReadFile(filepath.Join(gitDir, "HEAD"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(got)) == "" {
		t.Fatalf("restored HEAD = %q, want non-empty", got)
	}
	got, err = os.ReadFile(filepath.Join(gitDir, "config"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(got), "[broken]") {
		t.Fatalf("invalid git state was reused: %q", got)
	}
}

func TestArchiveLocalGitStateDirSkipsObjectDatabases(t *testing.T) {
	gitDir := t.TempDir()
	config := "[remote \"origin\"]\n\turl = https://secret-token@github.com/mem9-ai/drive9.git\n"
	if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte(config), 0o644); err != nil {
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
	gotConfig, err := os.ReadFile(filepath.Join(dst, "config"))
	if err != nil {
		t.Fatalf("config missing from objectless archive: %v", err)
	}
	if strings.Contains(string(gotConfig), "secret-token") || !strings.Contains(string(gotConfig), "https://github.com/mem9-ai/drive9.git") {
		t.Fatalf("config credentials were not sanitized: %q", gotConfig)
	}
	if _, err := os.Stat(filepath.Join(dst, "objects")); !os.IsNotExist(err) {
		t.Fatalf("objects restored from objectless archive, err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "modules", "sub", "objects")); !os.IsNotExist(err) {
		t.Fatalf("module objects restored from objectless archive, err=%v", err)
	}
}

func TestExtractGitArchiveRejectsSymlinkTraversal(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "safe", Mode: 0o777}); err != nil {
		t.Fatal(err)
	}
	if err := tw.WriteHeader(&tar.Header{Name: "link/config", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len("x"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}

	err := extractGitArchive(buf.Bytes(), t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "traverses symlink") {
		t.Fatalf("extractGitArchive err = %v, want symlink traversal rejection", err)
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

func TestGitObjectDatabasePathsSkipDirectCheckpoint(t *testing.T) {
	if !localPathMayBeGitState("/repo/.git/index") {
		t.Fatal(".git/index should be git state")
	}
	if !localPathShouldCheckpointGitState("/repo/.git/index") {
		t.Fatal(".git/index should checkpoint")
	}
	if !localPathMayBeGitState("/repo/.git/objects/aa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
		t.Fatal(".git/objects path should still be recognized as git state")
	}
	if localPathShouldCheckpointGitState("/repo/.git/objects/aa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
		t.Fatal(".git/objects path should not checkpoint directly")
	}
	if localPathShouldCheckpointGitState("/repo/.git/objects") {
		t.Fatal(".git/objects directory should not checkpoint directly")
	}
	if localPathShouldCheckpointGitState("/repo/.git/modules/sub/objects/aa/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb") {
		t.Fatal("submodule object database path should not checkpoint directly")
	}
}

func TestGitWorkspaceCleanReadUsesBlobCache(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	localRoot := t.TempDir()
	if err := gitcache.WriteBlob(context.Background(), localRoot, "ws1", fixtureHeadCommit, fixture.readmeObjectSHA, []byte("cached blob\n")); err != nil {
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

func TestGitWorkspaceOverlayReadFetchesMissingContent(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	content := []byte("overlay content\n")
	fixture.overlay["generated.ts"] = client.GitOverlayEntry{
		WorkspaceID: "ws1",
		Path:        "generated.ts",
		Op:          "upsert",
		Kind:        "file",
		Mode:        "100644",
		SizeBytes:   int64(len(content)),
	}

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	if entry, handled := fs.gitEntry(context.Background(), "/repo/generated.ts", true); !handled || entry == nil {
		t.Fatalf("gitEntry handled=%t entry=%v, want overlay entry", handled, entry)
	}
	fs.git.mu.Lock()
	fs.git.loadedAt = time.Now().Add(gitWorkspaceRefreshInterval)
	fs.git.mu.Unlock()
	fixture.mu.Lock()
	entry := fixture.overlay["generated.ts"]
	entry.Content = append([]byte(nil), content...)
	fixture.overlay["generated.ts"] = entry
	fixture.mu.Unlock()

	got, err := fs.readGitFile(context.Background(), "/repo/generated.ts", 0, -1)
	if err != nil {
		t.Fatalf("readGitFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("readGitFile = %q, want %q", got, content)
	}
	fixture.mu.Lock()
	fixture.overlay["generated.ts"] = client.GitOverlayEntry{
		WorkspaceID: "ws1",
		Path:        "generated.ts",
		Op:          "upsert",
		Kind:        "file",
		Mode:        "100644",
		SizeBytes:   int64(len(content)),
	}
	fixture.mu.Unlock()
	got, err = fs.readGitFile(context.Background(), "/repo/generated.ts", 0, -1)
	if err != nil {
		t.Fatalf("cached readGitFile: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("cached readGitFile = %q, want %q", got, content)
	}
}

func TestGitWorkspaceOverlayReadErrorsWhenContentMissing(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	fixture.overlay["missing.ts"] = client.GitOverlayEntry{
		WorkspaceID: "ws1",
		Path:        "missing.ts",
		Op:          "upsert",
		Kind:        "file",
		Mode:        "100644",
		SizeBytes:   8,
	}

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	got, err := fs.readGitFile(context.Background(), "/repo/missing.ts", 0, -1)
	if err == nil {
		t.Fatalf("readGitFile = %q, nil error; want missing content error", got)
	}
}

func TestGitWorkspaceWritablePreloadFromCacheDoesNotWriteOverlay(t *testing.T) {
	fixture := newGitWorkspaceFixture(t)
	localRoot := t.TempDir()
	if err := gitcache.WriteBlob(context.Background(), localRoot, "ws1", fixtureHeadCommit, fixture.readmeObjectSHA, []byte("hello base\n")); err != nil {
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

	forbidGitSubcommandForTest(t, "rev-list")
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

func TestGitStateCheckpointIsDebouncedAndDrained(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	src := createGitRepoWithReadme(t, []byte("hello base\n"))
	fixture := newGitWorkspaceFixture(t)
	fixture.repoURL = src
	fixture.headCommit = fuseGitOutputForTest(t, src, "rev-parse", "HEAD")
	fixture.readmeObjectSHA = fuseGitOutputForTest(t, src, "hash-object", "README.md")
	fixture.readmeSize = int64(len("hello base\n"))

	localRoot := t.TempDir()
	runFuseTestGit(t, "", "clone", "--no-checkout", src, filepath.Join(localRoot, "overlay", "repo"))

	opts := &MountOptions{
		LocalRoot:           localRoot,
		Profile:             MountProfileCodingAgent,
		EnableGitWorkspaces: true,
	}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	if err := fs.ensureGitWorkspaces(context.Background()); err != nil {
		t.Fatalf("ensureGitWorkspaces: %v", err)
	}

	fs.scheduleGitStateCheckpoint("/repo/.git/index")
	fixture.mu.Lock()
	immediate := fixture.gitStatePuts
	fixture.mu.Unlock()
	if immediate != 0 {
		t.Fatalf("git state PUTs immediately after schedule = %d, want 0", immediate)
	}

	fs.drainGitStateCheckpoints()
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.gitStatePuts != 1 {
		t.Fatalf("git state PUTs after drain = %d, want 1", fixture.gitStatePuts)
	}
	if fixture.stateStorage != gitStateStorageTarGzNoObjects {
		t.Fatalf("git state storage = %q, want %q", fixture.stateStorage, gitStateStorageTarGzNoObjects)
	}
	if len(fixture.state) == 0 {
		t.Fatal("git state checkpoint content is empty")
	}
}

func TestGitStateCheckpointSkipsTransientLockFiles(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	src := createGitRepoWithReadme(t, []byte("hello base\n"))
	fixture := newGitWorkspaceFixture(t)
	fixture.repoURL = src
	fixture.headCommit = fuseGitOutputForTest(t, src, "rev-parse", "HEAD")
	fixture.readmeObjectSHA = fuseGitOutputForTest(t, src, "hash-object", "README.md")
	fixture.readmeSize = int64(len("hello base\n"))

	localRoot := t.TempDir()
	runFuseTestGit(t, "", "clone", "--no-checkout", src, filepath.Join(localRoot, "overlay", "repo"))

	opts := &MountOptions{
		LocalRoot:           localRoot,
		Profile:             MountProfileCodingAgent,
		EnableGitWorkspaces: true,
	}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	if err := fs.ensureGitWorkspaces(context.Background()); err != nil {
		t.Fatalf("ensureGitWorkspaces: %v", err)
	}

	fs.scheduleGitStateCheckpoint("/repo/.git/index.lock")
	fs.scheduleGitStateCheckpoint("/repo/.git/refs/heads/main.lock")
	fs.drainGitStateCheckpoints()
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.gitStatePuts != 0 {
		t.Fatalf("git state PUTs for lock files = %d, want 0", fixture.gitStatePuts)
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
	if entry, ok := fs.inodes.GetEntry(lookupOut.NodeId); !ok || entry.Size != int64(len(content)) {
		t.Fatalf("inode size after clean read = entry %v ok %t, want %d", entry, ok, len(content))
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

func TestGitWorkspaceAppendOpenUsesCurrentBufferEnd(t *testing.T) {
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
	fixture.readmeSize = int64(len(content))

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	fs.inodes.UpdateSize(lookupOut.NodeId, 0)

	var openOut gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_APPEND),
	}, &openOut); st != gofuse.OK {
		t.Fatalf("Open status = %v, want OK", st)
	}
	if openOut.OpenFlags&gofuse.FOPEN_KEEP_CACHE != 0 {
		t.Fatalf("append open flags = %d, want kernel cache invalidation", openOut.OpenFlags)
	}

	appendix := []byte("tail\n")
	written, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Fh:       openOut.Fh,
		Offset:   0,
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

func TestGitWorkspaceTruncateOpenRefreshesDirtyMirror(t *testing.T) {
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
	fixture.readmeSize = int64(len(content))

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}

	var appendOpen gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_APPEND),
	}, &appendOpen); st != gofuse.OK {
		t.Fatalf("append Open status = %v, want OK", st)
	}
	appendix := []byte("tail\n")
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Fh:       appendOpen.Fh,
		Offset:   0,
		Size:     uint32(len(appendix)),
	}, appendix); st != gofuse.OK {
		t.Fatalf("append Write status = %v, want OK", st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: appendOpen.Fh}); st != gofuse.OK {
		t.Fatalf("append Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: appendOpen.Fh})
	appended := append(append([]byte{}, content...), appendix...)
	got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
	if err != nil {
		t.Fatalf("read appended git file: %v", err)
	}
	if !bytes.Equal(got, appended) {
		t.Fatalf("appended readGitFile = %q, want %q", got, appended)
	}

	var restoreOpen gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_TRUNC),
	}, &restoreOpen); st != gofuse.OK {
		t.Fatalf("restore Open status = %v, want OK", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Fh:       restoreOpen.Fh,
		Offset:   0,
		Size:     uint32(len(content)),
	}, content); st != gofuse.OK {
		t.Fatalf("restore Write status = %v, want OK", st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: restoreOpen.Fh}); st != gofuse.OK {
		t.Fatalf("restore Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: restoreOpen.Fh})
	fs.drainGitOverlayWrites()

	got, err = fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
	if err != nil {
		t.Fatalf("read restored git file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("restored readGitFile = %q, want %q", got, content)
	}
	fixture.mu.Lock()
	entry, ok := fixture.overlay["README.md"]
	fixture.mu.Unlock()
	if !ok {
		t.Fatalf("overlay entry missing for README.md")
	}
	if !bytes.Equal(entry.Content, content) {
		t.Fatalf("restored overlay content = %q, want %q", entry.Content, content)
	}
}

func TestGitWorkspaceUnlinkCreateRefreshesDirtyMirror(t *testing.T) {
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
	fixture.readmeSize = int64(len(content))

	opts := &MountOptions{LocalRoot: t.TempDir(), EnableGitWorkspaces: true}
	opts.setDefaults()
	fs := NewDat9FS(fixture.client(), opts)
	repoIno := fs.inodes.Lookup("/repo", true, 0, time.Now())

	var lookupOut gofuse.EntryOut
	if st := fs.Lookup(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md", &lookupOut); st != gofuse.OK {
		t.Fatalf("Lookup status = %v, want OK", st)
	}
	var appendOpen gofuse.OpenOut
	if st := fs.Open(nil, &gofuse.OpenIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_APPEND),
	}, &appendOpen); st != gofuse.OK {
		t.Fatalf("append Open status = %v, want OK", st)
	}
	appendix := []byte("tail\n")
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: lookupOut.NodeId},
		Fh:       appendOpen.Fh,
		Offset:   0,
		Size:     uint32(len(appendix)),
	}, appendix); st != gofuse.OK {
		t.Fatalf("append Write status = %v, want OK", st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: appendOpen.Fh}); st != gofuse.OK {
		t.Fatalf("append Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: appendOpen.Fh})

	if st := fs.Unlink(nil, &gofuse.InHeader{NodeId: repoIno}, "README.md"); st != gofuse.OK {
		t.Fatalf("Unlink status = %v, want OK", st)
	}
	var createOut gofuse.CreateOut
	if st := fs.Create(nil, &gofuse.CreateIn{
		InHeader: gofuse.InHeader{NodeId: repoIno},
		Flags:    uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_EXCL),
		Mode:     0o644,
	}, "README.md", &createOut); st != gofuse.OK {
		t.Fatalf("Create status = %v, want OK", st)
	}
	if _, st := fs.Write(nil, &gofuse.WriteIn{
		InHeader: gofuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
		Size:     uint32(len(content)),
	}, content); st != gofuse.OK {
		t.Fatalf("restore Write status = %v, want OK", st)
	}
	if st := fs.Flush(nil, &gofuse.FlushIn{Fh: createOut.Fh}); st != gofuse.OK {
		t.Fatalf("restore Flush status = %v, want OK", st)
	}
	fs.Release(nil, &gofuse.ReleaseIn{Fh: createOut.Fh})
	fs.drainGitOverlayWrites()

	got, err := fs.readGitFile(context.Background(), "/repo/README.md", 0, -1)
	if err != nil {
		t.Fatalf("read recreated git file: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("recreated readGitFile = %q, want %q", got, content)
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

func forbidGitSubcommandForTest(t *testing.T, forbidden string) {
	t.Helper()
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Skip("git not found")
	}
	binDir := t.TempDir()
	wrapper := filepath.Join(binDir, "git")
	script := fmt.Sprintf(`#!/bin/sh
for arg in "$@"; do
	if [ "$arg" = %q ]; then
		echo "forbidden git subcommand: $arg" >&2
		exit 123
	fi
done
exec %q "$@"
`, forbidden, realGit)
	if err := os.WriteFile(wrapper, []byte(script), 0o755); err != nil {
		t.Fatalf("write git wrapper: %v", err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
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
