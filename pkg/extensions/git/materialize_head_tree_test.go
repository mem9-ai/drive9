package git

import (
	"context"
	"fmt"
	"net/http"
	"net/http/cgi"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/gitcache"
)

func requireGitHTTPBackend(t *testing.T) string {
	t.Helper()
	gitBin(t)
	execPathOut, err := exec.Command("git", "--exec-path").Output()
	require.NoError(t, err)
	backend := filepath.Join(strings.TrimSpace(string(execPathOut)), "git-http-backend")
	if _, statErr := os.Stat(backend); statErr != nil {
		t.Skipf("git-http-backend unavailable at %s: %v", backend, statErr)
	}
	return backend
}

type countingGitHTTPServer struct {
	URL             string
	BareDir         string
	UploadPackPOSTs atomic.Int32
	FailRequests    atomic.Bool
}

func startCountingGitHTTPServer(t *testing.T, srcRepo string) *countingGitHTTPServer {
	t.Helper()
	backend := requireGitHTTPBackend(t)
	projectRoot := t.TempDir()
	bareDir := filepath.Join(projectRoot, "remote.git")
	run(t, srcRepo, "clone", "-q", "--bare", "--no-local", srcRepo, bareDir)
	run(t, bareDir, "config", "uploadpack.allowFilter", "true")
	run(t, bareDir, "config", "uploadpack.allowAnySHA1InWant", "true")
	run(t, bareDir, "config", "http.uploadpack", "true")

	s := &countingGitHTTPServer{BareDir: bareDir}
	cgiHandler := &cgi.Handler{
		Path: backend,
		Env: []string{
			"GIT_PROJECT_ROOT=" + projectRoot,
			"GIT_HTTP_EXPORT_ALL=1",
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.FailRequests.Load() {
			http.Error(w, "injected remote failure", http.StatusInternalServerError)
			return
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/git-upload-pack") {
			s.UploadPackPOSTs.Add(1)
		}
		cgiHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	s.URL = ts.URL + "/remote.git"
	return s
}

func newTestGitClientOnly(t *testing.T) (*Git, *client.Client) {
	t.Helper()
	b := testbackend.NewBackend()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	g, err := New(Options{Client: cl, LocalRoot: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = g.Stop(context.Background()) })
	return g, cl
}

// MaterializeHeadTree must deduplicate HEAD blob OIDs and fetch all missing
// blobs in a single promisor git-upload-pack round-trip rather than spawning
// O(N) per-blob lazy fetches via `git cat-file --batch`.
func TestMaterializeHeadTreeBatchesFetchInSingleUploadPack(t *testing.T) {
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	const uniqueCount = 40
	for i := range uniqueCount {
		p := filepath.Join(src, fmt.Sprintf("dir-%02d", i%5), fmt.Sprintf("file-%03d.txt", i))
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(fmt.Sprintf("unique-blob-payload-%03d\n", i)), 0o644))
	}
	// Duplicate content across 10 extra paths to verify OID deduplication.
	for i := range 10 {
		dup := filepath.Join(src, "dups", fmt.Sprintf("dup-%02d.txt", i))
		require.NoError(t, os.MkdirAll(filepath.Dir(dup), 0o755))
		require.NoError(t, os.WriteFile(dup, []byte("unique-blob-payload-000\n"), 0o644))
	}
	run(t, src, "add", ".")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "seed")

	remote := startCountingGitHTTPServer(t, src)

	partial := filepath.Join(t.TempDir(), "partial")
	run(t, filepath.Dir(partial), "clone", "-q", "--no-checkout", "--filter=blob:none", remote.URL, partial)
	gitDir := filepath.Join(partial, ".git")

	missingBefore := run(t, partial, "rev-list", "--objects", "--missing=print", "HEAD")
	require.Contains(t, missingBefore, "?", "precondition: partial clone must have missing blobs")

	remote.UploadPackPOSTs.Store(0)
	g, _ := newTestGitClientOnly(t)
	require.NoError(t, g.MaterializeHeadTree(context.Background(), gitDir))

	require.Equal(t, int32(1), remote.UploadPackPOSTs.Load(),
		"MaterializeHeadTree must fetch all %d missing blobs in 1 upload-pack POST", uniqueCount)

	missingAfter := run(t, partial, "rev-list", "--objects", "--missing=print", "HEAD")
	require.NotContains(t, missingAfter, "?")

	// Every file in HEAD must be readable with GIT_NO_LAZY_FETCH=1.
	cmd := exec.Command("git", "--git-dir", gitDir, "cat-file", "-p", "HEAD:dir-00/file-000.txt")
	cmd.Env = append(os.Environ(), "GIT_NO_LAZY_FETCH=1")
	out, err := cmd.Output()
	require.NoError(t, err)
	require.Equal(t, "unique-blob-payload-000\n", string(out))
}

// Already-hydrated repositories and linked worktrees sharing a hydrated common
// object store must perform zero remote fetches, while a linked worktree with
// new branch blobs fetches only the missing delta in a single request.
func TestMaterializeHeadTreeZeroFetchWhenLocalAndInLinkedWorktree(t *testing.T) {
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	for i := range 6 {
		require.NoError(t, os.WriteFile(filepath.Join(src, fmt.Sprintf("base-%d.txt", i)), []byte(fmt.Sprintf("base-%d\n", i)), 0o644))
	}
	run(t, src, "add", ".")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "base")

	// Create a feature branch in src with 3 additional blobs.
	run(t, src, "checkout", "-q", "-b", "feature")
	for i := range 3 {
		require.NoError(t, os.WriteFile(filepath.Join(src, fmt.Sprintf("feat-%d.txt", i)), []byte(fmt.Sprintf("feat-%d\n", i)), 0o644))
	}
	run(t, src, "add", ".")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "feature")
	featHead := run(t, src, "rev-parse", "HEAD")
	run(t, src, "checkout", "-q", "main")

	remote := startCountingGitHTTPServer(t, src)

	common := filepath.Join(t.TempDir(), "common")
	run(t, filepath.Dir(common), "clone", "-q", "--no-checkout", "--filter=blob:none", remote.URL, common)
	commonGitDir := filepath.Join(common, ".git")
	// Fetch feature commit+trees (still blobless) so a linked worktree can attach to it.
	run(t, common, "fetch", "-q", "--filter=blob:none", "origin", "feature:refs/remotes/origin/feature")

	linkedSame := filepath.Join(t.TempDir(), "linked-same")
	run(t, common, "worktree", "add", "--no-checkout", "-q", linkedSame, "-b", "wt-same", "HEAD")
	linkedSameGitDir := filepath.Join(commonGitDir, "worktrees", "linked-same")

	linkedFeat := filepath.Join(t.TempDir(), "linked-feat")
	run(t, common, "worktree", "add", "--no-checkout", "-q", linkedFeat, "-b", "wt-feat", featHead)
	linkedFeatGitDir := filepath.Join(commonGitDir, "worktrees", "linked-feat")

	g, _ := newTestGitClientOnly(t)
	ctx := context.Background()

	// 1. Hydrate common once (1 upload-pack POST).
	remote.UploadPackPOSTs.Store(0)
	require.NoError(t, g.MaterializeHeadTree(ctx, commonGitDir))
	require.Equal(t, int32(1), remote.UploadPackPOSTs.Load())

	// 2. Repeat on common + run on linked worktree at same HEAD while remote fails if touched -> 0 POSTs.
	remote.UploadPackPOSTs.Store(0)
	remote.FailRequests.Store(true)
	require.NoError(t, g.MaterializeHeadTree(ctx, commonGitDir))
	require.NoError(t, g.MaterializeHeadTree(ctx, linkedSameGitDir))
	require.Equal(t, int32(0), remote.UploadPackPOSTs.Load())

	// 3. Linked worktree at feature commit fetches only the 3 missing feature blobs in 1 POST.
	remote.FailRequests.Store(false)
	require.NoError(t, g.MaterializeHeadTree(ctx, linkedFeatGitDir))
	require.Equal(t, int32(1), remote.UploadPackPOSTs.Load())

	missingFeat := run(t, linkedFeat, "rev-list", "--objects", "--missing=print", "HEAD")
	require.NotContains(t, missingFeat, "?")
}

// Security and failure-semantics gates: missing promisor config, '-' remote
// names, disallowed URL transports, unsafe remote keys, and remote fetch errors
// must all return explicit errors without silently succeeding.
func TestMaterializeHeadTreeRejectsMissingOrUnsafePromisorConfig(t *testing.T) {
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("payload\n"), 0o644))
	run(t, src, "add", "a.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "init")

	remote := startCountingGitHTTPServer(t, src)
	g, _ := newTestGitClientOnly(t)
	ctx := context.Background()

	newPartial := func(t *testing.T) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "partial")
		run(t, filepath.Dir(dir), "clone", "-q", "--no-checkout", "--filter=blob:none", remote.URL, dir)
		remote.UploadPackPOSTs.Store(0)
		return filepath.Join(dir, ".git")
	}

	t.Run("no_promisor_remote", func(t *testing.T) {
		gitDir := newPartial(t)
		_ = exec.Command("git", "--git-dir", gitDir, "config", "--unset-all", "extensions.partialClone").Run()
		_ = exec.Command("git", "--git-dir", gitDir, "config", "--unset-all", "remote.origin.promisor").Run()
		err := g.MaterializeHeadTree(ctx, gitDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "no promisor remote")
		require.Equal(t, int32(0), remote.UploadPackPOSTs.Load())
	})

	t.Run("last_promisor_value_disables_remote", func(t *testing.T) {
		gitDir := newPartial(t)
		_ = exec.Command("git", "--git-dir", gitDir, "config", "--unset-all", "extensions.partialClone").Run()
		run(t, gitDir, "config", "--add", "remote.origin.promisor", "false")
		require.ErrorContains(t, g.MaterializeHeadTree(ctx, gitDir), "no promisor remote")
		require.Zero(t, remote.UploadPackPOSTs.Load())
	})

	t.Run("fetch_uses_same_config_as_remote_selection", func(t *testing.T) {
		gitDir := newPartial(t)
		global := filepath.Join(t.TempDir(), "gitconfig")
		run(t, gitDir, "config", "--file", global, "http.proxy", "http://127.0.0.1:1")
		// A global URL rewrite to an empty local repository would lose the objects.
		run(t, gitDir, "config", "--file", global, "url.file:///missing-test-repository.insteadOf", remote.URL)
		t.Setenv("GIT_CONFIG_GLOBAL", global)
		require.NoError(t, g.MaterializeHeadTree(ctx, gitDir))
		require.Equal(t, int32(1), remote.UploadPackPOSTs.Load())
	})

	t.Run("dash_prefixed_partial_clone_remote", func(t *testing.T) {
		gitDir := newPartial(t)
		run(t, gitDir, "--git-dir", gitDir, "config", "extensions.partialClone", "--upload-pack=/bin/false")
		err := g.MaterializeHeadTree(ctx, gitDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must not start with '-'")
		require.Equal(t, int32(0), remote.UploadPackPOSTs.Load())
	})

	t.Run("dash_prefixed_promisor_section", func(t *testing.T) {
		gitDir := newPartial(t)
		_ = exec.Command("git", "--git-dir", gitDir, "config", "--unset-all", "extensions.partialClone").Run()
		cfgPath := filepath.Join(gitDir, "config")
		f, err := os.OpenFile(cfgPath, os.O_APPEND|os.O_WRONLY, 0o600)
		require.NoError(t, err)
		_, err = f.WriteString("\n[remote \"-bad\"]\n\turl = " + remote.URL + "\n\tpromisor = true\n")
		require.NoError(t, f.Close())
		require.NoError(t, err)

		err = g.MaterializeHeadTree(ctx, gitDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must not start with '-'")
		require.Equal(t, int32(0), remote.UploadPackPOSTs.Load())
	})

	t.Run("disallowed_local_file_url_when_disabled", func(t *testing.T) {
		t.Setenv(gitcache.EnvGitAllowLocalRepos, "0")
		gitDir := newPartial(t)
		run(t, gitDir, "--git-dir", gitDir, "config", "remote.origin.url", "file://"+remote.BareDir)
		err := g.MaterializeHeadTree(ctx, gitDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "url rejected")
		require.Equal(t, int32(0), remote.UploadPackPOSTs.Load())
	})

	t.Run("disallowed_ext_transport", func(t *testing.T) {
		gitDir := newPartial(t)
		run(t, gitDir, "--git-dir", gitDir, "config", "remote.origin.url", "ext::sh -c false")
		err := g.MaterializeHeadTree(ctx, gitDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "url rejected")
		require.Equal(t, int32(0), remote.UploadPackPOSTs.Load())
	})

	t.Run("unsafe_remote_key", func(t *testing.T) {
		gitDir := newPartial(t)
		run(t, gitDir, "--git-dir", gitDir, "config", "remote.origin.uploadpack", "/bin/false")
		err := g.MaterializeHeadTree(ctx, gitDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsafe config key")
		require.Equal(t, int32(0), remote.UploadPackPOSTs.Load())
	})

	t.Run("remote_fetch_failure", func(t *testing.T) {
		gitDir := newPartial(t)
		remote.FailRequests.Store(true)
		defer remote.FailRequests.Store(false)
		err := g.MaterializeHeadTree(ctx, gitDir)
		require.Error(t, err)
	})
}

// Context cancellation and Git.Stop() must promptly abort an in-flight
// MaterializeHeadTree fetch without latching an incomplete workspace restore.
func TestMaterializeHeadTreeCancelsPromptlyOnCtxAndStop(t *testing.T) {
	backend := requireGitHTTPBackend(t)
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	require.NoError(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("payload\n"), 0o644))
	run(t, src, "add", "a.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "init")
	head := run(t, src, "rev-parse", "HEAD")

	projectRoot := t.TempDir()
	bareDir := filepath.Join(projectRoot, "remote.git")
	run(t, src, "clone", "-q", "--bare", "--no-local", src, bareDir)
	run(t, bareDir, "config", "uploadpack.allowFilter", "true")
	run(t, bareDir, "config", "uploadpack.allowAnySHA1InWant", "true")
	run(t, bareDir, "config", "http.uploadpack", "true")

	cgiHandler := &cgi.Handler{
		Path: backend,
		Env: []string{
			"GIT_PROJECT_ROOT=" + projectRoot,
			"GIT_HTTP_EXPORT_ALL=1",
		},
	}

	testDone := make(chan struct{})
	var blockUploadPack atomic.Bool
	enteredFetch := make(chan struct{}, 2)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blockUploadPack.Load() && r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/git-upload-pack") {
			select {
			case enteredFetch <- struct{}{}:
			default:
			}
			select {
			case <-r.Context().Done():
				return
			case <-testDone:
				return
			}
		}
		cgiHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	defer close(testDone)

	repoURL := ts.URL + "/remote.git"

	// 1. Direct ctx cancellation aborts MaterializeHeadTree promptly.
	partial := filepath.Join(t.TempDir(), "partial")
	run(t, filepath.Dir(partial), "clone", "-q", "--no-checkout", "--filter=blob:none", repoURL, partial)
	blockUploadPack.Store(true)

	g1, _ := newTestGitClientOnly(t)
	callCtx, callCancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- g1.MaterializeHeadTree(callCtx, filepath.Join(partial, ".git"))
	}()
	select {
	case <-enteredFetch:
	case <-time.After(5 * time.Second):
		t.Fatal("MaterializeHeadTree did not enter git-upload-pack")
	}
	callCancel()
	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("MaterializeHeadTree did not exit promptly after context cancel")
	}

	// 2. Git.Stop() cancels an in-flight ensureWorkspaceState during MaterializeHeadTree
	// and leaves restoreLatched false.
	blockUploadPack.Store(false)
	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("ws-stop-mat", "/repo", repoURL, head)
	apiSrv := httptest.NewServer(b)
	t.Cleanup(apiSrv.Close)
	cl := client.New(apiSrv.URL, "")

	localRootA := t.TempDir()
	repoA := filepath.Join(localRootA, "overlay", "repo")
	require.NoError(t, os.MkdirAll(filepath.Dir(repoA), 0o755))
	run(t, filepath.Dir(repoA), "clone", "-q", "--no-checkout", "--filter=blob:none", repoURL, repoA)
	gA, err := New(Options{Client: cl, LocalRoot: localRootA})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gA.Stop(context.Background()) })
	wsA := &workspace{id: "ws-stop-mat", root: "/repo", headCommit: head, repoURL: repoURL, fastBlobless: true}
	require.NoError(t, gA.checkpointWorkspace(context.Background(), wsA, filepath.Join(repoA, ".git")))

	// Allow the initial clone, then block the request for missing blobs.
	var postCount atomic.Int32
	blockSecondPOSTDone := make(chan struct{})
	defer close(blockSecondPOSTDone)
	hydrationEntered := make(chan struct{}, 1)
	ts2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/git-upload-pack") {
			if postCount.Add(1) >= 2 {
				select {
				case hydrationEntered <- struct{}{}:
				default:
				}
				select {
				case <-r.Context().Done():
					return
				case <-blockSecondPOSTDone:
					return
				}
			}
		}
		cgiHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(ts2.Close)
	repoURL2 := ts2.URL + "/remote.git"
	b.SeedGitWorkspaceFull("ws-stop-mat", "/repo", repoURL2, head)
	run(t, repoA, "remote", "set-url", "origin", repoURL2)
	wsA.repoURL = repoURL2
	require.NoError(t, gA.checkpointWorkspace(context.Background(), wsA, filepath.Join(repoA, ".git")))

	localRootB := t.TempDir()
	gB, err := New(Options{Client: cl, LocalRoot: localRootB})
	require.NoError(t, err)
	wsB := &workspace{id: "ws-stop-mat", root: "/repo", headCommit: head, repoURL: repoURL2, fastBlobless: true}

	ensureDone := make(chan error, 1)
	go func() {
		ensureDone <- gB.ensureWorkspaceState(context.Background(), wsB)
	}()

	select {
	case <-hydrationEntered:
	case <-time.After(10 * time.Second):
		t.Fatal("ensureWorkspaceState did not enter MaterializeHeadTree upload-pack")
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()
	require.NoError(t, gB.Stop(stopCtx))

	select {
	case <-ensureDone:
	case <-time.After(2 * time.Second):
		t.Fatal("ensureWorkspaceState did not return after Stop")
	}
	require.False(t, wsB.isRestoreLatched(), "canceled MaterializeHeadTree must not latch workspace restore")
}
