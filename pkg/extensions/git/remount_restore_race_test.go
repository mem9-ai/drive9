package git

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/localize"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func buildLocalizeGitStack(t *testing.T, cl *client.Client, localRoot string, onRefresh func(bool)) (*Git, *fs.Chain) {
	t.Helper()
	bus := fs.NewEventBus()
	g, err := New(Options{
		Client:             cl,
		Events:             bus,
		LocalRoot:          localRoot,
		OnWorkspaceRefresh: onRefresh,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = g.Stop(context.Background()) })

	loc, err := localize.New(localize.Options{
		LocalRoot:            localRoot,
		Profile:              "coding-agent",
		ApplyBuiltinDefaults: true,
		DeclarativeLocal:     g.RoutesLocalCtx,
	})
	require.NoError(t, err)

	terminal, err := core.New(cl, core.Options{})
	require.NoError(t, err)

	ch, err := fs.NewChain(loc, g, terminal)
	require.NoError(t, err)
	return g, ch
}

// A fresh-LocalRoot remount discovers workspaces asynchronously in Start() and
// clones/extracts .git in the background. Immediate .git lookups (test -d
// $repo/.git, git status) and workspace-root listings must wait for that
// workspace's restore instead of racing Localize's Lstat/ReadDir and returning
// ENOENT while .drive9-git-state-restore-* is still running.
func TestFreshLocalRootDotGitLookupWaitsForAsyncRestore(t *testing.T) {
	gitBin(t)
	ctx := context.Background()
	b := testbackend.NewBackend()

	// Seed a bare remote repository for restoreGitObjectsFromRemote.
	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	require.NoError(t, os.WriteFile(filepath.Join(src, "README.md"), []byte("hello\n"), 0o644))
	run(t, src, "add", "README.md")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "initial")
	head := run(t, src, "rev-parse", "HEAD")
	bare := filepath.Join(t.TempDir(), "remote.git")
	run(t, src, "clone", "-q", "--bare", "--no-local", src, bare)
	run(t, bare, "config", "uploadpack.allowFilter", "true")

	// Mount A builds a blobless checkout + dirty staged index and uploads the workspace checkpoint + index.
	localRootA := t.TempDir()
	repoA := filepath.Join(localRootA, "overlay", "repo")
	require.NoError(t, os.MkdirAll(filepath.Dir(repoA), 0o755))
	run(t, filepath.Dir(repoA), "clone", "-q", "--filter=blob:none", "file://"+bare, repoA)
	require.NoError(t, os.WriteFile(filepath.Join(repoA, "staged.txt"), []byte("staged-change\n"), 0o644))
	run(t, repoA, "add", "staged.txt")

	b.SeedGitWorkspaceFull("ws-restore", "/repo", "file://"+bare, head)

	testDone := make(chan struct{})
	defer close(testDone)
	restoreStarted := make(chan struct{})
	allowRestore := make(chan struct{})
	var gateRestore atomic.Bool
	var startedOnce sync.Once

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gateRestore.Load() && r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/git-state") {
			startedOnce.Do(func() { close(restoreStarted) })
			select {
			case <-allowRestore:
			case <-testDone:
			case <-r.Context().Done():
				return
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	cl := client.New(ts.URL, "")
	gA, err := New(Options{Client: cl, LocalRoot: localRootA})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gA.Stop(context.Background()) })
	wsA := &workspace{id: "ws-restore", root: "/repo", headCommit: head, repoURL: "file://" + bare, fastBlobless: true}
	require.NoError(t, gA.checkpointWorkspace(ctx, wsA, filepath.Join(repoA, ".git")))
	require.NoError(t, cl.UpsertGitWorkspaceIndexEntry(ctx, client.GitWorkspaceIndexEntry{
		WorkspaceID: "ws-restore",
		RootPath:    "/repo",
	}))
	require.NoError(t, cl.WriteCtx(ctx, "/repo/.keep", []byte("1")))

	// Mount B starts with a completely empty LocalRoot (dormant at construction)
	// while the checkpoint download is held mid-flight.
	gateRestore.Store(true)
	localRootB := t.TempDir()
	gB, chB := buildLocalizeGitStack(t, cl, localRootB, nil)
	require.NoError(t, gB.Start(ctx))

	select {
	case <-restoreStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("background git state restore did not start")
	}

	type attrOutcome struct {
		res fs.Result
		err error
	}
	dotGitCh := make(chan attrOutcome, 1)
	go func() {
		res, derr := chB.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/.git"})
		dotGitCh <- attrOutcome{res: res, err: derr}
	}()
	listCh := make(chan attrOutcome, 1)
	go func() {
		res, lerr := chB.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo"})
		listCh <- attrOutcome{res: res, err: lerr}
	}()

	select {
	case early := <-dotGitCh:
		t.Fatalf(".git lookup returned before restore completed: %+v err=%v", early.res, early.err)
	case <-time.After(50 * time.Millisecond):
	}

	close(allowRestore)

	var gotAttr attrOutcome
	select {
	case gotAttr = <-dotGitCh:
	case <-time.After(10 * time.Second):
		t.Fatal(".git lookup timed out after releasing restore")
	}
	require.NoError(t, gotAttr.err)
	require.NotNil(t, gotAttr.res.Attr)
	require.Equal(t, fs.KindDir, gotAttr.res.Attr.Kind)

	var gotList attrOutcome
	select {
	case gotList = <-listCh:
	case <-time.After(10 * time.Second):
		t.Fatal("workspace root list timed out after releasing restore")
	}
	require.NoError(t, gotList.err)
	names := map[string]bool{}
	for _, e := range gotList.res.Entries {
		names[e.Name] = true
	}
	require.True(t, names[".git"], "workspace root listing must include restored .git: %v", names)

	restoredRepo := filepath.Join(localRootB, "overlay", "repo")
	require.NoError(t, os.WriteFile(filepath.Join(restoredRepo, "README.md"), []byte("hello\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(restoredRepo, "staged.txt"), []byte("staged-change\n"), 0o644))
	status := run(t, restoredRepo, "status", "--porcelain=v1")
	require.Contains(t, status, "A  staged.txt")
}

// Probing a linked worktree's .git file first on a cold fresh-LocalRoot mount
// restores both the common repository and the linked worktree pointer file
// before Localize stats the path.
func TestFreshLocalRootLinkedWorktreeDotGitLookupWaitsForRestore(t *testing.T) {
	gitBin(t)
	ctx := context.Background()
	b := testbackend.NewBackend()

	src := t.TempDir()
	run(t, src, "init", "-q", "-b", "main")
	require.NoError(t, os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello\n"), 0o644))
	run(t, src, "add", "f.txt")
	run(t, src, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "-m", "one")
	head := run(t, src, "rev-parse", "HEAD")
	bare := filepath.Join(t.TempDir(), "remote.git")
	run(t, src, "clone", "-q", "--bare", "--no-local", src, bare)
	run(t, bare, "config", "uploadpack.allowFilter", "true")

	localRootA := t.TempDir()
	baseRepoA := filepath.Join(localRootA, "overlay", "base")
	linkedRepoA := filepath.Join(localRootA, "overlay", "linked")
	require.NoError(t, os.MkdirAll(filepath.Dir(baseRepoA), 0o755))
	run(t, filepath.Dir(baseRepoA), "clone", "-q", "--filter=blob:none", "file://"+bare, baseRepoA)
	run(t, baseRepoA, "worktree", "add", "-q", linkedRepoA, "-b", "wt-branch")
	wtAdminA := filepath.Join(baseRepoA, ".git", "worktrees", "linked")
	require.NoError(t, os.WriteFile(filepath.Join(linkedRepoA, "f.txt"), []byte("linked commit\n"), 0o644))
	run(t, linkedRepoA, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qam", "linked commit")
	linkedHead := run(t, linkedRepoA, "rev-parse", "HEAD")
	require.NoError(t, os.WriteFile(filepath.Join(linkedRepoA, "staged.txt"), []byte("staged payload\n"), 0o644))
	run(t, linkedRepoA, "add", "staged.txt")
	indexEntries := run(t, linkedRepoA, "ls-files", "--stage")

	b.SeedGitWorkspaceFull("ws-main", "/base", "file://"+bare, head)
	b.SeedGitWorktreeNamed("ws-linked", "/linked", "file://"+bare, head, "ws-main", ".git/worktrees/linked", "linked")

	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")

	gA, err := New(Options{Client: cl, LocalRoot: localRootA})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gA.Stop(context.Background()) })
	mainWS := &workspace{id: "ws-main", root: "/base", headCommit: head, repoURL: "file://" + bare, fastBlobless: true}
	linkedWS := &workspace{
		id: "ws-linked", root: "/linked", headCommit: head, repoURL: "file://" + bare,
		fastBlobless: true, workspaceKind: "linked", commonID: "ws-main", gitDirRel: ".git/worktrees/linked", worktreeName: "linked",
	}
	gA.workspaces = []*workspace{mainWS, linkedWS}
	require.NoError(t, gA.checkpointWorkspace(ctx, mainWS, filepath.Join(baseRepoA, ".git")))
	require.NoError(t, gA.checkpointWorkspace(ctx, linkedWS, wtAdminA))
	require.NoError(t, cl.UpsertGitWorkspaceIndexEntry(ctx, client.GitWorkspaceIndexEntry{WorkspaceID: "ws-main", RootPath: "/base"}))
	require.NoError(t, cl.UpsertGitWorkspaceIndexEntry(ctx, client.GitWorkspaceIndexEntry{WorkspaceID: "ws-linked", RootPath: "/linked"}))

	// Cold Mount B probes /linked/.git BEFORE Start() even runs.
	localRootB := t.TempDir()
	gB, chB := buildLocalizeGitStack(t, cl, localRootB, nil)

	linkedAttr, err := chB.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/linked/.git"})
	require.NoError(t, err)
	require.NotNil(t, linkedAttr.Attr)
	require.Equal(t, fs.KindFile, linkedAttr.Attr.Kind)

	baseAttr, err := chB.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/base/.git"})
	require.NoError(t, err)
	require.NotNil(t, baseAttr.Attr)
	require.Equal(t, fs.KindDir, baseAttr.Attr.Kind)
	linkedRepoB := filepath.Join(localRootB, "overlay", "linked")
	require.Equal(t, linkedHead, run(t, linkedRepoB, "rev-parse", "HEAD"))
	require.Equal(t, indexEntries, run(t, linkedRepoB, "ls-files", "--stage"))
	require.Equal(t, "staged payload", run(t, linkedRepoB, "show", ":staged.txt"))
	// Retrying hydration after the admin directory was installed must retain
	// any new local index content, even if the completion latch is still open.
	run(t, linkedRepoB, "rm", "--cached", "staged.txt")
	require.NoError(t, gB.restoreWorkspaceState(ctx, gB.workspaceByID("ws-linked")))
	require.NotContains(t, run(t, linkedRepoB, "ls-files"), "staged.txt")

}

// Dormant G0 invariant: .git-segment probes on a workspace-free root confirm
// dormancy via the index once and never issue ListGitWorkspaces (refresh=0).
func TestDormantDotGitLookupDoesNotListWorkspaces(t *testing.T) {
	ctx := context.Background()
	b := testbackend.NewBackend()
	var refreshes atomic.Int32
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")

	localRoot := t.TempDir()
	g, ch := buildLocalizeGitStack(t, cl, localRoot, func(_ bool) {
		refreshes.Add(1)
	})

	for range 5 {
		_, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/plain/.git"})
		require.Error(t, err)
	}
	require.Equal(t, int32(0), refreshes.Load())
	g.mu.RLock()
	confirmed := g.dormantConfirmed
	armed := g.armed
	g.mu.RUnlock()
	require.True(t, confirmed)
	require.False(t, armed)
}

// Foreground .git restore joins the same Stop barrier as background tasks:
// Stop cancels an in-flight foreground restore immediately and leaves zero
// active directory owners.
func TestForegroundDotGitRestoreCancelsOnStop(t *testing.T) {
	ctx := context.Background()
	b := testbackend.NewBackend()
	b.SeedGitWorkspaceFull("ws-stop", "/repo", "https://example.invalid/repo.git", "0123456789abcdef0123456789abcdef01234567")

	testDone := make(chan struct{})
	defer close(testDone)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	var once sync.Once

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/git-state") {
			once.Do(func() {
				close(entered)
				select {
				case <-r.Context().Done():
					close(canceled)
				case <-testDone:
				}
			})
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)

	cl := client.New(ts.URL, "")
	require.NoError(t, cl.UpsertGitWorkspaceIndexEntry(ctx, client.GitWorkspaceIndexEntry{
		WorkspaceID: "ws-stop",
		RootPath:    "/repo",
	}))

	localRoot := t.TempDir()
	g, ch := buildLocalizeGitStack(t, cl, localRoot, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = ch.Dispatch(ctx, &fs.Op{Kind: fs.OpGetAttr, Path: "/repo/.git"})
	}()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("foreground .git restore did not start")
	}

	stopCtx, stopCancel := context.WithTimeout(ctx, 2*time.Second)
	defer stopCancel()
	require.NoError(t, g.Stop(stopCtx))

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel in-flight foreground restore")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("foreground dispatch did not return after Stop")
	}
	require.ErrorIs(t, g.ensureWorkspaceState(ctx, &workspace{id: "ws-stop", root: "/repo"}), errGitRuntimeStopped)
}
