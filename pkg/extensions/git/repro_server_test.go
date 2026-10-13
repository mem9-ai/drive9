package git

// Temporary reproduction harness against a real drive9-server (not part of
// the suite; run with -tags ReproServer and a running server):
//
//	go test ./pkg/extensions/git/ -tags ReproServer -run TestReproServerCleanRead -v
//
// DRIVE9_REPRO_BASE defaults to http://127.0.0.1:19009.

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/bulkfile"
	"github.com/mem9-ai/drive9/pkg/extensions/cache"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/localize"
	"github.com/mem9-ai/drive9/pkg/extensions/renameguard"
	"github.com/mem9-ai/drive9/pkg/extensions/specials"
	"github.com/mem9-ai/drive9/pkg/extensions/sse"
	"github.com/mem9-ai/drive9/pkg/extensions/writeback"
	"github.com/mem9-ai/drive9/pkg/extensions/xattr"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func reproServerClient(t *testing.T) *client.Client {
	t.Helper()
	base := os.Getenv("DRIVE9_REPRO_BASE")
	if base == "" {
		base = "http://127.0.0.1:19009"
	}
	var out struct {
		APIKey string `json:"api_key"`
	}
	for i := 0; i < 30; i++ {
		resp, err := http.Post(base+"/v1/provision", "application/json", nil)
		if err != nil {
			t.Skipf("no server at %s: %v", base, err)
		}
		if resp.StatusCode == 200 || resp.StatusCode == 202 {
			if err := json.NewDecoder(resp.Body).Decode(&out); err == nil && out.APIKey != "" {
				_ = resp.Body.Close()
				break
			}
		}
		_ = resp.Body.Close()
		time.Sleep(2 * time.Second)
	}
	if out.APIKey == "" {
		t.Fatal("provision never returned an api key")
	}
	// Wait until the tenant is active.
	c := client.New(base, out.APIKey)
	for i := 0; i < 30; i++ {
		if st, err := c.StatCtx(context.Background(), "/"); err == nil && st != nil && st.IsDir {
			return c
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatal("tenant never became active")
	return nil
}

func TestReproServerCleanRead(t *testing.T) {
	cl := reproServerClient(t)
	ctx := context.Background()
	const commit = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

	// CLI side: register + tree index + tree content.
	ws, err := cl.GitWorkspaceFSUpsert(ctx, client.GitWorkspaceRequest{
		RootPath:   "/repo",
		RepoURL:    "file:///fixture.git",
		HeadCommit: commit,
		Mode:       "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := []client.GitTreeNode{
		{Path: "README.md", Kind: "file", Mode: "100644", SizeBytes: int64(len("Drive9 fixture"))},
	}
	if err := cl.GitWorkspaceFSPutTreeIndex(ctx, ws.WorkspaceID, commit, nodes); err != nil {
		t.Fatal(err)
	}
	if err := cl.GitWorkspaceFSPutTreeFile(ctx, ws.WorkspaceID, commit, "README.md", strings.NewReader("Drive9 fixture"), int64(len("Drive9 fixture"))); err != nil {
		t.Fatal(err)
	}

	// Server-side view of the store: read the tree body back directly.
	body, err := cl.ReadCtx(ctx, "/.drive9/git-workspaces/"+ws.WorkspaceID+"/tree/"+commit+"/README.md")
	if err != nil || string(body) != "Drive9 fixture" {
		t.Fatalf("direct store read = %q %v", body, err)
	}

	// Mount side: store tail = core with tenant-absolute paths, git on top.
	storeCore, err := core.New(cl, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	storeChain, err := fs.NewChain(storeCore)
	if err != nil {
		t.Fatal(err)
	}
	g, err := New(Options{Store: fs.HandlerFilesystem(storeChain.Dispatch)})
	if err != nil {
		t.Fatal(err)
	}
	ch, err := fs.NewChain(g, storeCore)
	if err != nil {
		t.Fatal(err)
	}
	g.Reload(ctx)

	attr, err := get(t, ch, "/repo/README.md")
	if err != nil {
		t.Fatalf("clean attr: %v", err)
	}
	if attr.Size != int64(len("Drive9 fixture")) {
		t.Fatalf("attr size = %d", attr.Size)
	}
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpRead, Path: "/repo/README.md"})
	if err != nil {
		t.Fatalf("clean read: %v", err)
	}
	if string(res.Data) != "Drive9 fixture" {
		t.Fatalf("clean read = %q", res.Data)
	}
	_ = time.Now
}

// TestReproServerFullChainRead reproduces the e2e read path through the
// full default extension stack (as MountWithConfig wires it) minus FUSE:
// read-only open of a clean tree file and a handle-level read.
func TestReproServerFullChainRead(t *testing.T) {
	cl := reproServerClient(t)
	ctx := context.Background()
	const commit = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	ws, err := cl.GitWorkspaceFSUpsert(ctx, client.GitWorkspaceRequest{
		RootPath: "/repo", RepoURL: "file:///fixture.git", HeadCommit: commit, Mode: "fast",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := []client.GitTreeNode{{Path: "README.md", Kind: "file", Mode: "100644", SizeBytes: int64(len("Drive9 fixture"))}}
	if err := cl.GitWorkspaceFSPutTreeIndex(ctx, ws.WorkspaceID, commit, nodes); err != nil {
		t.Fatal(err)
	}
	if err := cl.GitWorkspaceFSPutTreeFile(ctx, ws.WorkspaceID, commit, "README.md", strings.NewReader("Drive9 fixture"), int64(len("Drive9 fixture"))); err != nil {
		t.Fatal(err)
	}

	bus := fs.NewEventBus()
	storeCore, _ := core.New(cl, core.Options{})
	sseExt, _ := sse.New(sse.Options{Client: cl, Actor: "repro", RemoteRoot: "/", Events: bus})
	wb, _ := writeback.New(writeback.Options{Policy: writeback.PolicyWriteBack, SyncMode: "interactive", Client: cl, RemoteRoot: "/", Events: bus})
	xe, _ := xattr.New(xattr.Options{Events: bus})
	sp, _ := specials.New(specials.Options{Events: bus})
	bf, _ := bulkfile.New(bulkfile.Options{Client: cl, RemoteRoot: "/", Events: bus, Policy: "writeback"})
	lo, _ := localize.New(localize.Options{})
	rg, _ := renameguard.New(renameguard.Options{})
	rc, _ := cache.New(cache.Options{Events: bus, RemoteRoot: "/"})
	g, _ := New(Options{Store: nil, Events: bus, RemoteRoot: "/", Logger: nil})

	// The store tail mirrors the main chain below git, with the terminal
	// swapped for the tenant-absolute core.
	tailHandlers := []fs.Handler{rc, rg, lo, bf, sp, xe, wb, sseExt, storeCore}
	tailChain, _ := fs.NewChain(tailHandlers...)
	g.AttachStore(fs.HandlerFilesystem(tailChain.Dispatch))

	ch, err := fs.NewChain(g, rc, rg, lo, bf, sp, xe, wb, sseExt, storeCore)
	if err != nil {
		t.Fatal(err)
	}
	g.Reload(ctx)

	// e2e path: read-only open, then handle-level reads.
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/repo/README.md", Flags: uint32(syscall.O_RDONLY)})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if res.Handle == nil {
		t.Fatal("open returned no handle")
	}
	buf := make([]byte, 64)
	n, err := res.Handle.Read(ctx, 0, buf)
	if err != nil {
		t.Fatalf("handle read: %v", err)
	}
	if string(buf[:n]) != "Drive9 fixture" {
		t.Fatalf("handle read = %q", buf[:n])
	}
}

// TestReproServerE2EShape mirrors the on-demand smoke exactly: subtree
// mount (RemoteRoot=/e2e-root), workspace registered tenant-absolutely
// under it, localize enabled with a real LocalRoot and the coding-agent
// profile, pending marker for the clone window.
func TestReproServerE2EShape(t *testing.T) {
	cl := reproServerClient(t)
	ctx := context.Background()
	const commit = "cccccccccccccccccccccccccccccccccccccccc"
	const remoteRoot = "/e2e-root"
	ws, err := cl.GitWorkspaceFSUpsert(ctx, client.GitWorkspaceRequest{
		RootPath: remoteRoot + "/repo-main", RepoURL: "file:///fixture.git", HeadCommit: commit, Mode: "fast-blobless",
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := []client.GitTreeNode{{Path: "README.md", Kind: "file", Mode: "100644", SizeBytes: int64(len("Drive9 fixture"))}}
	if err := cl.GitWorkspaceFSPutTreeIndex(ctx, ws.WorkspaceID, commit, nodes); err != nil {
		t.Fatal(err)
	}
	if err := cl.GitWorkspaceFSPutTreeFile(ctx, ws.WorkspaceID, commit, "README.md", strings.NewReader("Drive9 fixture"), int64(len("Drive9 fixture"))); err != nil {
		t.Fatal(err)
	}

	localRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(localRoot, "overlay", "repo-main"), 0o755); err != nil {
		t.Fatal(err)
	}
	bus := fs.NewEventBus()
	storeCore, _ := core.New(cl, core.Options{})
	sseExt, _ := sse.New(sse.Options{Client: cl, Actor: "repro", RemoteRoot: remoteRoot, Events: bus})
	wb, _ := writeback.New(writeback.Options{Policy: writeback.PolicyWriteBack, SyncMode: "interactive", Client: cl, RemoteRoot: remoteRoot, Events: bus})
	xe, _ := xattr.New(xattr.Options{Events: bus})
	sp, _ := specials.New(specials.Options{Events: bus})
	bf, _ := bulkfile.New(bulkfile.Options{Client: cl, RemoteRoot: remoteRoot, Events: bus, Policy: "writeback"})
	g, gerr := New(Options{Events: bus, LocalRoot: localRoot, RemoteRoot: remoteRoot})
	if gerr != nil {
		t.Fatal(gerr)
	}
	lo, _ := localize.New(localize.Options{LocalRoot: localRoot, Profile: "coding-agent", ApplyBuiltinDefaults: true, DeclarativeLocal: g.RoutesLocal})
	rg, _ := renameguard.New(renameguard.Options{})
	rc, _ := cache.New(cache.Options{Events: bus, RemoteRoot: remoteRoot})

	tailHandlers := []fs.Handler{rc, rg, lo, bf, sp, xe, wb, sseExt, storeCore}
	tailChain, _ := fs.NewChain(tailHandlers...)
	g.AttachStore(fs.HandlerFilesystem(tailChain.Dispatch))

	ch, err := fs.NewChain(g, rc, rg, lo, bf, sp, xe, wb, sseExt, storeCore)
	if err != nil {
		t.Fatal(err)
	}
	g.Reload(ctx)

	// attr via the re-rooted view
	if _, err := get(t, ch, "/repo-main/README.md"); err != nil {
		t.Fatalf("re-rooted attr: %v", err)
	}
	// e2e read: read-only open then handle read
	res, err := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/repo-main/README.md", Flags: uint32(syscall.O_RDONLY) | 0x8000, Ino: 42})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	buf := make([]byte, 64)
	n, rerr := res.Handle.Read(ctx, 0, buf)
	if rerr != nil || string(buf[:n]) != "Drive9 fixture" {
		t.Fatalf("handle read = %q n=%d err=%v", buf[:n], n, rerr)
	}
	// listing the workspace root should include the clean tree entry
	list, lerr := ch.Dispatch(ctx, &fs.Op{Kind: fs.OpList, Path: "/repo-main"})
	if lerr != nil {
		t.Fatalf("list: %v", lerr)
	}
	found := false
	for _, e := range list.Entries {
		if e.Name == "README.md" {
			found = true
		}
	}
	if !found {
		t.Fatalf("listing missing README.md: %+v", list.Entries)
	}
}
