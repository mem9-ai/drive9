package git

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
	"github.com/mem9-ai/drive9/pkg/gitcache"
	"github.com/stretchr/testify/require"
)

func TestCleanGitHandleRetainsBlobAfterHeadRefresh(t *testing.T) {
	root := t.TempDir()
	w := &workspace{id: root, root: "/repo"}
	g := &Git{localRoot: root, loaded: true, workspaces: []*workspace{w}}
	install := func(head, sha, data string) {
		t.Helper()
		w.headCommit, w.treeLoaded = head, head
		w.tree = map[string]client.GitTreeNode{"readme": {Kind: "file", ObjectSHA: sha, SizeBytes: -1}}
		dir := gitcache.TreeRoot(root, w.id, head)
		require.NoError(t, os.MkdirAll(dir, 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "readme"), []byte(data), 0644))
	}
	open := func() *gitHandle {
		t.Helper()
		res, err := g.Do(t.Context(), &fs.Op{Kind: fs.OpCreate, Path: "/repo/readme"}, nil)
		require.NoError(t, err)
		return res.Handle.(*gitHandle)
	}
	read := func(h *gitHandle) string {
		t.Helper()
		buf := make([]byte, 32)
		n, err := h.Read(t.Context(), 0, buf)
		require.NoError(t, err)
		return string(buf[:n])
	}

	install("old-head", strings.Repeat("1", 40), "old")
	old := open()
	install("new-head", strings.Repeat("2", 40), "replacement")
	require.EqualValues(t, 3, old.attr(t.Context()).Size)
	require.Equal(t, "old", read(old))
	require.EqualValues(t, -1, w.tree["readme"].SizeBytes)
	current := open()
	require.EqualValues(t, 11, current.attr(t.Context()).Size)
	require.Equal(t, "replacement", read(current))
}

func TestLateGitTreeRefreshLeavesNewHeadVisible(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		head := r.URL.Query().Get("commit_sha")
		var sha string
		switch head {
		case "A":
			close(started)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			sha = strings.Repeat("a", 40)
		case "B":
			sha = strings.Repeat("b", 40)
		default:
			http.NotFound(w, r)
			return
		}
		node := client.GitTreeNode{Path: "from-" + head, Kind: "file", ObjectSHA: sha, SizeBytes: -1}
		if err := json.NewEncoder(w).Encode(map[string][]client.GitTreeNode{"nodes": {node}}); err != nil {
			t.Errorf("serve tree %s: %v", head, err)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	root := t.TempDir()
	w := &workspace{id: root, root: "/repo", headCommit: "A"}
	g := &Git{localRoot: root, cl: client.New(server.URL, ""), loaded: true, workspaces: []*workspace{w}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		g.refreshTree(ctx, w)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first tree request did not reach server")
	}
	w.mu.Lock()
	w.headCommit = "B"
	w.mu.Unlock()
	g.refreshTree(ctx, w)
	close(release)
	<-done

	dir := gitcache.TreeRoot(root, w.id, "B")
	require.NoError(t, os.MkdirAll(dir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "from-B"), []byte("new tree"), 0644))
	res, err := g.Do(ctx, &fs.Op{Kind: fs.OpCreate, Path: "/repo/from-B"}, func(context.Context, *fs.Op) (fs.Result, error) {
		return fs.Result{}, fs.ErrNotFound
	})
	require.NoError(t, err)
	require.EqualValues(t, 8, res.Attr.Size)
	buf := make([]byte, 16)
	n, err := res.Handle.(*gitHandle).Read(ctx, 0, buf)
	require.NoError(t, err)
	require.Equal(t, "new tree", string(buf[:n]))
}
