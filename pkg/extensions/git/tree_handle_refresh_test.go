package git

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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
