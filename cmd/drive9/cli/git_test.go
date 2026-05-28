package cli

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/pkg/client"
	"github.com/mem9-ai/dat9/pkg/mountstate"
)

func TestParseGitLsTree(t *testing.T) {
	raw := "" +
		"040000 tree 1111111111111111111111111111111111111111       -\tsrc\x00" +
		"100644 blob 2222222222222222222222222222222222222222      12\tsrc/main.go\x00" +
		"120000 blob 3333333333333333333333333333333333333333       6\tlink\x00" +
		"160000 commit 4444444444444444444444444444444444444444       -\tdeps/lib\x00"

	nodes, err := parseGitLsTree([]byte(raw))
	if err != nil {
		t.Fatalf("parseGitLsTree: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("len(nodes) = %d, want 4", len(nodes))
	}
	checks := []struct {
		i      int
		path   string
		parent string
		name   string
		kind   string
		size   int64
	}{
		{0, "src", "", "src", "dir", -1},
		{1, "src/main.go", "src", "main.go", "file", 12},
		{2, "link", "", "link", "symlink", 6},
		{3, "deps/lib", "deps", "lib", "submodule", -1},
	}
	for _, check := range checks {
		got := nodes[check.i]
		if got.Path != check.path || got.ParentPath != check.parent || got.Name != check.name || got.Kind != check.kind || got.SizeBytes != check.size {
			t.Fatalf("node[%d] = %+v, want path=%q parent=%q name=%q kind=%q size=%d", check.i, got, check.path, check.parent, check.name, check.kind, check.size)
		}
	}
}

func TestParseGitLsTreeWithoutSizes(t *testing.T) {
	raw := "" +
		"040000 tree 1111111111111111111111111111111111111111\tsrc\x00" +
		"100644 blob 2222222222222222222222222222222222222222\tsrc/main.go\x00" +
		"120000 blob 3333333333333333333333333333333333333333\tlink\x00" +
		"160000 commit 4444444444444444444444444444444444444444\tdeps/lib\x00"

	nodes, err := parseGitLsTree([]byte(raw))
	if err != nil {
		t.Fatalf("parseGitLsTree: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("len(nodes) = %d, want 4", len(nodes))
	}
	for i, node := range nodes {
		if node.SizeBytes != -1 {
			t.Fatalf("node[%d].SizeBytes = %d, want -1", i, node.SizeBytes)
		}
	}
}

func TestGitListTreeOmitsBlobSizes(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	root := t.TempDir()
	runTestGit(t, "", "init", "-b", "main", root)
	runTestGit(t, root, "config", "user.email", "drive9-test@example.invalid")
	runTestGit(t, root, "config", "user.name", "Drive9 Test")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	runTestGit(t, root, "add", ".")
	runTestGit(t, root, "commit", "-m", "initial")
	head := gitOutputForTest(t, root, "rev-parse", "HEAD")

	nodes, err := gitListTree(root, head)
	if err != nil {
		t.Fatalf("gitListTree: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("len(nodes) = %d, want 1", len(nodes))
	}
	if nodes[0].Path != "README.md" {
		t.Fatalf("node path = %q, want README.md", nodes[0].Path)
	}
	if nodes[0].SizeBytes != -1 {
		t.Fatalf("SizeBytes = %d, want -1", nodes[0].SizeBytes)
	}
}

func TestGitFastCloneArgs(t *testing.T) {
	full := gitFastCloneArgs("https://example.test/repo.git", "/mnt/repo", false)
	if got, want := strings.Join(full, " "), "clone --no-checkout https://example.test/repo.git /mnt/repo"; got != want {
		t.Fatalf("full clone args = %q, want %q", got, want)
	}
	blobless := gitFastCloneArgs("https://example.test/repo.git", "/mnt/repo", true)
	if got, want := strings.Join(blobless, " "), "clone --filter=blob:none --no-checkout https://example.test/repo.git /mnt/repo"; got != want {
		t.Fatalf("blobless clone args = %q, want %q", got, want)
	}
}

func TestGitHubTreeSizeEnrichment(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/mem9-ai/drive9/git/trees/tree-sha" {
			t.Fatalf("path = %q, want /repos/mem9-ai/drive9/git/trees/tree-sha", r.URL.Path)
		}
		if r.URL.Query().Get("recursive") != "1" {
			t.Fatalf("recursive = %q, want 1", r.URL.Query().Get("recursive"))
		}
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewEncoder(w).Encode(githubTreeResponse{Tree: []githubTreeEntry{
			{Path: "README.md", Type: "blob", Size: int64Ptr(6)},
			{Path: "src", Type: "tree"},
			{Path: "src/main.go", Type: "blob", Size: int64Ptr(12)},
		}})
	}))
	defer srv.Close()

	sizes, err := fetchGitHubTreeSizes(context.Background(), srv.Client(), srv.URL, githubRepoRef{Owner: "mem9-ai", Repo: "drive9"}, "tree-sha", "secret")
	if err != nil {
		t.Fatalf("fetchGitHubTreeSizes: %v", err)
	}
	if gotAuth != "Bearer secret" {
		t.Fatalf("Authorization = %q, want Bearer secret", gotAuth)
	}
	nodes := applyGitHubTreeSizes([]client.GitTreeNode{
		{Path: "README.md", Kind: "file", SizeBytes: -1},
		{Path: "src", Kind: "dir", SizeBytes: -1},
		{Path: "src/main.go", Kind: "file", SizeBytes: -1},
	}, sizes)
	if nodes[0].SizeBytes != 6 {
		t.Fatalf("README size = %d, want 6", nodes[0].SizeBytes)
	}
	if nodes[1].SizeBytes != -1 {
		t.Fatalf("dir size = %d, want -1", nodes[1].SizeBytes)
	}
	if nodes[2].SizeBytes != 12 {
		t.Fatalf("src/main.go size = %d, want 12", nodes[2].SizeBytes)
	}
}

func TestGitHubTreeSizeEnrichmentWalksTruncatedTree(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/repos/mem9-ai/drive9/git/trees/root-sha" && r.URL.Query().Get("recursive") == "1":
			_ = json.NewEncoder(w).Encode(githubTreeResponse{Truncated: true})
		case r.URL.Path == "/repos/mem9-ai/drive9/git/trees/root-sha":
			_ = json.NewEncoder(w).Encode(githubTreeResponse{Tree: []githubTreeEntry{
				{Path: "README.md", Type: "blob", Size: int64Ptr(6)},
				{Path: "src", Type: "tree", SHA: "src-sha"},
			}})
		case r.URL.Path == "/repos/mem9-ai/drive9/git/trees/src-sha":
			_ = json.NewEncoder(w).Encode(githubTreeResponse{Tree: []githubTreeEntry{
				{Path: "main.go", Type: "blob", Size: int64Ptr(12)},
			}})
		default:
			t.Fatalf("unexpected request path=%q query=%q", r.URL.Path, r.URL.RawQuery)
		}
	}))
	defer srv.Close()

	sizes, err := fetchGitHubTreeSizes(context.Background(), srv.Client(), srv.URL, githubRepoRef{Owner: "mem9-ai", Repo: "drive9"}, "root-sha", "")
	if err != nil {
		t.Fatalf("fetchGitHubTreeSizes: %v", err)
	}
	if sizes["README.md"] != 6 {
		t.Fatalf("README.md size = %d, want 6", sizes["README.md"])
	}
	if sizes["src/main.go"] != 12 {
		t.Fatalf("src/main.go size = %d, want 12", sizes["src/main.go"])
	}
}

func TestParseGitHubRepoURL(t *testing.T) {
	for _, raw := range []string{
		"https://github.com/mem9-ai/drive9.git",
		"git@github.com:mem9-ai/drive9.git",
		"ssh://git@github.com/mem9-ai/drive9.git",
	} {
		ref, ok := parseGitHubRepoURL(raw)
		if !ok {
			t.Fatalf("parseGitHubRepoURL(%q) ok = false, want true", raw)
		}
		if ref.Owner != "mem9-ai" || ref.Repo != "drive9" {
			t.Fatalf("parseGitHubRepoURL(%q) = %+v, want mem9-ai/drive9", raw, ref)
		}
	}
	if _, ok := parseGitHubRepoURL("https://example.com/mem9-ai/drive9.git"); ok {
		t.Fatalf("non-GitHub URL parsed as GitHub")
	}
}

func TestArchiveGitStateDirSkipsObjectDatabases(t *testing.T) {
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

	state, err := archiveGitStateDir(gitDir)
	if err != nil {
		t.Fatalf("archiveGitStateDir: %v", err)
	}
	names := gitArchiveNames(t, state)
	if !names["config"] {
		t.Fatalf("config missing from objectless archive")
	}
	for name := range names {
		if name == "objects" || strings.HasPrefix(name, "objects/") || strings.Contains(name, "/objects/") {
			t.Fatalf("object database path %q included in objectless archive", name)
		}
	}
}

func TestResolveMountedGitTargetUsesMountMetadata(t *testing.T) {
	mountPoint := t.TempDir()
	localRoot := t.TempDir()
	pidPath, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID:        os.Getpid(),
		MountPoint: mountPoint,
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
		LocalRoot:  localRoot,
	})
	if err != nil {
		t.Fatalf("WriteProcessState: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(pidPath) })

	target := filepath.Join(mountPoint, "repos", "drive9")
	resolved, err := resolveMountedGitTarget(target)
	if err != nil {
		t.Fatalf("resolveMountedGitTarget: %v", err)
	}
	if resolved.RemotePath != "/remote/repos/drive9/" {
		t.Fatalf("RemotePath = %q, want /remote/repos/drive9/", resolved.RemotePath)
	}
	if resolved.RemoteRoot != "/remote" {
		t.Fatalf("RemoteRoot = %q, want /remote", resolved.RemoteRoot)
	}
	wantLocalGitDir := filepath.Join(localRoot, "overlay", "repos", "drive9", ".git")
	if resolved.LocalGitDir != wantLocalGitDir {
		t.Fatalf("LocalGitDir = %q, want %q", resolved.LocalGitDir, wantLocalGitDir)
	}
}

func TestInitializeFastCloneIndexMakesStatusClean(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not found")
	}
	root := t.TempDir()
	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")
	if err := os.Mkdir(src, 0o755); err != nil {
		t.Fatalf("mkdir src: %v", err)
	}
	runTestGit(t, "", "init", "-b", "main", src)
	runTestGit(t, src, "config", "user.email", "drive9-test@example.invalid")
	runTestGit(t, src, "config", "user.name", "Drive9 Test")
	if err := os.WriteFile(filepath.Join(src, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write README: %v", err)
	}
	if err := os.WriteFile(filepath.Join(src, "index.html"), []byte("<h1>hi</h1>\n"), 0o644); err != nil {
		t.Fatalf("write index: %v", err)
	}
	runTestGit(t, src, "add", ".")
	runTestGit(t, src, "commit", "-m", "initial")
	head := gitOutputForTest(t, src, "rev-parse", "HEAD")

	runTestGit(t, "", "clone", "--no-checkout", src, dst)
	if err := initializeFastCloneIndex(dst, head); err != nil {
		t.Fatalf("initializeFastCloneIndex: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dst, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("write virtual README: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dst, "index.html"), []byte("<h1>hi</h1>\n"), 0o644); err != nil {
		t.Fatalf("write virtual index: %v", err)
	}

	if got := gitOutputForTest(t, dst, "status", "--porcelain=v1"); got != "" {
		t.Fatalf("status = %q, want clean", got)
	}
}

func runTestGit(t *testing.T, dir string, args ...string) {
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

func gitOutputForTest(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return string(bytes.TrimSpace(out))
}

func int64Ptr(v int64) *int64 {
	return &v
}

func gitArchiveNames(t *testing.T, content []byte) map[string]bool {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("gzip.NewReader: %v", err)
	}
	defer func() { _ = gz.Close() }()
	tr := tar.NewReader(gz)
	names := make(map[string]bool)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return names
		}
		if err != nil {
			t.Fatalf("tar.Next: %v", err)
		}
		names[hdr.Name] = true
	}
}
