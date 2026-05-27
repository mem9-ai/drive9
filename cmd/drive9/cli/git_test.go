package cli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

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
