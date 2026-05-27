package cli

import (
	"os"
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
	pidPath, err := mountstate.WriteProcessState(mountPoint, mountstate.ProcessState{
		PID:        os.Getpid(),
		MountPoint: mountPoint,
		RemoteRoot: "/remote",
		Profile:    "coding-agent",
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
}
