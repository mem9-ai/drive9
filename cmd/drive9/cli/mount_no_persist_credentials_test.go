package cli

import (
	"strings"
	"testing"
)

func TestMountNoPersistCredentialsPropagatesToFuse(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	stubMountProfileAppendLogProbe(t)
	oldMountFuse := mountFuse
	t.Cleanup(func() { mountFuse = oldMountFuse })

	var got *mountFuseOptions
	mountFuse = func(opts *mountFuseOptions) error {
		copied := *opts
		got = &copied
		return nil
	}
	if err := fsMountCmd([]string{
		"--mode=fuse", "--server=https://drive9.example", "--api-key=sk-runtime-secret",
		"--profile=none", "--no-persist-credentials", t.TempDir(),
	}); err != nil {
		t.Fatalf("fsMountCmd: %v", err)
	}
	if got == nil || !got.OmitProcessStateCredentials {
		t.Fatalf("mount options = %#v, want omitted process-state credentials", got)
	}
}

func TestMountNoPersistCredentialsRejectsSupervisor(t *testing.T) {
	resetCredentialCacheForTest()
	t.Cleanup(resetCredentialCacheForTest)
	err := MountCmd([]string{
		"--mode=fuse", "--server=https://drive9.example", "--api-key=sk-runtime-secret",
		"--profile=none", "--no-persist-credentials", t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "requires --foreground or --no-supervise") {
		t.Fatalf("error = %v", err)
	}
}
