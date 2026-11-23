//go:build failpoint && !windows

package fuse

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountstate"
)

func TestPrepareMountViewFailpointCrashAfterServerBindRecoversOnRestart(t *testing.T) {
	shortDir, err := os.MkdirTemp("/tmp", "d9fp")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
	t.Setenv("XDG_RUNTIME_DIR", shortDir)
	t.Setenv("HOME", shortDir)

	fb, srv := newFakeSwitchBackend(t)
	mp := filepath.Join(shortDir, "mnt")
	require.NoError(t, os.MkdirAll(mp, 0o755))

	cfg := MountConfig{
		Server:           srv.URL,
		APIKey:           "test-key-placeholder",
		MountPoint:       mp,
		LayerRef:         "L1",
		CacheDir:         filepath.Join(shortDir, "cache"),
		WritePolicy:      "writeback",
		SyncMode:         "strict",
		MemoryBudget:     64 << 20,
		MemoryBudgetMode: "enforce",
		Extensions:       []string{"core", "writeback", "cache"},
	}
	baseClient := client.New(srv.URL, cfg.APIKey)

	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/fuse/prepareMountViewAfterInitialServerBind", func(errp *error) {
		*errp = errors.New("simulated process crash after server BindLayerMount")
	}))
	_, err = prepareMountView(context.Background(), cfg, baseClient, "mount-fp-1")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/mem9-ai/drive9/pkg/fuse/prepareMountViewAfterInitialServerBind"))

	const l1 = "layer_01j00000000000000000000001"
	fb.mu.Lock()
	require.Equal(t, int64(1), fb.layers[l1].Info.Fence)
	require.Equal(t, "mount-fp-1", fb.layers[l1].OwnerMountID)
	fb.mu.Unlock()

	scope, err := mountstate.SwitchScope(cfg.Server, mountCredentialKind(&cfg)+":"+sha256HexString(mountCredentialSecret(&cfg)), "/", resolveCanonicalStateMountPoint(mp))
	require.NoError(t, err)
	oldJPath, err := mountstate.SwitchScopedJournalPath(scope, "mount-fp-1")
	require.NoError(t, err)
	_, err = os.Stat(oldJPath)
	require.NoError(t, err)

	// Restarting prepareMountView on the same mountpoint with a new actorID ("mount-fp-2")
	// drains "mount-fp-1"'s orphaned journal, releases Fence 1 -> 2, unlinks oldJPath, and binds Fence 3!
	mv, err := prepareMountView(context.Background(), cfg, baseClient, "mount-fp-2")
	require.NoError(t, err)

	_, err = os.Stat(oldJPath)
	require.True(t, os.IsNotExist(err))

	fb.mu.Lock()
	require.Equal(t, int64(3), fb.layers[l1].Info.Fence)
	require.Equal(t, "mount-fp-2", fb.layers[l1].OwnerMountID)
	fb.mu.Unlock()

	newJPath, err := mountstate.SwitchScopedJournalPath(scope, "mount-fp-2")
	require.NoError(t, err)
	require.NoError(t, mv.close(context.Background()))
	_, err = os.Stat(newJPath)
	require.True(t, os.IsNotExist(err))
}
