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

func TestPrepareMountViewDirSyncFailureAndPreBindCrashRecovery(t *testing.T) {
	shortDir, err := os.MkdirTemp("/tmp", "d9fpds")
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
	const l1 = "layer_01j00000000000000000000001"
	scope, err := mountstate.SwitchScope(cfg.Server, mountCredentialKind(&cfg)+":"+sha256HexString(mountCredentialSecret(&cfg)), "/", resolveCanonicalStateMountPoint(mp))
	require.NoError(t, err)

	// 1. Parent directory fsync failure during journal publish MUST fail closed
	// without issuing BindLayerMount on the server!
	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync", func(errp *error) {
		*errp = errors.New("simulated parent dir fsync failure")
	}))
	_, err = prepareMountView(context.Background(), cfg, baseClient, "mount-dirfail-1")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync"))

	fb.mu.Lock()
	require.Equal(t, 0, fb.bindCalls, "server BindLayerMount must never run when journal parent dir sync fails")
	require.Equal(t, int64(0), fb.layers[l1].Info.Fence)
	fb.mu.Unlock()
	failJPath, err := mountstate.SwitchScopedJournalPath(scope, "mount-dirfail-1")
	require.NoError(t, err)
	_, err = os.Stat(failJPath)
	require.True(t, os.IsNotExist(err))

	// Track parent directory sync count across publish and cleanup recovery steps.
	dirSyncCount := 0
	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync", func(_ *error) {
		dirSyncCount++
	}))
	defer func() {
		_ = failpoint.Disable("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync")
	}()

	// 2. Crash AFTER journal publish + parent dir fsync, BEFORE bind_intent & BindLayerMount:
	// restart cleans up the published journal (with dir sync) and binds Fence = 1.
	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/fuse/prepareMountViewAfterJournalOpen", func(errp *error) {
		*errp = errors.New("simulated crash after journal publish before bind_intent")
	}))
	_, err = prepareMountView(context.Background(), cfg, baseClient, "mount-preintent-1")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/mem9-ai/drive9/pkg/fuse/prepareMountViewAfterJournalOpen"))
	require.Equal(t, 1, dirSyncCount, "publish must have synced parent directory before returning")
	fb.mu.Lock()
	require.Equal(t, 0, fb.bindCalls)
	fb.mu.Unlock()

	preIntentJPath, err := mountstate.SwitchScopedJournalPath(scope, "mount-preintent-1")
	require.NoError(t, err)
	_, err = os.Stat(preIntentJPath)
	require.NoError(t, err)

	mv1, err := prepareMountView(context.Background(), cfg, baseClient, "mount-preintent-2")
	require.NoError(t, err)
	_, err = os.Stat(preIntentJPath)
	require.True(t, os.IsNotExist(err), "DrainPriorScopeJournals must unlink published pre-intent journal")
	require.GreaterOrEqual(t, dirSyncCount, 3, "new journal publish and stale journal unlink must both sync parent directory")
	fb.mu.Lock()
	require.Equal(t, int64(1), fb.layers[l1].Info.Fence)
	require.Equal(t, "mount-preintent-2", fb.layers[l1].OwnerMountID)
	fb.mu.Unlock()
	require.NoError(t, mv1.close(context.Background()))
	fb.mu.Lock()
	require.Equal(t, int64(2), fb.layers[l1].Info.Fence)
	fb.mu.Unlock()

	// 3. Crash AFTER journal publish + bind_intent fsync, BEFORE BindLayerMount:
	// restart drains UnbindKindOp(Fence=2) -> advances server Fence 2 -> 4 (fencing off
	// any delayed in-flight ExpectedFence=2 packet!), unlinks stale journal, and binds Fence = 5!
	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/fuse/prepareMountViewBeforeInitialServerBind", func(errp *error) {
		*errp = errors.New("simulated crash after bind_intent before BindLayerMount")
	}))
	_, err = prepareMountView(context.Background(), cfg, baseClient, "mount-prebind-1")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/mem9-ai/drive9/pkg/fuse/prepareMountViewBeforeInitialServerBind"))

	preBindJPath, err := mountstate.SwitchScopedJournalPath(scope, "mount-prebind-1")
	require.NoError(t, err)
	_, err = os.Stat(preBindJPath)
	require.NoError(t, err)
	fb.mu.Lock()
	require.Equal(t, int64(2), fb.layers[l1].Info.Fence, "server bind did not execute before crash")
	fb.mu.Unlock()

	mv2, err := prepareMountView(context.Background(), cfg, baseClient, "mount-prebind-2")
	require.NoError(t, err)
	_, err = os.Stat(preBindJPath)
	require.True(t, os.IsNotExist(err), "DrainPriorScopeJournals must drain and unlink pre-bind journal")

	// A delayed in-flight BindLayerMount(ExpectedFence=2) from mount-prebind-1 is now fenced off!
	_, lateErr := baseClient.BindLayerMount(context.Background(), l1, client.LayerMountBindRequest{
		MountID: "mount-prebind-1", OperationID: "late_op", ExpectedFence: 2,
	})
	require.Error(t, lateErr)

	fb.mu.Lock()
	require.Equal(t, int64(5), fb.layers[l1].Info.Fence)
	require.Equal(t, "mount-prebind-2", fb.layers[l1].OwnerMountID)
	fb.mu.Unlock()
	require.NoError(t, mv2.close(context.Background()))
}
