//go:build !windows

package fuse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/extent"
	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/fusecomp/perf"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountRuntimeDeterministicCachePathsAcrossSwitches(t *testing.T) {
	cfg := MountConfig{
		Server:      "https://drive9.test",
		APIKey:      "test-key-placeholder",
		MountPoint:  "/tmp/mnt-det",
		RemoteRoot:  "/repo",
		CacheDir:    filepath.Join(t.TempDir(), "cache-root"),
		WritePolicy: "writeback",
	}

	mainWriteA, mainReadA, err := mountRuntimeCacheHashes(cfg, "/repo", mountstate.SwitchBinding{})
	require.NoError(t, err)
	mainWriteB, mainReadB, err := mountRuntimeCacheHashes(cfg, "/repo", mountstate.SwitchBinding{})
	require.NoError(t, err)
	require.Equal(t, mainWriteA, mainWriteB)
	require.Equal(t, mainReadA, mainReadB)
	require.Equal(t, MountHash(cfg.Server, cfg.MountPoint, "/repo"), mainWriteA)

	const l1 = "layer_01j00000000000000000000001"
	layerWriteA, layerReadA, err := mountRuntimeCacheHashes(cfg, "/repo", mountstate.SwitchBinding{LayerID: l1, Fence: 1})
	require.NoError(t, err)
	layerWriteB, layerReadB, err := mountRuntimeCacheHashes(cfg, "/repo", mountstate.SwitchBinding{LayerID: l1, Fence: 9})
	require.NoError(t, err)
	require.Equal(t, layerWriteA, layerWriteB)
	require.Equal(t, layerReadA, layerReadB)
	require.NotEqual(t, mainWriteA, layerWriteA)
	require.NotEqual(t, mainReadA, layerReadA)
	require.Equal(t, MountLayerHash(cfg.Server, cfg.MountPoint, "/repo", l1), layerWriteA)

	sw, _, _, _ := viewSwitchFixture(t)
	ctrl := startViewControlFixture(t, sw)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		toL1, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_l1_" + string(rune('a'+i)), ExpectedGeneration: uint64(1 + i*2), Layer: "L1",
		})
		require.NoError(t, err)
		require.Equal(t, uint64(2+i*2), toL1.Receipt.Generation)
		require.Eventually(t, func() bool { return len(sw.journal.Snapshot().PendingUnbinds) == 0 }, 2*time.Second, 10*time.Millisecond)

		toMain, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_main_" + string(rune('a'+i)), ExpectedGeneration: uint64(2 + i*2), Layer: "main",
		})
		require.NoError(t, err)
		require.Equal(t, uint64(3+i*2), toMain.Receipt.Generation)
		require.Eventually(t, func() bool { return len(sw.journal.Snapshot().PendingUnbinds) == 0 }, 2*time.Second, 10*time.Millisecond)
	}

	entries, err := os.ReadDir(sw.cfg.CacheDir)
	require.NoError(t, err)
	require.LessOrEqual(t, len(entries), 4)
}

func TestMountViewSwitchSelfFencedPendingLeaseAndControlResponsiveness(t *testing.T) {
	sw, fb, _, _ := viewSwitchFixture(t)
	ctrl := startViewControlFixture(t, sw)
	ctx := context.Background()

	const l1 = "layer_01j00000000000000000000001"
	var failFirstBuild atomic.Bool
	failFirstBuild.Store(true)
	origBuild := sw.buildRuntime
	sw.mu.Lock()
	sw.buildRuntime = func(bctx context.Context, target mountstate.SwitchBinding, lease *layerMountBinding) (*mountRuntime, error) {
		if failFirstBuild.CompareAndSwap(true, false) {
			return nil, errors.New("transient runtime build failure")
		}
		return origBuild(bctx, target, lease)
	}
	sw.mu.Unlock()

	_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_selffence_retry", ExpectedGeneration: 1, Layer: "L1",
	})
	var swErr *mountcontrol.SwitchError
	require.ErrorAs(t, err, &swErr)
	require.True(t, swErr.Unknown)
	require.Equal(t, mountcontrol.CodeSwitchRecovering, swErr.Code)

	sw.mu.Lock()
	require.NotNil(t, sw.pendingLease)
	sw.pendingLease.triggerSelfFence("test self-fence")
	sw.mu.Unlock()

	stRecovering, err := mountcontrol.RequestStatus(ctx, ctrl.SocketPath(), time.Second)
	require.NoError(t, err)
	require.True(t, stRecovering.Recovering)
	require.Equal(t, "/", stRecovering.RemoteRoot)

	retryResp, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_selffence_retry", ExpectedGeneration: 1, Layer: "L1",
	})
	require.NoError(t, err)
	require.Equal(t, mountstate.SwitchDone, retryResp.Receipt.State)
	require.Equal(t, uint64(2), retryResp.Receipt.Generation)

	cur := sw.currentRuntime()
	require.NotNil(t, cur.layerLease)
	require.NoError(t, cur.layerLease.ViewClient.WriteCtx(ctx, "/after_rebuild.txt", []byte("ok")))

	unbindEntered := make(chan struct{})
	releaseUnbind := make(chan struct{})
	fb.mu.Lock()
	fb.onUnbind = func(layerID string, _ client.LayerMountUnbindRequest) {
		if layerID == l1 {
			select {
			case <-unbindEntered:
			default:
				close(unbindEntered)
			}
			<-releaseUnbind
		}
	}
	fb.mu.Unlock()

	toMain, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_slow_unbind", ExpectedGeneration: 2, Layer: "main",
	})
	require.NoError(t, err)
	require.Equal(t, mountstate.SwitchDone, toMain.Receipt.State)

	select {
	case <-unbindEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("async unbind did not start")
	}

	statusCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	liveStatus, err := mountcontrol.RequestStatus(statusCtx, ctrl.SocketPath(), 250*time.Millisecond)
	require.NoError(t, err)
	require.True(t, liveStatus.OK)
	require.Equal(t, uint64(3), liveStatus.Generation)

	mv := &mountView{switcher: sw, remoteRoot: "/"}
	mv.ready.Store(true)
	closeDone := make(chan error, 1)
	go func() { closeDone <- mv.close(ctx) }()
	close(releaseUnbind)
	require.NoError(t, <-closeDone)
}

func TestMountViewSwitchUncertainStatusAndRecoveringPausedGuard(t *testing.T) {
	t.Run("uncertain_status_maps_to_recovering_not_unsupported", func(t *testing.T) {
		shortDir, err := os.MkdirTemp("/tmp", "d9unc")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
		sockPath := filepath.Join(shortDir, "ctrl.sock")
		ln, err := net.Listen("unix", sockPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			for {
				conn, aerr := ln.Accept()
				if aerr != nil {
					return
				}
				go func(c net.Conn) {
					defer func() { _ = c.Close() }()
					var req mountcontrol.DrainRequest
					if json.NewDecoder(bufio.NewReader(c)).Decode(&req) != nil {
						return
					}
					_ = json.NewEncoder(c).Encode(mountcontrol.StatusResponse{
						OK:         false,
						MountPoint: "/tmp/mnt-uncertain",
						RemoteRoot: "/",
						ViewSwitch: true,
						Recovering: true,
						MountID:    "mount-1",
						Generation: 2,
						Error:      mountstate.ErrSwitchUncertain.Error(),
					})
				}(conn)
			}
		}()

		ctx := context.Background()
		_, err = mountcontrol.RequestSwitch(ctx, sockPath, mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_uncertain", ExpectedGeneration: 2, Layer: "L1",
		})
		var swErr *mountcontrol.SwitchError
		require.ErrorAs(t, err, &swErr)
		require.True(t, swErr.Unknown)
		require.Equal(t, mountcontrol.CodeSwitchRecovering, swErr.Code)

		_, err = mountcontrol.QuerySwitch(ctx, sockPath, mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_uncertain", ExpectedGeneration: 2, Layer: "L1",
		})
		require.ErrorAs(t, err, &swErr)
		require.True(t, swErr.Unknown)
		require.Equal(t, mountcontrol.CodeSwitchRecovering, swErr.Code)
	})

	t.Run("recover_replay_and_recovering_paused_gate", func(t *testing.T) {
		sw, fb, _, _ := viewSwitchFixture(t)
		ctrl := startViewControlFixture(t, sw)
		ctx := context.Background()
		const l1 = "layer_01j00000000000000000000001"

		var failBuild atomic.Bool
		failBuild.Store(true)
		origBuild := sw.buildRuntime
		sw.mu.Lock()
		sw.buildRuntime = func(bctx context.Context, target mountstate.SwitchBinding, lease *layerMountBinding) (*mountRuntime, error) {
			if failBuild.Load() {
				return nil, errors.New("runtime build stalled")
			}
			return origBuild(bctx, target, lease)
		}
		sw.mu.Unlock()

		var swErr *mountcontrol.SwitchError
		_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_revoked_target", ExpectedGeneration: 1, Layer: "L1",
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, mountcontrol.CodeSwitchRecovering, swErr.Code)

		fb.mu.Lock()
		savedL1 := *fb.layers[l1]
		delete(fb.layers, l1)
		fb.mu.Unlock()

		checkConflicts := func() {
			t.Helper()
			for _, ref := range []string{"layer_01j00000000000000000000002", "L2", "missing", "main"} {
				_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
					MountID: "mount-1", OperationID: "op_revoked_target", ExpectedGeneration: 1, Layer: ref,
				})
				var conflict *mountcontrol.SwitchError
				require.ErrorAs(t, err, &conflict)
				require.Equal(t, viewprotocol.CodeConflict, conflict.Code, ref)
			}
		}
		checkConflicts()

		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_revoked_target", ExpectedGeneration: 1, Layer: "L1",
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, mountcontrol.CodeSwitchTargetRevoked, swErr.Code)

		checkConflicts()
		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_revoked_target", ExpectedGeneration: 1, Layer: l1,
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, mountcontrol.CodeSwitchTargetRevoked, swErr.Code)

		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_revoked_target", ExpectedGeneration: 1, Layer: l1, Recover: true,
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, viewprotocol.CodeConflict, swErr.Code)

		fb.mu.Lock()
		fb.layers[l1] = &savedL1
		fb.mu.Unlock()
		_, err = mountcontrol.QuerySwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_revoked_target", ExpectedGeneration: 1, Layer: "L1",
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, mountcontrol.CodeSwitchTargetRevoked, swErr.Code)

		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_recover_retry", ExpectedGeneration: 2, Layer: "main", Recover: true,
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, mountcontrol.CodeSwitchRecovering, swErr.Code)

		stPaused, err := mountcontrol.RequestStatus(ctx, ctrl.SocketPath(), time.Second)
		require.NoError(t, err)
		require.True(t, stPaused.Recovering)

		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_ordinary_blocked", ExpectedGeneration: 2, Layer: "L1",
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, mountcontrol.CodeSwitchTargetRevoked, swErr.Code)

		failBuild.Store(false)
		recDone, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_recover_retry", ExpectedGeneration: 2, Layer: "main", Recover: true,
		})
		require.NoError(t, err)
		require.Equal(t, mountstate.SwitchDone, recDone.Receipt.State)
		require.False(t, sw.recoveringPaused.Load())

		stRecovered, err := mountcontrol.RequestStatus(ctx, ctrl.SocketPath(), time.Second)
		require.NoError(t, err)
		require.False(t, stRecovered.Recovering)
	})

	t.Run("retry_after_pending_target_built_keeps_shared_extent_engine_alive", func(t *testing.T) {
		sw, _, _, mb := viewSwitchFixture(t)
		extCfg := sw.cfg
		extCfg.Extensions = []string{"core", "writeback", "cache", "extent"}
		counters := perf.NewCounters(false)
		var sharedExt *extent.Engine
		initRT, err := buildViewRuntime(extCfg, sw.baseClient, "/", "mount-1", mountstate.SwitchBinding{}, nil, mb, counters, &sharedExt)
		require.NoError(t, err)
		initRT.fs.Init2(context.Background())
		t.Cleanup(func() { _ = initRT.stop(context.Background()) })
		require.NotNil(t, sharedExt)

		sw.mu.Lock()
		prevRT := sw.current
		sw.cfg = extCfg
		sw.current = initRT
		sw.adapter.inner = initRT.fs
		sw.adapter.selected.Store(&viewRequestTarget{fs: initRT.fs, generation: 1})
		sw.buildRuntime = func(_ context.Context, target mountstate.SwitchBinding, lease *layerMountBinding) (*mountRuntime, error) {
			vc := sw.baseClient
			if target.LayerID != "" && lease != nil && lease.ViewClient != nil {
				vc = lease.ViewClient
			}
			return buildViewRuntime(extCfg, vc, "/", "mount-1", target, lease, mb, counters, &sharedExt)
		}
		sw.mu.Unlock()
		_ = prevRT.stop(context.Background())

		ctrl := startViewControlFixture(t, sw)
		ctx := context.Background()

		var fenceCalls atomic.Int32
		origFence := sw.fence
		sw.mu.Lock()
		sw.fence = func(fctx context.Context) error {
			if fenceCalls.Add(1) == 2 {
				return errors.New("injected post-build root fence interruption")
			}
			if origFence != nil {
				return origFence(fctx)
			}
			return nil
		}
		sw.mu.Unlock()

		var swErr *mountcontrol.SwitchError
		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_shared_extent_retry", ExpectedGeneration: 1, Layer: "L1",
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, mountcontrol.CodeSwitchRecovering, swErr.Code)

		sw.mu.Lock()
		require.True(t, sw.outgoingStopped)
		require.NotNil(t, sw.pendingTarget)
		origOutgoing := sw.current
		origOutgoing.stopped = false
		sw.mu.Unlock()
		require.NotNil(t, origOutgoing)

		doneResp, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_shared_extent_retry", ExpectedGeneration: 1, Layer: "L1",
		})
		require.NoError(t, err)
		require.Equal(t, mountstate.SwitchDone, doneResp.Receipt.State)
		require.False(t, origOutgoing.stopped)

		var sliceID uint64
		require.Zero(t, sharedExt.Bridge.NewSlice(nil, &sliceID))
		require.NotZero(t, sliceID)
		rc, gerr := sharedExt.Bridge.Storage().Get(ctx, "0_0_1", 0, 1)
		if rc != nil {
			_ = rc.Close()
		}
		require.NotErrorIs(t, gerr, context.Canceled)
	})
}

func TestInodeBudgetSymmetryStartupClassificationAndLayerGitGate(t *testing.T) {
	mb := membudget.New(64<<20, membudget.ModeEnforce, nil)
	tbl := NewInodeToPath()
	tbl.SetMemoryBudget(mb)
	require.Equal(t, int64(inodeTableEntryBytes), mb.Snapshot().Classes[membudget.ClassInodeTable])
	ino := tbl.Lookup("/file.txt", false, 1, time.Time{})
	require.Equal(t, int64(2*inodeTableEntryBytes), mb.Snapshot().Classes[membudget.ClassInodeTable])
	tbl.Forget(ino, 1)
	tbl.ReleaseMemoryBudget()
	require.Zero(t, mb.Snapshot().Classes[membudget.ClassInodeTable])

	permCorrupt := ClassifyLayerStartupError("drain prior journal", mountstate.ErrSwitchCorrupt)
	assertMountExit(t, permCorrupt, ExitStartupPermanent, ExitReasonStartupPermanent)
	permInvalid := ClassifyLayerStartupError("validate journal", mountstate.ErrSwitchInvalid)
	assertMountExit(t, permInvalid, ExitStartupPermanent, ExitReasonStartupPermanent)

	_, srv := newFakeSwitchBackend(t)
	shortDir, err := os.MkdirTemp("/tmp", "d9git")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
	cfg := MountConfig{
		Server:              srv.URL,
		APIKey:              "test-key-placeholder",
		MountPoint:          filepath.Join(shortDir, "mnt"),
		RemoteRoot:          "/",
		LocalRoot:           filepath.Join(shortDir, "local"),
		CacheDir:            filepath.Join(shortDir, "cache"),
		WritePolicy:         "writeback",
		EnableGitWorkspaces: true,
		Extensions:          []string{"core", "writeback", "git"},
	}
	require.NoError(t, os.MkdirAll(cfg.MountPoint, 0o755))
	require.NoError(t, os.MkdirAll(cfg.LocalRoot, 0o755))
	baseClient := client.New(srv.URL, cfg.APIKey)
	counters := perf.NewCounters(false)
	var sharedExt *extent.Engine

	rtMain, err := buildViewRuntime(cfg, baseClient, "/", "m1", mountstate.SwitchBinding{}, nil, mb, counters, &sharedExt)
	require.NoError(t, err)
	preMain := rtMain.fs.ReexecPreflight()
	require.False(t, preMain.OK)
	require.Contains(t, preMain.Refusals, ReexecRefusal{Gate: "extension_state", Reason: "git_workspace", Count: 1})
	require.NoError(t, rtMain.stop(context.Background()))

	const l1 = "layer_01j00000000000000000000001"
	vc, err := baseClient.WithView(client.ViewBinding{LayerID: l1, Fence: 1})
	require.NoError(t, err)
	rtLayer, err := buildViewRuntime(cfg, vc, "/", "m1", mountstate.SwitchBinding{LayerID: l1, Fence: 1, BaseRootPath: "/"}, nil, mb, counters, &sharedExt)
	require.NoError(t, err)
	preLayer := rtLayer.fs.ReexecPreflight()
	require.True(t, preLayer.OK)
	require.Empty(t, preLayer.Refusals)
	require.NoError(t, rtLayer.stop(context.Background()))
}

func TestSwitchAfterUnmountLeavesProcessStateRemoved(t *testing.T) {
	sw, _, _, _ := viewSwitchFixture(t)
	view := &mountView{switcher: sw}
	mp := t.TempDir()
	view.bindProcessStateUpdater(func(layer string, readOnly bool) {
		_, err := mountstate.WriteProcessState(mp, mountstate.ProcessState{PID: os.Getpid(), MountPoint: mp, LayerID: layer, LayerReadOnly: readOnly})
		require.NoError(t, err)
	})
	view.bindProcessStateUpdater(nil)
	path := mountstate.PIDFilePath(mp)
	require.NoError(t, os.Remove(path))
	resp := sw.apply(context.Background(), mountcontrol.SwitchRequest{MountID: "mount-1", OperationID: "op_after_unregister", ExpectedGeneration: 1, Layer: "L1"})
	require.NotNil(t, resp.Receipt, resp.Error)
	require.Equal(t, mountstate.SwitchDone, resp.Receipt.State)
	_, err := os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}
