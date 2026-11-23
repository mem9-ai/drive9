//go:build !windows

package fuse

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountViewSwitchPreflightBusyAndPendingUnbindGate(t *testing.T) {
	sw, fb, _, _ := viewSwitchFixture(t)
	ctrl := startViewControlFixture(t, sw)
	ctx := context.Background()

	// 1. Pre-bind EBUSY: non-root nlookup > 0 blocks switch before acquiring any target lease.
	childIno := sw.current.fs.inodes.Lookup("/seed.txt", false, 7, time.Time{})

	_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_busy_lookup", ExpectedGeneration: 1, Layer: "L1",
	})
	var swErr *mountcontrol.SwitchError
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeSwitchBusy, swErr.Code)
	require.Empty(t, sw.journal.Snapshot().PendingUnbinds)
	fb.mu.Lock()
	require.Equal(t, int64(0), fb.layers["layer_01j00000000000000000000001"].Info.Fence)
	fb.mu.Unlock()
	sw.current.fs.inodes.Forget(childIno, 1)

	// 2. Post-bind rollback when checkViewSwitch detects busy state before Decide:
	// first with clean unbind (Fence 1 -> 2), then with transient unbind failure -> BIND_CLEANUP_PENDING.
	var hookedChild uint64
	fb.mu.Lock()
	fb.onBind = func(string) {
		hookedChild = sw.current.fs.inodes.Lookup("/seed.txt", false, 7, time.Time{})
	}
	fb.mu.Unlock()

	_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_postbind_busy", ExpectedGeneration: 1, Layer: "L1",
	})
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeSwitchBusy, swErr.Code)
	require.Empty(t, sw.journal.Snapshot().PendingUnbinds)
	fb.mu.Lock()
	require.Equal(t, int64(2), fb.layers["layer_01j00000000000000000000001"].Info.Fence)
	fb.layers["layer_01j00000000000000000000001"].UnbindFailCnt = 5
	fb.mu.Unlock()
	sw.current.fs.inodes.Forget(hookedChild, 1)

	_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_cleanup_pend", ExpectedGeneration: 1, Layer: "L1",
	})
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeSwitchBindCleanupPending, swErr.Code)
	require.Len(t, sw.journal.Snapshot().PendingUnbinds, 1)

	_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_blocked_by_unbind", ExpectedGeneration: 1, Layer: "L2",
	})
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeSwitchUnbindCleanupPending, swErr.Code)

	// Clear injected busy state (root ino 1 lookup is allowed!) and let unbind succeed.
	fb.mu.Lock()
	fb.onBind = nil
	fb.layers["layer_01j00000000000000000000001"].UnbindFailCnt = 0
	fb.mu.Unlock()
	sw.current.fs.inodes.Forget(hookedChild, 1)
	sw.current.fs.inodes.IncrementLookup(1)

	resp, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_after_drain", ExpectedGeneration: 1, Layer: "L2",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resp.Receipt.Generation)
	require.Equal(t, "layer_01j00000000000000000000002", resp.Receipt.Target.LayerID)
}

func TestMountViewSwitchTargetRevokedDeterministicExits(t *testing.T) {
	t.Run("recover_to_main", func(t *testing.T) {
		sw, fb, _, _ := viewSwitchFixture(t)
		ctrl := startViewControlFixture(t, sw)
		ctx := context.Background()

		_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_l1", ExpectedGeneration: 1, Layer: "L1",
		})
		require.NoError(t, err)

		fb.mu.Lock()
		fb.onBind = func(layerID string) {
			if layerID == "layer_01j00000000000000000000002" {
				// Simulate external fence revocation right after bind-mount before finishPending renew!
				fb.layers["layer_01j00000000000000000000002"].Info.Fence += 2
				fb.layers["layer_01j00000000000000000000002"].OwnerMountID = ""
			}
		}
		fb.mu.Unlock()

		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_revoked", ExpectedGeneration: 2, Layer: "L2",
		})
		var swErr *mountcontrol.SwitchError
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, viewprotocol.CodeSwitchTargetRevoked, swErr.Code)
		require.NotNil(t, sw.journal.Snapshot().Pending)
		require.Equal(t, mountstate.SwitchTargetRevoked, sw.journal.Snapshot().Pending.State)

		// Ordinary FUSE admission MUST remain paused (EAGAIN) while TARGET_REVOKED!
		var attrOut gofuse.AttrOut
		require.Equal(t, gofuse.EAGAIN, sw.adapter.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: 1}}, &attrOut))

		// Ordinary switch without --recover is rejected with TARGET_REVOKED.
		_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_no_recover", ExpectedGeneration: 3, Layer: "main",
		})
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, viewprotocol.CodeSwitchTargetRevoked, swErr.Code)

		// Deterministic Exit #2: mount switch --recover --layer main
		recResp, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_recover_main", ExpectedGeneration: 3, Layer: "main", Recover: true,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(4), recResp.Receipt.Generation)
		require.Empty(t, recResp.Receipt.Target.LayerID)
		require.Equal(t, gofuse.OK, sw.adapter.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: 1}}, &attrOut))

		require.Eventually(t, func() bool {
			fb.mu.Lock()
			defer fb.mu.Unlock()
			return fb.layers["layer_01j00000000000000000000001"].Info.Fence == 2 && len(sw.journal.Snapshot().PendingUnbinds) == 0
		}, 2*time.Second, 20*time.Millisecond)
	})

	t.Run("unmount_exit", func(t *testing.T) {
		sw, fb, _, _ := viewSwitchFixture(t)
		ctrl := startViewControlFixture(t, sw)
		ctx := context.Background()
		mv := &mountView{switcher: sw}
		mv.ready.Store(true)

		fb.mu.Lock()
		fb.onViewList = func(layerID string) {
			if layerID == "layer_01j00000000000000000000001" {
				delete(fb.layers, "layer_01j00000000000000000000001")
			}
		}
		fb.mu.Unlock()

		_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
			MountID: "mount-1", OperationID: "op_deleted", ExpectedGeneration: 1, Layer: "L1",
		})
		var swErr *mountcontrol.SwitchError
		require.ErrorAs(t, err, &swErr)
		require.Equal(t, viewprotocol.CodeSwitchTargetRevoked, swErr.Code)

		drainResp := mv.drainShutdownResponse(ctx)
		require.True(t, drainResp.OK)
		require.Nil(t, sw.journal.Snapshot().Pending)
	})
}

func TestPrepareMountViewDrainsCrashedPriorIncarnationJournalAndPreservesOtherScopes(t *testing.T) {
	shortDir, err := os.MkdirTemp("/tmp", "d9pmv")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(shortDir) })
	t.Setenv("XDG_RUNTIME_DIR", shortDir)
	t.Setenv("HOME", shortDir)
	require.NoError(t, mountstate.EnsureStateDir())

	fb, srv := newFakeSwitchBackend(t)
	mp := filepath.Join(shortDir, "mnt")
	otherMP := filepath.Join(shortDir, "mnt-other")
	require.NoError(t, os.MkdirAll(mp, 0o755))
	require.NoError(t, os.MkdirAll(otherMP, 0o755))

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

	// 1. Seed unrelated journals in stateDir():
	//    (a) valid other-scope journal with corrupt trailing bytes (must NOT block startup!)
	//    (b) unscoped corrupt init-frame journal (must NOT block startup!)
	//    (c) concurrent 0-byte journal file (must NOT be unlinked!)
	otherScope, err := mountstate.SwitchScope(cfg.Server, mountCredentialKind(&cfg)+":"+sha256HexString(mountCredentialSecret(&cfg)), "/", resolveCanonicalStateMountPoint(otherMP))
	require.NoError(t, err)
	otherJPath, err := mountstate.SwitchJournalPath("mount-other")
	require.NoError(t, err)
	otherJ, err := mountstate.OpenSwitchJournal(otherJPath, "mount-other", otherScope, viewprotocol.SwitchBinding{})
	require.NoError(t, err)
	require.NoError(t, otherJ.Close())
	fCorruptOther, err := os.OpenFile(otherJPath, os.O_WRONLY|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = fCorruptOther.Write([]byte("corrupt-trailing-frame"))
	require.NoError(t, err)
	require.NoError(t, fCorruptOther.Close())

	corruptHeaderJPath, err := mountstate.SwitchJournalPath("mount-foreign-corrupt0")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(corruptHeaderJPath, []byte("bad0"), 0o600))

	zeroByteRacerPath, err := mountstate.SwitchJournalPath("mount-racer-zero")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(zeroByteRacerPath, nil, 0o600))

	// 2. Simulate crashed prior incarnation ("mount-crashed") on the SAME mountpoint/scope:
	// persisted bind_intent + server BindLayerMount (active odd Fence=1 under "mount-crashed"), then process died.
	sameScope, err := mountstate.SwitchScope(cfg.Server, mountCredentialKind(&cfg)+":"+sha256HexString(mountCredentialSecret(&cfg)), "/", resolveCanonicalStateMountPoint(mp))
	require.NoError(t, err)
	crashedJPath, err := mountstate.SwitchScopedJournalPath(sameScope, "mount-crashed")
	require.NoError(t, err)
	crashedJ, err := mountstate.OpenSwitchJournal(crashedJPath, "mount-crashed", sameScope, viewprotocol.SwitchBinding{})
	require.NoError(t, err)
	const l1 = "layer_01j00000000000000000000001"
	require.NoError(t, crashedJ.AppendBindIntent(mountstate.PendingUnbind{
		LayerID: l1, MountID: "mount-crashed", Kind: mountstate.UnbindKindOp, OperationID: "bind_crashed", Fence: 0,
	}))
	_, err = baseClient.BindLayerMount(context.Background(), l1, client.LayerMountBindRequest{
		MountID: "mount-crashed", OperationID: "bind_crashed", ExpectedFence: 0,
	})
	require.NoError(t, err)
	require.NoError(t, crashedJ.Close()) // simulate OS releasing flock on process death

	// 3. Restart on the SAME mountpoint with a fresh actorID ("mount-restarted"):
	// prepareMountView must drain "mount-crashed" (Fence 1 -> 2), unlink its stale journal,
	// ignore foreign corrupt/zero-byte files, refresh L1, and bind Fence=3 under "mount-restarted"!
	mv, err := prepareMountView(context.Background(), cfg, baseClient, "mount-restarted")
	require.NoError(t, err)

	restartedJPath, err := mountstate.SwitchScopedJournalPath(sameScope, "mount-restarted")
	require.NoError(t, err)
	_, statLiveErr := os.Stat(restartedJPath)
	require.NoError(t, statLiveErr, "active mount journal must exist while mounted")

	_, statCrashedErr := os.Stat(crashedJPath)
	require.True(t, os.IsNotExist(statCrashedErr), "drained prior scope journal must be unlinked")
	_, statOtherErr := os.Stat(otherJPath)
	require.NoError(t, statOtherErr, "corrupt journal for unrelated mount scope must be preserved and ignored")
	_, statRacerErr := os.Stat(zeroByteRacerPath)
	require.NoError(t, statRacerErr, "concurrent zero-byte journal file must never be unlinked")

	fb.mu.Lock()
	require.Equal(t, int64(3), fb.layers[l1].Info.Fence)
	require.Equal(t, "mount-restarted", fb.layers[l1].OwnerMountID)
	require.NotEmpty(t, fb.unbindOps)
	require.Equal(t, "mount-crashed", fb.unbindOps[0].MountID)
	require.Equal(t, mountstate.UnbindKindOp, fb.unbindOps[0].Kind)
	fb.mu.Unlock()

	// 4. Normal unmount (mv.close) drains current lease and unlinks its own journal file!
	require.NoError(t, mv.close(context.Background()))
	_, statClosedErr := os.Stat(restartedJPath)
	require.True(t, os.IsNotExist(statClosedErr), "normal mountView.close must unlink drained journal file")
}

func TestMountViewSwitchRecoverStopsOutgoingLeaseQueryAbortedAndReplaysIdempotently(t *testing.T) {
	sw, fb, _, _ := viewSwitchFixture(t)
	sw.mu.Lock()
	sw.timing = layerLeaseTiming{renewInterval: 15 * time.Millisecond, callTimeout: 500 * time.Millisecond}
	sw.mu.Unlock()
	ctrl := startViewControlFixture(t, sw)
	ctx := context.Background()

	const l1 = "layer_01j00000000000000000000001"
	const l2 = "layer_01j00000000000000000000002"

	// 1. Switch to L1 so outgoing runtime has a live, fast-renewing layerLease.
	var l1Renews atomic.Int32
	fb.mu.Lock()
	fb.onRenew = func(layerID string) {
		if layerID == l1 {
			l1Renews.Add(1)
		}
	}
	fb.mu.Unlock()

	_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_fast_l1", ExpectedGeneration: 1, Layer: "L1",
	})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return l1Renews.Load() >= 2 }, 2*time.Second, 10*time.Millisecond)

	// 2. Switch to L2 and force TARGET_REVOKED after Decide.
	fb.mu.Lock()
	fb.onBind = func(layerID string) {
		if layerID == l2 {
			fb.layers[l2].Info.Fence += 2
			fb.layers[l2].OwnerMountID = ""
		}
	}
	fb.mu.Unlock()

	_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_rev_l2", ExpectedGeneration: 2, Layer: "L2",
	})
	var swErr *mountcontrol.SwitchError
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeSwitchTargetRevoked, swErr.Code)

	// 3. Verify no L1 renew races with L1 unbind DURING drainPendingUnbindsLocked (before finishPendingLocked),
	// QuerySwitch sees ABORTED on op_rev_l2, and retrying --recover replays SwitchDone idempotently!
	var unbindSawActiveRenew atomic.Bool
	var l1UnbindStarted atomic.Bool
	fb.mu.Lock()
	fb.onBind = nil
	fb.onUnbind = func(layerID string, _ client.LayerMountUnbindRequest) {
		if layerID == l1 {
			l1UnbindStarted.Store(true)
			// Hold unbind open across 4x renewInterval (60ms vs 15ms) BEFORE finishPendingLocked runs;
			// if StopRenew() was not called prior to unbind, background renews will fire inside this window!
			time.Sleep(60 * time.Millisecond)
		}
	}
	fb.onRenew = func(layerID string) {
		if layerID == l1 && l1UnbindStarted.Load() {
			unbindSawActiveRenew.Store(true)
		}
	}
	fb.mu.Unlock()

	recReq := mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_rec_main", ExpectedGeneration: 3, Layer: "main", Recover: true,
	}
	recResp, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), recReq)
	require.NoError(t, err)
	require.True(t, l1UnbindStarted.Load(), "server must have processed L1 unbind during --recover")
	require.Equal(t, uint64(4), recResp.Receipt.Generation)
	require.Equal(t, mountstate.SwitchDone, recResp.Receipt.State)

	time.Sleep(35 * time.Millisecond)
	require.False(t, unbindSawActiveRenew.Load(), "outgoing layerLease renew must be stopped before AbortPendingForRecover + unbind")

	// QuerySwitch on the aborted operation returns SwitchAborted cleanly!
	abortedQuery, err := mountcontrol.QuerySwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_rev_l2", ExpectedGeneration: 2, Layer: "L2",
	})
	require.NoError(t, err)
	require.NotNil(t, abortedQuery.Receipt)
	require.Equal(t, mountstate.SwitchAborted, abortedQuery.Receipt.State)

	// Idempotent retry of the --recover request replays SwitchDone!
	recReplay, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), recReq)
	require.NoError(t, err)
	require.Equal(t, recResp.Receipt, recReplay.Receipt)

	// Idempotent mountRuntime.stop can be called repeatedly without error or panic.
	cur := sw.currentRuntime()
	require.NoError(t, cur.stop(ctx))
	require.NoError(t, cur.stop(ctx))
}
