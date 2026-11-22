//go:build !windows

package fuse

import (
	"context"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

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
