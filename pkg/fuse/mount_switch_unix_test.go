//go:build !windows

package fuse

import (
	"context"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/mountcontrol"
	"github.com/mem9-ai/drive9/pkg/mountstate"
	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestMountViewSwitchEndToEndAndIdempotentReplay(t *testing.T) {
	sw, fb, _, mb := viewSwitchFixture(t)
	ctrl := startViewControlFixture(t, sw)
	ctx := context.Background()

	st, err := mountcontrol.RequestStatus(ctx, ctrl.SocketPath(), 2*time.Second)
	require.NoError(t, err)
	require.True(t, st.ViewSwitch)
	require.Equal(t, "mount-1", st.MountID)
	require.Equal(t, uint64(1), st.Generation)
	require.Empty(t, st.Binding.LayerID)

	req1 := mountcontrol.SwitchRequest{MountID: "mount-1", OperationID: "op_to_l1", ExpectedGeneration: 1, Layer: "L1"}
	resp1, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), req1)
	require.NoError(t, err)
	require.NotNil(t, resp1.Receipt)
	require.Equal(t, mountstate.SwitchDone, resp1.Receipt.State)
	require.Equal(t, uint64(2), resp1.Receipt.Generation)
	require.Equal(t, "layer_01j00000000000000000000001", resp1.Receipt.Target.LayerID)
	require.Equal(t, int64(1), resp1.Receipt.Target.Fence)

	fb.mu.Lock()
	bindsBeforeReplay := fb.bindCalls
	fb.mu.Unlock()

	replay, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), req1)
	require.NoError(t, err)
	require.Equal(t, resp1.Receipt, replay.Receipt)
	queried, err := mountcontrol.QuerySwitch(ctx, ctrl.SocketPath(), req1)
	require.NoError(t, err)
	require.Equal(t, resp1.Receipt, queried.Receipt)

	fb.mu.Lock()
	require.Equal(t, bindsBeforeReplay, fb.bindCalls)
	fb.mu.Unlock()

	_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_to_l1", ExpectedGeneration: 1, Layer: "L2",
	})
	var swErr *mountcontrol.SwitchError
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeConflict, swErr.Code)

	resp2, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_to_l2", ExpectedGeneration: 2, Layer: "L2",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), resp2.Receipt.Generation)
	require.Equal(t, "layer_01j00000000000000000000002", resp2.Receipt.Target.LayerID)

	require.Eventually(t, func() bool {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		return fb.layers["layer_01j00000000000000000000001"].Info.Fence == 2 && fb.layers["layer_01j00000000000000000000001"].OwnerMountID == ""
	}, 2*time.Second, 20*time.Millisecond)

	resp3, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_to_main", ExpectedGeneration: 3, Layer: "main",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(4), resp3.Receipt.Generation)
	require.Empty(t, resp3.Receipt.Target.LayerID)

	require.Eventually(t, func() bool {
		fb.mu.Lock()
		defer fb.mu.Unlock()
		return fb.layers["layer_01j00000000000000000000002"].Info.Fence == 2 && fb.layers["layer_01j00000000000000000000002"].OwnerMountID == ""
	}, 2*time.Second, 20*time.Millisecond)

	snap := mb.Snapshot()
	require.Zero(t, snap.Classes[membudget.ClassReadCache])
	require.LessOrEqual(t, snap.Classes[membudget.ClassInodeTable], int64(4096))
}

func TestMountViewSwitchPrePauseGatesAndExpiredLeaseRecovery(t *testing.T) {
	sw, fb, _, _ := viewSwitchFixture(t)
	ctrl := startViewControlFixture(t, sw)
	ctx := context.Background()

	sw.mu.Lock()
	sw.goos = "darwin"
	sw.mu.Unlock()
	_, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_mac", ExpectedGeneration: 1, Layer: "L1",
	})
	var swErr *mountcontrol.SwitchError
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeSwitchUnsupported, swErr.Code)

	sw.mu.Lock()
	sw.goos = "linux"
	sw.mu.Unlock()

	_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_outside_root", ExpectedGeneration: 1, Layer: "sub",
	})
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, "FORBIDDEN_OUTSIDE_BASE_ROOT", swErr.Code)
	require.Empty(t, sw.journal.Snapshot().PendingUnbinds)

	fb.mu.Lock()
	fb.layers["layer_01j00000000000000000000001"].Info.Fence = 3
	fb.layers["layer_01j00000000000000000000001"].Info.MountExpired = false
	fb.layers["layer_01j00000000000000000000001"].OwnerMountID = "other-mount"
	fb.mu.Unlock()

	_, err = mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_active_conflict", ExpectedGeneration: 1, Layer: "L1",
	})
	require.ErrorAs(t, err, &swErr)
	require.Equal(t, viewprotocol.CodeLayerMountConflict, swErr.Code)
	require.Empty(t, sw.journal.Snapshot().PendingUnbinds)
	var attrOut gofuse.AttrOut
	require.Equal(t, gofuse.OK, sw.adapter.GetAttr(nil, &gofuse.GetAttrIn{InHeader: gofuse.InHeader{NodeId: 1}}, &attrOut))

	fb.mu.Lock()
	fb.layers["layer_01j00000000000000000000001"].Info.MountExpired = true
	fb.mu.Unlock()

	resp, err := mountcontrol.RequestSwitch(ctx, ctrl.SocketPath(), mountcontrol.SwitchRequest{
		MountID: "mount-1", OperationID: "op_expire_takeover", ExpectedGeneration: 1, Layer: "L1",
	})
	require.NoError(t, err)
	require.Equal(t, int64(5), resp.Receipt.Target.Fence)
}
