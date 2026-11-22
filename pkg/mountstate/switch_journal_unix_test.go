//go:build !windows

package mountstate

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

const (
	testLayerA = "layer_01j8z4r9w3m5n7p9q2s4t6v8xz"
	testLayerB = "layer_01j8z4r9w3m5n7p9q2s4t6v8ya"
)

func TestSwitchJournalLifecycleReplayAndPendingUnbinds(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "drive9-view-m1.log")
	scope, err := SwitchScope("http://127.0.0.1:9009", "tenant-1", "/workspace", filepath.Join(dir, "mnt"))
	require.NoError(t, err)

	j, err := OpenSwitchJournal(jPath, "mnt-1", scope, viewprotocol.SwitchBinding{})
	require.NoError(t, err)
	defer func() { _ = j.Close() }()

	// Reject non-canonical layer refs and odd F_obs in bind_intent.
	require.ErrorIs(t, j.AppendBindIntent(PendingUnbind{
		LayerID: "named-layer", MountID: "mnt-1", Kind: UnbindKindOp, OperationID: "op-1", Fence: 0,
	}), ErrSwitchInvalid)
	require.ErrorIs(t, j.AppendBindIntent(PendingUnbind{
		LayerID: testLayerA, MountID: "mnt-1", Kind: UnbindKindOp, OperationID: "op-1", Fence: 1,
	}), ErrSwitchInvalid)

	// Persist bind_intent(F_obs=0) -> Decide(L_A, fence=1) -> Complete.
	require.NoError(t, j.AppendBindIntent(PendingUnbind{
		LayerID: testLayerA, MountID: "mnt-1", Kind: UnbindKindOp, OperationID: "op-1", Fence: 0,
	}))
	targetA := viewprotocol.SwitchBinding{LayerID: testLayerA, Fence: 1, BaseRootPath: "/workspace"}
	rcpt1, replayed, err := j.Decide("op-1", 1, targetA)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, SwitchRecovering, rcpt1.State)
	require.Empty(t, j.Snapshot().PendingUnbinds, "Decide promotes target bind_intent into pending")

	// Crash-recovery reopen preserves RECOVERING and replays Decide idempotently.
	require.NoError(t, j.Close())
	j, err = OpenExistingSwitchJournal(jPath)
	require.NoError(t, err)
	defer func() { _ = j.Close() }()

	rcpt1Replay, replayed, err := j.Decide("op-1", 1, targetA)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, rcpt1, rcpt1Replay)

	done1, err := j.Complete("op-1")
	require.NoError(t, err)
	require.Equal(t, SwitchDone, done1.State)
	require.Empty(t, j.Snapshot().PendingUnbinds, "switching from main queues zero unbinds")

	// Switching L_A -> L_B atomically queues outgoing L_A(fence=1) in pending_unbinds on Complete!
	require.NoError(t, j.AppendBindIntent(PendingUnbind{
		LayerID: testLayerB, MountID: "mnt-1", Kind: UnbindKindOp, OperationID: "op-2", Fence: 2,
	}))
	targetB := viewprotocol.SwitchBinding{LayerID: testLayerB, Fence: 3, BaseRootPath: "/workspace"}
	_, _, err = j.Decide("op-2", 2, targetB)
	require.NoError(t, err)
	_, err = j.Complete("op-2")
	require.NoError(t, err)

	snap := j.Snapshot()
	require.Equal(t, uint64(3), snap.Generation)
	require.Equal(t, []PendingUnbind{{
		LayerID: testLayerA, MountID: "mnt-1", Kind: UnbindKindFence, Fence: 1,
	}}, snap.PendingUnbinds)

	// New switch is blocked with ErrSwitchUnbindPending until pending_unbinds drains!
	_, _, err = j.Decide("op-3", 3, viewprotocol.SwitchBinding{})
	require.ErrorIs(t, err, ErrSwitchUnbindPending)

	var drained []PendingUnbind
	require.NoError(t, j.DrainPendingUnbinds(context.Background(), func(_ context.Context, item PendingUnbind) error {
		drained = append(drained, item)
		return nil
	}))
	require.Len(t, drained, 1)
	require.Empty(t, j.Snapshot().PendingUnbinds)
}

func TestSwitchJournalTargetRevokedDualExitAndCorruption(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "drive9-view-m2.log")
	scope, err := SwitchScope("http://127.0.0.1:9009", "tenant-1", "/workspace", filepath.Join(dir, "mnt2"))
	require.NoError(t, err)

	j, err := OpenSwitchJournal(jPath, "mnt-2", scope, viewprotocol.SwitchBinding{})
	require.NoError(t, err)

	// Initial mount --layer via BindInitial, then switch L_A -> L_B hits TARGET_REVOKED.
	require.NoError(t, j.AppendBindIntent(PendingUnbind{
		LayerID: testLayerA, MountID: "mnt-2", Kind: UnbindKindOp, OperationID: "init-op", Fence: 0,
	}))
	require.NoError(t, j.BindInitial(viewprotocol.SwitchBinding{
		LayerID: testLayerA, Fence: 1, BaseRootPath: "/workspace",
	}, "init-op"))

	require.NoError(t, j.AppendBindIntent(PendingUnbind{
		LayerID: testLayerB, MountID: "mnt-2", Kind: UnbindKindOp, OperationID: "op-rev", Fence: 0,
	}))
	_, _, err = j.Decide("op-rev", 1, viewprotocol.SwitchBinding{
		LayerID: testLayerB, Fence: 1, BaseRootPath: "/workspace",
	})
	require.NoError(t, err)

	// AbortPendingForRecover is rejected while RECOVERING; requires TARGET_REVOKED.
	_, err = j.AbortPendingForRecover("op-rev")
	require.ErrorIs(t, err, ErrSwitchNotRevoked)

	rev, err := j.MarkTargetRevoked("op-rev")
	require.NoError(t, err)
	require.Equal(t, SwitchTargetRevoked, rev.State)

	aborted, err := j.AbortPendingForRecover("op-rev")
	require.NoError(t, err)
	require.Equal(t, SwitchAborted, aborted.State)
	// Both outgoing L_A and target L_B are queued in pending_unbinds (2 items).
	require.Len(t, j.Snapshot().PendingUnbinds, 2)
	require.NoError(t, j.Close())

	// Corrupting a byte causes OpenExistingSwitchJournal to fail closed with ErrSwitchCorrupt.
	raw, err := os.ReadFile(jPath)
	require.NoError(t, err)
	raw[len(raw)-1] ^= 0xff
	require.NoError(t, os.WriteFile(jPath, raw, 0o600))
	_, err = OpenExistingSwitchJournal(jPath)
	require.ErrorIs(t, err, ErrSwitchCorrupt)
}
