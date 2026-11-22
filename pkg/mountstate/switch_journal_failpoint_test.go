//go:build failpoint && !windows

package mountstate

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"

	"github.com/mem9-ai/drive9/pkg/viewprotocol"
)

func TestSwitchJournalBeforeSyncFailpointLatchesUncertain(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "drive9-view-fp.log")
	scope, err := SwitchScope("http://127.0.0.1:9009", "tenant-fp", "/workspace", filepath.Join(dir, "mnt"))
	require.NoError(t, err)

	j, err := OpenSwitchJournal(jPath, "mnt-fp", scope, viewprotocol.SwitchBinding{})
	require.NoError(t, err)
	defer func() { _ = j.Close() }()

	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeSync", func(errp *error) {
		*errp = errors.New("injected fsync io error")
	}))
	defer func() {
		_ = failpoint.Disable("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeSync")
	}()

	err = j.AppendBindIntent(PendingUnbind{
		LayerID: "layer_01j8z4r9w3m5n7p9q2s4t6v8xz", MountID: "mnt-fp", Kind: UnbindKindOp, OperationID: "op-fp", Fence: 0,
	})
	require.ErrorIs(t, err, ErrSwitchUncertain)
	require.True(t, j.Uncertain())

	// Even after disabling the failpoint, the journal remains latched fail-closed!
	require.NoError(t, failpoint.Disable("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeSync"))
	_, _, err = j.Decide("op-fp2", 1, viewprotocol.SwitchBinding{})
	require.ErrorIs(t, err, ErrSwitchUncertain)
}
