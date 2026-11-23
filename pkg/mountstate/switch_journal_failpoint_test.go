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

func TestSwitchJournalDirSyncOnPublishRemoveAndFailClosed(t *testing.T) {
	dir := t.TempDir()
	jPath := filepath.Join(dir, "drive9-view-dirsync.log")
	scope, err := SwitchScope("http://127.0.0.1:9009", "tenant-ds", "/workspace", filepath.Join(dir, "mnt"))
	require.NoError(t, err)

	// 1. If parent directory fsync fails during atomic publish, OpenSwitchJournal
	// fails closed and unlinks both tmp and target journal names.
	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync", func(errp *error) {
		*errp = errors.New("injected parent dir fsync error")
	}))
	_, err = OpenSwitchJournal(jPath, "mnt-ds-fail", scope, viewprotocol.SwitchBinding{})
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync"))

	entries, rerr := filepath.Glob(filepath.Join(dir, "*"))
	require.NoError(t, rerr)
	require.Empty(t, entries, "failed parent dir sync must not leave published or temp journal files behind")

	// 2. Successful publish invokes parent dir sync AFTER linking jPath and unlinking .tmp;
	// RemoveSwitchJournal invokes parent dir sync AFTER unlinking jPath.
	dirSyncCalls := 0
	require.NoError(t, failpoint.EnableCall("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync", func(_ *error) {
		dirSyncCalls++
		tmps, _ := filepath.Glob(filepath.Join(dir, ".drive9-view-init-*.tmp"))
		require.Empty(t, tmps, "temp journal dentry must already be removed before parent dir fsync")
	}))
	defer func() {
		_ = failpoint.Disable("github.com/mem9-ai/drive9/pkg/mountstate/switchJournalBeforeDirSync")
	}()

	j, err := OpenSwitchJournal(jPath, "mnt-ds-ok", scope, viewprotocol.SwitchBinding{})
	require.NoError(t, err)
	require.Equal(t, 1, dirSyncCalls)
	require.NoError(t, j.Close())

	require.NoError(t, RemoveSwitchJournal(jPath))
	require.Equal(t, 2, dirSyncCalls)
}
