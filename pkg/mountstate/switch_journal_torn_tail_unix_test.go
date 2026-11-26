//go:build !windows

package mountstate

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestSwitchJournalRecoversTornTrailingFrameAndRejectsMidFileCorruption(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(validPrefix, full []byte) []byte
	}{
		{
			name: "truncated_body_7_bytes",
			mutate: func(_ []byte, full []byte) []byte {
				return append([]byte(nil), full[:len(full)-7]...)
			},
		},
		{
			name: "truncated_header_2_bytes",
			mutate: func(validPrefix, full []byte) []byte {
				return append([]byte(nil), full[:len(validPrefix)+2]...)
			},
		},
		{
			name: "trailing_zero_padding",
			mutate: func(validPrefix, _ []byte) []byte {
				out := make([]byte, len(validPrefix)+64)
				copy(out, validPrefix)
				return out
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("XDG_RUNTIME_DIR", dir)
			t.Setenv("HOME", dir)
			require.NoError(t, EnsureStateDir())

			scope, err := SwitchScope("http://127.0.0.1:9009", "api_key:test", "/workspace", filepath.Join(dir, "mnt"))
			require.NoError(t, err)
			priorPath, err := SwitchScopedJournalPath(scope, "mnt-prior")
			require.NoError(t, err)

			j, err := OpenSwitchJournal(priorPath, "mnt-prior", scope, SwitchBinding{})
			require.NoError(t, err)
			require.NoError(t, j.AppendBindIntent(PendingUnbind{
				LayerID: testLayerA, MountID: "mnt-prior", Kind: UnbindKindOp, OperationID: "op-init", Fence: 0,
			}))
			require.NoError(t, j.BindInitial(SwitchBinding{
				LayerID: testLayerA, Fence: 1, BaseRootPath: "/workspace",
			}, "op-init"))
			validRaw, err := os.ReadFile(priorPath)
			require.NoError(t, err)

			require.NoError(t, j.AppendBindIntent(PendingUnbind{
				LayerID: testLayerB, MountID: "mnt-prior", Kind: UnbindKindOp, OperationID: "op-torn", Fence: 0,
			}))
			require.NoError(t, j.Close())

			fullRaw, err := os.ReadFile(priorPath)
			require.NoError(t, err)
			require.Greater(t, len(fullRaw), len(validRaw)+8)
			damaged := tc.mutate(validRaw, fullRaw)
			require.NoError(t, os.WriteFile(priorPath, damaged, 0o600))
			var diagnostic bytes.Buffer
			prevLogger := logger.L()
			core := zapcore.NewCore(
				zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
				zapcore.AddSync(&diagnostic),
				zap.DebugLevel,
			)
			logger.Set(zap.New(core))
			t.Cleanup(func() { logger.Set(prevLogger) })

			reopened, err := OpenExistingSwitchJournal(priorPath)
			require.NoError(t, err)
			require.Contains(t, diagnostic.String(), "discarded torn mount switch journal tail")
			require.Contains(t, diagnostic.String(), fmt.Sprintf(`"bytes":%d`, len(damaged)-len(validRaw)))
			st, err := os.Stat(priorPath)
			require.NoError(t, err)
			require.Equal(t, int64(len(validRaw)), st.Size())
			require.Equal(t, testLayerA, reopened.Snapshot().Binding.LayerID)
			require.NoError(t, reopened.Close())

			require.NoError(t, os.WriteFile(priorPath, tc.mutate(validRaw, fullRaw), 0o600))
			currentPath, err := SwitchScopedJournalPath(scope, "mnt-next")
			require.NoError(t, err)
			var unbound []PendingUnbind
			drained, err := DrainPriorScopeJournals(context.Background(), currentPath, scope, func(_ context.Context, item PendingUnbind) error {
				unbound = append(unbound, item)
				return nil
			})
			require.NoError(t, err)
			require.True(t, drained)
			require.Equal(t, []PendingUnbind{{
				LayerID: testLayerA, MountID: "mnt-prior", Kind: UnbindKindFence, Fence: 1,
			}}, unbound)
			_, statErr := os.Stat(priorPath)
			require.True(t, os.IsNotExist(statErr))
		})
	}

	dir := t.TempDir()
	jPath := filepath.Join(dir, "drive9-view-corrupt-mid.log")
	scope, err := SwitchScope("http://127.0.0.1:9009", "api_key:test", "/workspace", filepath.Join(dir, "mnt"))
	require.NoError(t, err)
	j, err := OpenSwitchJournal(jPath, "mnt-mid", scope, SwitchBinding{})
	require.NoError(t, err)
	firstFrameRaw, err := os.ReadFile(jPath)
	require.NoError(t, err)
	require.NoError(t, j.AppendBindIntent(PendingUnbind{
		LayerID: testLayerA, MountID: "mnt-mid", Kind: UnbindKindOp, OperationID: "op-mid", Fence: 0,
	}))
	require.NoError(t, j.Close())

	corruptMid, err := os.ReadFile(jPath)
	require.NoError(t, err)
	require.Greater(t, len(firstFrameRaw), 16)
	corruptMid[8] ^= 0xff
	require.False(t, bytes.Equal(corruptMid[:len(firstFrameRaw)], firstFrameRaw))
	require.NoError(t, os.WriteFile(jPath, corruptMid, 0o600))
	_, err = OpenExistingSwitchJournal(jPath)
	require.ErrorIs(t, err, ErrSwitchCorrupt)
}

func TestSwitchJournalClosedGuardAndExpireUnbindValidation(t *testing.T) {
	require.NoError(t, (PendingUnbind{
		LayerID: testLayerA,
		MountID: "mnt-1",
		Kind:    UnbindKindExpire,
	}).Validate())

	dir := t.TempDir()
	jPath := filepath.Join(dir, "drive9-view-closed.log")
	scope, err := SwitchScope("http://127.0.0.1:9009", "api_key:test", "/workspace", filepath.Join(dir, "mnt"))
	require.NoError(t, err)
	j, err := OpenSwitchJournal(jPath, "mnt-closed", scope, SwitchBinding{})
	require.NoError(t, err)
	item := PendingUnbind{LayerID: testLayerA, MountID: "mnt-closed", Kind: UnbindKindOp, OperationID: "op-closed", Fence: 0}
	require.NoError(t, j.AppendBindIntent(item))
	require.NoError(t, j.Close())

	require.ErrorIs(t, j.AppendUnbindDone(item), ErrSwitchClosed)
	require.ErrorIs(t, j.QueueCurrentUnbindForUnmount(), ErrSwitchClosed)
}
