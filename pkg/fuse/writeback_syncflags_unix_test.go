//go:build unix

package fuse

import (
	"syscall"
	"testing"
)

func TestODSyncOpenPromotesCloseSyncToWriteSync(t *testing.T) {
	opts := &MountOptions{WritePolicy: WritePolicyCloseSync}
	opts.setDefaults()
	fs := NewDat9FS(newTestClient("http://127.0.0.1"), opts)

	if got := fs.writePolicyForOpen(uint32(syscall.O_DSYNC)); got != WritePolicyWriteSync {
		t.Errorf("O_DSYNC policy = %v, want write-sync", got)
	}
}
