//go:build !windows

package cli

import (
	"fmt"
	"os"

	"github.com/mem9-ai/drive9/pkg/mountstate"
)

// pinMainMount keeps a root directory descriptor open while a helper uses main's
// out-of-band Git/pack API and local directory. The switch preflight rejects
// open directory handles, including root handles. Open BEFORE reading the live
// binding so a concurrent switch either finishes first or remains blocked.
func pinMainMount(point string) (func(), error) {
	return pinMainMountWithStatus(point, readMountStatusView)
}

func pinMainMountWithStatus(point string, status func(string) (*mountstate.SwitchSnapshot, error)) (func(), error) {
	dir, err := os.Open(point)
	if err != nil {
		return nil, fmt.Errorf("pin mount for main-only helper: %w", err)
	}
	closeDir := func() { _ = dir.Close() }
	info, err := dir.Stat()
	if err != nil || !info.IsDir() {
		closeDir()
		return nil, fmt.Errorf("main-only helper requires a mounted directory")
	}
	view, err := status(point)
	if err != nil {
		closeDir()
		return nil, fmt.Errorf("check mount view before main-only helper: %w", err)
	}
	if view != nil && (view.Pending != nil || view.Binding.LayerID != "") {
		closeDir()
		return nil, fmt.Errorf("this helper requires the mount to select main; use ordinary filesystem Git commands in a Layer")
	}
	return closeDir, nil
}
