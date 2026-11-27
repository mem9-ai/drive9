//go:build !linux

package fuse

import "context"

func fenceViewRootUpdates(ctx context.Context, _ viewRootNotifier) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return errViewSwitchKernelUnsupported
}
