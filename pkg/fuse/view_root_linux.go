//go:build linux

package fuse

import (
	"context"
	"fmt"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

const viewRootFenceName = ".drive9-view-switch-fence"

// fenceViewRootUpdates orders root metadata changes before the subsequent root
// attribute invalidation. Linux fuse_reverse_inval_entry takes the parent inode
// lock before looking up the name, including when that name is absent. chmod,
// chown and utimes hold that same lock through notify_change and SETATTR reply
// processing. Root GETATTR/STATX use the separate attr_version invalidation fence.
//
// Ordinary callback admission must already be closed and drained. Do not hold
// admission/inode locks here: the old request still needs to send its reply.
// Old non-root references and negative/root reply TTLs need separate checks.
// This helper alone does not authorize a switch. The Linux kernel contract is
// exercised by the opt-in delayed-reply test; no database is involved.
//
// References (Linux v6.12): fs/fuse/dir.c:1360,2004; fs/open.c:647,783;
// fs/utimes.c:65. No equivalent contract is assumed for other kernels.
func fenceViewRootUpdates(ctx context.Context, notifier viewRootNotifier) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if notifier == nil {
		return errViewSwitchKernelUnsupported
	}
	// This invalidates only a name's cache, never creates or deletes a file.
	// Reference safety must be established before invalidating any directory.
	status := notifier.EntryNotify(gofuse.FUSE_ROOT_ID, viewRootFenceName)
	if status != gofuse.OK && status != gofuse.ENOENT {
		return fmt.Errorf("fuse: root update fence: %w", syscall.Errno(status))
	}
	// A notify syscall cannot be canceled midway. Never report completion if
	// the caller's deadline elapsed while it waited for the kernel lock.
	return ctx.Err()
}
