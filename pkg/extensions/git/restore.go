package git

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// ensureWorkspaceState lazily restores a workspace's local .git once its
// server-side state exists: the discovery-time restore races the CLI's
// checkpoint upload (git clone --fast registers the workspace BEFORE the
// state lands). Sync per workspace, bounded by the hydrate budget.
func (g *Git) ensureWorkspaceState(ctx context.Context, w *workspace) error {
	if g.localRoot == "" || w == nil || w.isRestoreLatched() {
		return nil
	}
	return g.runForeground(ctx, func(workCtx context.Context) error {
		if !w.acquireRestore(workCtx) {
			return workCtx.Err()
		}
		defer w.releaseRestore()
		if w.isRestoreLatched() {
			return nil
		}
		rctx, cancel := context.WithTimeout(workCtx, gitWorkspaceHydrateTimeout)
		defer cancel()
		if err := g.restoreWorkspaceState(rctx, w); err != nil {
			// Not latched: a transient failure (or a checkpoint the CLI
			// has not uploaded yet — discovery races the upload) retries
			// at the next entry point. A not-yet-uploaded checkpoint or
			// intentional cancellation is quiet; real failures surface.
			if !errors.Is(err, errGitStateNotReady) && !errors.Is(err, context.Canceled) && !errors.Is(err, errGitRuntimeStopped) {
				fmt.Fprintf(os.Stderr, "drive9: git state restore failed for workspace %s: %v\n", w.id, err)
			}
			return nil
		}
		if w.blobless() {
			// A blobless restore prefetches the HEAD tree's blobs before
			// serving. Linked worktrees resolve objects through their admin
			// dir under the common repository rather than the root .git file.
			// Do not latch restore completion until blob hydration succeeds.
			gitDir, ok := g.checkpointGitDir(w)
			if !ok {
				return errGitStateNotReady
			}
			if err := g.MaterializeHeadTree(rctx, gitDir); err != nil {
				if workCtx.Err() != nil {
					return workCtx.Err()
				}
				if !errors.Is(err, context.Canceled) && !errors.Is(err, errGitRuntimeStopped) {
					fmt.Fprintf(os.Stderr, "drive9: git head tree materialize failed for workspace %s: %v\n", w.id, err)
				}
				return err
			}
		}
		if workCtx.Err() != nil {
			return workCtx.Err()
		}
		w.markRestoreLatched()
		return nil
	})
}
