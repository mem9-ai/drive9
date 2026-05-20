package fuse

import (
	"context"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// SSEWatcher connects to the dat9 server SSE endpoint and invalidates
// local FUSE caches when remote changes are detected.
type SSEWatcher struct {
	fs     *Dat9FS
	actor  string // our own actor ID for self-filtering
	cancel context.CancelFunc
	doneCh chan struct{}
}

// StartSSEWatcher starts a background goroutine that connects to the
// server's /v1/events SSE endpoint and invalidates local caches.
func StartSSEWatcher(fs *Dat9FS, c *client.Client, actor string) *SSEWatcher {
	if fs != nil {
		fs.markStatCacheUnverified()
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &SSEWatcher{
		fs:     fs,
		actor:  actor,
		cancel: cancel,
		doneCh: make(chan struct{}),
	}
	go func() {
		defer close(w.doneCh)
		c.WatchEventsWithLifecycle(ctx, actor, w.handleEvent, client.EventLifecycle{
			OnDisconnected: func(error) {
				if w.fs != nil {
					w.fs.markStatCacheUnverified()
				}
			},
			OnCurrent: w.handleStreamCurrent,
		})
	}()
	return w
}

// Stop signals the watcher to disconnect and waits for it to finish.
func (w *SSEWatcher) Stop() {
	w.cancel()
	<-w.doneCh
}

func (w *SSEWatcher) handleStreamCurrent(uint64) {
	if w.fs != nil {
		w.fs.markStatCacheVerified()
	}
}

func (w *SSEWatcher) handleEvent(change *client.ChangeEvent, reset *client.ResetEvent) {
	if change != nil {
		// Skip events from our own mount.
		if w.actor != "" && change.Actor == w.actor {
			if w.fs != nil && w.fs.perf != nil {
				w.fs.perf.sseSelfFiltered.Add(1)
			}
			return
		}
		if w.fs != nil && w.fs.perf != nil {
			w.fs.perf.sseChange.Add(1)
		}
		w.handleChange(change)
		return
	}
	if reset != nil && reset.Reason != "" {
		if w.actor != "" && reset.Reason == "structural_change" && reset.Actor == w.actor {
			if w.fs != nil && w.fs.perf != nil {
				w.fs.perf.sseSelfFiltered.Add(1)
			}
			return
		}
		if w.fs != nil && w.fs.perf != nil {
			w.fs.perf.sseReset.Add(1)
		}
		// Only handle resets with an explicit reason (not heartbeats).
		w.handleReset(reset)
		if w.fs != nil {
			w.fs.markStatCacheVerified()
		}
	}
}

func (w *SSEWatcher) handleChange(ce *client.ChangeEvent) {
	remotePath := ce.Path
	if remotePath == "" {
		return
	}
	p, ok := w.fs.localPath(remotePath)
	if !ok {
		return
	}

	fmt.Fprintf(os.Stderr, "drive9: SSE event op=%s path=%s local=%s actor=%s\n", ce.Op, remotePath, p, ce.Actor)

	// Invalidate read cache and resolved read targets for the changed file.
	w.fs.invalidateReadCacheAndTargets(p)

	// Invalidate directory cache for the parent directory.
	w.fs.dirCache.Invalidate(parentDir(p))
	w.fs.dirCache.InvalidatePrefix(p)

	// Notify kernel about the change.
	if ino, ok := w.fs.inodes.GetInode(p); ok {
		w.fs.notifyInode(ino)
	}
	if parentIno, ok := w.fs.inodes.GetInode(parentDir(p)); ok {
		w.fs.notifyEntry(parentIno, path.Base(p))
	}
}

func (w *SSEWatcher) handleReset(resets ...*client.ResetEvent) {
	start := time.Now()
	var seq uint64
	reason := "unknown"
	if len(resets) > 0 && resets[0] != nil {
		seq = resets[0].Seq
		reason = resets[0].Reason
	}
	fmt.Fprintf(os.Stderr, "drive9: SSE reset — invalidating all caches\n")
	w.fs.debugf("sse reset start seq=%d reason=%s", seq, reason)

	// 1. Clear all user-space caches.
	w.fs.readCache.InvalidateAll()
	if w.fs.diskReadCache != nil {
		w.fs.diskReadCache.InvalidateAll()
	}
	w.fs.clearAllReadTargets()
	w.fs.dirCache.InvalidateAll()

	// 2. Best-effort kernel cache invalidation for all known inodes.
	//    Snapshot entries first so we don't hold the inode map lock during
	//    potentially slow kernel notify calls.
	//    InodeToPath is kept intact (stale but resolvable).
	entries := w.fs.inodes.Snapshot()
	w.fs.debugf("sse reset snapshot seq=%d reason=%s inodes=%d", seq, reason, len(entries))
	for _, entry := range entries {
		w.fs.notifyInode(entry.Ino)

		// Do not invalidate a directory's own parent dentry during a broad reset.
		// Linux getcwd(2) walks parent dentries; dropping the dentry for a
		// process's current working directory can make git's remote helpers fail
		// with ENOENT even though the directory still exists. The reset already
		// clears userspace directory caches and notifies the directory inode, so
		// future readdir/attr paths are refreshed without detaching cwd names.
		if entry.Path != "/" && !entry.IsDir {
			parent := parentDir(entry.Path)
			if parentIno, ok := w.fs.inodes.GetInode(parent); ok {
				w.fs.notifyEntry(parentIno, path.Base(entry.Path))
			}
		}
	}
	w.fs.debugf("sse reset done seq=%d reason=%s inodes=%d dur=%s", seq, reason, len(entries), time.Since(start))
}
