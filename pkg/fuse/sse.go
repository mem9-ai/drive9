package fuse

import (
	"context"
	"fmt"
	"os"
	"path"

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
	ctx, cancel := context.WithCancel(context.Background())
	w := &SSEWatcher{
		fs:     fs,
		actor:  actor,
		cancel: cancel,
		doneCh: make(chan struct{}),
	}
	go func() {
		defer close(w.doneCh)
		c.WatchEvents(ctx, actor, w.handleEvent)
	}()
	return w
}

// Stop signals the watcher to disconnect and waits for it to finish.
func (w *SSEWatcher) Stop() {
	w.cancel()
	<-w.doneCh
}

func (w *SSEWatcher) handleEvent(change *client.ChangeEvent, reset *client.ResetEvent) {
	if change != nil {
		// Skip events from our own mount.
		if w.actor != "" && change.Actor == w.actor {
			return
		}
		w.handleChange(change)
		return
	}
	if reset != nil && reset.Reason != "" {
		// Only handle resets with an explicit reason (not heartbeats).
		w.handleReset()
	}
}

func (w *SSEWatcher) handleChange(ce *client.ChangeEvent) {
	p := ce.Path
	if p == "" {
		return
	}

	fmt.Fprintf(os.Stderr, "dat9: SSE event op=%s path=%s actor=%s\n", ce.Op, p, ce.Actor)

	// Invalidate read cache for the changed file.
	w.fs.readCache.Invalidate(p)

	// Invalidate directory cache for the parent directory.
	w.fs.dirCache.Invalidate(parentDir(p))

	// Notify kernel about the change.
	if ino, ok := w.fs.inodes.GetInode(p); ok {
		w.fs.notifyInode(ino)
	}
	if parentIno, ok := w.fs.inodes.GetInode(parentDir(p)); ok {
		w.fs.notifyEntry(parentIno, path.Base(p))
	}
}

func (w *SSEWatcher) handleReset() {
	fmt.Fprintf(os.Stderr, "dat9: SSE reset — invalidating all caches\n")

	// 1. Clear all user-space caches.
	w.fs.readCache.InvalidateAll()
	w.fs.dirCache.InvalidateAll()

	// 2. Best-effort kernel cache invalidation for all known inodes.
	//    Snapshot entries first so we don't hold the inode map lock during
	//    potentially slow kernel notify calls.
	//    InodeToPath is kept intact (stale but resolvable).
	entries := w.fs.inodes.Snapshot()
	for _, entry := range entries {
		w.fs.notifyInode(entry.Ino)

		if entry.Path != "/" {
			parent := parentDir(entry.Path)
			if parentIno, ok := w.fs.inodes.GetInode(parent); ok {
				w.fs.notifyEntry(parentIno, path.Base(entry.Path))
			}
		}
	}
}
