package fuse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const defaultLayerEventPollInterval = time.Second

// StartLayerEventWatcher refreshes a layer mount from fs_layer_events written
// by other clients mounting or writing the same layer.
func StartLayerEventWatcher(fs *Dat9FS, c *client.Client, opts *MountOptions, shadows *ShadowStore, pending *PendingIndex) func() {
	if fs == nil || c == nil || opts == nil || shadows == nil || pending == nil || strings.TrimSpace(opts.LayerRef) == "" || strings.TrimSpace(opts.CheckpointRef) != "" {
		return func() {}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(defaultLayerEventPollInterval)
		defer ticker.Stop()
		var lastSeq int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				nextSeq, err := refreshLayerEvents(ctx, c, opts, shadows, pending, fs, lastSeq)
				if err != nil {
					fmt.Fprintf(os.Stderr, "drive9: fs layer refresh failed: %v\n", err)
					if errors.Is(err, errLayerRestoreStateUncertain) {
						return
					}
					continue
				}
				if nextSeq > lastSeq {
					lastSeq = nextSeq
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
}

func refreshLayerEvents(ctx context.Context, c *client.Client, opts *MountOptions, shadows *ShadowStore, pending *PendingIndex, fs *Dat9FS, since int64) (int64, error) {
	if fs != nil && fs.promotionBlocked.Load() {
		return since, errLayerRestoreStateUncertain
	}
	events, err := c.ListFSLayerEvents(ctx, opts.LayerRef, since)
	if err != nil {
		return since, err
	}
	if len(events) == 0 {
		return since, nil
	}
	maxSeq := since
	rolledBack := false
	for i := range events {
		if events[i].Seq > maxSeq {
			maxSeq = events[i].Seq
		}
		if events[i].Op == client.FSLayerEventOpRollback {
			rolledBack = true
		}
	}
	// When a rollback event is observed, clear the overlay and halt the
	// mount's layer write path — do NOT call restoreLayerEntries, which
	// would re-replay the abandoned layer's still-present fs_layer_entries
	// and re-add the overlay we just cleared.
	if rolledBack {
		fs.applyLayerRollback(shadows, pending)
		return maxSeq, nil
	}
	if err := restoreLayerEntriesInternal(ctx, c, opts, shadows, pending, fs); err != nil {
		return since, err
	}
	// restoreLayerEntries publishes the overlay and invalidates every affected
	// source/target cache before releasing its lifecycle barrier. Doing another
	// invalidation here would run after a queued local writer and could erase
	// that writer's freshly-published cache state.
	return maxSeq, nil
}

func invalidateLayerRefreshPath(fs *Dat9FS, p string) {
	if fs == nil || p == "" || p == "/" {
		return
	}
	fs.invalidateReadCacheAndTargets(p)
	fs.invalidateExtentReaders(p)
	fs.dirCache.Invalidate(parentDir(p))
	fs.dirCache.InvalidatePrefix(p)
	if ino, ok := fs.inodes.GetInode(p); ok {
		fs.notifyInode(ino)
	}
	if parentIno, ok := fs.inodes.GetInode(parentDir(p)); ok {
		fs.notifyEntry(parentIno, path.Base(p))
	}
}
