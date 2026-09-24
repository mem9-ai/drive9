package fuse

import (
	"context"
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
	renameTargets := make(map[string]string)
	if err := restoreLayerEntriesWithRenameTargets(ctx, c, opts, shadows, pending, fs, renameTargets); err != nil {
		return since, err
	}
	// The restored shadow is now authoritative for these paths. Drop all
	// userspace and kernel-facing read state so a long-lived same-layer mount
	// observes the refreshed entry rather than a cached pre-event generation.
	for i := range events {
		p, ok := fs.localPath(events[i].Path)
		if !ok || p == "" || p == "/" {
			continue
		}
		invalidateLayerRefreshPath(fs, p)
		if target := renameTargets[p]; target != "" && target != p {
			invalidateLayerRefreshPath(fs, target)
		}
	}
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
