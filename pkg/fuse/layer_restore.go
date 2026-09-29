package fuse

import (
	"context"
	"fmt"
	"strings"
	"syscall"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/mountpath"
)

func restoreLayerEntries(ctx context.Context, c *client.Client, opts *MountOptions, shadows *ShadowStore, pending *PendingIndex, fs *Dat9FS) error {
	if c == nil || opts == nil || shadows == nil || pending == nil || strings.TrimSpace(opts.LayerRef) == "" {
		return nil
	}
	var maxSeq int64
	hasCheckpoint := false
	if strings.TrimSpace(opts.CheckpointRef) != "" {
		checkpoint, err := c.GetFSLayerCheckpoint(ctx, opts.CheckpointRef)
		if err != nil {
			return fmt.Errorf("read fs layer checkpoint %s: %w", opts.CheckpointRef, err)
		}
		if checkpoint.LayerID != opts.LayerRef {
			return fmt.Errorf("checkpoint %s belongs to layer %s, want %s", opts.CheckpointRef, checkpoint.LayerID, opts.LayerRef)
		}
		maxSeq = checkpoint.DurableSeq
		hasCheckpoint = true
	}
	var entries []client.FSLayerEntry
	var err error
	if hasCheckpoint {
		entries, err = c.ReplayFSLayerAtSeq(ctx, opts.LayerRef, maxSeq)
	} else {
		entries, err = c.ReplayFSLayer(ctx, opts.LayerRef)
	}
	if err != nil {
		return err
	}
	r := layerRestorer{client: c, opts: opts, shadows: shadows, pending: pending, fs: fs, maxSeq: maxSeq, hasCheckpoint: hasCheckpoint, tips: make(map[string]client.FSLayerEntry)}
	for _, entry := range entries {
		r.tips[entry.Path] = entry
	}
	for _, entry := range layerReplayContent(entries) {
		if err := r.restoreEntry(ctx, entry); err != nil {
			return err
		}
	}
	return nil
}

// layerReplayContent drops overwritten payloads, retaining namespace operations
// and the last body before chmod. A rename can consume an earlier snapshot
// (including descendants of a directory), so it is a dependency barrier.
func layerReplayContent(entries []client.FSLayerEntry) []client.FSLayerEntry {
	superseded := make(map[string]bool)
	skip := make([]bool, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		e := entries[i]
		switch e.Op {
		case "rename":
			clear(superseded)
		case "upsert":
			if e.Kind == "file" {
				skip[i] = superseded[e.Path]
				superseded[e.Path] = true
			}
		case "whiteout", "mkdir", "symlink":
			superseded[e.Path] = true
		}
	}
	out := make([]client.FSLayerEntry, 0, len(entries))
	for i, entry := range entries {
		if !skip[i] {
			out = append(out, entry)
		}
	}
	return out
}

type layerRestorer struct {
	client        *client.Client
	opts          *MountOptions
	shadows       *ShadowStore
	pending       *PendingIndex
	fs            *Dat9FS
	maxSeq        int64
	hasCheckpoint bool
	tips          map[string]client.FSLayerEntry
}

func (r *layerRestorer) cacheCoversReplay(meta *WriteBackMeta, tip client.FSLayerEntry) bool {
	id := layerCacheIdentity(&tip, r.opts.LayerRef)
	if !meta.LayerClean || meta.Kind == PendingConflict || meta.LayerEntrySeq <= 0 || id.EntrySeq <= 0 || meta.LayerID != id.LayerID {
		return false
	}
	if meta.LayerEntrySeq == id.EntrySeq {
		return tip.Op == "upsert" || tip.Op == "chmod"
	}
	// A local upload may finish after the replay request took its snapshot.
	// Only the active tip's sequence can prove that the cache is newer.
	// Checkpoints and ancestor pins must still restore their older version.
	return !r.hasCheckpoint && meta.LayerID == r.opts.LayerRef && meta.LayerEntrySeq > id.EntrySeq
}

func (r *layerRestorer) restoreEntry(ctx context.Context, entry client.FSLayerEntry) (retErr error) {
	c, opts, shadows, pending, fs := r.client, r.opts, r.shadows, r.pending, r.fs
	localPath, ok := mountpath.ToLocal(opts.RemoteRoot, entry.Path)
	if !ok {
		return nil
	}
	var handles []*FileHandle
	if fs != nil {
		unlock := fs.lockRemoteCommitPath(localPath)
		defer unlock()
		var unlockHandles func()
		var err error
		handles, unlockHandles, err = fs.lockLayerReplayHandles(localPath)
		if err != nil {
			return err
		}
		defer unlockHandles()
	}
	// An orphan must not suppress the authoritative Layer replay. Recovery
	// runs after restore, so prune it here under the same publication fence.
	if meta, exists := pending.GetMeta(localPath); exists {
		size, present := shadows.ContentSize(localPath)
		if meta.LayerClean && meta.Kind != PendingConflict && (!present || size < meta.Size) {
			// A clean cache is replaceable. Drop the torn publication durably
			// and restore from the Layer; never upload its incomplete payload.
			if err := pending.discardLayerCache(localPath, meta.Generation, shadows); err != nil {
				return fmt.Errorf("discard incomplete fs layer cache %s: %w", localPath, err)
			}
		} else if !present {
			pending.RemoveIfGeneration(localPath, meta.Generation)
		}
	}
	if fs != nil {
		if !opts.ReadOnly {
			preserved, err := fs.preserveLayerReplayConflict(localPath, r.tips[entry.Path], handles, pending)
			if preserved || err != nil {
				return err
			}
		}
		defer func() {
			if retErr == nil {
				retErr = fs.rebaseLayerReplayHandles(localPath, handles)
			}
		}()
	}
	// Recovery must upload local dirty content before it can be replaced by
	// a server replay. Clean overlays can be refreshed normally.
	if meta, exists := pending.GetMeta(localPath); exists && (!meta.LayerClean || meta.Kind == PendingConflict) && !opts.ReadOnly {
		return nil
	}
	tip := r.tips[entry.Path]
	if meta, exists := pending.GetMeta(localPath); exists && r.cacheCoversReplay(meta, tip) {
		// The full replay can include older whiteouts and upserts. Compare
		// against its final path identity before replaying any of that history.
		// Matching content identity must not skip a replayed permission change.
		if tip.Op == "chmod" && meta.LayerEntrySeq == tip.EntrySeq && (!meta.HasMode || meta.Mode != tip.Mode&posixPermissionModeMask) {
			if err := pending.MarkLayerCommittedIfGeneration(localPath, meta.Generation, meta.BaseRev, tip.Mode, true, layerCacheIdentity(&tip, opts.LayerRef)); err != nil {
				return fmt.Errorf("restore fs layer cached mode %s: %w", localPath, err)
			}
			meta.Mode = tip.Mode & posixPermissionModeMask
		}
		if meta.shadowSource.store != shadows && pending.recoverShadowSource(localPath, meta.Generation, shadows) == 0 {
			return fmt.Errorf("restore fs layer cache %s: missing or short shadow", localPath)
		}
		if fs != nil {
			fs.markLayerFileMode(localPath, meta.Mode)
		}
		return nil
	}
	switch entry.Op {
	case "whiteout":
		if meta, exists := pending.GetMeta(localPath); exists && meta.LayerClean {
			if err := pending.discardLayerCache(localPath, meta.Generation, shadows); err != nil {
				return fmt.Errorf("restore fs layer whiteout %s: %w", localPath, err)
			}
		}
		if fs != nil {
			fs.markLayerWhiteout(localPath)
		}
		return nil
	case "mkdir":
		if meta, exists := pending.GetMeta(localPath); exists && meta.LayerClean {
			if err := pending.discardLayerCache(localPath, meta.Generation, shadows); err != nil {
				return err
			}
		}
		if fs != nil {
			fs.markLayerDir(localPath, entry.Mode)
		}
		return nil
	case "chmod":
		if pending != nil {
			if meta, ok := pending.GetMeta(localPath); ok {
				if err := pending.MarkLayerCommittedIfGeneration(localPath, meta.Generation, meta.BaseRev, entry.Mode, true, layerCacheIdentity(&entry, opts.LayerRef)); err != nil {
					return fmt.Errorf("restore fs layer chmod pending %s: %w", localPath, err)
				}
			}
		}
		if fs != nil {
			switch entry.Kind {
			case "file":
				fs.markLayerFileMode(localPath, entry.Mode)
			case "dir":
				fs.markLayerDir(localPath, entry.Mode)
			case "symlink":
				if target, existingMode, ok := fs.layerSymlink(localPath); ok {
					nextMode := (existingMode &^ uint32(0o777)) | (entry.Mode & 0o777)
					if nextMode&uint32(syscall.S_IFMT) == 0 {
						nextMode |= uint32(syscall.S_IFLNK)
					}
					fs.markLayerSymlink(localPath, target, nextMode)
				}
			}
		}
		return nil
	case "rename":
		if err := restoreLayerRenameEntry(ctx, c, opts, shadows, pending, fs, localPath, &entry, layerEntryFetchMaxSeq(&entry, r.hasCheckpoint, r.maxSeq)); err != nil {
			return err
		}
		return nil
	case "symlink":
		if meta, exists := pending.GetMeta(localPath); exists && meta.LayerClean {
			if err := pending.discardLayerCache(localPath, meta.Generation, shadows); err != nil {
				return err
			}
		}
		fullEntry := &entry
		if strings.TrimSpace(fullEntry.ContentText) == "" && len(fullEntry.Content) == 0 {
			fetched, err := getLayerEntryForRestore(ctx, c, opts.LayerRef, entry.Path, layerEntryFetchMaxSeq(&entry, r.hasCheckpoint, r.maxSeq))
			if err != nil {
				return fmt.Errorf("restore fs layer symlink entry %s: %w", entry.Path, err)
			}
			fullEntry = fetched
		}
		target := strings.TrimSpace(fullEntry.ContentText)
		if target == "" && len(fullEntry.Content) > 0 {
			target = string(fullEntry.Content)
		}
		if target == "" {
			return fmt.Errorf("restore fs layer symlink entry %s: missing target", entry.Path)
		}
		if fs != nil {
			fs.markLayerSymlink(localPath, target, entry.Mode)
		}
		return nil
	}
	if entry.Op != "upsert" || entry.Kind != "file" {
		return nil
	}

	entryMaxSeq := layerEntryFetchMaxSeq(&entry, r.hasCheckpoint, r.maxSeq)
	fullEntry, err := getLayerEntryForRestore(ctx, c, opts.LayerRef, entry.Path, entryMaxSeq)
	if err != nil {
		return fmt.Errorf("restore fs layer entry %s: %w", entry.Path, err)
	}
	var sizeBytes int64
	if fullEntry.StorageRef != "" || fullEntry.StorageType == "s3" {
		rc, err := c.ReadFSLayerFileStream(ctx, opts.LayerRef, entry.Path, entryMaxSeq)
		if err != nil {
			return fmt.Errorf("restore fs layer object %s: %w", entry.Path, err)
		}
		n, writeErr := shadows.WriteStream(localPath, rc, fullEntry.BaseRevision)
		closeErr := rc.Close()
		if writeErr != nil {
			return fmt.Errorf("restore fs layer shadow %s: %w", localPath, writeErr)
		}
		if closeErr != nil {
			return fmt.Errorf("restore fs layer object %s: %w", entry.Path, closeErr)
		}
		sizeBytes = n
	} else {
		content := fullEntry.Content
		if err := shadows.WriteFull(localPath, content, fullEntry.BaseRevision); err != nil {
			return fmt.Errorf("restore fs layer shadow %s: %w", localPath, err)
		}
		sizeBytes = int64(len(content))
	}
	if fullEntry.SizeBytes > 0 && sizeBytes != fullEntry.SizeBytes {
		return fmt.Errorf("restore fs layer object %s: copied %d bytes, want %d", entry.Path, sizeBytes, fullEntry.SizeBytes)
	}
	if _, err := pending.PutLayerCache(localPath, sizeBytes, fullEntry.BaseRevision, fullEntry.Mode, fullEntry.Mode != 0, layerCacheIdentity(fullEntry, opts.LayerRef)); err != nil {
		return fmt.Errorf("restore fs layer pending %s: %w", localPath, err)
	}
	if fs != nil {
		if fullEntry.Mode != 0 {
			fs.markLayerFileMode(localPath, fullEntry.Mode)
		} else {
			fs.markLayerFile(localPath)
		}
	}
	return nil
}

func layerEntryFetchMaxSeq(entry *client.FSLayerEntry, hasCheckpoint bool, checkpointMaxSeq int64) *int64 {
	// Checkpoint restores must stay pinned to the checkpoint tip. Ancestor
	// rows can carry a larger entry_seq than the child tip; using that seq
	// against the child layer would leak later child writes into the view.
	if hasCheckpoint {
		seq := checkpointMaxSeq
		return &seq
	}
	if entry != nil && entry.EntrySeq > 0 {
		seq := entry.EntrySeq
		return &seq
	}
	return nil
}

func fsLayerMountStateAllowed(state string, checkpoint bool) bool {
	if checkpoint {
		switch state {
		case "active", "sealed", "committed":
			return true
		default:
			return false
		}
	}
	return state == "active"
}

func getLayerEntryForRestore(ctx context.Context, c *client.Client, layerID, path string, maxSeq *int64) (*client.FSLayerEntry, error) {
	if maxSeq != nil {
		return c.GetFSLayerEntryAtSeq(ctx, layerID, path, *maxSeq)
	}
	return c.GetFSLayerEntry(ctx, layerID, path)
}

func restoreLayerRenameEntry(ctx context.Context, c *client.Client, opts *MountOptions, shadows *ShadowStore, pending *PendingIndex, fs *Dat9FS, oldLocalPath string, entry *client.FSLayerEntry, maxSeq *int64) error {
	if entry == nil {
		return nil
	}
	fullEntry := entry
	if strings.TrimSpace(fullEntry.ContentText) == "" && len(fullEntry.Content) == 0 {
		fetched, err := getLayerEntryForRestore(ctx, c, opts.LayerRef, entry.Path, maxSeq)
		if err != nil {
			return fmt.Errorf("restore fs layer rename entry %s: %w", entry.Path, err)
		}
		fullEntry = fetched
	}
	targetRemote := fullEntry.ContentText
	if targetRemote == "" && len(fullEntry.Content) > 0 {
		targetRemote = string(fullEntry.Content)
	}
	if targetRemote == "" {
		return fmt.Errorf("restore fs layer rename entry %s: missing target", entry.Path)
	}
	if fs != nil {
		fs.markLayerWhiteout(oldLocalPath)
	}
	newLocalPath, ok := mountpath.ToLocal(opts.RemoteRoot, targetRemote)
	if !ok {
		return nil
	}
	movedShadow := shadows.Rename(oldLocalPath, newLocalPath)
	movedPending := pending.RenamePending(oldLocalPath, newLocalPath)
	if movedShadow || movedPending {
		return nil
	}
	if pending.HasPending(newLocalPath) {
		return nil
	}
	data, err := c.ReadCtx(ctx, fullEntry.Path)
	if err != nil {
		return fmt.Errorf("restore fs layer renamed source %s: %w", fullEntry.Path, err)
	}
	if err := shadows.WriteFull(newLocalPath, data, 0); err != nil {
		return fmt.Errorf("restore fs layer renamed shadow %s: %w", newLocalPath, err)
	}
	if _, err := pending.PutLayerCache(newLocalPath, int64(len(data)), 0, fullEntry.Mode, fullEntry.Mode != 0, layerCacheIdentity(fullEntry, opts.LayerRef)); err != nil {
		return fmt.Errorf("restore fs layer renamed pending %s: %w", newLocalPath, err)
	}
	return nil
}
