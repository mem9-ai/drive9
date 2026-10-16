// Mount restore: replay the layer log into the overlay, and the rollback
// that abandons it.
package layer

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fs"
)

// Restore replays the layer log and rebuilds the overlay with the
// supersede rules. File entries are fetched at their pinned sequence and
// S3-stored bodies stream into the overlay instead of the replay list's
// inline bytes (which large entries never carry).
func (l *Layer) Restore(ctx context.Context) error {
	var entries []client.FSLayerEntry
	var err error
	checkpointSeq := int64(0)
	hasCheckpoint := false
	if l.checkpoint != "" {
		cp, cerr := l.cl.GetFSLayerCheckpoint(ctx, l.checkpoint)
		if cerr != nil {
			return fmt.Errorf("layer: read checkpoint %s: %w", l.checkpoint, cerr)
		}
		// The mount's layer ref may be a name/tag; the checkpoint carries
		// the layer ID. Resolve the ref once (v1 layer status) so the
		// ownership check compares IDs, not name-vs-id (which rejected every
		// mount, or with a foreign checkpoint slipped past unresolved).
		wantLayer := l.layerRef
		if st, serr := l.cl.GetFSLayer(ctx, l.layerRef); serr == nil && st != nil && st.LayerID != "" {
			wantLayer = st.LayerID
		}
		if cp.LayerID != wantLayer {
			return fmt.Errorf("layer: checkpoint %s belongs to layer %s, want %s", l.checkpoint, cp.LayerID, wantLayer)
		}
		checkpointSeq = cp.DurableSeq
		hasCheckpoint = true
		entries, err = l.cl.ReplayFSLayerAtSeq(ctx, l.layerRef, checkpointSeq)
	} else {
		entries, err = l.cl.ReplayFSLayer(ctx, l.layerRef)
	}
	if err != nil {
		return err
	}
	var max int64
	for _, entry := range replayContent(entries) {
		if err := l.applyReplayEntry(ctx, entry, hasCheckpoint, checkpointSeq); err != nil {
			return err
		}
		if entry.EntrySeq > max {
			max = entry.EntrySeq
		}
	}
	l.mu.Lock()
	l.lastSeq = max
	l.mu.Unlock()
	return nil
}

// replayContent drops overwritten payloads, keeping namespace operations
// and the last body before chmod; a rename can consume an earlier snapshot
// (including descendants of a directory), so it is a dependency barrier.
func replayContent(entries []client.FSLayerEntry) []client.FSLayerEntry {
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

// layerEntryFetchMaxSeq pins entry fetches: checkpoint restores must stay
// at the checkpoint tip (ancestor rows can carry a larger entry_seq than a
// child tip; using that seq against the child layer would leak later child
// writes into the view), live restores fetch at the entry's own sequence.
func layerEntryFetchMaxSeq(entry *client.FSLayerEntry, hasCheckpoint bool, checkpointMaxSeq int64) *int64 {
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

func (l *Layer) getLayerEntryForRestore(ctx context.Context, path string, maxSeq *int64) (*client.FSLayerEntry, error) {
	if maxSeq != nil {
		return l.cl.GetFSLayerEntryAtSeq(ctx, l.layerRef, path, *maxSeq)
	}
	return l.cl.GetFSLayerEntry(ctx, l.layerRef, path)
}

// layerPathInsideRoot reports whether the layer-wide absolute path lies
// inside the mount root with a segment boundary (/team matches /team/x but
// not /team2/x).
func layerPathInsideRoot(p, root string) bool {
	if root == "/" || p == root {
		return true
	}
	return strings.HasPrefix(p, strings.TrimSuffix(root, "/")+"/")
}

func (l *Layer) applyReplayEntry(ctx context.Context, e client.FSLayerEntry, hasCheckpoint bool, checkpointSeq int64) error {
	// Replay entries carry layer-wide absolute paths; a subtree mount
	// (/team) must convert with a boundary check and SKIP everything
	// outside its root — TrimPrefix alone left /secret intact (visible
	// inside the /team mount) and mishandled /team2-style collisions.
	if !layerPathInsideRoot(e.Path, l.root) {
		return nil
	}
	local := l.localPathOf(e.Path)
	maxSeq := layerEntryFetchMaxSeq(&e, hasCheckpoint, checkpointSeq)
	switch e.Op {
	case "whiteout":
		l.markLayerWhiteout(local)
		return nil
	case "mkdir":
		l.markLayerDir(local, e.Mode)
		return nil
	case "symlink":
		target := strings.TrimSpace(e.ContentText)
		if target == "" && len(e.Content) > 0 {
			target = string(e.Content)
		}
		if target == "" {
			full, err := l.getLayerEntryForRestore(ctx, e.Path, maxSeq)
			if err != nil {
				return fmt.Errorf("layer: restore symlink entry %s: %w", e.Path, err)
			}
			target = strings.TrimSpace(full.ContentText)
			if target == "" && len(full.Content) > 0 {
				target = string(full.Content)
			}
		}
		if target == "" {
			return fmt.Errorf("layer: restore symlink entry %s: missing target", e.Path)
		}
		l.markLayerSymlink(local, target, e.Mode)
		return nil
	case "rename":
		return l.replayRename(ctx, e, local, maxSeq)
	case "chmod":
		switch {
		case func() bool { _, ok := l.dirs[local]; return ok }():
			l.markLayerDir(local, e.Mode)
		case func() bool { _, ok := l.symlinks[local]; return ok }():
			if st, ok := l.symlinks[local]; ok {
				nextMode := (st.Mode &^ uint32(0o777)) | (e.Mode & 0o777)
				l.markLayerSymlink(local, st.Target, nextMode)
			}
		case func() bool { _, ok := l.contents[local]; return ok }():
			// A content image exists in the overlay: the chmod re-marks it.
			l.markLayerFileMode(local, e.Mode)
		default:
			// Base-backed file (content still from the base): a
			// markLayerFileMode here would put the path in l.files with
			// no content, and lookups would report an empty file — the
			// mode-only override keeps the base as the content source.
			l.markLayerBaseFileMode(local, e.Mode)
		}
		return nil
	case "upsert":
		switch e.Kind {
		case "file":
			return l.replayUpsert(ctx, e, local, maxSeq)
		case "dir":
			l.markLayerDir(local, e.Mode)
		}
	}
	return nil
}

// replayUpsert installs one replayed file entry: the authoritative body is
// the full entry at the pinned sequence — S3-stored entries stream their
// bytes, inline entries carry them.
func (l *Layer) replayUpsert(ctx context.Context, e client.FSLayerEntry, local string, maxSeq *int64) error {
	full, err := l.getLayerEntryForRestore(ctx, e.Path, maxSeq)
	if err != nil {
		return fmt.Errorf("layer: restore entry %s: %w", e.Path, err)
	}
	var content []byte
	if full.StorageRef != "" || full.StorageType == "s3" {
		rc, serr := l.cl.ReadFSLayerFileStream(ctx, l.layerRef, e.Path, maxSeq)
		if serr != nil {
			return fmt.Errorf("layer: restore object %s: %w", e.Path, serr)
		}
		content, err = io.ReadAll(rc)
		closeErr := rc.Close()
		if err != nil {
			return fmt.Errorf("layer: restore object %s: %w", e.Path, err)
		}
		if closeErr != nil {
			return fmt.Errorf("layer: restore object %s: %w", e.Path, closeErr)
		}
	} else {
		// Empty inline content is still authoritative layer data; text
		// bodies ride content_text.
		content = append([]byte{}, full.Content...)
		if len(content) == 0 && full.ContentText != "" {
			content = []byte(full.ContentText)
		}
	}
	if full.SizeBytes > 0 && int64(len(content)) != full.SizeBytes {
		return fmt.Errorf("layer: restore object %s: copied %d bytes, want %d", e.Path, len(content), full.SizeBytes)
	}
	mode := full.Mode
	if mode == 0 {
		mode = e.Mode
	}
	l.mu.Lock()
	l.files[local] = mode & 0o777
	l.contents[local] = append([]byte(nil), content...)
	delete(l.whiteouts, local)
	delete(l.dirs, local)
	delete(l.symlinks, local)
	l.mu.Unlock()
	return nil
}

// replayRename moves the overlay image of a renamed path. Overlay content
// moves directly; when neither side carries it, the source's base object
// hydrates the destination once (the destination's own upsert entry, if
// any, replays later and wins).
func (l *Layer) replayRename(ctx context.Context, e client.FSLayerEntry, oldLocal string, maxSeq *int64) error {
	full := &e
	if strings.TrimSpace(e.ContentText) == "" && len(e.Content) == 0 {
		fetched, err := l.getLayerEntryForRestore(ctx, e.Path, maxSeq)
		if err != nil {
			return fmt.Errorf("layer: restore rename entry %s: %w", e.Path, err)
		}
		full = fetched
	}
	targetRemote := full.ContentText
	if targetRemote == "" && len(full.Content) > 0 {
		targetRemote = string(full.Content)
	}
	if targetRemote == "" {
		return fmt.Errorf("layer: restore rename entry %s: missing target", e.Path)
	}
	l.markLayerWhiteout(oldLocal)
	newLocal := targetRemote
	if l.root != "/" {
		if !strings.HasPrefix(newLocal, l.root) {
			return nil
		}
		newLocal = strings.TrimPrefix(newLocal, l.root)
	}
	l.mu.Lock()
	data, moved := l.contents[oldLocal]
	mode, hasMode := l.files[oldLocal]
	if moved {
		delete(l.contents, oldLocal)
		delete(l.files, oldLocal)
		l.contents[newLocal] = data
		if hasMode {
			l.files[newLocal] = mode
		}
	}
	l.mu.Unlock()
	if moved {
		return nil
	}
	if _, exists := l.contents[newLocal]; exists {
		return nil
	}
	if full.Kind == "dir" {
		l.markLayerDir(newLocal, full.Mode)
		return nil
	}
	st, err := l.cl.StatCtx(ctx, e.Path)
	if err != nil {
		if client.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("layer: restore renamed source %s: %w", e.Path, err)
	}
	if st.IsDir {
		return nil
	}
	data, err = l.cl.ReadCtx(ctx, e.Path)
	if err != nil {
		return fmt.Errorf("layer: restore renamed source %s: %w", e.Path, err)
	}
	l.mu.Lock()
	l.files[newLocal] = st.Mode & 0o777
	l.contents[newLocal] = append([]byte(nil), data...)
	l.mu.Unlock()
	return nil
}

// Rollback reacts to a rollback event: the overlay clears so the base view
// reappears, and every later write fails with the rolled-back error.
func (l *Layer) Rollback() {
	l.mu.Lock()
	l.abandoned = true
	l.whiteouts = map[string]struct{}{}
	l.files = map[string]uint32{}
	l.dirs = map[string]uint32{}
	l.symlinks = map[string]symlinkState{}
	l.contents = map[string][]byte{}
	hooks := append([]func(){}, l.onRollback...)
	l.mu.Unlock()
	for _, f := range hooks {
		f()
	}
	l.emit(fs.Event{Kind: fs.EventCheckout, Path: "/"})
}
