package staging

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// RetireFixedUploadGeneration durably retires only a remotely confirmed pending
// generation. The index must already be recovered. A newer generation is left
// intact, including after an empty index is reopened: the retired generation
// floor is persisted before deleting metadata. Unlike best-effort cleanup, errors
// keep the completion's WAL
// record outstanding so the caller can retry before SettleFixedUpload.
// The bool reports whether this call owned the matching pending metadata.
func (idx *PendingIndex) RetireFixedUploadGeneration(remotePath string, generation uint64) (bool, error) {
	if generation == 0 {
		return false, fmt.Errorf("fixed upload cleanup requires a generation")
	}
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)
	return idx.retireFixedUploadGenerationLocked(remotePath, generation)
}

func (idx *PendingIndex) retireFixedUploadGenerationLocked(remotePath string, generation uint64) (bool, error) {
	idx.mu.RLock()
	current, exists := idx.items[remotePath]
	journal := idx.journal
	matches := exists && current.Generation == generation
	idx.mu.RUnlock()
	// Even a skipped cleanup must reserve the old identity until settlement:
	// the newer entry may itself disappear before a crash and old replay.
	if err := idx.fixedFloor.advance(idx.dir, generation, &idx.nextGen); err != nil {
		return false, err
	}
	if exists && !matches {
		return false, nil
	}
	// Publish under the path lock, so no newer pending frame can precede this
	// marker. Even a retry with absent metadata must fence older WAL frames.
	if journal != nil {
		if err := journal.Append(JournalEntry{Op: JournalCommit, Path: remotePath}); err != nil {
			return false, err
		}
		if err := journal.FsyncShared(); err != nil {
			return false, err
		}
	}
	if err := os.Remove(filepath.Join(idx.dir, HashPath(remotePath)+".meta")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := fsyncDir(idx.dir); err != nil {
		return false, err
	}
	idx.mu.Lock()
	delete(idx.items, remotePath)
	idx.mu.Unlock()
	return matches, nil
}

// RetireFixedUploadGeneration removes only the confirmed cache generation and
// synchronizes the deletion before the caller settles its completion record.
func (c *WriteBackCache) RetireFixedUploadGeneration(remotePath string, generation uint64) (bool, error) {
	if generation == 0 {
		return false, fmt.Errorf("fixed upload cleanup requires a generation")
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)
	c.mu.Lock()
	defer c.mu.Unlock()
	meta, exists := c.metas[remotePath]
	if err := c.fixedFloor.advance(c.dir, generation, &c.nextGen); err != nil {
		return false, err
	}
	if exists && meta.Generation != generation {
		return false, nil
	}
	for _, file := range []string{c.MetaFile(remotePath), c.DatFile(remotePath)} {
		if err := os.Remove(file); err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if err := fsyncDir(c.dir); err != nil {
		return false, err
	}
	delete(c.metas, remotePath)
	c.deleteDataLocked(remotePath)
	return exists, nil
}

// RetireFixedShadowUpload serializes the two local owners. A later pending
// generation may still depend on the shadow, so neither is removed in that case.
func (idx *PendingIndex) RetireFixedShadowUpload(path string, generation uint64, shadows *ShadowStore, token string) (bool, error) {
	if generation == 0 || shadows == nil {
		return false, fmt.Errorf("fixed shadow cleanup requires its owners")
	}
	pl := idx.acquirePathLock(path)
	defer idx.releasePathLock(path, pl)
	if err := idx.fixedFloor.advance(idx.dir, generation, &idx.nextGen); err != nil {
		return false, err
	}
	idx.mu.RLock()
	current, exists := idx.items[path]
	newer := exists && current.Generation != generation
	idx.mu.RUnlock()
	if newer {
		return false, nil
	}
	retired, err := shadows.RetireFixedShadow(path, token)
	if err != nil {
		return false, err
	}
	if !retired {
		return false, fmt.Errorf("fixed shadow changed without matching pending metadata")
	}
	if _, err := idx.retireFixedUploadGenerationLocked(path, generation); err != nil {
		return false, err
	}
	return true, nil
}
