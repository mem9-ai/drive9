package staging

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// RetireFixedUploadGeneration durably retires only a remotely confirmed pending
// generation. The index must already be recovered. A newer generation is left
// intact. Unlike ordinary best-effort cleanup, errors keep the completion's WAL
// record outstanding so the caller can retry before SettleFixedUpload.
// The bool reports whether this call owned the matching pending metadata.
func (idx *PendingIndex) RetireFixedUploadGeneration(remotePath string, generation uint64) (bool, error) {
	if generation == 0 {
		return false, fmt.Errorf("fixed upload cleanup requires a generation")
	}
	pl := idx.acquirePathLock(remotePath)
	defer idx.releasePathLock(remotePath, pl)
	idx.mu.RLock()
	current, exists := idx.items[remotePath]
	journal := idx.journal
	matches := exists && current.Generation == generation
	idx.mu.RUnlock()
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
