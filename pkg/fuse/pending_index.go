package fuse

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PendingIndex is an in-memory authoritative index for pending file metadata.
// All metadata reads are served from memory (O(1), no disk I/O, no JSON parse).
// Disk writes happen on Put/Remove/Rename for durability and crash recovery.
type PendingIndex struct {
	mu      sync.RWMutex
	items   map[string]*WriteBackMeta // path → metadata
	dir     string                    // directory for .meta persistence
	nextGen atomic.Uint64
}

// NewPendingIndex creates a PendingIndex backed by the given directory.
// The directory is created if it does not exist.
func NewPendingIndex(dir string) (*PendingIndex, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("pending index dir: %w", err)
	}
	idx := &PendingIndex{
		items: make(map[string]*WriteBackMeta),
		dir:   dir,
	}
	return idx, nil
}

// RecoverFromDisk scans .meta files in the directory and rebuilds in-memory
// state. Called once at startup for crash recovery.
func (idx *PendingIndex) RecoverFromDisk() error {
	entries, err := os.ReadDir(idx.dir)
	if err != nil {
		return fmt.Errorf("pending index recovery: %w", err)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	var maxGen uint64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".meta") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(idx.dir, name))
		if err != nil {
			continue
		}
		var meta WriteBackMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			// Corrupt meta — remove it.
			_ = os.Remove(filepath.Join(idx.dir, name))
			continue
		}
		cp := meta
		idx.items[meta.Path] = &cp
		if meta.Generation > maxGen {
			maxGen = meta.Generation
		}
	}
	idx.nextGen.Store(maxGen)
	return nil
}

// Put stores metadata for the given path in both memory and disk.
func (idx *PendingIndex) Put(remotePath string, size int64, kind PendingKind) (uint64, error) {
	return idx.PutWithBaseRev(remotePath, size, kind, 0)
}

// PutWithBaseRev stores metadata for the given path together with the base
// revision observed when the local edit session started.
func (idx *PendingIndex) PutWithBaseRev(remotePath string, size int64, kind PendingKind, baseRev int64) (uint64, error) {
	gen := idx.nextGen.Add(1)
	meta := &WriteBackMeta{
		Path:       remotePath,
		Size:       size,
		Mtime:      time.Now(),
		CreatedAt:  time.Now(),
		Generation: gen,
		Kind:       kind,
		BaseRev:    baseRev,
	}

	// Write to disk first for durability.
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return 0, fmt.Errorf("pending index marshal: %w", err)
	}
	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return 0, fmt.Errorf("pending index put meta: %w", err)
	}

	idx.mu.Lock()
	idx.items[remotePath] = meta
	idx.mu.Unlock()

	return gen, nil
}

// GetMeta reads metadata from memory only. O(1), no disk I/O.
func (idx *PendingIndex) GetMeta(remotePath string) (*WriteBackMeta, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	meta, ok := idx.items[remotePath]
	if !ok {
		return nil, false
	}
	cp := *meta
	return &cp, true
}

// HasPending reports whether a pending entry exists for the path.
func (idx *PendingIndex) HasPending(remotePath string) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	_, ok := idx.items[remotePath]
	return ok
}

// Remove deletes metadata for the given path from memory and disk.
func (idx *PendingIndex) Remove(remotePath string) {
	idx.mu.Lock()
	delete(idx.items, remotePath)
	idx.mu.Unlock()

	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	_ = os.Remove(metaPath)
}

// Generation returns the current generation for a path, or 0 if not found.
func (idx *PendingIndex) Generation(remotePath string) uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if meta, ok := idx.items[remotePath]; ok {
		return meta.Generation
	}
	return 0
}

// RenamePending atomically moves a pending entry from oldPath to newPath.
// Returns true if there was a pending entry to rename.
func (idx *PendingIndex) RenamePending(oldPath, newPath string) bool {
	idx.mu.RLock()
	meta, ok := idx.items[oldPath]
	if !ok {
		idx.mu.RUnlock()
		return false
	}
	// Copy fields under read lock.
	gen := idx.nextGen.Add(1)
	newMeta := &WriteBackMeta{
		Path:       newPath,
		Size:       meta.Size,
		Mtime:      meta.Mtime,
		CreatedAt:  meta.CreatedAt,
		Generation: gen,
		Kind:       meta.Kind,
		BaseRev:    meta.BaseRev,
	}
	idx.mu.RUnlock()

	// Persist new meta to disk BEFORE updating memory so that crash
	// recovery always has a consistent view.
	metaBytes, _ := json.Marshal(newMeta)
	newMetaPath := filepath.Join(idx.dir, hashPath(newPath)+".meta")
	if err := atomicWrite(newMetaPath, metaBytes); err != nil {
		return false
	}

	idx.mu.Lock()
	delete(idx.items, oldPath)
	idx.items[newPath] = newMeta
	idx.mu.Unlock()

	oldMetaPath := filepath.Join(idx.dir, hashPath(oldPath)+".meta")
	_ = os.Remove(oldMetaPath)

	return true
}

// ListPendingPaths returns the set of remote paths that have pending entries.
func (idx *PendingIndex) ListPendingPaths() map[string]struct{} {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if len(idx.items) == 0 {
		return nil
	}
	result := make(map[string]struct{}, len(idx.items))
	for k := range idx.items {
		result[k] = struct{}{}
	}
	return result
}

// ListByPrefix returns metadata for all paths with the given prefix.
func (idx *PendingIndex) ListByPrefix(prefix string) []*WriteBackMeta {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var result []*WriteBackMeta
	for p, meta := range idx.items {
		if strings.HasPrefix(p, prefix) {
			cp := *meta
			result = append(result, &cp)
		}
	}
	return result
}

// UpdateSize updates only the size field for an existing entry.
func (idx *PendingIndex) UpdateSize(remotePath string, size int64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	if meta, ok := idx.items[remotePath]; ok {
		meta.Size = size
		meta.Mtime = time.Now()
	}
}

// MarkConflict marks a pending entry as conflicted so that RecoverPending
// skips it on restart. The entry is kept on disk for manual recovery.
func (idx *PendingIndex) MarkConflict(remotePath string) {
	idx.mu.Lock()
	meta, ok := idx.items[remotePath]
	if !ok {
		idx.mu.Unlock()
		return
	}
	meta.Kind = PendingConflict
	idx.mu.Unlock()

	// Persist to disk so crash recovery also sees the conflict marker.
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return
	}
	metaPath := filepath.Join(idx.dir, hashPath(remotePath)+".meta")
	_ = atomicWrite(metaPath, metaBytes)
}

// Count returns the number of pending entries.
func (idx *PendingIndex) Count() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.items)
}
