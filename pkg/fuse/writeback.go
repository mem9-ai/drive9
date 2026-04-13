package fuse

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// writeBackThreshold is the maximum file size that will be written to the
// local write-back cache during Flush. Files larger than this are uploaded
// directly (streaming or multipart) to avoid filling local disk.
const writeBackThreshold = 10 << 20 // 10MB

// PendingKind distinguishes newly created files from overwrites of existing
// remote files. This affects Rename behaviour: a pending-new file can be
// renamed locally (fast path), while a pending-overwrite must be uploaded
// before the remote rename so the server-side object exists.
type PendingKind int

const (
	// PendingNew means the file was created locally and has never existed on
	// the remote server.  Rename can use the local fast path.
	PendingNew PendingKind = iota
	// PendingOverwrite means the file already existed on the remote server
	// and local edits have been cached.  Rename must upload first.
	PendingOverwrite
	// PendingConflict means the upload failed terminally (conflict or max
	// retries). The local data is preserved for manual recovery but will
	// not be re-enqueued by RecoverPending.
	PendingConflict
)

// WriteBackMeta stores metadata alongside cached file data so that the
// background uploader (and crash-recovery) knows the remote path and size.
type WriteBackMeta struct {
	Path       string      `json:"path"`
	Size       int64       `json:"size"`
	Mtime      time.Time   `json:"mtime"`
	CreatedAt  time.Time   `json:"created_at"`
	Generation uint64      `json:"generation,omitempty"`
	Kind       PendingKind `json:"kind"`
	BaseRev    int64       `json:"base_rev,omitempty"`
}

// WriteBackCache manages a local disk cache of pending (not-yet-uploaded)
// file data. Each entry consists of a .dat file (raw data) and a .meta file
// (JSON metadata). Writes are atomic: data is written to a temp file and
// renamed into place. The .dat file is fsync'd before rename to survive
// power loss.
type WriteBackCache struct {
	dir     string // e.g. ~/.cache/drive9/{mount-hash}/pending
	mu      sync.Mutex
	nextGen atomic.Uint64
	// pending is an in-memory index of remote paths with cache entries.
	// Kept in sync with disk by Put/Remove/RenamePending so that
	// ListPendingPaths avoids scanning the directory each time.
	pending map[string]struct{}
}

// NewWriteBackCache creates (or opens) a write-back cache rooted at dir.
// The directory is created if it does not exist. Existing entries on disk
// are loaded into the in-memory pending index.
func NewWriteBackCache(dir string) (*WriteBackCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("writeback cache dir: %w", err)
	}
	c := &WriteBackCache{dir: dir, pending: make(map[string]struct{})}
	// Populate in-memory index from disk (crash recovery).
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".meta") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		var meta WriteBackMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			continue
		}
		c.pending[meta.Path] = struct{}{}
	}
	return c, nil
}

// hashPath returns a stable, filesystem-safe key for the remote path.
func hashPath(remotePath string) string {
	h := sha256.Sum256([]byte(remotePath))
	return hex.EncodeToString(h[:])
}

// datFile returns the path to the .dat file for a remote path.
func (c *WriteBackCache) datFile(remotePath string) string {
	return filepath.Join(c.dir, hashPath(remotePath)+".dat")
}

// metaFile returns the path to the .meta file for a remote path.
func (c *WriteBackCache) metaFile(remotePath string) string {
	return filepath.Join(c.dir, hashPath(remotePath)+".meta")
}

// Put atomically writes data and metadata for remotePath into the cache.
// The data file is fsync'd before the atomic rename to ensure durability.
// kind indicates whether this is a newly created file or an overwrite of an
// existing remote file (affects Rename fast-path eligibility).
func (c *WriteBackCache) Put(remotePath string, data []byte, size int64, kind PendingKind) error {
	return c.PutWithBaseRev(remotePath, data, size, kind, 0)
}

// PutWithBaseRev is like Put, but also persists the remote base revision used
// for CAS-protected overwrite uploads. baseRev is ignored for PendingNew.
func (c *WriteBackCache) PutWithBaseRev(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Write .dat via temp + fsync + rename (atomic).
	datPath := c.datFile(remotePath)
	if err := atomicWrite(datPath, data); err != nil {
		return fmt.Errorf("writeback put data: %w", err)
	}

	// Write .meta via temp + rename.
	meta := WriteBackMeta{
		Path:       remotePath,
		Size:       size,
		Mtime:      time.Now(),
		CreatedAt:  time.Now(),
		Generation: c.nextGen.Add(1),
		Kind:       kind,
		BaseRev:    baseRev,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("writeback marshal meta: %w", err)
	}
	metaPath := c.metaFile(remotePath)
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		// Best-effort cleanup of the .dat file if meta write fails.
		_ = os.Remove(datPath)
		return fmt.Errorf("writeback put meta: %w", err)
	}
	c.pending[remotePath] = struct{}{}
	return nil
}

// Get reads the cached data for remotePath. Returns nil, false if not cached.
func (c *WriteBackCache) Get(remotePath string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := os.ReadFile(c.datFile(remotePath))
	if err != nil {
		return nil, false
	}
	return data, true
}

// GetMeta reads the metadata for remotePath. Returns nil, false if not cached.
func (c *WriteBackCache) GetMeta(remotePath string) (*WriteBackMeta, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	raw, err := os.ReadFile(c.metaFile(remotePath))
	if err != nil {
		return nil, false
	}
	var meta WriteBackMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil, false
	}
	return &meta, true
}

// Remove deletes the cached data and metadata for remotePath.
func (c *WriteBackCache) Remove(remotePath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	_ = os.Remove(c.datFile(remotePath))
	_ = os.Remove(c.metaFile(remotePath))
	delete(c.pending, remotePath)
}

// RenamePending atomically moves a pending cache entry from oldPath to newPath.
// The .dat file is moved via os.Rename (no data copy); only the .meta is
// rewritten with the new path and a fresh generation.
// Returns true if there was a pending entry to rename.
func (c *WriteBackCache) RenamePending(oldPath, newPath string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	oldDat := c.datFile(oldPath)
	oldMeta := c.metaFile(oldPath)

	// Read existing meta.
	raw, err := os.ReadFile(oldMeta)
	if err != nil {
		return false
	}
	var meta WriteBackMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return false
	}

	// Rename .dat file directly — same directory, no data copy needed.
	newDat := c.datFile(newPath)
	if err := os.Rename(oldDat, newDat); err != nil {
		return false
	}

	// Write new .meta with updated path and fresh generation.
	meta.Path = newPath
	meta.Generation = c.nextGen.Add(1)
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		_ = os.Rename(newDat, oldDat)
		return false
	}
	newMeta := c.metaFile(newPath)
	if err := atomicWrite(newMeta, metaBytes); err != nil {
		// Restore .dat to old location on meta-write failure.
		_ = os.Rename(newDat, oldDat)
		return false
	}

	// Remove old meta and update index.
	_ = os.Remove(oldMeta)
	delete(c.pending, oldPath)
	c.pending[newPath] = struct{}{}
	return true
}

// ListPendingPaths returns the set of remote paths that have pending entries.
// Returns the in-memory index directly (O(1) copy) instead of scanning disk.
func (c *WriteBackCache) ListPendingPaths() map[string]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.pending) == 0 {
		return nil
	}
	// Return a copy so callers can iterate without holding the lock.
	result := make(map[string]struct{}, len(c.pending))
	for k := range c.pending {
		result[k] = struct{}{}
	}
	return result
}

// PendingEntry represents one file waiting to be uploaded.
type PendingEntry struct {
	Meta WriteBackMeta
	Data []byte
}

// ListPending scans the cache directory and returns all pending entries.
// Entries with missing or corrupt metadata are skipped (and cleaned up).
func (c *WriteBackCache) ListPending() []PendingEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil
	}

	var result []PendingEntry
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".meta") {
			continue
		}
		metaPath := filepath.Join(c.dir, name)
		raw, err := os.ReadFile(metaPath)
		if err != nil {
			continue
		}
		var meta WriteBackMeta
		if err := json.Unmarshal(raw, &meta); err != nil {
			// Corrupt meta — remove both files.
			base := strings.TrimSuffix(name, ".meta")
			_ = os.Remove(metaPath)
			_ = os.Remove(filepath.Join(c.dir, base+".dat"))
			continue
		}

		datPath := filepath.Join(c.dir, strings.TrimSuffix(name, ".meta")+".dat")
		data, err := os.ReadFile(datPath)
		if err != nil {
			// Data file missing — remove orphaned meta.
			_ = os.Remove(metaPath)
			continue
		}

		result = append(result, PendingEntry{Meta: meta, Data: data})
	}
	return result
}

// atomicWrite writes data to path atomically using a temp file + fsync + rename.
func atomicWrite(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()

	_, writeErr := tmp.Write(data)
	if writeErr != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return writeErr
	}

	// Fsync to ensure data reaches disk before rename.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	// Atomic rename.
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}

	// Fsync parent directory so the rename is durable across power loss.
	return fsyncDir(dir)
}

// fsyncDir fsyncs a directory to ensure directory entry changes (renames)
// are persisted to disk.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

// MountHash computes a short hash to distinguish cache directories for
// different server+mountpoint combinations.
func MountHash(serverURL, mountPoint string) string {
	h := sha256.Sum256([]byte(serverURL + "\x00" + mountPoint))
	return hex.EncodeToString(h[:8]) // 16 hex chars
}
