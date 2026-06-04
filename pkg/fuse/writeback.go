package fuse

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// writeBackInMemoryDataThreshold is the maximum cached write-back payload size
// retained in memory for hot reopen/read paths. Larger entries stay on disk
// only to avoid turning write-back into a large in-memory file store. Tied
// to the static defaultSmallFileThreshold rather than the server-advertised
// inline_threshold so raising the latter doesn't silently expand FUSE's
// per-mount RAM footprint.
const writeBackInMemoryDataThreshold = defaultSmallFileThreshold

// defaultWriteBackDataCacheMaxSize bounds the aggregate in-memory footprint of
// small write-back payloads retained for hot reopen/read/uploader paths.
// Entries are evicted with an LRU policy once this budget is exceeded.
const defaultWriteBackDataCacheMaxSize = 32 << 20 // 32MB

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
	// PendingChmod means file data has already been uploaded remotely, but
	// the post-upload chmod step still needs to be retried.
	PendingChmod
)

// WriteBackMeta stores metadata alongside cached file data so that the
// background uploader (and crash-recovery) knows the remote path and size.
type WriteBackMeta struct {
	Path        string      `json:"path"`
	Size        int64       `json:"size"`
	Mtime       time.Time   `json:"mtime"`
	CreatedAt   time.Time   `json:"created_at"`
	Generation  uint64      `json:"generation,omitempty"`
	Kind        PendingKind `json:"kind"`
	BaseRev     int64       `json:"base_rev,omitempty"`
	ShadowSpill bool        `json:"shadow_spill,omitempty"`
	Mode        uint32      `json:"mode,omitempty"`
	HasMode     bool        `json:"has_mode,omitempty"`
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
	// metas is the authoritative in-memory metadata index.
	// Kept in sync with disk by Put/Remove/RenamePending so that GetMeta and
	// ListPendingPaths avoid local disk I/O and JSON parsing on hot paths.
	metas map[string]*WriteBackMeta
	// data keeps small cached payloads in memory so writable reopen/read paths
	// can avoid re-reading the .dat file from disk.
	data map[string][]byte
	// dataOrder tracks recency for the in-memory payload cache.
	dataOrder *list.List
	dataElems map[string]*list.Element
	dataBytes int64
	// dataMaxBytes is the aggregate memory budget for data.
	dataMaxBytes int64
}

func (c *WriteBackCache) pruneScannedEntry(scannedMetaName string) {
	base := strings.TrimSuffix(scannedMetaName, ".meta")
	_ = os.Remove(filepath.Join(c.dir, scannedMetaName))
	_ = os.Remove(filepath.Join(c.dir, base+".dat"))
}

func (c *WriteBackCache) validateScannedMeta(scannedMetaName string, meta *WriteBackMeta) bool {
	if meta == nil || meta.Path == "" {
		return false
	}
	return scannedMetaName == hashPath(meta.Path)+".meta"
}

// NewWriteBackCache creates (or opens) a write-back cache rooted at dir.
// The directory is created if it does not exist. Existing entries on disk
// are loaded into the in-memory pending index.
func NewWriteBackCache(dir string) (*WriteBackCache, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("writeback cache dir: %w", err)
	}
	c := &WriteBackCache{
		dir:          dir,
		metas:        make(map[string]*WriteBackMeta),
		data:         make(map[string][]byte),
		dataOrder:    list.New(),
		dataElems:    make(map[string]*list.Element),
		dataMaxBytes: defaultWriteBackDataCacheMaxSize,
	}
	// Populate in-memory index from disk (crash recovery).
	entries, _ := os.ReadDir(dir)
	var maxGen uint64
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
			c.pruneScannedEntry(name)
			continue
		}
		if !c.validateScannedMeta(name, &meta) {
			c.pruneScannedEntry(name)
			continue
		}
		cp := meta
		c.metas[meta.Path] = &cp
		if meta.Generation > maxGen {
			maxGen = meta.Generation
		}
	}
	c.nextGen.Store(maxGen)
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
	return c.PutWithBaseRevAndMode(remotePath, data, size, kind, baseRev, 0, false)
}

// PutWithBaseRevAndMode is like PutWithBaseRev, but also persists the file
// permission bits that should be applied once the pending data is remote.
func (c *WriteBackCache) PutWithBaseRevAndMode(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool) error {
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
		Mode:       mode & posixPermissionModeMask,
		HasMode:    hasMode,
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
	cp := meta
	c.metas[remotePath] = &cp
	c.cacheDataLocked(remotePath, data, size)
	return nil
}

// Get reads the cached data for remotePath. Returns nil, false if not cached.
func (c *WriteBackCache) Get(remotePath string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, ok := c.getViewLocked(remotePath)
	if !ok {
		return nil, false
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	return copyData, true
}

func (c *WriteBackCache) pruneEntryLocked(remotePath string, removeDataFile bool) {
	if removeDataFile {
		_ = os.Remove(c.datFile(remotePath))
	}
	_ = os.Remove(c.metaFile(remotePath))
	delete(c.metas, remotePath)
	c.deleteDataLocked(remotePath)
}

// getViewLocked returns a read-only payload view for remotePath.
// The caller must hold c.mu and must not mutate the returned slice.
func (c *WriteBackCache) getViewLocked(remotePath string) ([]byte, bool) {
	if data, ok := c.data[remotePath]; ok {
		c.touchDataLocked(remotePath)
		return data, true
	}
	meta, ok := c.metas[remotePath]
	if !ok {
		return nil, false
	}
	data, err := os.ReadFile(c.datFile(remotePath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.pruneEntryLocked(remotePath, false)
		}
		return nil, false
	}
	c.cacheDataLocked(remotePath, data, meta.Size)
	if cached, ok := c.data[remotePath]; ok {
		return cached, true
	}
	return data, true
}

// getView returns a read-only payload view for remotePath.
// Callers must not mutate the returned slice.
func (c *WriteBackCache) getView(remotePath string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.getViewLocked(remotePath)
}

func (c *WriteBackCache) cacheDataLocked(remotePath string, data []byte, size int64) {
	if size > writeBackInMemoryDataThreshold || len(data) > writeBackInMemoryDataThreshold || c.dataMaxBytes <= 0 {
		c.deleteDataLocked(remotePath)
		return
	}
	if existing, ok := c.data[remotePath]; ok {
		c.dataBytes -= int64(len(existing))
	} else {
		c.dataElems[remotePath] = c.dataOrder.PushFront(remotePath)
	}
	stored := make([]byte, len(data))
	copy(stored, data)
	c.data[remotePath] = stored
	c.dataBytes += int64(len(stored))
	c.touchDataLocked(remotePath)
	for c.dataBytes > c.dataMaxBytes && c.dataOrder.Len() > 0 {
		tail := c.dataOrder.Back()
		if tail == nil {
			break
		}
		path, _ := tail.Value.(string)
		c.deleteDataLocked(path)
	}
}

func (c *WriteBackCache) touchDataLocked(remotePath string) {
	if elem, ok := c.dataElems[remotePath]; ok {
		c.dataOrder.MoveToFront(elem)
	}
}

func (c *WriteBackCache) deleteDataLocked(remotePath string) {
	if data, ok := c.data[remotePath]; ok {
		c.dataBytes -= int64(len(data))
		delete(c.data, remotePath)
	}
	if elem, ok := c.dataElems[remotePath]; ok {
		c.dataOrder.Remove(elem)
		delete(c.dataElems, remotePath)
	}
}

// GetMeta reads the metadata for remotePath. Returns nil, false if not cached.
func (c *WriteBackCache) GetMeta(remotePath string) (*WriteBackMeta, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta, ok := c.metas[remotePath]
	if !ok {
		return nil, false
	}
	cp := *meta
	return &cp, true
}

// Remove deletes the cached data and metadata for remotePath.
func (c *WriteBackCache) Remove(remotePath string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pruneEntryLocked(remotePath, true)
}

// UpdateMode updates the pending mode metadata for an existing cache entry.
func (c *WriteBackCache) UpdateMode(remotePath string, mode uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta, ok := c.metas[remotePath]
	if !ok {
		return nil
	}
	updated := *meta
	updated.Mode = mode & posixPermissionModeMask
	updated.HasMode = true
	updated.Generation = c.nextGen.Add(1)

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("writeback marshal mode meta: %w", err)
	}
	if err := atomicWrite(c.metaFile(remotePath), metaBytes); err != nil {
		return fmt.Errorf("writeback update mode meta: %w", err)
	}
	cp := updated
	c.metas[remotePath] = &cp
	return nil
}

// MarkChmodPending records that data has reached the remote server and only
// the chmod step remains. The generation guard prevents an old upload from
// overwriting metadata for a newer local write.
func (c *WriteBackCache) MarkChmodPending(remotePath string, generation uint64) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	meta, ok := c.metas[remotePath]
	if !ok || meta.Generation != generation {
		return false, nil
	}
	updated := *meta
	updated.Kind = PendingChmod
	updated.BaseRev = 0
	updated.Generation = c.nextGen.Add(1)

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return false, fmt.Errorf("writeback marshal chmod-pending meta: %w", err)
	}
	if err := atomicWrite(c.metaFile(remotePath), metaBytes); err != nil {
		return false, fmt.Errorf("writeback update chmod-pending meta: %w", err)
	}
	cp := updated
	c.metas[remotePath] = &cp
	return true, nil
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

	meta, ok := c.metas[oldPath]
	if !ok {
		return false
	}
	updated := *meta

	// Rename .dat file directly — same directory, no data copy needed.
	newDat := c.datFile(newPath)
	if err := os.Rename(oldDat, newDat); err != nil {
		return false
	}

	// Write new .meta with updated path and fresh generation.
	updated.Path = newPath
	updated.Generation = c.nextGen.Add(1)
	metaBytes, err := json.Marshal(updated)
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
	delete(c.metas, oldPath)
	cp := updated
	c.metas[newPath] = &cp
	if data, ok := c.data[oldPath]; ok {
		c.deleteDataLocked(oldPath)
		c.deleteDataLocked(newPath)
		c.data[newPath] = data
		c.dataBytes += int64(len(data))
		c.dataElems[newPath] = c.dataOrder.PushFront(newPath)
		for c.dataBytes > c.dataMaxBytes && c.dataOrder.Len() > 0 {
			tail := c.dataOrder.Back()
			if tail == nil {
				break
			}
			path, _ := tail.Value.(string)
			c.deleteDataLocked(path)
		}
	}
	return true
}

// ListPendingPaths returns the set of remote paths that have pending entries.
// Returns the in-memory index directly (O(1) copy) instead of scanning disk.
func (c *WriteBackCache) ListPendingPaths() map[string]struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.metas) == 0 {
		return nil
	}
	// Return a copy so callers can iterate without holding the lock.
	result := make(map[string]struct{}, len(c.metas))
	for k := range c.metas {
		result[k] = struct{}{}
	}
	return result
}

// ListByPrefix returns metadata for pending entries under prefix.
func (c *WriteBackCache) ListByPrefix(prefix string) []*WriteBackMeta {
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []*WriteBackMeta
	for p, meta := range c.metas {
		if strings.HasPrefix(p, prefix) {
			cp := *meta
			result = append(result, &cp)
		}
	}
	return result
}

// PendingEntry represents one file waiting to be uploaded.
type PendingEntry struct {
	Meta WriteBackMeta
	Data []byte
}

// ListPending returns all pending entries using the in-memory metadata index.
// It also reconciles any on-disk .meta files not yet in memory so crash
// recovery and corruption cleanup keep the legacy behavior on this cold path.
func (c *WriteBackCache) ListPending() []PendingEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	entries, err := os.ReadDir(c.dir)
	if err == nil {
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
				c.pruneScannedEntry(name)
				continue
			}
			if !c.validateScannedMeta(name, &meta) {
				c.pruneScannedEntry(name)
				continue
			}
			if _, ok := c.metas[meta.Path]; !ok {
				cp := meta
				c.metas[meta.Path] = &cp
				if meta.Generation > c.nextGen.Load() {
					c.nextGen.Store(meta.Generation)
				}
			}
		}
	}

	if len(c.metas) == 0 {
		return nil
	}

	result := make([]PendingEntry, 0, len(c.metas))
	for path, meta := range c.metas {
		if meta.Kind == PendingConflict {
			continue
		}
		datPath := c.datFile(path)
		data, err := os.ReadFile(datPath)
		if err != nil {
			// Data file missing — remove orphaned metadata.
			c.pruneEntryLocked(path, false)
			continue
		}

		c.cacheDataLocked(path, data, meta.Size)
		result = append(result, PendingEntry{Meta: *meta, Data: data})
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
// different server+mountpoint+remoteRoot combinations.
//
// For backward compatibility, when remoteRoot is "/" (the default), the hash
// is computed without the remoteRoot segment, matching the pre-subtree-mount
// hash: sha256(server + "\x00" + mountPoint). This ensures existing caches
// are not orphaned on upgrade.
func MountHash(serverURL, mountPoint string, remoteRoot ...string) string {
	root := "/"
	if len(remoteRoot) > 0 && remoteRoot[0] != "" {
		root = remoteRoot[0]
	}
	var input string
	if root == "/" {
		// Backward-compatible: same hash as pre-subtree-mount versions.
		input = serverURL + "\x00" + mountPoint
	} else {
		input = serverURL + "\x00" + mountPoint + "\x00" + root
	}
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:8]) // 16 hex chars
}

// MountReadCacheHash computes the persistent read-cache namespace. Unlike the
// write-back cache namespace, it includes a credential digest because read-cache
// entries contain remote file bytes and can survive remounts.
func MountReadCacheHash(serverURL, mountPoint, remoteRoot, credentialKind, credential string) string {
	if remoteRoot == "" {
		remoteRoot = "/"
	}
	credentialDigest := sha256.Sum256([]byte(credentialKind + "\x00" + credential))
	input := serverURL + "\x00" + mountPoint + "\x00" + remoteRoot + "\x00" + credentialKind + "\x00" + hex.EncodeToString(credentialDigest[:])
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:8])
}
