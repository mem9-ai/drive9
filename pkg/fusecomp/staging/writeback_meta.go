package staging

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/membudget"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

var errSyncfsUnsupported = errors.New("syncfs unsupported")

// local write-back cache during Flush. Files larger than this are uploaded
// directly (streaming or multipart) to avoid filling local disk.

// WriteBackInMemoryDataThreshold re-exports the in-memory small-payload
// cutoff for the ported uploader tests.
const WriteBackInMemoryDataThreshold = writeBackInMemoryDataThreshold

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

// writeBackInMemoryDataThreshold is the maximum cached write-back payload size
// retained in memory for hot reopen/read paths. Larger entries stay on disk
// only to avoid turning write-back into a large in-memory file store. Tied
// to the static writebuffer.SmallFileThreshold rather than the server-advertised
// inline_threshold so raising the latter doesn't silently expand FUSE's
// per-mount RAM footprint.
const writeBackInMemoryDataThreshold = writebuffer.SmallFileThreshold

// defaultWriteBackDataCacheMaxSize bounds the aggregate in-memory footprint of
// small write-back payloads retained for hot reopen/read/uploader paths.
// Entries are evicted with an LRU policy once this budget is exceeded.
const defaultWriteBackDataCacheMaxSize = 32 << 20 // 32MB

// PathLockEntry is the write-back cache's refcounted per-path mutex
// (write serialization for one path's .dat/.meta pair).
type PathLockEntry struct {
	mu      sync.Mutex
	waiters int32 // number of goroutines that have acquired or are waiting
}

// WriteBackCache manages a local disk cache of pending (not-yet-uploaded)
// file data. Each entry consists of a .dat file (raw data) and a .meta file
// (JSON metadata). Writes are atomic: data is written to a temp file and
// renamed into place. The .dat file is fsync'd before rename to survive
// power loss.
//
// Locking strategy: mu protects in-memory state (metas, data, LRU). File I/O
// (.dat/.meta atomicWrite) in PutWithBaseRevAndModeTimings runs outside mu,
// serialized per-path by pathLocks. This allows different files to write
// concurrently while keeping same-path operations safe.
type WriteBackCache struct {
	dir     string // e.g. ~/.cache/drive9/{mount-hash}/pending
	mu      sync.Mutex
	nextGen atomic.Uint64
	// metas is the authoritative in-memory metadata index.
	// Kept in sync with disk by Put/Remove/RenamePending so that GetMeta and
	// ListPendingPaths avoid local disk I/O and JSON parsing on hot paths.
	metas      map[string]*WriteBackMeta
	totalBytes int64
	// unsynced tracks entries written with wbSyncNone whose .dat/.meta files
	// have not yet been covered by SyncPending.
	unsynced map[string]uint64
	// OnViewLost reports a pruned staged pair to the owner (the write-back
	// engine records it as a terminal failure so a barrier on the path fails
	// instead of reading "nothing staged" as success). It is the ONLY recorder
	// for pruned pairs: callers invoke it with c.mu released but the per-path
	// lock still held, so the record exists before another same-path operation
	// can observe the pruned pair as "nothing staged". It must stay
	// non-blocking, and it is never invoked for a transient read error (the
	// entry is kept, so a retry can still succeed). PendingChmod entries never
	// reach it: their missing .dat is by design.
	OnViewLost func(remotePath string, err error)
	syncMu     sync.Mutex
	durableGen atomic.Uint64
	// data keeps small cached payloads in memory so writable reopen/read paths
	// can avoid re-reading the .dat file from disk.
	data map[string][]byte
	// dataOrder tracks recency for the in-memory payload cache.
	dataOrder *list.List
	dataElems map[string]*list.Element
	dataBytes int64
	// dataMaxBytes is the aggregate memory budget for data.
	dataMaxBytes int64
	// budget, when set, is charged for the in-memory payloads this cache
	// retains, so the write-back cache shows up in the process-level memory
	// budget alongside the read cache and the write buffers.
	budget *membudget.Budget

	// pathLocks serializes file I/O for the same remotePath. Each entry
	// tracks a mutex and the number of goroutines waiting to use it.
	// Protected by mu; entries are created on demand and removed when the
	// last user releases.
	pathLocks map[string]*PathLockEntry
}

// PendingKind distinguishes newly created files from overwrites of existing
// remote files. This affects Rename behaviour: a pending-new file can be
// renamed locally (fast path), while a pending-overwrite must be uploaded
// before the remote rename so the server-side object exists.
type PendingKind int

// WriteBackMeta stores metadata alongside cached file data so that the
// background uploader (and crash-recovery) knows the remote path and size.
type WriteBackMeta struct {
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	Mtime      time.Time `json:"mtime"`
	CreatedAt  time.Time `json:"created_at"`
	Generation uint64    `json:"generation,omitempty"`
	// PutGeneration records the generation allocated when this payload
	// incarnation was installed at Path (Put or RenamePending). In-place
	// metadata updates (UpdateMode, MarkChmodPending, rebase) bump Generation
	// while preserving PutGeneration so path-incarnation retirement (OpRename
	// target replacement) survives concurrent chmods without retiring newer Puts.
	PutGeneration    uint64      `json:"put_generation,omitempty"`
	Kind             PendingKind `json:"kind"`
	BaseRev          int64       `json:"base_rev,omitempty"`
	ShadowSpill      bool        `json:"shadow_spill,omitempty"`
	Mode             uint32      `json:"mode,omitempty"`
	HasMode          bool        `json:"has_mode,omitempty"`
	SnapshotID       string      `json:"-"`
	ParentSnapshotID string      `json:"-"`
	// SupersedesPendingGen records the pending-index generation this image
	// composed over at staging time (0 = unknown/legacy). A later landing of
	// this entry may retire failure state whose pending generation is at or
	// below it: those images are contained in this one. Persisted so a
	// post-restart landing keeps the same fence.
	SupersedesPendingGen uint64 `json:"supersedes_pending_gen,omitempty"`
	// CacheLineageGen is the write-back-cache generation root this image's
	// content descends from. Unlike SupersedesPendingGen (pending-index
	// space), this is persisted for cache-only lineage: an append opened over
	// a still-uploading overwrite must adopt the revision of the ancestor
	// landing even when several descendants staged in between.
	CacheLineageGen uint64 `json:"cache_lineage_gen,omitempty"`
	// CacheAncestorGens lists the cache generations this image composed over
	// (newest first). A predecessor's upload callback can remove its cache
	// entry before ack; this persisted chain still lets the successor prove
	// the ancestry and rebase.
	CacheAncestorGens []uint64 `json:"cache_ancestor_gens,omitempty"`
	// DataCRC binds the .dat payload to this metadata record (CRC32 of
	// the staged bytes), guarded by DataBound — zero is a LEGITIMATE
	// checksum (an empty payload binds with CRC 0 and Size 0), so absence
	// of the binding must not be inferred from the value. The batched-
	// dir-sync put stages the pair through two renames that a crash
	// between them can mix — a surviving new .meta over a stale .dat
	// would serve or upload OLD bytes at the NEW base revision; the
	// binding detects such a torn pair at read time (see viewMatchesMeta)
	// and prunes it instead.
	DataCRC uint32 `json:"data_crc,omitempty"`
	// DataBound marks a record whose DataCRC/Size binding is authoritative
	// (every put sets it); legacy records without it are unbound.
	DataBound bool `json:"data_bound,omitempty"`
	// WitnessOnly marks a shadowless conflict marker installed purely as a
	// failure witness (a rename's standalone terminal record): it carries no
	// payload, and its Size/Mode are deliberately NOT authoritative file
	// metadata — the metadata overlays must not serve it as a staged image
	// (that would overwrite a nonempty remote's size with zero).
	WitnessOnly bool `json:"witness_only,omitempty"`
	// LayerClean retains a remotely durable overlay for reads, not recovery.
	// Every new staging publication resets it; legacy records remain dirty.
	LayerClean bool `json:"layer_clean,omitempty"`
	// LayerID and LayerEntrySeq identify the server publication behind a clean
	// cache. Missing identity is legacy/unknown and must be refreshed.
	LayerID       string `json:"layer_id,omitempty"`
	LayerEntrySeq int64  `json:"layer_entry_seq,omitempty"`
	// lineageTrusted is deliberately process-local and never serialized.
	// Recovering a mutable path-keyed payload cannot prove that the bytes and
	// JSON metadata were replaced atomically across a crash.
	lineageTrusted bool
	// liveAncestors is immutable process-local ancestry for SnapshotID. It is
	// copied with in-memory re-home/rename metadata but deliberately omitted
	// from JSON, so recovery remains fail-closed.
	liveAncestors []string
	// LiveChecksum binds only the actual uploader payload to this live
	// snapshot id (main's liveChecksum): process-local, never serialized.
	LiveChecksum string `json:"-"`
}

// LineageTrusted reports whether the meta's snapshot lineage is
// process-local trusted state (test hook for the lineage rows).
func (m WriteBackMeta) LineageTrusted() bool { return m.lineageTrusted }

// LiveAncestors returns the meta's live ancestor chain (test hook).
func (m WriteBackMeta) LiveAncestors() []string { return m.liveAncestors }

// WriteBackPutTimings captures sub-phase durations inside PutWithBaseRevAndMode
// for diagnostic instrumentation. All fields are zero when not instrumented.
type WriteBackPutTimings struct {
	LockWait  time.Duration // time spent waiting for WriteBackCache.mu
	DatWrite  time.Duration // atomicWrite of the .dat file (data + fsync + rename)
	MetaWrite time.Duration // atomicWrite of the .meta file (meta + fsync + rename)
}

// PendingEntry represents one file waiting to be uploaded.
type PendingEntry struct {
	Meta WriteBackMeta
	Data []byte
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
		unsynced:     make(map[string]uint64),
		data:         make(map[string][]byte),
		dataOrder:    list.New(),
		dataElems:    make(map[string]*list.Element),
		dataMaxBytes: defaultWriteBackDataCacheMaxSize,
		pathLocks:    make(map[string]*PathLockEntry),
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
		c.setMetaLocked(meta.Path, &cp)
		if meta.Generation > maxGen {
			maxGen = meta.Generation
		}
	}
	c.nextGen.Store(maxGen)
	c.durableGen.Store(maxGen)
	return c, nil
}

// hashPath returns a stable, filesystem-safe key for the remote path.
// HashPath re-exports the .dat/.meta name hasher for ported tests.
func HashPath(remotePath string) string {
	h := sha256.Sum256([]byte(remotePath))
	return hex.EncodeToString(h[:])
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

// MountLayerHash derives the mount-hash input for a layered view: the
// base mount hash plus layer and checkpoint references.
func MountLayerHash(serverURL, mountPoint, remoteRoot, layerRef, checkpointRef string) string {
	base := MountHash(serverURL, mountPoint, remoteRoot)
	input := base + "\x00layer\x00" + strings.TrimSpace(layerRef) + "\x00checkpoint\x00" + strings.TrimSpace(checkpointRef)
	h := sha256.Sum256([]byte(input))
	return hex.EncodeToString(h[:8])
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

// Test hooks for the ported cache tests: inspect/adapt the in-memory small
// payload state under the cache lock.
func (c *WriteBackCache) HasInMemoryPayload(remotePath string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.data[remotePath]
	return ok
}

// SetMemoryBudget wires the process-wide memory budget this cache charges its
// in-memory payloads to. A nil budget disables the charge (the local
// dataMaxBytes LRU still applies).
func (c *WriteBackCache) SetMemoryBudget(b *membudget.Budget) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.budget = b
	c.mu.Unlock()
	if b != nil {
		// The .dat files stay authoritative, so dropping the in-memory
		// copies is a safe way to give bytes back under pressure.
		b.RegisterReclaimer(membudget.ClassWriteCache, c.ReclaimBytes)
	}
}

// ReclaimBytes drops in-memory payloads (LRU order) until at least target
// bytes are released and reports how many bytes it actually freed. It is the
// budget's reclaimer hook: the .dat file on disk stays authoritative, so a
// dropped payload only costs a re-read.
func (c *WriteBackCache) ReclaimBytes(target int64) int64 {
	if c == nil || target <= 0 {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var freed int64
	for freed < target {
		tail := c.dataOrder.Back()
		if tail == nil {
			break
		}
		path, _ := tail.Value.(string)
		if data, ok := c.data[path]; ok {
			freed += int64(len(data))
		}
		c.deleteDataLocked(path)
	}
	return freed
}

// SetInMemoryBudget overrides the in-memory small-payload byte budget.
func (c *WriteBackCache) SetInMemoryBudget(maxBytes int64) {
	c.mu.Lock()
	c.dataMaxBytes = maxBytes
	c.mu.Unlock()
}

// InMemoryUsage reports the current in-memory small-payload bytes.
func (c *WriteBackCache) InMemoryUsage() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dataBytes
}

// DropInMemoryPayload removes one path's in-memory payload (test hook for
// the missing-.dat recovery row).
func (c *WriteBackCache) DropInMemoryPayload(remotePath string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleteDataLocked(remotePath)
}

// PublishMeta installs a metadata entry directly (test hook simulating the
// post-I/O phase of an in-flight Put).
func (c *WriteBackCache) PublishMeta(meta WriteBackMeta) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setMetaLocked(meta.Path, &meta)
}

func (c *WriteBackCache) setMetaLocked(remotePath string, meta *WriteBackMeta) {
	if meta == nil {
		c.deleteMetaLocked(remotePath)
		return
	}
	if meta.PutGeneration == 0 {
		meta.PutGeneration = meta.Generation
	}
	if prev, ok := c.metas[remotePath]; ok && prev != nil {
		c.totalBytes -= prev.Size
	}
	c.metas[remotePath] = meta
	c.totalBytes += meta.Size
}

// PutGen returns the generation allocated when this cached payload incarnation
// was installed at its current path, falling back to Generation for legacy rows.
func (m *WriteBackMeta) PutGen() uint64 {
	if m == nil {
		return 0
	}
	if m.PutGeneration != 0 {
		return m.PutGeneration
	}
	return m.Generation
}

func (c *WriteBackCache) deleteMetaLocked(remotePath string) {
	if prev, ok := c.metas[remotePath]; ok && prev != nil {
		c.totalBytes -= prev.Size
	}
	delete(c.metas, remotePath)
	delete(c.unsynced, remotePath)
}

// HasMeta reports whether remotePath currently has an in-memory pending record.
func (c *WriteBackCache) HasMeta(remotePath string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	_, ok := c.metas[remotePath]
	c.mu.Unlock()
	return ok
}

// PendingBytes reports the total byte size of all cached pending entries in O(1).
func (c *WriteBackCache) PendingBytes() int64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	b := c.totalBytes
	c.mu.Unlock()
	return b
}

// MetaGeneration reports the current metadata generation of one path
// (test hook for the chmod-pending rows).
func (c *WriteBackCache) MetaGeneration(remotePath string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if meta, ok := c.metas[remotePath]; ok {
		return meta.Generation
	}
	return 0
}

// acquirePathLock returns a per-path mutex for serializing file I/O on the
// same remotePath. The caller must call releasePathLock when done.
func (c *WriteBackCache) AcquirePathLock(remotePath string) *PathLockEntry {
	c.mu.Lock()
	pl, ok := c.pathLocks[remotePath]
	if !ok {
		pl = &PathLockEntry{}
		c.pathLocks[remotePath] = pl
	}
	pl.waiters++
	c.mu.Unlock()

	pl.mu.Lock()
	return pl
}

// releasePathLock releases the per-path mutex and removes it from the map
// when no other goroutine is waiting.
func (c *WriteBackCache) ReleasePathLock(remotePath string, pl *PathLockEntry) {
	c.mu.Lock()
	pl.waiters--
	if pl.waiters == 0 {
		delete(c.pathLocks, remotePath)
	}
	c.mu.Unlock()
	pl.mu.Unlock()
}

// datFile returns the path to the .dat file for a remote path.
func (c *WriteBackCache) DatFile(remotePath string) string {
	return filepath.Join(c.dir, HashPath(remotePath)+".dat")
}

// metaFile returns the path to the .meta file for a remote path.
func (c *WriteBackCache) MetaFile(remotePath string) string {
	return filepath.Join(c.dir, HashPath(remotePath)+".meta")
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
	_, _, err := c.PutWithBaseRevAndModeTimings(remotePath, data, size, kind, baseRev, mode, hasMode)
	return err
}

// PutWithBaseRevAndModeTimings is like PutWithBaseRevAndMode but also returns
// the allocated store generation and sub-phase timings for diagnostic
// instrumentation. The generation lets callers perform ownership-scoped
// cleanup later (RemoveIfGeneration) so a stale owner never removes a fresher
// same-path entry.
//
// File I/O (.dat/.meta atomicWrite) runs outside the global WriteBackCache.mu,
// serialized per-path by a per-path lock. The global lock is only held briefly
// to allocate the generation, compute file paths, and publish in-memory state.
func (c *WriteBackCache) PutWithBaseRevAndModeTimings(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool) (uint64, WriteBackPutTimings, error) {
	return c.PutWithBaseRevAndModeAndLineageTimings(remotePath, data, size, kind, baseRev, mode, hasMode, "", "", false)
}

// PutWithBaseRevAndModeAndLineageTimings is the process-local lineage variant
// used by FileHandle staging. Legacy callers deliberately store empty lineage,
// which is safe because such metadata cannot authorize a growth rebase.
func (c *WriteBackCache) PutWithBaseRevAndModeAndLineageTimings(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, liveAncestors ...string) (uint64, WriteBackPutTimings, error) {
	return c.putWithBaseRevAndModeAndLineage(remotePath, data, size, kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, 0, nil, wbSyncFull, liveAncestors)
}

// PutWithBaseRevAndModeAndLineageTimingsNoSync is the non-durable variant of
// PutWithBaseRevAndModeAndLineageTimings: the .dat/.meta snapshot files are
// written atomically (tmp+rename) but without their own fsyncs. Callers must
// only use it when the durable authority for the payload is already persisted
// elsewhere — the shadow store + pending index — so at most the snapshot's
// freshness (not its integrity) is lost on a crash.
func (c *WriteBackCache) PutWithBaseRevAndModeAndLineageTimingsNoSync(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, liveAncestors ...string) (uint64, WriteBackPutTimings, error) {
	return c.putWithBaseRevAndModeAndLineage(remotePath, data, size, kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, 0, nil, wbSyncNone, liveAncestors)
}

// PutWithBaseRevAndModeAndLineageTimingsDirSync is the v1 close-path stage
// variant of PutWithBaseRevAndModeAndLineageTimings: both files' contents
// are fsynced before their renames and ONE directory fsync after both
// renames makes the pair durable together — three barriers (two file syncs
// + one directory sync) instead of the durable put's four. Once the call
// returns the entry is durable and neither file's contents are torn. The
// renames themselves are not ordered against each other before the shared
// directory sync: a crash inside the call can leave a surviving new .meta
// over a stale .dat, and the DataCRC binding catches exactly that at read
// time (the entry is pruned — lost cleanly, matching v1's crash-before-
// WAL-fsync window — instead of uploading stale bytes at the new base).
func (c *WriteBackCache) PutWithBaseRevAndModeAndLineageTimingsDirSync(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, liveAncestors ...string) (uint64, WriteBackPutTimings, error) {
	return c.putWithBaseRevAndModeAndLineage(remotePath, data, size, kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, 0, nil, wbSyncDirOnce, liveAncestors)
}

// PutWithBaseRevAndModeAndLineageTimingsCacheLineage is the full-durability
// variant that persists the write-back-cache lineage root in the same meta
// write (no post-put rewrite/fsync).
func (c *WriteBackCache) PutWithBaseRevAndModeAndLineageTimingsCacheLineage(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, cacheLineage uint64, cacheAncestors []uint64, liveAncestors ...string) (uint64, WriteBackPutTimings, error) {
	return c.putWithBaseRevAndModeAndLineage(remotePath, data, size, kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, cacheLineage, cacheAncestors, wbSyncFull, liveAncestors)
}

// PutWithBaseRevAndModeAndLineageTimingsDirSyncCacheLineage is the
// three-barrier close variant that persists the cache lineage root in the
// same meta write.
func (c *WriteBackCache) PutWithBaseRevAndModeAndLineageTimingsDirSyncCacheLineage(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, cacheLineage uint64, cacheAncestors []uint64, liveAncestors ...string) (uint64, WriteBackPutTimings, error) {
	return c.putWithBaseRevAndModeAndLineage(remotePath, data, size, kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, cacheLineage, cacheAncestors, wbSyncDirOnce, liveAncestors)
}

// PutWithBaseRevAndModeAndLineageTimingsNoSyncCacheLineage stages a .dat/.meta
// pair via atomic tmp+rename without per-close fsync barriers (--writeback-sync-window
// lazy staging), marking the entry unsynced for background SyncPending/SyncLoop.
func (c *WriteBackCache) PutWithBaseRevAndModeAndLineageTimingsNoSyncCacheLineage(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, cacheLineage uint64, cacheAncestors []uint64, liveAncestors ...string) (uint64, WriteBackPutTimings, error) {
	return c.putWithBaseRevAndModeAndLineage(remotePath, data, size, kind, baseRev, mode, hasMode, snapshotID, parentSnapshotID, lineageTrusted, cacheLineage, cacheAncestors, wbSyncNone, liveAncestors)
}

// Get reads the cached data for remotePath. Returns nil, false if not cached.
// Acquires per-path lock to prevent reading a .dat file that is being
// overwritten by a concurrent Put (which writes .dat outside c.mu).
func (c *WriteBackCache) Get(remotePath string) ([]byte, bool) {
	pl := c.AcquirePathLock(remotePath)
	c.mu.Lock()
	data, state, viewErr, pruned := c.getViewStateLocked(remotePath)
	c.mu.Unlock()
	if state != StagedViewOK {
		// Report while the path lock is still held: a same-path barrier that
		// takes the lock after this point must already see the loss record, or
		// it would read the pruned pair as "another upload consumed it".
		c.reportViewLoss(remotePath, pruned, viewErr)
		c.ReleasePathLock(remotePath, pl)
		return nil, false
	}
	copyData := make([]byte, len(data))
	copy(copyData, data)
	c.ReleasePathLock(remotePath, pl)
	return copyData, true
}

// getView returns a read-only payload view for remotePath.
// Callers must not mutate the returned slice.
// Acquires per-path lock to prevent reading a .dat file that is being
// overwritten by a concurrent Put (which writes .dat outside c.mu).
func (c *WriteBackCache) GetView(remotePath string) ([]byte, bool) {
	pl := c.AcquirePathLock(remotePath)
	c.mu.Lock()
	data, state, viewErr, pruned := c.getViewStateLocked(remotePath)
	c.mu.Unlock()
	if state != StagedViewOK {
		c.reportViewLoss(remotePath, pruned, viewErr)
		c.ReleasePathLock(remotePath, pl)
		return nil, false
	}
	c.ReleasePathLock(remotePath, pl)
	return data, true
}

// GetMeta reads the metadata for remotePath. Returns nil, false if not cached.
// GetMeta returns a copy of the metadata for remotePath.
// Acquires per-path lock to serialize with concurrent Put, ensuring callers
// always see either the pre-Put or post-Put state, never a mid-write gap
// where new data is on disk but metadata hasn't been published yet.
func (c *WriteBackCache) GetMeta(remotePath string) (*WriteBackMeta, bool) {
	pl := c.AcquirePathLock(remotePath)
	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok {
		c.mu.Unlock()
		c.ReleasePathLock(remotePath, pl)
		return nil, false
	}
	cp := cloneWriteBackMeta(meta)
	c.mu.Unlock()
	c.ReleasePathLock(remotePath, pl)
	return &cp, true
}

// viewMatchesMeta reports whether a data image is the one its metadata
// record describes: a bound record (DataBound — every put sets it) carries
// the payload's length and CRC32, and zero is a legitimate checksum (an
// empty payload binds with CRC 0 and Size 0). Unbound legacy records are
// accepted as-is. A mismatch is a TORN PAIR — a crash between the two
// renames of a batched-dir-sync put left a surviving .meta over a stale
// .dat — and must never be served or uploaded: the stale bytes would
// publish at the NEW base revision.
func viewMatchesMeta(meta *WriteBackMeta, data []byte) bool {
	if meta == nil || !meta.DataBound {
		return true
	}
	return int64(len(data)) == meta.Size && crc32.ChecksumIEEE(data) == meta.DataCRC
}

// GetMetaAndView atomically reads both metadata and data for remotePath under
// the same per-path lock, guaranteeing that meta and data are from the same
// generation. This prevents an interleaving where a concurrent Put replaces
// the .dat file between a separate GetMeta and getView call, causing the
// uploader to upload new data with old meta (wrong baseRev/generation).
// The view load itself enforces the metadata binding: a torn pair is pruned
// by getViewLocked, never returned.
func (c *WriteBackCache) GetMetaAndView(remotePath string) (*WriteBackMeta, []byte, bool) {
	meta, data, state, _ := c.getMetaAndView(remotePath, nil)
	return meta, data, state == StagedViewOK
}

// errStagedViewTorn reports a staged pair whose data no longer matches its
// metadata binding. The pair is pruned and reported to the owner, so a retried
// barrier cannot treat the missing record as a successful landing.
var errStagedViewTorn = errors.New("staged image failed its meta/data binding")

// StagedViewState explains why a staged meta+data read did not produce a pair.
type StagedViewState int

const (
	// StagedViewOK means meta and data were returned together.
	StagedViewOK StagedViewState = iota
	// StagedViewNoMeta means nothing is staged for the path: it was consumed
	// by an upload, or never staged. Callers may treat it as "nothing to do".
	StagedViewNoMeta
	// StagedViewNoData means a metadata record exists but its payload is not
	// available: the pair was torn or its .dat file was gone (both pruned), or
	// the read failed for another reason (entry kept, error returned). A caller
	// that reports success here would claim durability for bytes it never saw.
	StagedViewNoData
	// StagedViewPendingChmod is a record whose data already landed and only a
	// chmod is outstanding: the .dat file is legitimately absent, so the caller
	// must apply the chmod instead of reading (and pruning) a view.
	StagedViewPendingChmod
)

// GetMetaAndViewState is GetMetaAndView with the miss reason and the read
// error. Callers that treat a miss as success (a durability barrier) need the
// distinction: only StagedViewNoMeta means another upload already consumed the
// image.
func (c *WriteBackCache) GetMetaAndViewState(remotePath string) (*WriteBackMeta, []byte, StagedViewState, error) {
	return c.getMetaAndView(remotePath, nil)
}

// GetMetaAndViewStateWithCallback is GetMetaAndViewState with the path lock
// held across onLocked, for callers that capture staging generations with the
// snapshot. A background upload uses the state to tell "another upload took it"
// from "the pair was pruned", which is worth a log line rather than a silent
// skip.
func (c *WriteBackCache) GetMetaAndViewStateWithCallback(remotePath string, onLocked func()) (*WriteBackMeta, []byte, StagedViewState, error) {
	return c.getMetaAndView(remotePath, onLocked)
}

// getMetaAndView is the one implementation of the meta+data protocol: the path
// lock is held across the snapshot and onLocked, so a concurrent same-path Put
// can neither split the pair nor slip a newer generation past a caller.
func (c *WriteBackCache) getMetaAndView(remotePath string, onLocked func()) (*WriteBackMeta, []byte, StagedViewState, error) {
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)

	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok {
		c.mu.Unlock()
		return nil, nil, StagedViewNoMeta, nil
	}
	cp := cloneWriteBackMeta(meta)
	if cp.Kind == PendingChmod {
		// The data already landed; only the mode is outstanding, so the .dat is
		// absent by design and reading it would prune a healthy record.
		c.mu.Unlock()
		return &cp, nil, StagedViewPendingChmod, nil
	}
	data, state, viewErr, pruned := c.getViewStateLocked(remotePath)
	c.mu.Unlock()

	if state != StagedViewOK {
		c.reportViewLoss(remotePath, pruned, viewErr)
		return nil, nil, state, viewErr
	}

	// Invoke the callback while the path lock is still held so staging-gen
	// snapshots are consistent with this meta+data read. Any loss report has
	// already run by now, so the snapshot cannot capture a sequence number
	// that a later drain would then exceed.
	if onLocked != nil {
		onLocked()
	}

	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return &cp, dataCopy, StagedViewOK, nil
}

// GetMetaAndViewWithCallback is like GetMetaAndView but invokes onLocked while
// the per-path lock is still held (after the meta+data snapshot is read). This
// lets callers atomically capture associated staging-store generations (e.g.
// pendingIndex/shadowStore) under the same serialization window so a concurrent
// same-path Put cannot race the snapshot and leave a stale upload holding newer
// generations it is not responsible for. The view load itself enforces the
// metadata binding: a torn pair is pruned by getViewLocked, never returned.
func (c *WriteBackCache) GetMetaAndViewWithCallback(remotePath string, onLocked func()) (*WriteBackMeta, []byte, bool) {
	meta, data, state, _ := c.getMetaAndView(remotePath, onLocked)
	return meta, data, state == StagedViewOK
}

// Remove deletes the cached data and metadata for remotePath.
func (c *WriteBackCache) Remove(remotePath string) {
	pl := c.AcquirePathLock(remotePath)
	c.mu.Lock()
	c.pruneEntryLocked(remotePath, true)
	c.mu.Unlock()
	c.ReleasePathLock(remotePath, pl)
}

// RemoveIfGeneration atomically checks that the current generation matches
// expectedGen before removing the entry. Returns true if the entry was removed.
// This prevents a stale upload from accidentally removing a fresher cache entry
// that was written between the generation check and the remove call.
func (c *WriteBackCache) RemoveIfGeneration(remotePath string, expectedGen uint64) bool {
	pl := c.AcquirePathLock(remotePath)
	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if ok && meta.Generation == expectedGen {
		c.pruneEntryLocked(remotePath, true)
		c.mu.Unlock()
		c.ReleasePathLock(remotePath, pl)
		return true
	}
	c.mu.Unlock()
	c.ReleasePathLock(remotePath, pl)
	return false
}

// RemoveIfPutGeneration atomically removes the entry at remotePath if its
// payload-installation generation (PutGen) matches expectedPutGen, even when an
// in-place metadata update (e.g. UpdateMode) bumped Generation afterwards.
func (c *WriteBackCache) RemoveIfPutGeneration(remotePath string, expectedPutGen uint64) bool {
	if expectedPutGen == 0 {
		return false
	}
	pl := c.AcquirePathLock(remotePath)
	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if ok && meta != nil && meta.PutGen() == expectedPutGen {
		c.pruneEntryLocked(remotePath, true)
		c.mu.Unlock()
		c.ReleasePathLock(remotePath, pl)
		return true
	}
	c.mu.Unlock()
	c.ReleasePathLock(remotePath, pl)
	return false
}

// UpdateMode updates the pending mode metadata for an existing cache entry.
// Acquires per-path lock to serialize .meta writes with concurrent Put.
func (c *WriteBackCache) UpdateMode(remotePath string, mode uint32) error {
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)

	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	updated := cloneWriteBackMeta(meta)
	updated.Mode = mode & posixPermissionModeMask
	updated.HasMode = true
	updated.Generation = c.nextGen.Add(1)
	metaPath := c.MetaFile(remotePath)
	c.mu.Unlock()

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("writeback marshal mode meta: %w", err)
	}
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return fmt.Errorf("writeback update mode meta: %w", err)
	}

	c.mu.Lock()
	cp := updated
	c.setMetaLocked(remotePath, &cp)
	c.mu.Unlock()
	return nil
}

// RebaseBaseRevIfGeneration advances a staged entry's CAS base to baseRev
// — the same-origin conflict repair: an entry that lost a duplicate-commit
// race (another flush of this mount landed a newer revision between its
// staging and its upload) retries against the revision the race winner
// recorded instead of conflicting forever. The generation guard (checked
// under the path lock) keeps a rebased upload from pairing with a fresher
// same-path staging; the entry's generation is unchanged — its payload
// did not change, and the uploader's success cleanup keys on it.
func (c *WriteBackCache) RebaseBaseRevIfGeneration(remotePath string, generation uint64, baseRev int64) bool {
	if baseRev <= 0 {
		return false
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)

	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok || meta.Generation != generation || baseRev <= meta.BaseRev {
		c.mu.Unlock()
		return false
	}
	updated := cloneWriteBackMeta(meta)
	updated.BaseRev = baseRev
	metaPath := c.MetaFile(remotePath)
	c.mu.Unlock()

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return false
	}
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return false
	}

	c.mu.Lock()
	cp := updated
	c.setMetaLocked(remotePath, &cp)
	c.mu.Unlock()
	return true
}

// RebasePendingNew carries a real create's revision to a newer pending image.
func (c *WriteBackCache) RebasePendingNew(remotePath string, baseRev int64) error {
	if baseRev <= 0 {
		return nil
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)

	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok || meta.Kind != PendingNew || meta.BaseRev != 0 {
		c.mu.Unlock()
		return nil
	}
	updated := cloneWriteBackMeta(meta)
	updated.Kind = PendingOverwrite
	updated.BaseRev = baseRev
	updated.Generation = c.nextGen.Add(1)
	c.mu.Unlock()

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return fmt.Errorf("writeback marshal create acknowledgement: %w", err)
	}
	if err := atomicWrite(c.MetaFile(remotePath), metaBytes); err != nil {
		return fmt.Errorf("writeback persist create acknowledgement: %w", err)
	}
	c.mu.Lock()
	c.setMetaLocked(remotePath, &updated)
	c.mu.Unlock()
	return nil
}

// MarkChmodPending records that data has reached the remote server and only
// the chmod step remains. The generation guard prevents an old upload from
// overwriting metadata for a newer local write.
// Acquires per-path lock to serialize .meta writes with concurrent Put.
func (c *WriteBackCache) MarkChmodPending(remotePath string, generation uint64) (bool, error) {
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)

	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok || meta.Generation != generation {
		c.mu.Unlock()
		return false, nil
	}
	updated := cloneWriteBackMeta(meta)
	updated.Kind = PendingChmod
	updated.BaseRev = 0
	updated.Generation = c.nextGen.Add(1)
	metaPath := c.MetaFile(remotePath)
	c.mu.Unlock()

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return false, fmt.Errorf("writeback marshal chmod-pending meta: %w", err)
	}
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return false, fmt.Errorf("writeback update chmod-pending meta: %w", err)
	}

	c.mu.Lock()
	cp := updated
	c.setMetaLocked(remotePath, &cp)
	c.mu.Unlock()
	return true, nil
}

// MarkSupersedesPendingGen records the pending-index generation the entry's
// image composed over when it was staged. The generation guard prevents an
// old staging from stamping a newer same-path entry. Acquires the per-path
// lock to serialize the .meta rewrite with concurrent Put.
func (c *WriteBackCache) MarkSupersedesPendingGen(remotePath string, supersedesGen, expectedGen uint64) bool {
	if supersedesGen == 0 || expectedGen == 0 {
		return false
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)

	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok || meta.Generation != expectedGen {
		c.mu.Unlock()
		return false
	}
	updated := cloneWriteBackMeta(meta)
	updated.SupersedesPendingGen = supersedesGen
	metaPath := c.MetaFile(remotePath)
	c.mu.Unlock()

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return false
	}
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return false
	}

	c.mu.Lock()
	cp := updated
	c.setMetaLocked(remotePath, &cp)
	c.mu.Unlock()
	return true
}

// MarkCacheLineage records the write-back-cache generation root an image
// descends from. The expected generation fence is the same as
// MarkSupersedesPendingGen's, and the rewrite is serialized by the path lock.
func (c *WriteBackCache) MarkCacheLineage(remotePath string, lineage, expectedGen uint64) bool {
	if lineage == 0 || expectedGen == 0 {
		return false
	}
	pl := c.AcquirePathLock(remotePath)
	defer c.ReleasePathLock(remotePath, pl)

	c.mu.Lock()
	meta, ok := c.metas[remotePath]
	if !ok || meta.Generation != expectedGen {
		c.mu.Unlock()
		return false
	}
	updated := cloneWriteBackMeta(meta)
	updated.CacheLineageGen = lineage
	metaPath := c.MetaFile(remotePath)
	c.mu.Unlock()

	metaBytes, err := json.Marshal(updated)
	if err != nil {
		return false
	}
	if err := atomicWrite(metaPath, metaBytes); err != nil {
		return false
	}

	c.mu.Lock()
	cp := updated
	c.setMetaLocked(remotePath, &cp)
	c.mu.Unlock()
	return true
}

// RenamePending atomically moves a pending cache entry from oldPath to newPath.
// The .dat file is moved via os.Rename (no data copy); only the .meta is
// rewritten with the new path and a fresh generation.
// Returns true if there was a pending entry to rename.
func (c *WriteBackCache) RenamePending(oldPath, newPath string) bool {
	if oldPath == newPath {
		return false // no-op rename
	}
	// Acquire path locks in sorted order to avoid deadlock.
	first, second := oldPath, newPath
	if second < first {
		first, second = second, first
	}
	pl1 := c.AcquirePathLock(first)
	pl2 := c.AcquirePathLock(second)
	defer func() {
		c.ReleasePathLock(second, pl2)
		c.ReleasePathLock(first, pl1)
	}()

	c.mu.Lock()
	defer c.mu.Unlock()

	oldDat := c.DatFile(oldPath)
	oldMeta := c.MetaFile(oldPath)

	meta, ok := c.metas[oldPath]
	if !ok {
		return false
	}
	updated := cloneWriteBackMeta(meta)

	// Rename .dat file directly — same directory, no data copy needed.
	newDat := c.DatFile(newPath)
	if err := os.Rename(oldDat, newDat); err != nil {
		return false
	}

	// Write new .meta with updated path and fresh generation.
	newGen := c.nextGen.Add(1)
	updated.Path = newPath
	updated.Generation = newGen
	updated.PutGeneration = newGen
	metaBytes, err := json.Marshal(updated)
	if err != nil {
		_ = os.Rename(newDat, oldDat)
		return false
	}
	newMeta := c.MetaFile(newPath)
	if err := atomicWrite(newMeta, metaBytes); err != nil {
		// Restore .dat to old location on meta-write failure.
		_ = os.Rename(newDat, oldDat)
		return false
	}

	// Remove old meta and update index.
	_ = os.Remove(oldMeta)
	c.deleteMetaLocked(oldPath)
	cp := updated
	c.setMetaLocked(newPath, &cp)
	if data, ok := c.data[oldPath]; ok {
		c.deleteDataLocked(oldPath)
		c.deleteDataLocked(newPath)
		c.cacheDataLocked(newPath, data, int64(len(data)))
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

// PendingSummary reports all cached write-back entries that still require
// upload, chmod retry, or local recovery.
func (c *WriteBackCache) PendingSummary() (count int, bytes int64, firstPath string) {
	if c == nil {
		return 0, 0, ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	count = len(c.metas)
	bytes = c.totalBytes
	for path, meta := range c.metas {
		if meta != nil {
			firstPath = path
			break
		}
	}
	return count, bytes, firstPath
}

// ListByPrefix returns metadata for pending entries under prefix.
// It first waits for any in-flight per-path operations (Put, Remove, etc.)
// under the prefix to complete, so callers in mutation paths (rmdir, rename)
// see a consistent snapshot that includes entries currently being written.
func (c *WriteBackCache) ListByPrefix(prefix string) []*WriteBackMeta {
	// Collect paths with in-flight operations under prefix.
	c.mu.Lock()
	var inflightPaths []string
	for p := range c.pathLocks {
		if strings.HasPrefix(p, prefix) {
			inflightPaths = append(inflightPaths, p)
		}
	}
	c.mu.Unlock()

	// Wait for each in-flight operation to finish by acquiring and immediately
	// releasing its per-path lock. This guarantees the operation has completed
	// and published its results to c.metas before we scan.
	for _, p := range inflightPaths {
		pl := c.AcquirePathLock(p)
		c.ReleasePathLock(p, pl)
	}

	// Now scan metadata — all in-flight operations under prefix have completed.
	c.mu.Lock()
	defer c.mu.Unlock()

	var result []*WriteBackMeta
	for p, meta := range c.metas {
		if strings.HasPrefix(p, prefix) {
			cp := cloneWriteBackMeta(meta)
			result = append(result, &cp)
		}
	}
	return result
}

// ListPending returns all pending entries using the in-memory metadata index.
// It also reconciles any on-disk .meta files not yet in memory so crash
// recovery and corruption cleanup keep the legacy behavior on this cold path.
func (c *WriteBackCache) ListPending() []PendingEntry {
	type prunedView struct {
		path string
		err  error
	}
	var pruned []prunedView
	c.mu.Lock()
	defer func() {
		c.mu.Unlock()
		// Report each prune under its own path lock, so a barrier that races
		// recovery cannot read the dropped pair as a consumed one.
		for _, p := range pruned {
			pl := c.AcquirePathLock(p.path)
			c.reportViewLoss(p.path, true, p.err)
			c.ReleasePathLock(p.path, pl)
		}
	}()

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
				c.setMetaLocked(meta.Path, &cp)
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
		if meta.Kind == PendingConflict || meta.Kind == PendingChmod {
			// Not recoverable upload work: a conflict is preserved for manual
			// recovery and a chmod has no payload.
			continue
		}
		datPath := c.DatFile(path)
		data, err := os.ReadFile(datPath)
		if err != nil {
			// Data file missing — remove orphaned metadata, and record the
			// loss so a barrier that later finds nothing staged does not read
			// it as success.
			c.pruneEntryLocked(path, false)
			pruned = append(pruned, prunedView{path: path, err: os.ErrNotExist})
			continue
		}
		// Recovery admits the payload through the same binding check as
		// every other load: caching a torn pair here would hand the stale
		// bytes to later memory hits (getViewLocked's fast path skips the
		// recheck) and to the uploader — republishing old contents at the
		// new base revision instead of pruning the pair.
		if !viewMatchesMeta(meta, data) {
			c.pruneEntryLocked(path, true)
			pruned = append(pruned, prunedView{path: path, err: errStagedViewTorn})
			continue
		}

		c.cacheDataLocked(path, data, meta.Size)
		result = append(result, PendingEntry{Meta: *meta, Data: data})
	}
	return result
}

// ListPendingMetas enumerates the pending entries' METADATA only. It is
// the recovery-scheduling path: unlike ListPending it never loads a
// payload, so a mount recovering a large on-disk backlog stays bounded by
// the per-upload working set (the worker loads one image at a time through
// GetMetaAndView) instead of materializing the backlog's total bytes.
func (c *WriteBackCache) ListPendingMetas() []WriteBackMeta {
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
				c.setMetaLocked(meta.Path, &cp)
				if meta.Generation > c.nextGen.Load() {
					c.nextGen.Store(meta.Generation)
				}
			}
		}
	}

	result := make([]WriteBackMeta, 0, len(c.metas))
	for _, meta := range c.metas {
		if meta.Kind == PendingConflict {
			continue
		}
		result = append(result, *meta)
	}
	return result
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
	return scannedMetaName == HashPath(meta.Path)+".meta"
}

// wbSyncMode selects a put's crash cost (see the exported variants).
type wbSyncMode uint8

const (
	// wbSyncFull fsyncs each file and the directory rename after each
	// rename — four barriers. The pair is the only durable authority.
	wbSyncFull wbSyncMode = iota
	// wbSyncDirOnce is the v1 close-path stage cost: fsync both files'
	// contents before their renames, then ONE directory fsync publishing
	// the pair together — three barriers. The two renames are not ordered
	// against each other before that sync, so a crash in between can mix
	// a surviving new .meta over a stale .dat; the DataCRC binding detects
	// such a torn pair at read time and prunes it (a cleanly lost entry,
	// never stale bytes uploaded at the new base). After the sync the pair
	// is durable together.
	wbSyncDirOnce
	// wbSyncNone writes atomically without fsyncs; the durable authority
	// must already exist elsewhere (shadow store + pending index).
	wbSyncNone
)

func (c *WriteBackCache) putWithBaseRevAndModeAndLineage(remotePath string, data []byte, size int64, kind PendingKind, baseRev int64, mode uint32, hasMode bool, snapshotID, parentSnapshotID string, lineageTrusted bool, cacheLineage uint64, cacheAncestors []uint64, syncMode wbSyncMode, liveAncestors []string) (uint64, WriteBackPutTimings, error) {
	var t WriteBackPutTimings

	// Phase 1: acquire per-path lock (serializes same-path Put/Remove/etc.)
	lockStart := time.Now()
	pl := c.AcquirePathLock(remotePath)
	t.LockWait = time.Since(lockStart)

	// Compute file paths and allocate generation (no global lock needed;
	// datFile/metaFile are pure functions of dir+remotePath, nextGen is atomic).
	datPath := c.DatFile(remotePath)
	metaPath := c.MetaFile(remotePath)
	gen := c.nextGen.Add(1)

	// Phase 2: file I/O outside global lock (only per-path serialized).
	writeDat := atomicWrite
	writeMeta := atomicWrite
	switch syncMode {
	case wbSyncNone:
		writeDat = atomicWriteNoSync
		writeMeta = atomicWriteNoSync
	case wbSyncDirOnce:
		// Both files' CONTENTS are synced before their renames; the single
		// trailing directory sync (below) publishes the pair together. A
		// per-file directory sync here would both cost a third barrier and
		// make the new .dat recoverable ahead of the .meta that describes
		// it.
		writeDat = atomicWriteFileSyncOnly
		writeMeta = atomicWriteFileSyncOnly
	}
	datStart := time.Now()
	if err := writeDat(datPath, data); err != nil {
		c.ReleasePathLock(remotePath, pl)
		return 0, t, fmt.Errorf("writeback put data: %w", err)
	}
	t.DatWrite = time.Since(datStart)

	if cacheLineage == 0 {
		cacheLineage = gen
	}
	now := time.Now()
	meta := WriteBackMeta{
		Path:              remotePath,
		CacheLineageGen:   cacheLineage,
		CacheAncestorGens: append([]uint64(nil), cacheAncestors...),
		Size:              size,
		Mtime:             now,
		CreatedAt:         now,
		Generation:        gen,
		PutGeneration:     gen,
		Kind:              kind,
		BaseRev:           baseRev,
		Mode:              mode & posixPermissionModeMask,
		HasMode:           hasMode,
		DataCRC:           crc32.ChecksumIEEE(data),
		DataBound:         true,
		SnapshotID:        snapshotID,
		ParentSnapshotID:  parentSnapshotID,
		lineageTrusted:    lineageTrusted,
		liveAncestors:     append([]string(nil), liveAncestors...),
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		c.ReleasePathLock(remotePath, pl)
		return 0, t, fmt.Errorf("writeback marshal meta: %w", err)
	}
	metaStart := time.Now()
	if err := writeMeta(metaPath, metaBytes); err != nil {
		// Best-effort cleanup of the .dat file if meta write fails.
		_ = os.Remove(datPath)
		c.ReleasePathLock(remotePath, pl)
		return 0, t, fmt.Errorf("writeback put meta: %w", err)
	}
	t.MetaWrite = time.Since(metaStart)
	if syncMode == wbSyncDirOnce {
		// One directory fsync makes both renames durable together — with
		// the two content syncs above, three barriers total instead of the
		// durable put's four (v1's close-path stage paid two for its
		// shadow + WAL pair; this track's pair is one file larger).
		if err := fsyncDir(filepath.Dir(datPath)); err != nil {
			c.ReleasePathLock(remotePath, pl)
			return 0, t, fmt.Errorf("writeback put dir sync: %w", err)
		}
	}

	// Phase 3: publish in-memory state under global lock (fast, no I/O).
	c.mu.Lock()
	cp := meta
	c.setMetaLocked(remotePath, &cp)
	if syncMode == wbSyncNone {
		if c.unsynced == nil {
			c.unsynced = make(map[string]uint64)
		}
		c.unsynced[remotePath] = gen
	} else {
		delete(c.unsynced, remotePath)
		for {
			cur := c.durableGen.Load()
			if gen <= cur || c.durableGen.CompareAndSwap(cur, gen) {
				break
			}
		}
	}
	c.cacheDataLocked(remotePath, data, size)
	c.mu.Unlock()

	c.ReleasePathLock(remotePath, pl)
	return gen, t, nil
}

// DurableGeneration reports the highest cache generation guaranteed synced to disk.
func (c *WriteBackCache) DurableGeneration() uint64 {
	if c == nil {
		return 0
	}
	return c.durableGen.Load()
}

// SyncPending flushes all wbSyncNone entries currently awaiting disk sync.
func (c *WriteBackCache) SyncPending() error {
	if c == nil {
		return nil
	}
	c.syncMu.Lock()
	defer c.syncMu.Unlock()

	c.mu.Lock()
	if len(c.unsynced) == 0 {
		c.mu.Unlock()
		return nil
	}
	snapshot := make(map[string]uint64, len(c.unsynced))
	var maxGen uint64
	for p, g := range c.unsynced {
		snapshot[p] = g
		if g > maxGen {
			maxGen = g
		}
	}
	c.mu.Unlock()

	if err := syncFilesystemDir(c.dir); err != nil {
		if !errors.Is(err, errSyncfsUnsupported) {
			return err
		}
		for p := range snapshot {
			if err := syncExistingFile(c.DatFile(p)); err != nil {
				return err
			}
			if err := syncExistingFile(c.MetaFile(p)); err != nil {
				return err
			}
		}
		if err := fsyncDir(c.dir); err != nil {
			return err
		}
	}

	c.mu.Lock()
	for p, g := range snapshot {
		if cur, ok := c.unsynced[p]; ok && cur <= g {
			delete(c.unsynced, p)
		}
	}
	for {
		cur := c.durableGen.Load()
		if maxGen <= cur || c.durableGen.CompareAndSwap(cur, maxGen) {
			break
		}
	}
	c.mu.Unlock()
	return nil
}

// SyncLoop periodically calls SyncPending to bound the power-loss window of
// lazy-staged small-file entries (--writeback-sync-window).
func (c *WriteBackCache) SyncLoop(ctx context.Context, interval time.Duration) {
	if c == nil || interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = c.SyncPending()
		}
	}
}

func syncExistingFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	err = f.Sync()
	_ = f.Close()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (c *WriteBackCache) pruneEntryLocked(remotePath string, removeDataFile bool) {
	if removeDataFile {
		_ = os.Remove(c.DatFile(remotePath))
	}
	_ = os.Remove(c.MetaFile(remotePath))
	c.deleteMetaLocked(remotePath)
	c.deleteDataLocked(remotePath)
}

// getViewLocked returns a read-only payload view for remotePath.
// The caller must hold c.mu and must not mutate the returned slice.
// A payload loaded from disk is validated against its metadata binding
// BEFORE it is served or cached: a torn pair (a surviving .meta over a
// stale .dat — see viewMatchesMeta) is pruned here, so every consumer —
// Get, GetView, the meta+view getters, the pending-image capture, the
// writable-open preload — is protected at the one choke point. In-memory
// payloads skip the check: they were either written by a put (bound by
// construction) or admitted through this validated load.
// reportViewLoss forwards a prune to the owner. The caller holds the path
// lock (c.mu released), so the record is visible to any same-path barrier that
// follows. A transient read error never reports: the entry is kept and a retry
// can still publish it.
func (c *WriteBackCache) reportViewLoss(remotePath string, pruned bool, err error) {
	if !pruned {
		return
	}
	if err == nil {
		err = errStagedViewTorn
	}
	// Take the hook under c.mu: the caller holds the path lock, so this cannot
	// reorder against a same-path operation, and a hook installed after
	// construction is read without a data race.
	c.mu.Lock()
	hook := c.OnViewLost
	c.mu.Unlock()
	if hook == nil {
		return
	}
	hook(remotePath, err)
}

// getViewStateLocked reports the payload state for a path. pruned says the
// record was dropped by this read (torn pair or missing .dat): a transient
// read error keeps the entry and reports false.
func (c *WriteBackCache) getViewStateLocked(remotePath string) ([]byte, StagedViewState, error, bool) {
	if data, ok := c.data[remotePath]; ok {
		c.touchDataLocked(remotePath)
		return data, StagedViewOK, nil, false
	}
	meta, ok := c.metas[remotePath]
	if !ok {
		return nil, StagedViewNoMeta, nil, false
	}
	if meta.Kind == PendingChmod {
		// Data already landed; only the mode is outstanding, so the .dat is
		// absent by design. Reading it here would prune a healthy record.
		return nil, StagedViewPendingChmod, nil, false
	}
	data, err := os.ReadFile(c.DatFile(remotePath))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.pruneEntryLocked(remotePath, false)
			return nil, StagedViewNoData, os.ErrNotExist, true
		}
		// Kept: a transient read error is not evidence the image is gone.
		return nil, StagedViewNoData, err, false
	}
	if !viewMatchesMeta(meta, data) {
		// Torn pair: drop it — losing the entry cleanly beats serving or
		// uploading stale bytes at the new base.
		c.pruneEntryLocked(remotePath, true)
		return nil, StagedViewNoData, errStagedViewTorn, true
	}
	c.cacheDataLocked(remotePath, data, meta.Size)
	if cached, ok := c.data[remotePath]; ok {
		return cached, StagedViewOK, nil, false
	}
	return data, StagedViewOK, nil, false
}

func (c *WriteBackCache) getViewLocked(remotePath string) ([]byte, bool) {
	data, state, _, _ := c.getViewStateLocked(remotePath)
	if state != StagedViewOK {
		return nil, false
	}
	return data, true
}

func (c *WriteBackCache) cacheDataLocked(remotePath string, data []byte, size int64) {
	if size > writeBackInMemoryDataThreshold || len(data) > writeBackInMemoryDataThreshold || c.dataMaxBytes <= 0 {
		c.deleteDataLocked(remotePath)
		return
	}
	if !c.chargeDataLocked(int64(len(data))) {
		// Degrade: the process memory budget is full even after this cache
		// dropped its own LRU tail. The .dat file on disk is authoritative,
		// so serving the entry without the in-memory copy only costs a
		// re-read.
		c.deleteDataLocked(remotePath)
		return
	}
	if existing, ok := c.data[remotePath]; ok {
		old := int64(len(existing))
		c.dataBytes -= old
		if c.budget != nil {
			c.budget.Release(membudget.ClassWriteCache, old)
		}
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

// chargeDataLocked reserves n bytes of the process memory budget, dropping
// this cache's own LRU tail while the budget is short. It reports whether the
// payload may be retained. The caller must hold c.mu; the budget call here is
// Charge (never Acquire), so no reclaimer can re-enter this cache's lock.
func (c *WriteBackCache) chargeDataLocked(n int64) bool {
	if c.budget == nil {
		return true
	}
	for {
		if c.budget.Charge(membudget.ClassWriteCache, n) == nil {
			return true
		}
		tail := c.dataOrder.Back()
		if tail == nil {
			return false
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
		if c.budget != nil {
			c.budget.Release(membudget.ClassWriteCache, int64(len(data)))
		}
	}
	if elem, ok := c.dataElems[remotePath]; ok {
		c.dataOrder.Remove(elem)
		delete(c.dataElems, remotePath)
	}
}

func cloneWriteBackMeta(meta *WriteBackMeta) WriteBackMeta {
	cp := *meta
	if cp.PutGeneration == 0 {
		cp.PutGeneration = cp.Generation
	}
	cp.CacheAncestorGens = append([]uint64(nil), meta.CacheAncestorGens...)
	cp.liveAncestors = append([]string(nil), meta.liveAncestors...)
	return cp
}

// atomicWrite writes data to path atomically using a temp file + fsync + rename.
func atomicWrite(path string, data []byte) error {
	return atomicWriteMode(path, data, true, true)
}

// atomicWriteNoSync is atomicWrite without the durability fsyncs. The
// rename still keeps the visible file old-or-new across a crash (never
// torn); only the durability window changes. It is meant for snapshot
// writes whose durable authority is already carried elsewhere (shadow
// store + pending index), e.g. the post-durable-stage Flush snapshot —
// see Dat9FS.snapshotWriteBackLocked.
func atomicWriteNoSync(path string, data []byte) error {
	return atomicWriteMode(path, data, false, false)
}

// atomicWriteFileSyncOnly syncs the file's contents before the rename but
// leaves the directory entry change UNsynced: the caller batches its own
// directory fsync once every rename of a group has happened, so the group
// becomes durable together (no file of it is recoverable ahead of its
// metadata) at the cost of one shared directory sync.
func atomicWriteFileSyncOnly(path string, data []byte) error {
	return atomicWriteMode(path, data, true, false)
}

// atomicWriteMode is atomicWrite with the two durability levers split:
// syncFile fsyncs the temp file's contents before the rename, syncDir
// fsyncs the parent directory after it.
func atomicWriteMode(path string, data []byte, syncFile, syncDir bool) error {
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
	if syncFile {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
			return err
		}
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

	if !syncDir {
		return nil
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
