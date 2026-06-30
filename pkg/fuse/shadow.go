package fuse

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// smallFileShadowThreshold is the maximum file size for cached I/O mode.
const smallFileShadowThreshold = 64 << 20 // 64MB

// defaultWriteCacheFreeRatio is the default minimum free-space ratio for
// the cache-dir partition. When the ratio would drop below this after a
// write, ShadowStore refuses the write with ENOSPC.
const defaultWriteCacheFreeRatio = 0.10

// diskCheckInterval is the minimum time between disk space checks.
const diskCheckInterval = 5 * time.Second

// ShadowFile represents a per-path local shadow file with extent tracking.
type ShadowFile struct {
	fd      *os.File
	size    int64
	baseRev int64         // server revision at open/write time
	extents []DirtyExtent // dirty regions
}

// DirtyExtent tracks a dirty region in a shadow file.
type DirtyExtent struct {
	Offset int64
	Length int64
}

// retiredShadow holds a shadow file that has been logically removed from the
// active path map but still has pinned readers. When the last reader Unpins,
// the fd is closed and disk file deleted.
type retiredShadow struct {
	fd       *os.File
	diskPath string
	size     int64
}

// ShadowStore manages per-path shadow files for local staging of writes.
// Shadow files live at <dir>/<hash>.shadow and support pread/pwrite for
// efficient partial I/O without full-file materialization.
type ShadowStore struct {
	dir   string
	mu    sync.RWMutex
	files map[string]*ShadowFile // remote path → active shadow file

	// Generation-based pin for safe concurrent reads during commit cleanup.
	// Pin/PinIfExists return a generation token. Remove on a pinned path
	// retires the shadow (removes from files, moves fd to retired map) so
	// new writers get a fresh shadow. Old readers use their gen token to
	// read/unpin the retired fd.
	nextGen uint64                    // monotonic, 0 is reserved (no pin)
	active  map[string]uint64         // path → generation of current active shadow
	genFile map[uint64]*ShadowFile    // generation → active ShadowFile (for ReadAtGen on active gens)
	refs    map[uint64]int32          // generation → active pin count
	retired map[uint64]*retiredShadow // generation → retired shadow awaiting unpin

	// Write-back disk protection: configurable free-space ratio guard and
	// optional byte quota. See CheckWriteBackQuota.
	writeCacheFreeRatio float64      // minimum free-space ratio (default 0.10); 0 disables
	writeCacheMaxBytes  int64        // byte quota for write-back pending (0 = disabled)
	pendingBytes        atomic.Int64 // current pending bytes tracked by Add/SubPendingBytes

	// Throttled disk space check state (atomic for lock-free fast path).
	lastDiskCheck atomic.Int64 // unix nano of last check
	diskOK        atomic.Bool  // cached result of last check
}

// NewShadowStore creates a ShadowStore rooted at dir.
func NewShadowStore(dir string) (*ShadowStore, error) {
	return NewShadowStoreWithQuota(dir, defaultWriteCacheFreeRatio, 0)
}

// NewShadowStoreWithQuota creates a ShadowStore with configurable write-back
// disk protection. freeRatio is the minimum free-space ratio on the cache-dir
// partition (0 disables the check). maxBytes is an optional byte quota for
// total pending write-back data (0 disables).
func NewShadowStoreWithQuota(dir string, freeRatio float64, maxBytes int64) (*ShadowStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("shadow store dir: %w", err)
	}
	// Sweep retired shadows leaked by a previous process that died while
	// pinned readers still held them — pins cannot survive a restart, and
	// nothing else ever deletes these files.
	if entries, err := os.ReadDir(dir); err == nil {
		for _, e := range entries {
			if strings.Contains(e.Name(), ".shadow.retired.") {
				_ = os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}
	ss := &ShadowStore{
		dir:                 dir,
		files:               make(map[string]*ShadowFile),
		active:              make(map[string]uint64),
		genFile:             make(map[uint64]*ShadowFile),
		refs:                make(map[uint64]int32),
		retired:             make(map[uint64]*retiredShadow),
		writeCacheFreeRatio: freeRatio,
		writeCacheMaxBytes:  maxBytes,
	}
	ss.diskOK.Store(true)
	return ss, nil
}

// shadowPath returns the filesystem path for a shadow file.
func (s *ShadowStore) shadowPath(remotePath string) string {
	return filepath.Join(s.dir, hashPath(remotePath)+".shadow")
}

// CheckWriteBackQuota checks whether a write of requiredBytes would violate
// the write-back disk protection policy. Returns nil if the write is allowed,
// or syscall.ENOSPC if it would breach either the free-space ratio or the
// byte quota. The check is atomic: it predicts the post-write state and
// rejects before any bytes are written.
//
// This is the single guard checkpoint for all write-back local disk writes
// (shadow, pending, journal). Callers should invoke this before creating or
// extending shadow files.
func (s *ShadowStore) CheckWriteBackQuota(requiredBytes int64) error {
	// Check 1: byte quota (if enabled).
	if s.writeCacheMaxBytes > 0 {
		currentPending := s.pendingBytes.Load()
		if currentPending+requiredBytes > s.writeCacheMaxBytes {
			log.Printf("write-back quota rejected: cache_dir=%s pending_bytes=%d required_bytes=%d quota_bytes=%d reason=byte_quota_exceeded",
				s.dir, currentPending, requiredBytes, s.writeCacheMaxBytes)
			return syscall.ENOSPC
		}
	}

	// Check 2: free-space ratio (if enabled).
	if s.writeCacheFreeRatio > 0 {
		var stat syscall.Statfs_t
		if err := syscall.Statfs(s.dir, &stat); err == nil {
			totalBytes := int64(stat.Blocks) * int64(stat.Bsize)
			freeBytes := int64(stat.Bavail) * int64(stat.Bsize)
			freeAfterWrite := freeBytes - requiredBytes
			if totalBytes > 0 {
				ratioAfterWrite := float64(freeAfterWrite) / float64(totalBytes)
				if ratioAfterWrite < s.writeCacheFreeRatio {
					log.Printf("write-back quota rejected: cache_dir=%s free_bytes=%d required_bytes=%d free_ratio=%.4f threshold=%.4f reason=free_ratio_exceeded",
						s.dir, freeBytes, requiredBytes, ratioAfterWrite, s.writeCacheFreeRatio)
					return syscall.ENOSPC
				}
			}
		}
		// If Statfs fails, allow the write (cannot check).
	}

	return nil
}

// CheckWriteBackQuotaThrottled is a throttled version of CheckWriteBackQuota
// that caches the result for diskCheckInterval. Safe for hot-path use.
// requiredBytes is used for the actual Statfs check but the cached result
// applies to all callers during the throttle window.
func (s *ShadowStore) CheckWriteBackQuotaThrottled(requiredBytes int64) error {
	now := time.Now().UnixNano()
	last := s.lastDiskCheck.Load()
	if now-last < int64(diskCheckInterval) {
		if !s.diskOK.Load() {
			return syscall.ENOSPC
		}
		// Cached OK but still check byte quota (cheap, no syscall).
		if s.writeCacheMaxBytes > 0 {
			if s.pendingBytes.Load()+requiredBytes > s.writeCacheMaxBytes {
				return syscall.ENOSPC
			}
		}
		return nil
	}
	// CAS to avoid concurrent syscalls.
	if s.lastDiskCheck.CompareAndSwap(last, now) {
		err := s.CheckWriteBackQuota(requiredBytes)
		s.diskOK.Store(err == nil)
		return err
	}
	if !s.diskOK.Load() {
		return syscall.ENOSPC
	}
	// CAS loser: still check byte quota (cheap, no syscall).
	if s.writeCacheMaxBytes > 0 {
		if s.pendingBytes.Load()+requiredBytes > s.writeCacheMaxBytes {
			return syscall.ENOSPC
		}
	}
	return nil
}

// AddPendingBytes is a no-op. Pending byte accounting is now managed
// internally by ShadowStore write/remove methods.
// Deprecated: do not call; kept for backward compatibility.
func (s *ShadowStore) AddPendingBytes(n int64) {}

// SubPendingBytes is a no-op. Pending byte accounting is now managed
// internally by ShadowStore write/remove methods.
// Deprecated: do not call; kept for backward compatibility.
func (s *ShadowStore) SubPendingBytes(n int64) {}

// PendingBytes returns the current pending write-back byte count.
func (s *ShadowStore) PendingBytes() int64 {
	return s.pendingBytes.Load()
}

// RecoverPendingBytes scans the shadow directory and sets the pending byte
// counter to the sum of all shadow file sizes. Called once at startup after
// crash recovery to restore the accurate byte count.
func (s *ShadowStore) RecoverPendingBytes() {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".shadow" {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		total += fi.Size()
	}
	s.pendingBytes.Store(total)
}

// CheckDiskSpace returns true if there is sufficient disk space for writes.
// Deprecated: use CheckWriteBackQuota for configurable protection.
func (s *ShadowStore) CheckDiskSpace() bool {
	return s.CheckWriteBackQuota(0) == nil
}

// CheckDiskSpaceThrottled returns the cached disk space result, refreshing at
// most once per diskCheckInterval. Safe for hot-path use (lock-free fast path).
// Deprecated: use CheckWriteBackQuotaThrottled for configurable protection.
func (s *ShadowStore) CheckDiskSpaceThrottled() bool {
	return s.CheckWriteBackQuotaThrottled(0) == nil
}

func (s *ShadowStore) ensureShadowFile(remotePath string, baseRev int64) (*ShadowFile, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if sf, ok := s.files[remotePath]; ok {
		if baseRev != 0 {
			sf.baseRev = baseRev
		}
		return sf, nil
	}

	sp := s.shadowPath(remotePath)
	fd, err := os.OpenFile(sp, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("shadow open: %w", err)
	}
	fi, err := fd.Stat()
	if err != nil {
		_ = fd.Close()
		return nil, fmt.Errorf("shadow stat: %w", err)
	}
	sf := &ShadowFile{
		fd:      fd,
		size:    fi.Size(),
		baseRev: baseRev,
	}
	s.files[remotePath] = sf
	return sf, nil
}

// Ensure creates or opens a shadow file and makes its logical size match size.
// This is used to establish a local writable source of truth for new or
// truncating handles before any user writes arrive.
func (s *ShadowStore) Ensure(remotePath string, size int64, baseRev int64) error {
	sf, err := s.ensureShadowFile(remotePath, baseRev)
	if err != nil {
		return err
	}

	s.mu.Lock()
	if err := sf.fd.Truncate(size); err != nil {
		s.mu.Unlock()
		return fmt.Errorf("shadow ensure truncate: %w", err)
	}
	sf.size = size
	if baseRev != 0 {
		sf.baseRev = baseRev
	}
	s.mu.Unlock()
	return nil
}

// WriteAt writes user data directly into the shadow file without syncing.
// The caller decides when the shadow needs durable persistence.
func (s *ShadowStore) WriteAt(remotePath string, offset int64, data []byte, baseRev int64) (int, error) {
	sf, err := s.ensureShadowFile(remotePath, baseRev)
	if err != nil {
		return 0, err
	}

	n, err := sf.fd.WriteAt(data, offset)
	if err != nil {
		return n, fmt.Errorf("shadow write at %d: %w", offset, err)
	}

	end := offset + int64(n)
	s.mu.Lock()
	oldSize := sf.size
	if end > sf.size {
		sf.size = end
	}
	if baseRev != 0 {
		sf.baseRev = baseRev
	}
	newSize := sf.size
	s.mu.Unlock()
	if delta := newSize - oldSize; delta > 0 {
		s.pendingBytes.Add(delta)
	}
	return n, nil
}

// WriteFull replaces the shadow contents with a full snapshot.
func (s *ShadowStore) WriteFull(remotePath string, data []byte, baseRev int64) error {
	sf, err := s.ensureShadowFile(remotePath, baseRev)
	if err != nil {
		return err
	}
	if err := sf.fd.Truncate(0); err != nil {
		return fmt.Errorf("shadow reset truncate: %w", err)
	}
	if len(data) > 0 {
		n, err := sf.fd.WriteAt(data, 0)
		if err != nil {
			return fmt.Errorf("shadow write full: %w", err)
		}
		if n != len(data) {
			return fmt.Errorf("shadow write full: short write %d/%d", n, len(data))
		}
	}
	if err := sf.fd.Truncate(int64(len(data))); err != nil {
		return fmt.Errorf("shadow final truncate: %w", err)
	}

	newSize := int64(len(data))
	s.mu.Lock()
	oldSize := sf.size
	sf.size = newSize
	sf.baseRev = baseRev
	s.mu.Unlock()
	s.pendingBytes.Add(newSize - oldSize)
	return nil
}

// WriteStream replaces the shadow contents by streaming from r.
func (s *ShadowStore) WriteStream(remotePath string, r io.Reader, baseRev int64) (int64, error) {
	sf, err := s.ensureShadowFile(remotePath, baseRev)
	if err != nil {
		return 0, err
	}
	if err := sf.fd.Truncate(0); err != nil {
		return 0, fmt.Errorf("shadow reset truncate: %w", err)
	}
	if _, err := sf.fd.Seek(0, 0); err != nil {
		return 0, fmt.Errorf("shadow reset seek: %w", err)
	}
	n, err := io.Copy(sf.fd, r)
	if err != nil {
		return n, fmt.Errorf("shadow write stream: %w", err)
	}
	if err := sf.fd.Truncate(n); err != nil {
		return n, fmt.Errorf("shadow final truncate: %w", err)
	}

	s.mu.Lock()
	oldSize := sf.size
	sf.size = n
	sf.baseRev = baseRev
	s.mu.Unlock()
	s.pendingBytes.Add(n - oldSize)
	return n, nil
}

// Truncate updates the shadow file length without syncing it.
func (s *ShadowStore) Truncate(remotePath string, size int64, baseRev int64) error {
	sf, err := s.ensureShadowFile(remotePath, baseRev)
	if err != nil {
		return err
	}
	if err := sf.fd.Truncate(size); err != nil {
		return fmt.Errorf("shadow truncate: %w", err)
	}

	s.mu.Lock()
	oldSize := sf.size
	sf.size = size
	if baseRev != 0 {
		sf.baseRev = baseRev
	}
	s.mu.Unlock()
	s.pendingBytes.Add(size - oldSize)
	return nil
}

// Sync makes the shadow file durable on local disk.
func (s *ShadowStore) Sync(remotePath string) error {
	sf, err := s.ensureShadowFile(remotePath, 0)
	if err != nil {
		return err
	}
	if err := sf.fd.Sync(); err != nil {
		return fmt.Errorf("shadow sync: %w", err)
	}
	return nil
}

// WriteExtents writes only dirty parts from a WriteBuffer to a shadow file.
// Each dirty part is written at its correct offset via pwrite.
// This avoids full-file materialization — cost is O(dirty_parts) syscalls.
func (s *ShadowStore) WriteExtents(remotePath string, wb *WriteBuffer, baseRev int64) error {
	sf, err := s.ensureShadowFile(remotePath, baseRev)
	if err != nil {
		return err
	}

	// Write each dirty part at its correct offset.
	var extents []DirtyExtent
	partSize := wb.PartSize()

	// Small-file fast path: if the buffer is using a contiguous allocation,
	// write the entire content in one shot instead of iterating parts.
	if data, ok := wb.smallFileBytes(); ok {
		if len(data) > 0 {
			n, err := sf.fd.WriteAt(data, 0)
			if err != nil {
				return fmt.Errorf("shadow pwrite at 0: %w", err)
			}
			extents = append(extents, DirtyExtent{Offset: 0, Length: int64(n)})
		}
	} else {
		for idx, dirty := range wb.dirtyParts {
			if !dirty {
				continue
			}
			data := wb.PartData(idx + 1) // PartData is 1-based
			if data == nil {
				continue
			}
			offset := int64(idx) * partSize
			n, err := sf.fd.WriteAt(data, offset)
			if err != nil {
				return fmt.Errorf("shadow pwrite at %d: %w", offset, err)
			}
			extents = append(extents, DirtyExtent{Offset: offset, Length: int64(n)})
		}
	}

	// Update shadow file metadata.
	newSize := wb.Size()
	s.mu.Lock()
	oldSize := sf.size
	sf.size = newSize
	sf.extents = extents
	if baseRev != 0 {
		sf.baseRev = baseRev
	}
	s.mu.Unlock()
	s.pendingBytes.Add(newSize - oldSize)

	// Truncate to exact size if needed (handle shrinks).
	if err := sf.fd.Truncate(newSize); err != nil {
		return fmt.Errorf("shadow truncate: %w", err)
	}

	// Fsync the shadow file for durability.
	if err := sf.fd.Sync(); err != nil {
		return fmt.Errorf("shadow sync: %w", err)
	}

	return nil
}

// ReadAtGen reads from a shadow file identified by its generation token.
// Works for both active generations (shadow still in files map) and retired
// generations (shadow moved to retired map by Remove). Returns (0, error)
// if the generation is unknown.
func (s *ShadowStore) ReadAtGen(gen uint64, offset int64, buf []byte) (int, error) {
	s.mu.RLock()
	if sf, ok := s.genFile[gen]; ok {
		s.mu.RUnlock()
		return sf.fd.ReadAt(buf, offset)
	}
	rt, ok := s.retired[gen]
	s.mu.RUnlock()
	if !ok {
		return 0, fmt.Errorf("shadow gen %d not found", gen)
	}
	return rt.fd.ReadAt(buf, offset)
}

// SizeGen returns the size of a shadow file by generation token. Works for
// both active and retired generations. Returns -1 if the generation is unknown.
func (s *ShadowStore) SizeGen(gen uint64) int64 {
	s.mu.RLock()
	if sf, ok := s.genFile[gen]; ok {
		sz := sf.size
		s.mu.RUnlock()
		return sz
	}
	rt, ok := s.retired[gen]
	s.mu.RUnlock()
	if !ok {
		return -1
	}
	return rt.size
}

// ReadAt reads from a shadow file at the given offset. Uses pread for
// efficient partial reads without seeking.
func (s *ShadowStore) ReadAt(remotePath string, offset int64, buf []byte) (int, error) {
	s.mu.RLock()
	sf, ok := s.files[remotePath]
	s.mu.RUnlock()

	if !ok {
		// Not in memory — try opening from disk.
		sp := s.shadowPath(remotePath)
		fd, err := os.Open(sp)
		if err != nil {
			return 0, fmt.Errorf("shadow file not found: %s", remotePath)
		}
		s.mu.Lock()
		// Re-check after acquiring write lock.
		if existingSf, exists := s.files[remotePath]; exists {
			_ = fd.Close()
			sf = existingSf
		} else {
			fi, err := fd.Stat()
			if err != nil {
				_ = fd.Close()
				s.mu.Unlock()
				return 0, fmt.Errorf("shadow stat: %w", err)
			}
			sf = &ShadowFile{fd: fd, size: fi.Size()}
			s.files[remotePath] = sf
		}
		s.mu.Unlock()
	}

	return sf.fd.ReadAt(buf, offset)
}

// ReadAll reads the entire shadow file for the given path.
func (s *ShadowStore) ReadAll(remotePath string) ([]byte, error) {
	s.mu.RLock()
	sf, ok := s.files[remotePath]
	var size int64
	if ok {
		size = sf.size
	}
	s.mu.RUnlock()

	if !ok {
		// Try reading from disk.
		sp := s.shadowPath(remotePath)
		return os.ReadFile(sp)
	}

	data := make([]byte, size)
	_, err := sf.fd.ReadAt(data, 0)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Open opens the current shadow file for remotePath for streaming reads.
// The caller owns the returned file descriptor.
func (s *ShadowStore) Open(remotePath string) (*os.File, int64, error) {
	if s == nil {
		return nil, 0, fmt.Errorf("nil shadow store")
	}
	s.mu.RLock()
	sf, ok := s.files[remotePath]
	var size int64
	if ok {
		size = sf.size
	}
	s.mu.RUnlock()

	sp := s.shadowPath(remotePath)
	fd, err := os.Open(sp)
	if err != nil {
		return nil, 0, fmt.Errorf("shadow open stream: %w", err)
	}
	if ok {
		return fd, size, nil
	}
	fi, err := fd.Stat()
	if err != nil {
		_ = fd.Close()
		return nil, 0, fmt.Errorf("shadow stream stat: %w", err)
	}
	return fd, fi.Size(), nil
}

// Size returns the size of a shadow file, or -1 if not found.
func (s *ShadowStore) Size(remotePath string) int64 {
	s.mu.RLock()
	sf, ok := s.files[remotePath]
	var size int64
	if ok {
		size = sf.size
	}
	s.mu.RUnlock()

	if ok {
		return size
	}

	// Check disk.
	sp := s.shadowPath(remotePath)
	fi, err := os.Stat(sp)
	if err != nil {
		return -1
	}
	return fi.Size()
}

// BaseRev returns the base revision of a shadow file, or 0 if not found.
func (s *ShadowStore) BaseRev(remotePath string) int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sf, ok := s.files[remotePath]; ok {
		return sf.baseRev
	}
	return 0
}

// Pin increments the reference count for the active shadow at remotePath and
// returns a generation token. The caller must pass this token to Unpin on
// Release. Use after creating a new shadow (e.g. ShadowSpill O_TRUNC).
func (s *ShadowStore) Pin(remotePath string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	gen := s.active[remotePath]
	if gen == 0 {
		s.nextGen++
		gen = s.nextGen
		s.active[remotePath] = gen
		if sf, ok := s.files[remotePath]; ok {
			s.genFile[gen] = sf
		}
	}
	s.refs[gen]++
	return gen
}

// PinIfExists atomically checks whether an active shadow file exists for
// the given path (in memory or on disk) and, if so, increments its reference
// count. Returns a generation token and true, or (0, false) if no shadow
// exists. Loading from disk handles post-crash recovery where pending
// shadows are on disk but not yet in the files map.
func (s *ShadowStore) PinIfExists(remotePath string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sf, ok := s.files[remotePath]
	if !ok {
		// Try loading from disk (e.g. after crash/restart recovery).
		sp := s.shadowPath(remotePath)
		fd, err := os.OpenFile(sp, os.O_RDWR, 0o644)
		if err != nil {
			return 0, false
		}
		fi, err := fd.Stat()
		if err != nil {
			_ = fd.Close()
			return 0, false
		}
		sf = &ShadowFile{fd: fd, size: fi.Size()}
		s.files[remotePath] = sf
	}
	gen := s.active[remotePath]
	if gen == 0 {
		s.nextGen++
		gen = s.nextGen
		s.active[remotePath] = gen
		s.genFile[gen] = sf
	}
	s.refs[gen]++
	return gen, true
}

// Unpin decrements the reference count for the given generation. If the
// generation was retired (Remove called while pinned) and the count reaches
// zero, the retired fd is closed and the disk file deleted. Generation 0
// is a no-op (handle was never pinned).
func (s *ShadowStore) Unpin(gen uint64) {
	if gen == 0 {
		return
	}
	s.mu.Lock()
	s.refs[gen]--
	r := s.refs[gen]
	if r <= 0 {
		delete(s.refs, gen)
		r = 0
	}
	rt, isRetired := s.retired[gen]
	if isRetired && r == 0 {
		delete(s.retired, gen)
	}
	s.mu.Unlock()

	if isRetired && r == 0 && rt != nil {
		_ = rt.fd.Close()
		_ = os.Remove(rt.diskPath)
	}
}

// Remove removes a shadow file from memory and disk. If the path has active
// pins, the shadow is "retired" — removed from the active files map so new
// writers get a fresh shadow, but kept alive for existing readers until the
// last Unpin.
func (s *ShadowStore) Remove(remotePath string) {
	s.mu.Lock()
	gen := s.active[remotePath]
	if gen != 0 && s.refs[gen] > 0 {
		// Retire: remove from active maps, keep fd alive for pinned readers.
		sf := s.files[remotePath]
		delete(s.files, remotePath)
		delete(s.active, remotePath)
		delete(s.genFile, gen)
		diskPath := s.shadowPath(remotePath)
		// Rename disk file so Has() / ensureShadowFile don't see stale data.
		// If rename fails, delete the original to avoid new writers reusing
		// stale content via O_CREATE|O_RDWR. The retired fd remains valid
		// (unix: open fd survives unlink).
		retiredPath := fmt.Sprintf("%s.retired.%d", diskPath, gen)
		if err := os.Rename(diskPath, retiredPath); err != nil {
			_ = os.Remove(diskPath)
			retiredPath = diskPath // fd still valid, disk file gone
		}
		var removedSize int64
		if sf != nil {
			removedSize = sf.size
			s.retired[gen] = &retiredShadow{
				fd:       sf.fd,
				diskPath: retiredPath,
				size:     sf.size,
			}
		}
		s.mu.Unlock()
		if removedSize > 0 {
			s.pendingBytes.Add(-removedSize)
		}
		return
	}
	// No active pins — remove immediately.
	if gen != 0 {
		delete(s.active, remotePath)
		delete(s.genFile, gen)
		delete(s.refs, gen)
	}
	var removedSize int64
	sf, ok := s.files[remotePath]
	if ok {
		removedSize = sf.size
		_ = sf.fd.Close()
		delete(s.files, remotePath)
	}
	s.mu.Unlock()
	if removedSize > 0 {
		s.pendingBytes.Add(-removedSize)
	}

	// Always attempt disk cleanup — the shadow may exist only on disk
	// (e.g. after crash/restart recovery where it was never loaded into
	// the files map). Without this, a successfully committed shadow
	// would remain on disk and be served as stale local data by
	// Has()/ReadAt()/ReadAll() fallback paths.
	sp := s.shadowPath(remotePath)
	_ = os.Remove(sp)
}

// Rename moves a shadow file from oldPath to newPath.
func (s *ShadowStore) Rename(oldPath, newPath string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	sf, ok := s.files[oldPath]
	if !ok {
		return false
	}

	oldSP := s.shadowPath(oldPath)
	newSP := s.shadowPath(newPath)
	if err := os.Rename(oldSP, newSP); err != nil {
		return false
	}

	replacedSF := s.files[newPath]
	replacedGen := s.active[newPath]

	delete(s.files, oldPath)
	s.files[newPath] = sf
	// Transfer generation mapping to new path.
	oldGen := s.active[oldPath]
	if oldGen != 0 {
		s.active[newPath] = oldGen
		delete(s.active, oldPath)
	} else if replacedGen != 0 {
		delete(s.active, newPath)
	}
	if replacedGen != 0 && replacedGen != oldGen {
		delete(s.genFile, replacedGen)
		if s.refs[replacedGen] > 0 && replacedSF != nil {
			s.retired[replacedGen] = &retiredShadow{
				fd:   replacedSF.fd,
				size: replacedSF.size,
			}
		} else {
			delete(s.refs, replacedGen)
			if replacedSF != nil {
				_ = replacedSF.fd.Close()
			}
		}
	} else if replacedSF != nil && replacedSF != sf {
		_ = replacedSF.fd.Close()
	}
	return true
}

// Has reports whether a shadow file exists for the path.
func (s *ShadowStore) Has(remotePath string) bool {
	s.mu.RLock()
	_, ok := s.files[remotePath]
	s.mu.RUnlock()
	if ok {
		return true
	}
	// Check disk.
	sp := s.shadowPath(remotePath)
	_, err := os.Stat(sp)
	return err == nil
}

// Close closes all open shadow file descriptors.
func (s *ShadowStore) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sf := range s.files {
		_ = sf.fd.Close()
	}
	for _, rt := range s.retired {
		_ = rt.fd.Close()
	}
	s.files = make(map[string]*ShadowFile)
	s.active = make(map[string]uint64)
	s.genFile = make(map[uint64]*ShadowFile)
	s.refs = make(map[uint64]int32)
	s.retired = make(map[uint64]*retiredShadow)
}

// RecoverFromDisk scans the shadow directory and loads shadow files into memory.
func (s *ShadowStore) RecoverFromDisk() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range entries {
		if !isRegular(e) || filepath.Ext(e.Name()) != ".shadow" {
			continue
		}
		sp := filepath.Join(s.dir, e.Name())
		fi, err := e.Info()
		if err != nil {
			continue
		}
		fd, err := os.OpenFile(sp, os.O_RDWR, 0o644)
		if err != nil {
			// Corrupt shadow file — remove it.
			_ = os.Remove(sp)
			continue
		}
		// We don't know the remote path from the hash filename.
		// Shadow files recovered from disk will be matched via PendingIndex.
		// Store by the hash (filename without extension) temporarily.
		name := strings.TrimSuffix(e.Name(), ".shadow")
		s.files["__hash:"+name] = &ShadowFile{
			fd:   fd,
			size: fi.Size(),
		}
	}
	return nil
}

func isRegular(e os.DirEntry) bool {
	return !e.IsDir()
}
