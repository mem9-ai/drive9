package fuse

import (
	"log"
	"syscall"
)

const (
	defaultWriteBufferMaxSize = 64 << 20 // 64MB per file
	streamingWriteMaxSize     = 10 << 30 // 10GB — for sequential streaming writes
	DefaultPartSize           = 8 << 20  // 8MB - default for v1 uploads; v2 may use adaptive sizes
	maxPreloadSize            = 1 << 30  // 1GB - hard limit for preloading existing files into memory
	minPartGrowthCapacity     = 64 << 10 // 64KB lower bound for sparse part growth
)

// LoadPartFunc is called to lazily load part data from the remote server.
// partNum is 1-based. Returns the part data.
type LoadPartFunc func(partNum int) ([]byte, error)

// WriteBuffer accumulates write data for a single file.
// It uses a sparse part map: only parts that have been written to or
// explicitly loaded are held in memory. This enables lazy preloading
// (load parts on demand) and streaming uploads (notify when parts are full).
//
// For files that remain below the per-buffer smallFileMax, a fast path uses
// a single contiguous allocation instead of the sparse part map, avoiding
// map lookups and per-part allocations for the common case of editing small
// files.
//
// It tracks which parts have been modified so that on flush,
// only dirty parts need to be uploaded (unchanged parts are copied
// server-side via S3 UploadPartCopy).
// It is NOT thread-safe; callers must hold the FileHandle mutex.
type WriteBuffer struct {
	path      string
	totalSize int64 // current logical file size
	maxSize   int64
	partSize  int64
	// smallFileMax is the cutoff for the single-buffer fast path; equals the
	// server-advertised inline_threshold when known, otherwise the local
	// default. Stored per-buffer so per-mount values (server overrides,
	// tests) work without a global.
	smallFileMax int64
	parts        map[int][]byte // 0-based part index → part data
	dirtyParts   map[int]bool   // 0-based part index → dirty flag
	touched      bool

	// Callback (optional)
	LoadPart LoadPartFunc // called to lazily load part data

	// remoteSize is the original remote file size, set when lazy loading
	// is configured. ensurePart() only calls LoadPart for parts whose
	// start offset < remoteSize. Parts beyond this are new (zero-filled).
	// Zero means "no remote data" (e.g. new file or eager-loaded file).
	remoteSize int64

	// Memory tracking
	curMemory int64 // current bytes held in parts map

	// Sequential write detection
	appendCursor int64 // end of highest contiguous write; updated only on sequential writes
	sequential   bool  // false after any back-write

	// Streaming upload state
	uploadedParts map[int]bool // 0-based part indices uploaded by StreamUploader
	// OnPartFull is called when a sequential write fills a part completely.
	// partIdx is 0-based, data is the full part data (partSize bytes).
	// The callback should copy data if it needs to outlive the call.
	OnPartFull func(partIdx int, data []byte)

	// Small-file fast path: a single contiguous buffer used when the file
	// remains below smallFileThreshold. Avoids map lookups and per-part
	// allocations for the common case of editing small files (< 50KB).
	// When a write would exceed the threshold, the data is migrated to the
	// regular part map and this field is set to nil.
	smallFileData []byte
}

type writeBufferSnapshot struct {
	path          string
	totalSize     int64
	maxSize       int64
	partSize      int64
	smallFileMax  int64
	parts         map[int][]byte
	dirtyParts    map[int]bool
	touched       bool
	LoadPart      LoadPartFunc
	remoteSize    int64
	curMemory     int64
	appendCursor  int64
	sequential    bool
	uploadedParts map[int]bool
	OnPartFull    func(partIdx int, data []byte)
	smallFileData []byte
}

func (wb *WriteBuffer) snapshot() *writeBufferSnapshot {
	if wb == nil {
		return nil
	}
	return &writeBufferSnapshot{
		path:          wb.path,
		totalSize:     wb.totalSize,
		maxSize:       wb.maxSize,
		partSize:      wb.partSize,
		smallFileMax:  wb.smallFileMax,
		parts:         cloneByteMap(wb.parts),
		dirtyParts:    cloneBoolMap(wb.dirtyParts),
		touched:       wb.touched,
		LoadPart:      wb.LoadPart,
		remoteSize:    wb.remoteSize,
		curMemory:     wb.curMemory,
		appendCursor:  wb.appendCursor,
		sequential:    wb.sequential,
		uploadedParts: cloneBoolMap(wb.uploadedParts),
		OnPartFull:    wb.OnPartFull,
		smallFileData: cloneBytes(wb.smallFileData),
	}
}

func (wb *WriteBuffer) restore(snapshot *writeBufferSnapshot) {
	if wb == nil || snapshot == nil {
		return
	}
	wb.path = snapshot.path
	wb.totalSize = snapshot.totalSize
	wb.maxSize = snapshot.maxSize
	wb.partSize = snapshot.partSize
	wb.smallFileMax = snapshot.smallFileMax
	wb.parts = cloneByteMap(snapshot.parts)
	wb.dirtyParts = cloneBoolMap(snapshot.dirtyParts)
	wb.touched = snapshot.touched
	wb.LoadPart = snapshot.LoadPart
	wb.remoteSize = snapshot.remoteSize
	wb.curMemory = snapshot.curMemory
	wb.appendCursor = snapshot.appendCursor
	wb.sequential = snapshot.sequential
	wb.uploadedParts = cloneBoolMap(snapshot.uploadedParts)
	wb.OnPartFull = snapshot.OnPartFull
	wb.smallFileData = cloneBytes(snapshot.smallFileData)
}

func cloneByteMap(src map[int][]byte) map[int][]byte {
	if src == nil {
		return nil
	}
	dst := make(map[int][]byte, len(src))
	for k, v := range src {
		dst[k] = cloneBytes(v)
	}
	return dst
}

func cloneBoolMap(src map[int]bool) map[int]bool {
	if src == nil {
		return nil
	}
	dst := make(map[int]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func cloneBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

// NewWriteBuffer creates a new WriteBuffer for the given path.
// If maxSize <= 0, defaultWriteBufferMaxSize (64MB) is used.
// If partSize <= 0, DefaultPartSize (8MB) is used.
// The small-file fast path uses defaultSmallFileThreshold; production callers
// should immediately call SetSmallFileMax to track the server-advertised
// inline_threshold. Tests rely on the default and need not call it.
func NewWriteBuffer(path string, maxSize int64, partSize int64) *WriteBuffer {
	if maxSize <= 0 {
		maxSize = defaultWriteBufferMaxSize
	}
	if partSize <= 0 {
		partSize = DefaultPartSize
	}
	return &WriteBuffer{
		path:         path,
		maxSize:      maxSize,
		partSize:     partSize,
		smallFileMax: defaultSmallFileThreshold,
		parts:        make(map[int][]byte),
		dirtyParts:   make(map[int]bool),
	}
}

// SetSmallFileMax overrides the small-file fast-path cutoff. Must be called
// before any Write/Truncate; ignored when threshold <= 0.
func (wb *WriteBuffer) SetSmallFileMax(threshold int64) {
	if threshold <= 0 {
		return
	}
	wb.smallFileMax = threshold
}

// Write writes data at the given offset into the buffer.
// If offset is beyond the current length, the gap is zero-filled.
// Returns syscall.EFBIG if offset+len(data) would exceed maxSize.
// Returns the number of bytes written on success.
func (wb *WriteBuffer) Write(offset int64, data []byte) (uint32, error) {
	end := offset + int64(len(data))
	if end > wb.maxSize {
		return 0, syscall.EFBIG
	}

	// Small-file fast path: use a single contiguous allocation for files
	// that remain below the small-file threshold and still fit in a single
	// logical part. This avoids map lookups, per-part allocations, and
	// fragmentation for the common case.
	// Do NOT use the fast path when lazy loading is configured (LoadPart != nil)
	// because we may need to load existing remote data.
	if wb.smallFileData != nil || (wb.partSize >= wb.smallFileMax && len(wb.parts) == 0 && wb.LoadPart == nil && end <= wb.smallFileMax && wb.totalSize <= wb.smallFileMax) {
		return wb.writeSmallFile(offset, data)
	}

	// Update totalSize
	if end > wb.totalSize {
		wb.totalSize = end
	}

	// Write data across parts
	pos := offset
	dataOff := 0

	for dataOff < len(data) {
		partIdx := int(pos / wb.partSize)
		partOff := pos % wb.partSize

		// Ensure part exists (lazily load or create)
		if err := wb.ensurePart(partIdx); err != nil {
			return uint32(dataOff), err
		}

		part := wb.parts[partIdx]
		// How much can we write into this part?
		canWrite := wb.partSize - partOff
		remaining := int64(len(data) - dataOff)
		if canWrite > remaining {
			canWrite = remaining
		}

		// Extend part slice if needed
		neededLen := partOff + canWrite
		if neededLen > int64(len(part)) {
			if neededLen > wb.partSize {
				neededLen = wb.partSize
			}
			oldLen := int64(len(part))
			if neededLen <= int64(cap(part)) {
				part = part[:neededLen]
				clear(part[oldLen:])
			} else {
				grown := make([]byte, neededLen, growPartCapacity(int64(cap(part)), neededLen, wb.partSize))
				copy(grown, part)
				part = grown
			}
			wb.curMemory += neededLen - oldLen
			wb.parts[partIdx] = part
		}

		copy(part[partOff:partOff+canWrite], data[dataOff:dataOff+int(canWrite)])
		wb.dirtyParts[partIdx] = true

		pos += canWrite
		dataOff += int(canWrite)
	}

	// Handle gap zero-fill: if offset was beyond previous totalSize,
	// the parts in between need to exist and be zero-filled.
	// This is handled by ensurePart which creates zero-filled parts.

	wb.touched = true

	// Sequential write detection and streaming upload trigger
	prevCursor := wb.appendCursor
	if offset == wb.appendCursor {
		wb.appendCursor = end
	} else if offset > wb.appendCursor {
		// Gap write — still forward, cursor jumps to end
		wb.appendCursor = end
	} else {
		// Back-write: offset < appendCursor
		wb.sequential = false
	}

	// Check if we filled any parts (only in sequential mode with callback).
	// Part p is full when appendCursor >= (p+1)*partSize.
	if wb.sequential && wb.OnPartFull != nil && wb.appendCursor > prevCursor {
		if prevCursor < 0 {
			prevCursor = 0
		}
		// The first part that could have become newly full
		firstCandidate := int(prevCursor / wb.partSize)
		// The number of full parts based on current cursor
		numFullParts := int(wb.appendCursor / wb.partSize)
		for p := firstCandidate; p < numFullParts; p++ {
			if wb.uploadedParts != nil && wb.uploadedParts[p] {
				continue // already uploaded
			}
			if part, ok := wb.parts[p]; ok && int64(len(part)) == wb.partSize {
				wb.OnPartFull(p, part)
			}
		}
	}

	return uint32(len(data)), nil
}

func growPartCapacity(currentCap, neededLen, partSize int64) int64 {
	if partSize > 0 && neededLen > partSize {
		neededLen = partSize
	}
	newCap := currentCap
	if newCap < minPartGrowthCapacity {
		newCap = minPartGrowthCapacity
	}
	for newCap < neededLen && (partSize <= 0 || newCap < partSize) {
		newCap *= 2
	}
	if partSize > 0 && newCap > partSize {
		newCap = partSize
	}
	if newCap < neededLen {
		return neededLen
	}
	return newCap
}

// writeSmallFile is the fast-path Write for files that remain below
// smallFileThreshold. It uses a single contiguous []byte instead of the
// sparse part map.
func (wb *WriteBuffer) writeSmallFile(offset int64, data []byte) (uint32, error) {
	end := offset + int64(len(data))
	if end > wb.maxSize {
		return 0, syscall.EFBIG
	}
	if end > wb.smallFileMax {
		// Migrate to part mode and retry
		wb.migrateToPartMode()
		return wb.Write(offset, data)
	}

	oldLen := len(wb.smallFileData)
	needLen := end
	if wb.totalSize > needLen {
		needLen = wb.totalSize
	}

	// Ensure capacity (grow with headroom to reduce reallocations)
	if int(needLen) > cap(wb.smallFileData) {
		newCap := int(needLen)
		if newCap < 1024 {
			newCap = 1024
		}
		// Double the capacity to amortize reallocation cost
		if newCap < 2*cap(wb.smallFileData) {
			newCap = 2 * cap(wb.smallFileData)
		}
		if int64(newCap) > wb.smallFileMax {
			newCap = int(wb.smallFileMax)
		}
		grown := make([]byte, newCap)
		copy(grown, wb.smallFileData)
		wb.smallFileData = grown[:needLen]
	} else if int(needLen) > len(wb.smallFileData) {
		wb.smallFileData = wb.smallFileData[:needLen]
	}
	if oldLen < len(wb.smallFileData) {
		clear(wb.smallFileData[oldLen:])
	}

	copy(wb.smallFileData[offset:], data)
	if end > wb.totalSize {
		wb.totalSize = end
	}
	wb.touched = true
	wb.dirtyParts[0] = true // small files map to a single logical part

	// Sequential write detection
	if offset == wb.appendCursor {
		wb.appendCursor = end
	} else if offset > wb.appendCursor {
		wb.appendCursor = end
	} else {
		wb.sequential = false
	}

	return uint32(len(data)), nil
}

// migrateToPartMode transfers smallFileData into the sparse part map and
// clears smallFileData. Called when a write exceeds smallFileThreshold.
func (wb *WriteBuffer) migrateToPartMode() {
	if len(wb.smallFileData) == 0 {
		wb.smallFileData = nil
		return
	}
	// Transfer data to part 0
	wb.parts[0] = make([]byte, len(wb.smallFileData))
	copy(wb.parts[0], wb.smallFileData)
	wb.curMemory = int64(len(wb.smallFileData))
	wb.smallFileData = nil
}

// ensurePart makes sure a part exists in the map. If it doesn't exist
// and LoadPart is set, tries to load from remote. Otherwise creates a
// nil entry that will be grown as needed by Write.
func (wb *WriteBuffer) ensurePart(partIdx int) error {
	if wb.smallFileData != nil {
		// Small-file mode: all data is in smallFileData, no parts needed
		return nil
	}

	if _, ok := wb.parts[partIdx]; ok {
		return nil
	}

	// Check if this part was evicted after streaming upload.
	// Recreate as zero-filled — the original data is gone (already on S3).
	// Only the newly written bytes will be meaningful; other bytes are zeros.
	if wb.uploadedParts != nil && wb.uploadedParts[partIdx] {
		log.Printf("WARNING: back-write to evicted part %d of %s — "+
			"non-written bytes will be zero (original data already uploaded to S3)", partIdx, wb.path)
		data := make([]byte, wb.partSize)
		wb.parts[partIdx] = data
		wb.curMemory += wb.partSize
		return nil
	}

	// Try lazy load — only for parts that exist in the remote file.
	if wb.LoadPart != nil {
		partStart := int64(partIdx) * wb.partSize
		if partStart < wb.remoteSize {
			data, err := wb.LoadPart(partIdx + 1) // 1-based
			if err != nil {
				return err
			}
			wb.parts[partIdx] = data
			wb.curMemory += int64(len(data))
			return nil
		}
	}

	// Create empty part
	wb.parts[partIdx] = nil // will be grown as needed in Write
	return nil
}

// markDirty marks all parts that overlap with [start, end) as dirty.
func (wb *WriteBuffer) markDirty(start, end int64) {
	firstPart := int(start / wb.partSize)
	lastPart := int((end - 1) / wb.partSize)
	if end <= start {
		return
	}

	for i := firstPart; i <= lastPart; i++ {
		wb.dirtyParts[i] = true
	}
}

// Truncate resizes the buffer to size.
// Shrinks if size < current length, zero-extends if size > current length.
// Returns syscall.EFBIG if size exceeds maxSize.
func (wb *WriteBuffer) Truncate(size int64) error {
	if size > wb.maxSize {
		return syscall.EFBIG
	}
	if size < 0 {
		size = 0
	}
	wb.touched = true

	cur := wb.totalSize
	switch {
	case size < cur:
		if wb.smallFileData != nil {
			wb.smallFileData = wb.smallFileData[:size]
			wb.totalSize = size
			wb.touched = true
			wb.dirtyParts[0] = true
			return nil
		}

		// Mark affected parts as dirty
		if size > 0 {
			wb.markDirty(size, cur)
		} else {
			wb.markDirty(0, cur)
		}

		// Remove parts beyond the new size
		newPartCount := int((size + wb.partSize - 1) / wb.partSize)
		for idx := range wb.parts {
			if idx >= newPartCount {
				wb.curMemory -= int64(len(wb.parts[idx]))
				delete(wb.parts, idx)
				delete(wb.dirtyParts, idx)
			}
		}

		// Truncate the last surviving part if needed
		if size > 0 && newPartCount > 0 {
			lastIdx := newPartCount - 1
			partOff := size - int64(lastIdx)*wb.partSize
			if part, ok := wb.parts[lastIdx]; ok && int64(len(part)) > partOff {
				wb.curMemory -= int64(len(part)) - partOff
				wb.parts[lastIdx] = part[:partOff]
			}
		}

		// Mark exact-boundary shrinks
		if size > 0 && size%wb.partSize == 0 && newPartCount > 0 {
			wb.dirtyParts[newPartCount-1] = true
		}

		wb.totalSize = size

	case size > cur:
		if wb.smallFileData != nil {
			if size <= wb.smallFileMax {
				if int(size) > cap(wb.smallFileData) {
					newCap := int(size)
					if newCap < 1024 {
						newCap = 1024
					}
					grown := make([]byte, newCap)
					copy(grown, wb.smallFileData)
					wb.smallFileData = grown[:size]
				} else {
					oldLen := len(wb.smallFileData)
					wb.smallFileData = wb.smallFileData[:size]
					clear(wb.smallFileData[oldLen:])
				}
				wb.totalSize = size
				wb.touched = true
				wb.dirtyParts[0] = true
				return nil
			}
			// Migrate to part mode for larger size
			wb.migrateToPartMode()
			return wb.Truncate(size)
		}
		// Mark the extended region as dirty
		wb.markDirty(cur, size)
		// Ensure parts exist for the extended region (zero-filled on access)
		wb.totalSize = size
	}
	return nil
}

// Size returns the current logical file size.
func (wb *WriteBuffer) Size() int64 {
	return wb.totalSize
}

// PartSize returns the part size used for dirty-part boundary calculations.
func (wb *WriteBuffer) PartSize() int64 {
	return wb.partSize
}

// smallFileBytes returns a read-only view of the contiguous small-file buffer.
// The returned slice is clamped to totalSize so callers do not depend on the
// internal len(smallFileData) == totalSize invariant.
func (wb *WriteBuffer) smallFileBytes() ([]byte, bool) {
	if wb.smallFileData == nil {
		return nil, false
	}
	if wb.totalSize < int64(len(wb.smallFileData)) {
		return wb.smallFileData[:wb.totalSize], true
	}
	return wb.smallFileData, true
}

// bytesView returns a read-only view of the current buffer contents.
// For small-file mode this avoids materializing a second full copy.
// Callers must treat the returned slice as immutable and hold the file-handle
// lock while using it.
func (wb *WriteBuffer) bytesView() []byte {
	if wb.totalSize == 0 {
		return nil
	}
	if data, ok := wb.smallFileBytes(); ok {
		return data
	}
	return wb.Bytes()
}

func (wb *WriteBuffer) HasDirtyParts() bool {
	if wb.touched {
		return true
	}
	for _, d := range wb.dirtyParts {
		if d {
			return true
		}
	}
	return false
}

// Bytes returns the buffer contents as a contiguous byte slice.
// This materializes all parts into a single allocation.
// For backward compatibility with code that reads from the buffer.
func (wb *WriteBuffer) Bytes() []byte {
	if wb.totalSize == 0 {
		return nil
	}
	if data, ok := wb.smallFileBytes(); ok {
		buf := make([]byte, wb.totalSize)
		copy(buf, data)
		return buf
	}
	buf := make([]byte, wb.totalSize)
	numParts := int((wb.totalSize + wb.partSize - 1) / wb.partSize)
	for i := 0; i < numParts; i++ {
		part, ok := wb.parts[i]
		if !ok || part == nil {
			continue
		}
		start := int64(i) * wb.partSize
		copy(buf[start:], part)
	}
	return buf
}

// DirtyPartNumbers returns the 1-based part numbers that have been modified.
func (wb *WriteBuffer) DirtyPartNumbers() []int {
	var parts []int
	for idx, dirty := range wb.dirtyParts {
		if dirty {
			parts = append(parts, idx+1) // 1-based
		}
	}
	return parts
}

// MarkAllDirty marks every part in the current buffer as dirty.
// Used when the entire file content is loaded (e.g., new file or full rewrite).
func (wb *WriteBuffer) MarkAllDirty() {
	if wb.smallFileData != nil {
		wb.dirtyParts = map[int]bool{0: true}
		wb.touched = true
		return
	}
	n := int((wb.totalSize + wb.partSize - 1) / wb.partSize)
	wb.dirtyParts = make(map[int]bool, n)
	for i := 0; i < n; i++ {
		wb.dirtyParts[i] = true
	}
}

// PartData returns the data for a specific 1-based part number.
// Returns nil if the part is out of range (partNum beyond totalSize).
// Returns a zero-filled slice if the part is within range but not loaded.
func (wb *WriteBuffer) PartData(partNum int) []byte {
	partIdx := partNum - 1
	start := int64(partIdx) * wb.partSize
	if start >= wb.totalSize {
		return nil
	}

	if data, ok := wb.smallFileBytes(); ok {
		// Small-file mode: the entire file is logically part 1
		if partNum != 1 {
			return nil
		}
		end := wb.partSize
		if end > wb.totalSize {
			end = wb.totalSize
		}
		if int64(len(data)) < end {
			extended := make([]byte, end)
			copy(extended, data)
			return extended
		}
		buf := make([]byte, end)
		copy(buf, data)
		return buf
	}

	part, ok := wb.parts[partIdx]
	if !ok || part == nil {
		// Part not loaded — return zero-filled slice of the correct size
		end := start + wb.partSize
		if end > wb.totalSize {
			end = wb.totalSize
		}
		return make([]byte, end-start)
	}

	// The part may be shorter than expected if it's the last part
	expectedEnd := start + wb.partSize
	if expectedEnd > wb.totalSize {
		expectedEnd = wb.totalSize
	}
	expectedLen := expectedEnd - start

	if int64(len(part)) < expectedLen {
		// Extend with zeros
		extended := make([]byte, expectedLen)
		copy(extended, part)
		return extended
	}
	return part[:expectedLen]
}

// ReadAt reads up to len(buf) bytes from the buffer starting at offset.
// Returns the number of bytes read. This avoids materializing the entire
// sparse buffer — it reads directly from individual parts.
func (wb *WriteBuffer) ReadAt(offset int64, buf []byte) int {
	if offset >= wb.totalSize {
		return 0
	}
	end := offset + int64(len(buf))
	if end > wb.totalSize {
		end = wb.totalSize
	}
	total := int(end - offset)
	if total <= 0 {
		return 0
	}

	if data, ok := wb.smallFileBytes(); ok {
		start := int(offset)
		limit := start + total
		n := copy(buf[:total], data[start:limit])
		return n
	}

	pos := offset
	copied := 0
	for copied < total {
		partIdx := int(pos / wb.partSize)
		partOff := pos % wb.partSize

		// How much from this part?
		canRead := wb.partSize - partOff
		remaining := int64(total - copied)
		if canRead > remaining {
			canRead = remaining
		}

		part, ok := wb.parts[partIdx]
		if ok && part != nil {
			// Read from loaded part
			partEnd := partOff + canRead
			if partEnd > int64(len(part)) {
				// Part is shorter than expected — copy what exists, rest is zero
				if partOff < int64(len(part)) {
					n := copy(buf[copied:], part[partOff:])
					// Zero-fill the rest
					clear(buf[copied+n : copied+int(canRead)])
				} else {
					// Entirely beyond the part — zero-fill
					clear(buf[copied : copied+int(canRead)])
				}
			} else {
				copy(buf[copied:], part[partOff:partEnd])
			}
		} else {
			// Part not loaded — zero-fill
			clear(buf[copied : copied+int(canRead)])
		}

		pos += canRead
		copied += int(canRead)
	}
	return total
}

// IsPartLoaded reports whether the 0-based part index is in memory.
func (wb *WriteBuffer) IsPartLoaded(partIdx int) bool {
	if wb.smallFileData != nil {
		return partIdx == 0
	}
	_, ok := wb.parts[partIdx]
	return ok
}

// CanMaterializeFull reports whether Bytes() can reconstruct the full current
// file contents without silently zero-filling untouched remote-backed ranges.
// This is true for new files (remoteSize == 0) and for existing files whose
// entire retained remote prefix has been loaded into memory.
func (wb *WriteBuffer) CanMaterializeFull() bool {
	if wb.smallFileData != nil {
		return true
	}
	covered := wb.remoteSize
	if wb.totalSize < covered {
		covered = wb.totalSize
	}
	if covered <= 0 {
		return true
	}

	numParts := int((covered + wb.partSize - 1) / wb.partSize)
	for idx := 0; idx < numParts; idx++ {
		if !wb.IsPartLoaded(idx) {
			return false
		}
	}
	return true
}

// Reset clears the buffer, releasing the underlying memory.
func (wb *WriteBuffer) Reset() {
	wb.parts = make(map[int][]byte)
	wb.dirtyParts = make(map[int]bool)
	wb.totalSize = 0
	wb.curMemory = 0
	wb.smallFileData = nil
}

func (wb *WriteBuffer) ClearDirty() {
	wb.dirtyParts = make(map[int]bool)
	wb.touched = false
	// Note: we intentionally do NOT clear smallFileData here.
	// ClearDirty is called after successful flush/upload; the data remains
	// valid for reads until the handle is released or Reset is called.
}

// EnsureLoaded loads a part from the server if it's not already in memory.
// partIdx is 0-based. This is used by the Read path to load unmodified parts
// before serving from the buffer, avoiding returning zeros for unloaded parts.
func (wb *WriteBuffer) EnsureLoaded(partIdx int) error {
	return wb.ensurePart(partIdx)
}

// IsSequential reports whether the buffer is still in sequential append mode.
func (wb *WriteBuffer) IsSequential() bool {
	return wb.sequential
}

// ResetSequentialState resets the append cursor to newSize so that subsequent
// writes starting at newSize are correctly detected as sequential.
// This must be called after Truncate to avoid the stale appendCursor causing
// false back-write detection.
func (wb *WriteBuffer) ResetSequentialState(newSize int64) {
	wb.appendCursor = newSize
	// Re-enable sequential if it was broken — truncate is a new start.
	// Don't clear uploadedParts: already-uploaded parts are still on S3.
	wb.sequential = true
}

// EvictPart releases the memory for a 0-based part index after it has been
// uploaded via streaming. The part is recorded in uploadedParts so that
// subsequent reads/writes know it was already handled.
// If a back-write later touches this part, ensurePart will recreate it
// as a zero-filled slice and log a warning.
func (wb *WriteBuffer) EvictPart(partIdx int) {
	if wb.uploadedParts == nil {
		wb.uploadedParts = make(map[int]bool)
	}
	wb.uploadedParts[partIdx] = true

	if wb.smallFileData != nil {
		// Migrate to part mode before evicting
		wb.migrateToPartMode()
	}

	if part, ok := wb.parts[partIdx]; ok {
		wb.curMemory -= int64(len(part))
		delete(wb.parts, partIdx)
	}
}

// StreamedPartIndices returns the set of 0-based part indices that were
// uploaded during streaming (and whose memory was evicted).
func (wb *WriteBuffer) StreamedPartIndices() map[int]bool {
	return wb.uploadedParts
}

// DirtyStreamedParts returns parts that were evicted (streamed) but later
// back-written. These need to be re-uploaded at flush time.
// Returns a map of 1-based partNum → part data.
func (wb *WriteBuffer) DirtyStreamedParts() map[int][]byte {
	result := make(map[int][]byte)
	for idx := range wb.uploadedParts {
		if !wb.dirtyParts[idx] {
			continue // not back-written
		}
		// Part was back-written after eviction — get its data via PartData
		// which handles zero-extension and size clamping correctly.
		data := wb.PartData(idx + 1) // 1-based
		if data == nil {
			continue // part is beyond current totalSize (file was truncated)
		}
		cp := make([]byte, len(data))
		copy(cp, data)
		result[idx+1] = cp
	}
	return result
}
