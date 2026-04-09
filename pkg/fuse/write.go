package fuse

import "syscall"

const (
	defaultWriteBufferMaxSize = 64 << 20  // 64MB per file
	DefaultPartSize           = 8 << 20   // 8MB - default for v1 uploads; v2 may use adaptive sizes
	maxPreloadSize            = 256 << 20 // 256MB - hard limit for preloading existing files into memory
)

// OnPartReadyFunc is called when a part is fully written and ready for upload.
// partNum is 1-based. data is a snapshot that the callee may own.
type OnPartReadyFunc func(partNum int, data []byte)

// LoadPartFunc is called to lazily load part data from the remote server.
// partNum is 1-based. Returns the part data.
type LoadPartFunc func(partNum int) ([]byte, error)

// WriteBuffer accumulates write data for a single file.
// It uses a sparse part map: only parts that have been written to or
// explicitly loaded are held in memory. This enables lazy preloading
// (load parts on demand) and streaming uploads (notify when parts are full).
//
// It tracks which parts have been modified so that on flush,
// only dirty parts need to be uploaded (unchanged parts are copied
// server-side via S3 UploadPartCopy).
// It is NOT thread-safe; callers must hold the FileHandle mutex.
type WriteBuffer struct {
	path       string
	totalSize  int64          // current logical file size
	maxSize    int64
	partSize   int64
	parts      map[int][]byte // 0-based part index → part data
	dirtyParts map[int]bool   // 0-based part index → dirty flag
	touched    bool

	// Callbacks (optional)
	OnPartReady   OnPartReadyFunc // called when a part is fully written
	LoadPart      LoadPartFunc    // called to lazily load part data
	notifiedParts map[int]bool    // parts already notified via OnPartReady (prevents duplicates)

	// remoteSize is the original remote file size, set when lazy loading
	// is configured. ensurePart() only calls LoadPart for parts whose
	// start offset < remoteSize. Parts beyond this are new (zero-filled).
	// Zero means "no remote data" (e.g. new file or eager-loaded file).
	remoteSize int64

	// Memory tracking
	curMemory int64 // current bytes held in parts map
}

// NewWriteBuffer creates a new WriteBuffer for the given path.
// If maxSize <= 0, defaultWriteBufferMaxSize (64MB) is used.
// If partSize <= 0, DefaultPartSize (8MB) is used.
func NewWriteBuffer(path string, maxSize int64, partSize int64) *WriteBuffer {
	if maxSize <= 0 {
		maxSize = defaultWriteBufferMaxSize
	}
	if partSize <= 0 {
		partSize = DefaultPartSize
	}
	return &WriteBuffer{
		path:       path,
		maxSize:    maxSize,
		partSize:   partSize,
		parts:      make(map[int][]byte),
		dirtyParts: make(map[int]bool),
	}
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

	// Update totalSize
	if end > wb.totalSize {
		wb.totalSize = end
	}

	// Write data across parts
	pos := offset
	dataOff := 0
	var newlyFullParts []int // 1-based part numbers that just became full

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
			grown := make([]byte, neededLen)
			copy(grown, part)
			wb.curMemory += neededLen - int64(len(part))
			part = grown
			wb.parts[partIdx] = part
		}

		copy(part[partOff:partOff+canWrite], data[dataOff:dataOff+int(canWrite)])
		wb.dirtyParts[partIdx] = true

		// Check if this part is now full
		partEnd := int64(partIdx+1) * wb.partSize
		if wb.totalSize >= partEnd && int64(len(part)) == wb.partSize {
			newlyFullParts = append(newlyFullParts, partIdx+1) // 1-based
		}

		pos += canWrite
		dataOff += int(canWrite)
	}

	// Handle gap zero-fill: if offset was beyond previous totalSize,
	// the parts in between need to exist and be zero-filled.
	// This is handled by ensurePart which creates zero-filled parts.

	wb.touched = true

	// Notify about full parts (only once per part — skip already-notified parts)
	if wb.OnPartReady != nil {
		for _, pn := range newlyFullParts {
			if wb.notifiedParts != nil && wb.notifiedParts[pn] {
				continue // already notified (e.g. rewrite of a full part)
			}
			partData := wb.PartData(pn)
			if partData != nil && int64(len(partData)) == wb.partSize {
				// Make a snapshot for the callback
				snapshot := make([]byte, len(partData))
				copy(snapshot, partData)
				if wb.notifiedParts == nil {
					wb.notifiedParts = make(map[int]bool)
				}
				wb.notifiedParts[pn] = true
				wb.OnPartReady(pn, snapshot)
			}
		}
	}

	return uint32(len(data)), nil
}

// ensurePart makes sure a part exists in the map. If it doesn't exist
// and LoadPart is set, tries to load from remote. Otherwise creates a
// nil entry that will be grown as needed by Write.
func (wb *WriteBuffer) ensurePart(partIdx int) error {
	if _, ok := wb.parts[partIdx]; ok {
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
					for i := copied + n; i < copied+int(canRead); i++ {
						buf[i] = 0
					}
				} else {
					// Entirely beyond the part — zero-fill
					for i := copied; i < copied+int(canRead); i++ {
						buf[i] = 0
					}
				}
			} else {
				copy(buf[copied:], part[partOff:partEnd])
			}
		} else {
			// Part not loaded — zero-fill
			for i := copied; i < copied+int(canRead); i++ {
				buf[i] = 0
			}
		}

		pos += canRead
		copied += int(canRead)
	}
	return total
}

// IsPartLoaded reports whether the 0-based part index is in memory.
func (wb *WriteBuffer) IsPartLoaded(partIdx int) bool {
	_, ok := wb.parts[partIdx]
	return ok
}

// Reset clears the buffer, releasing the underlying memory.
func (wb *WriteBuffer) Reset() {
	wb.parts = make(map[int][]byte)
	wb.dirtyParts = make(map[int]bool)
	wb.notifiedParts = nil
	wb.totalSize = 0
	wb.curMemory = 0
}

func (wb *WriteBuffer) ClearDirty() {
	wb.dirtyParts = make(map[int]bool)
	wb.touched = false
}
