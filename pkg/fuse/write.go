package fuse

import "syscall"

const (
	defaultWriteBufferMaxSize = 64 << 20  // 64MB per file
	partSize                  = 8 << 20   // 8MB - must match s3client.PartSize
	maxPreloadSize            = 256 << 20 // 256MB - hard limit for preloading existing files into memory
)

// WriteBuffer accumulates write data for a single file.
// It tracks which 8MB parts have been modified so that on flush,
// only dirty parts need to be uploaded (unchanged parts are copied
// server-side via S3 UploadPartCopy).
// It is NOT thread-safe; callers must hold the FileHandle mutex.
type WriteBuffer struct {
	path       string
	buf        []byte
	maxSize    int64
	dirtyParts []bool
	touched    bool
}

// NewWriteBuffer creates a new WriteBuffer for the given path.
// If maxSize <= 0, defaultWriteBufferMaxSize (64MB) is used.
func NewWriteBuffer(path string, maxSize int64) *WriteBuffer {
	if maxSize <= 0 {
		maxSize = defaultWriteBufferMaxSize
	}
	return &WriteBuffer{
		path:    path,
		maxSize: maxSize,
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

	// Grow the buffer if needed.
	if end > int64(len(wb.buf)) {
		if end > int64(cap(wb.buf)) {
			// Allocate a new slice with enough capacity.
			grown := make([]byte, end)
			copy(grown, wb.buf)
			wb.buf = grown
		} else {
			// Extend length within existing capacity; new bytes are already zero.
			wb.buf = wb.buf[:end]
		}
	}

	copy(wb.buf[offset:], data)
	wb.markDirty(offset, end)
	wb.touched = true
	return uint32(len(data)), nil
}

// markDirty marks all parts that overlap with [start, end) as dirty.
func (wb *WriteBuffer) markDirty(start, end int64) {
	firstPart := int(start / partSize)
	lastPart := int((end - 1) / partSize)
	if end <= start {
		return
	}

	// Grow dirtyParts slice if needed.
	needed := lastPart + 1
	if needed > len(wb.dirtyParts) {
		grown := make([]bool, needed)
		copy(grown, wb.dirtyParts)
		wb.dirtyParts = grown
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

	cur := int64(len(wb.buf))
	switch {
	case size < cur:
		// Mark parts that are affected by the shrink as dirty.
		if size > 0 {
			wb.markDirty(size, cur)
		} else {
			// Truncate to zero: mark all existing parts dirty.
			wb.markDirty(0, cur)
		}
		wb.buf = wb.buf[:size]
		// Shrink dirtyParts if we now have fewer parts.
		newParts := int((size + partSize - 1) / partSize)
		if newParts < len(wb.dirtyParts) {
			wb.dirtyParts = wb.dirtyParts[:newParts]
		}
	case size > cur:
		if size > int64(cap(wb.buf)) {
			grown := make([]byte, size)
			copy(grown, wb.buf)
			wb.buf = grown
		} else {
			// Zero the extended region within existing capacity.
			prev := len(wb.buf)
			wb.buf = wb.buf[:size]
			for i := prev; i < len(wb.buf); i++ {
				wb.buf[i] = 0
			}
		}
		// Mark the extended region as dirty.
		wb.markDirty(cur, size)
	}
	return nil
}

// Size returns the current buffer length.
func (wb *WriteBuffer) Size() int64 {
	return int64(len(wb.buf))
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

// Bytes returns the buffer contents as a direct reference.
// The caller should not hold the returned slice for longer than necessary,
// since subsequent writes or truncations may invalidate it.
func (wb *WriteBuffer) Bytes() []byte {
	return wb.buf
}

// DirtyPartNumbers returns the 1-based part numbers that have been modified.
func (wb *WriteBuffer) DirtyPartNumbers() []int {
	var parts []int
	for i, dirty := range wb.dirtyParts {
		if dirty {
			parts = append(parts, i+1) // 1-based
		}
	}
	return parts
}

// MarkAllDirty marks every part in the current buffer as dirty.
// Used when the entire file content is loaded (e.g., new file or full rewrite).
func (wb *WriteBuffer) MarkAllDirty() {
	n := int((wb.Size() + partSize - 1) / partSize)
	wb.dirtyParts = make([]bool, n)
	for i := range wb.dirtyParts {
		wb.dirtyParts[i] = true
	}
}

// PartData returns the data for a specific 1-based part number.
// Returns nil if the part is out of range.
func (wb *WriteBuffer) PartData(partNum int) []byte {
	start := int64(partNum-1) * partSize
	if start >= int64(len(wb.buf)) {
		return nil
	}
	end := start + partSize
	if end > int64(len(wb.buf)) {
		end = int64(len(wb.buf))
	}
	return wb.buf[start:end]
}

// Reset clears the buffer, releasing the underlying memory.
func (wb *WriteBuffer) Reset() {
	wb.buf = nil
	wb.dirtyParts = nil
}

func (wb *WriteBuffer) ClearDirty() {
	for i := range wb.dirtyParts {
		wb.dirtyParts[i] = false
	}
	wb.touched = false
}
