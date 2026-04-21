package fuse

import "sync"

// FileHandle represents an open file in the FUSE filesystem.
// WriteBuffer is defined in write.go and supports offset-based writes.
type FileHandle struct {
	Ino          uint64
	Path         string
	Flags        uint32          // O_RDONLY, O_WRONLY, O_RDWR, O_APPEND, etc.
	OpenPID      uint32          // PID that opened the handle, when supplied by the kernel
	Dirty        *WriteBuffer    // write buffer, nil for read-only opens
	DirtySeq     uint64          // monotonic sequence for authoritative dirty-size tracking
	WriteBackSeq uint64          // DirtySeq at time of write-back cache snapshot (0 = no snapshot)
	OrigSize     int64           // original file size at open time (for patch detection)
	BaseRev      int64           // server revision at open time (for conflict detection)
	ZeroBase     bool            // true when the handle has adopted an explicit empty-file baseline
	IsNew        bool            // true only while this handle still needs create-if-absent server semantics
	ShadowReady  bool            // true when the local shadow file is a safe full snapshot
	Streamer     *StreamUploader // nil for small files / read-only; manages background part uploads
	Prefetch     *Prefetcher     // nil for writable handles; sequential read prefetcher
	mu           sync.Mutex
}

type livePendingSnapshot struct {
	Path      string
	Ino       uint64
	Size      int64
	Mtime     int64
	IsNew     bool
	BaseRev   int64
	HasShadow bool
	Data      []byte
}

// Lock acquires the file handle mutex.
func (fh *FileHandle) Lock() { fh.mu.Lock() }

// Unlock releases the file handle mutex.
func (fh *FileHandle) Unlock() { fh.mu.Unlock() }

// ---------------------------------------------------------------------------
// DirHandle / DirEntry
// ---------------------------------------------------------------------------

// DirHandle represents an open directory in the FUSE filesystem.
type DirHandle struct {
	Ino     uint64
	Path    string
	Entries []DirEntry // cached directory entries for ReadDir
}

// DirEntry is a simplified directory entry for FUSE readdir.
type DirEntry struct {
	Name string
	Ino  uint64
	Mode uint32 // S_IFDIR or S_IFREG
}

// ---------------------------------------------------------------------------
// HandleTable – generic handle allocator and lookup table
// ---------------------------------------------------------------------------

// HandleTable is a thread-safe, generic handle allocator and lookup table.
// Handle IDs start at 1 and are never reused within the lifetime of the table.
type HandleTable[T any] struct {
	mu     sync.Mutex
	table  map[uint64]T
	nextFh uint64 // starts at 1
}

// NewHandleTable creates a HandleTable with an empty map and nextFh set to 1.
func NewHandleTable[T any]() *HandleTable[T] {
	return &HandleTable[T]{
		table:  make(map[uint64]T),
		nextFh: 1,
	}
}

// Allocate assigns the next handle ID, stores val under that ID, and returns
// the newly allocated handle ID. It is safe for concurrent use.
func (ht *HandleTable[T]) Allocate(val T) uint64 {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	fh := ht.nextFh
	ht.table[fh] = val
	ht.nextFh++
	return fh
}

// Get looks up a value by handle ID. It returns the value and true if found,
// or the zero value of T and false otherwise.
func (ht *HandleTable[T]) Get(fh uint64) (T, bool) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	val, ok := ht.table[fh]
	return val, ok
}

// Delete removes the handle identified by fh from the table. It is a no-op if
// the handle does not exist.
func (ht *HandleTable[T]) Delete(fh uint64) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	delete(ht.table, fh)
}

// ForEach iterates over every handle in the table, calling fn for each one.
// The callback is invoked while the table lock is held, so fn must not call
// back into the HandleTable (doing so would deadlock).
func (ht *HandleTable[T]) ForEach(fn func(fh uint64, val T)) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	for fh, val := range ht.table {
		fn(fh, val)
	}
}

// Snapshot returns a copy of the handle values present at the time of call.
// The returned slice can be iterated without holding the table lock.
func (ht *HandleTable[T]) Snapshot() []T {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	vals := make([]T, 0, len(ht.table))
	for _, val := range ht.table {
		vals = append(vals, val)
	}
	return vals
}
