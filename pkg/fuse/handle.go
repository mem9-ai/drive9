package fuse

import (
	"os"
	"sync"
	"time"

	"github.com/mem9-ai/dat9/pkg/client"
)

// FileHandle represents an open file in the FUSE filesystem.
// WriteBuffer is defined in write.go and supports offset-based writes.
type FileHandle struct {
	Ino                uint64
	Path               string
	Layer              PathLayer
	LocalFile          *os.File
	Flags              uint32          // O_RDONLY, O_WRONLY, O_RDWR, O_APPEND, etc.
	OpenPID            uint32          // PID that opened the handle, when supplied by the kernel
	Dirty              *WriteBuffer    // write buffer, nil for read-only opens
	DirtySeq           uint64          // monotonic sequence for authoritative dirty-size tracking
	WriteBackSeq       uint64          // DirtySeq at time of write-back cache snapshot (0 = no snapshot)
	OrigSize           int64           // original file size at open time (for patch detection)
	BaseRev            int64           // server revision at open time (for conflict detection)
	ZeroBase           bool            // true when the handle has adopted an explicit empty-file baseline
	IsNew              bool            // true if created via Create() (no prior remote existence)
	ShadowReady        bool            // true when the local shadow file is a safe full snapshot
	ShadowSpill        bool            // true when shadow is the authoritative data source (large IsNew/ZeroBase files)
	ShadowCommitReady  bool            // true when ShadowSpill Flush has staged shadow for async commit
	ShadowCommitSeq    uint64          // DirtySeq captured when ShadowCommitReady was staged
	ShadowPinned       bool            // true when this handle has pinned the shadow (must Unpin on Release)
	ShadowGen          uint64          // generation token from Pin/PinIfExists (passed to Unpin)
	Streamer           *StreamUploader // nil for small files / read-only; manages background part uploads
	Prefetch           *Prefetcher     // nil for writable handles; sequential read prefetcher
	ReadTarget         *client.ReadTarget
	WritePolicy        WritePolicy // per-handle remote durability policy chosen at open/create
	GitWorkspaceID     string      // set for handles served by the git workspace layer
	GitRelPath         string
	GitKind            string
	GitMode            string
	GitBaseObjectSHA   string
	PendingMode        uint32 // mode change deferred because a dirty handle was open
	HasPendingMode     bool   // true when PendingMode should be applied on Release
	PendingModeGen     uint64 // generation for PendingMode, used to avoid clearing newer chmods
	PreviousMode       uint32 // mode before PendingMode was set (for rollback on flush failure)
	HasPreviousMode    bool   // true when previous mode state was snapshotted
	PreviousModeKnown  bool   // true when PreviousMode was authoritative
	Unlinked           bool   // true after the directory entry was removed while this handle stayed open
	UnlinkedSnapshot   bool   // true when UnlinkedData is an authoritative read snapshot
	UnlinkedData       []byte // read-only snapshot for open-but-unlinked remote files
	UnlinkedSize       int64
	RemoteCommitUnlock func() // held same-path commit lock while local shadow state is reserved
	mu                 sync.Mutex
}

// Lock acquires the file handle mutex.
func (fh *FileHandle) Lock() { fh.mu.Lock() }

// TryLock attempts to acquire the file handle mutex without blocking.
func (fh *FileHandle) TryLock() bool { return fh.mu.TryLock() }

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
	Name       string
	Ino        uint64
	Mode       uint32 // S_IFDIR or S_IFREG
	Size       int64
	Mtime      time.Time
	Revision   int64
	AttrMode   uint32
	HasMode    bool
	Uid        uint32
	Gid        uint32
	HasUID     bool
	HasGID     bool
	IsDir      bool
	ResourceID string
	Nlink      uint32

	HasMetadata bool
}

// ---------------------------------------------------------------------------
// HandleTable – generic handle allocator and lookup table
// ---------------------------------------------------------------------------

// HandleTable is a thread-safe, generic handle allocator and lookup table.
// Handle IDs start at 1 and are never reused within the lifetime of the table.
type HandleTable[T any] struct {
	mu     sync.RWMutex
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
	ht.mu.RLock()
	defer ht.mu.RUnlock()

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
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	vals := make([]T, 0, len(ht.table))
	for _, val := range ht.table {
		vals = append(vals, val)
	}
	return vals
}
