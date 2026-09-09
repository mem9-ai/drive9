package fuse

import (
	"sync"

	"github.com/mem9-ai/drive9/pkg/client"
)

// extentOpenFile is JuiceFS openfiles: one writer + logical size per inode,
// shared by every handle of that file (SQLite opens the same WAL many times).
type extentOpenFile struct {
	mu     sync.Mutex
	refs   int
	writer *extentFileWriter
	size   int64
	rev    int64
	gen    int64
	slices []client.SliceRow
	// zeroFrom is the lowest size this open file has been truncated to
	// since the last durable hole/shrink commit. -1 means unset. Reads at
	// offset >= zeroFrom must not serve committed remote slices until the
	// hole is committed (truncate-down then truncate-up is zeros).
	zeroFrom int64
	// committed is the last fsync/commit size on this inode. Sibling handles
	// keep their own OrigSize; reads must still fetch remote bytes up to this
	// size after another handle dropped the overlay.
	committed int64
	// keepCache is JuiceFS openFile.attr.KeepCache: the next Open may set
	// FOPEN_KEEP_CACHE only if gen/rev are unchanged since the last Open.
	keepCache bool
	attrRev   int64
	attrGen   int64
}

func newExtentOpenFile(size int64) *extentOpenFile {
	return &extentOpenFile{writer: newExtentFileWriter(), size: size, zeroFrom: -1}
}

func (of *extentOpenFile) putSlices(rev, gen, size int64, rows []client.SliceRow) {
	if of == nil {
		return
	}
	of.mu.Lock()
	of.rev = rev
	of.gen = gen
	of.slices = rows
	if size > of.size {
		of.size = size
	}
	of.mu.Unlock()
}

func (of *extentOpenFile) invalidateSlices() {
	if of == nil {
		return
	}
	of.mu.Lock()
	of.slices = nil
	of.mu.Unlock()
}

func (of *extentOpenFile) noteCommit(rev, gen, size int64) {
	if of == nil {
		return
	}
	of.mu.Lock()
	of.committed = size
	of.rev = rev
	of.gen = gen
	of.slices = nil
	// JuiceFS meta.Truncate is durable before the next Read. Once committed
	// size covers the logical file, hole rows are in meta.
	if of.zeroFrom >= 0 && size >= of.size {
		of.zeroFrom = -1
	}
	if size > of.size {
		of.size = size
	}
	of.mu.Unlock()
}

func (of *extentOpenFile) committedSize() int64 {
	if of == nil {
		return 0
	}
	of.mu.Lock()
	n := of.committed
	of.mu.Unlock()
	return n
}

// takeKeepCache is JuiceFS openfiles.Open + OpenCache: the first Open of a
// live open-file does not KEEP_CACHE (and InodeNotify). Later Opens of the
// same open-file keep the kernel page cache. JuiceFS OpenCheck hits while
// the inode is cached-open and skips GetAttr/mtime, which is what keeps
// MAP_SHARED -shm coherent across processes. Compact generation is
// userspace InvalidateChunk only.
func (of *extentOpenFile) takeKeepCache() bool {
	if of == nil {
		return false
	}
	of.mu.Lock()
	defer of.mu.Unlock()
	same := of.keepCache
	of.keepCache = true
	of.attrGen = of.gen
	of.attrRev = of.rev
	return same
}

func (fs *Dat9FS) extentOpenSize(ino uint64) (int64, bool) {
	if fs == nil || ino == 0 {
		return 0, false
	}
	fs.extentOpenMu.Lock()
	of := fs.extentOpens[ino]
	fs.extentOpenMu.Unlock()
	if of == nil {
		return 0, false
	}
	return of.logicalSize(), true
}

func (fs *Dat9FS) liveFileSize(entry *InodeEntry) int64 {
	if entry == nil {
		return 0
	}
	n := entry.Size
	if size, ok := fs.dirtyHandleSize(entry.Ino); ok && size > n {
		n = size
	}
	if size, ok := fs.extentOpenSize(entry.Ino); ok && size > n {
		n = size
	}
	return n
}

func (fs *Dat9FS) applyLiveFileSize(entry *InodeEntry) {
	if entry == nil || entry.IsDir {
		return
	}
	n := fs.liveFileSize(entry)
	if n <= entry.Size {
		return
	}
	entry.Size = n
	if fs.inodes != nil {
		fs.inodes.UpdateSize(entry.Ino, n)
	}
}

// adoptOpenExtentState copies layout/CAS identity from a sibling handle so a
// second Open of an unflushed extent file attaches the shared writer instead
// of materializing a zero Dirty buffer as a single-blob file.
func (fs *Dat9FS) refreshExtentHandlesAfterCommit(path string, skip *FileHandle, rev, gen, size int64) {
	if fs == nil || fs.openHandles == nil || path == "" || rev <= 0 {
		return
	}
	for _, h := range fs.openHandles.SnapshotPath(path) {
		if h == nil || h == skip {
			continue
		}
		if !h.TryLock() {
			continue
		}
		if h.isExtent() && rev >= h.BaseRev {
			h.BaseRev = rev
			h.SliceGeneration = gen
			h.OrigSize = size
			h.IsNew = false
			if !h.extentNeedTruncate {
				h.ZeroBase = false
			}
		}
		h.Unlock()
	}
}

func (fs *Dat9FS) adoptOpenExtentState(fh *FileHandle) {
	if fs == nil || fh == nil || fs.openHandles == nil {
		return
	}
	for _, src := range fs.openHandles.SnapshotPath(fh.Path) {
		if src == nil || src == fh {
			continue
		}
		if !src.TryLock() {
			continue
		}
		if !src.isExtent() {
			src.Unlock()
			continue
		}
		fh.ContentLayout = client.ContentLayoutExtent
		if fh.BaseRev == 0 {
			fh.BaseRev = src.BaseRev
			fh.SliceGeneration = src.SliceGeneration
			fh.OrigSize = src.OrigSize
			fh.IsNew = src.IsNew
		}
		src.Unlock()
		return
	}
}

func (fs *Dat9FS) attachExtentWriter(fh *FileHandle) {
	if fs == nil || fh == nil {
		return
	}
	if !fh.isExtent() {
		return
	}
	if fs.extentOpens == nil {
		fs.extentOpens = make(map[uint64]*extentOpenFile)
	}
	size := fh.OrigSize
	if fh.Dirty != nil && fh.Dirty.Size() > size {
		size = fh.Dirty.Size()
	}
	fs.extentOpenMu.Lock()
	of := fs.extentOpens[fh.Ino]
	if of == nil {
		of = newExtentOpenFile(size)
		fs.extentOpens[fh.Ino] = of
	}
	of.refs++
	if size > of.size {
		of.size = size
	}
	of.mu.Lock()
	if fh.BaseRev > of.rev {
		of.rev = fh.BaseRev
		of.gen = fh.SliceGeneration
	}
	if fh.OrigSize > of.committed && !fh.ZeroBase {
		of.committed = fh.OrigSize
	}
	committed := of.committed
	rev := of.rev
	gen := of.gen
	of.mu.Unlock()
	fs.extentOpenMu.Unlock()
	of.writer.bind(fs, fh.Path, fh.Ino, of)
	fh.extentWriter = of.writer
	fh.extentOpen = of
	if fh.extentDirty == nil {
		fh.extentDirty = &extentDirtySet{}
	}
	if committed > 0 {
		if fh.OrigSize < committed {
			fh.OrigSize = committed
		}
		fh.IsNew = false
		fh.ZeroBase = false
		if rev > fh.BaseRev {
			fh.BaseRev = rev
			fh.SliceGeneration = gen
		}
	}
}

func (fs *Dat9FS) detachExtentWriter(fh *FileHandle) {
	if fs == nil || fh == nil || fh.extentOpen == nil {
		return
	}
	fs.extentOpenMu.Lock()
	of := fh.extentOpen
	of.refs--
	if of.refs <= 0 && fs.extentOpens != nil {
		// JuiceFS keeps fileWriter until commitThread drops refs. A last
		// close Flush waits for pending; if anything is still in flight,
		// leave the open-file so commitLoop can finish.
		if of.writer == nil || !of.writer.hasInflight() {
			delete(fs.extentOpens, fh.Ino)
		}
	}
	fs.extentOpenMu.Unlock()
	fh.extentOpen = nil
}

func (fs *Dat9FS) extentWriteBufferCap() int64 {
	n := int64(defaultReadCacheMaxSize)
	if fs != nil && fs.opts != nil && fs.opts.CacheSize > 0 {
		n = fs.opts.CacheSize
	}
	capn := n * 8 / 10
	if capn < 32<<20 {
		capn = 32 << 20
	}
	return capn
}

func (fs *Dat9FS) extentTotalDirtyBytes() int64 {
	if fs == nil {
		return 0
	}
	fs.extentOpenMu.Lock()
	defer fs.extentOpenMu.Unlock()
	var n int64
	for _, of := range fs.extentOpens {
		if of != nil && of.writer != nil {
			n += of.writer.dirtyBytes()
		}
	}
	return n
}

func (of *extentOpenFile) setSize(n int64) {
	if of == nil {
		return
	}
	of.mu.Lock()
	if n > of.size {
		of.size = n
	}
	of.mu.Unlock()
}

func (of *extentOpenFile) logicalSize() int64 {
	if of == nil {
		return 0
	}
	of.mu.Lock()
	n := of.size
	of.mu.Unlock()
	return n
}

func (of *extentOpenFile) clipSize(n int64) {
	if of == nil {
		return
	}
	of.mu.Lock()
	of.size = n
	of.mu.Unlock()
}

func (of *extentOpenFile) markZeroFrom(n int64) {
	if of == nil {
		return
	}
	of.mu.Lock()
	if of.zeroFrom < 0 || n < of.zeroFrom {
		of.zeroFrom = n
	}
	of.mu.Unlock()
}

func (of *extentOpenFile) clearZeroFrom() {
	if of == nil {
		return
	}
	of.mu.Lock()
	of.zeroFrom = -1
	of.mu.Unlock()
}

func (of *extentOpenFile) setZeroFrom(n int64) {
	if of == nil {
		return
	}
	of.mu.Lock()
	of.zeroFrom = n
	of.mu.Unlock()
}

func (of *extentOpenFile) applyZeroFromAfterCommit(capturedZeroFrom, punchedEnd, resultSize int64, sentHoles bool) {
	if of == nil {
		return
	}
	of.mu.Lock()
	defer of.mu.Unlock()
	if of.zeroFrom >= 0 && capturedZeroFrom >= 0 && of.zeroFrom < capturedZeroFrom {
		return
	}
	if capturedZeroFrom >= 0 && resultSize <= capturedZeroFrom {
		of.zeroFrom = -1
		return
	}
	if sentHoles && punchedEnd >= resultSize && resultSize > capturedZeroFrom {
		of.zeroFrom = -1
		return
	}
	if sentHoles && punchedEnd > capturedZeroFrom {
		of.zeroFrom = punchedEnd
	}
}

// applyExtentZeroFromAfterCommit updates the local hole guard from the
// attempt captured before unlocked PUT. Clear when the commit removed the
// whole tail; otherwise raise the guard to the punched end.
func (fs *Dat9FS) applyExtentZeroFromAfterCommit(fh *FileHandle, capturedZeroFrom, capturedLocalSize int64, resultSize int64, sentHoles bool) {
	if fh == nil || fh.extentOpen == nil {
		return
	}
	punched := capturedLocalSize
	if logical := extentHandleLogicalSize(fh); logical >= 0 && logical < punched {
		punched = logical
	}
	fh.extentOpen.applyZeroFromAfterCommit(capturedZeroFrom, punched, resultSize, sentHoles)
}

func extentOpenZeroFrom(fh *FileHandle) int64 {
	if fh == nil || fh.extentOpen == nil {
		return -1
	}
	fh.extentOpen.mu.Lock()
	z := fh.extentOpen.zeroFrom
	fh.extentOpen.mu.Unlock()
	return z
}

func extentHolesForZeroFrom(fh *FileHandle, localSize int64) []client.SliceOp {
	z := extentOpenZeroFrom(fh)
	if z < 0 || localSize <= z {
		return nil
	}
	return holeOpsForRange(z, localSize-z)
}
