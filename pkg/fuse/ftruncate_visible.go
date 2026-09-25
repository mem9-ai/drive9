package fuse

import (
	"context"
	"errors"
	"io"
	"sort"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

type ftruncateWriteRange struct {
	start, end int64
	seq        uint64
}

func (fh *FileHandle) recordFtruncateWriteLocked(start, end int64) {
	if end <= start || fh.DirtySeq == 0 {
		return
	}
	if count := len(fh.ftruncateWrites); count != 0 {
		last := &fh.ftruncateWrites[count-1]
		if start >= last.end {
			fh.ftruncateWrites = append(fh.ftruncateWrites, ftruncateWriteRange{start, end, fh.DirtySeq})
			return
		}
		if start == last.start && end == last.end {
			last.seq = fh.DirtySeq
			return
		}
	}
	next := make([]ftruncateWriteRange, 0, len(fh.ftruncateWrites)+2)
	for _, r := range fh.ftruncateWrites {
		if r.end <= start || r.start >= end {
			next = append(next, r)
			continue
		}
		if r.start < start {
			next = append(next, ftruncateWriteRange{r.start, start, r.seq})
		}
		if r.end > end {
			next = append(next, ftruncateWriteRange{end, r.end, r.seq})
		}
	}
	next = append(next, ftruncateWriteRange{start, end, fh.DirtySeq})
	sort.Slice(next, func(i, j int) bool { return next[i].start < next[j].start })
	fh.ftruncateWrites = next
}

func (fh *FileHandle) discardFtruncateWritesFromSeqLocked(seq uint64) {
	kept := fh.ftruncateWrites[:0]
	for _, r := range fh.ftruncateWrites {
		if r.seq < seq {
			kept = append(kept, r)
		}
	}
	fh.ftruncateWrites = kept
}

func (fh *FileHandle) coversFtruncateWrites(start, end int64, markedSeq uint64) bool {
	if end <= start {
		return true
	}
	for _, r := range fh.ftruncateWrites {
		if r.end <= start {
			continue
		}
		if r.start > start {
			return false
		}
		if r.seq <= markedSeq || r.seq > fh.DirtySeq {
			return false
		}
		start = r.end
		if start >= end {
			return true
		}
	}
	return false
}

func (fs *Dat9FS) pendingFtruncateHandleSize(ino, seq uint64) (int64, bool, bool) {
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.Unlinked {
		return 0, false, false
	}
	var size int64
	found, busy := false, false
	for _, fh := range fs.openHandles.SnapshotInode(ino) {
		if fh == nil {
			continue
		}
		if !fh.TryLock() {
			busy = busy || fh.Flags&syscall.O_ACCMODE != syscall.O_RDONLY
			continue
		}
		_, linked := entry.Paths[fh.Path]
		if linked && fh.Dirty != nil && fh.DirtySeq == seq && !fh.Unlinked &&
			!fh.UnlinkedSnapshot && fh.UnlinkedData == nil {
			size, found = fh.Dirty.Size(), true
		}
		fh.Unlock()
	}
	if busy {
		return 0, false, true
	}
	return size, found, false
}

// readPendingFtruncateVisibleRange serves an fd truncate's uncommitted image
// before an older shadow, cache, or remote image can win a sibling read.
// The caller must not hold any FileHandle lock while scanning siblings.
func (fs *Dat9FS) readPendingFtruncateVisibleRange(ctx context.Context, reader *FileHandle, readerPath string, readerSeq uint64, readerMarked bool, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status) {
	if fs == nil || reader == nil || fs.openHandles == nil || reader.Ino == 0 || reqSize == 0 {
		return nil, 0, false, gofuse.OK
	}
	if offset < 0 {
		return nil, 0, false, gofuse.EINVAL
	}
	deadline := time.Now().Add(samePathDirtyWaitTimeout)
	for {
		latestSeq, pending := fs.pendingFtruncateSeq(reader.Ino)
		if !pending {
			return fs.readSupersededFtruncateRange(ctx, reader, readerPath, readerSeq, offset, reqSize)
		}
		entry, ok := fs.inodes.GetEntry(reader.Ino)
		if !ok || entry.Unlinked {
			return nil, 0, false, gofuse.OK
		}
		if _, linked := entry.Paths[readerPath]; !linked {
			return nil, 0, false, gofuse.OK
		}
		data, n, handled, status, retry := fs.readPendingFtruncateVisibleRangeOnce(ctx, reader, entry, readerSeq, readerMarked, latestSeq, offset, reqSize)
		if handled || status != gofuse.OK {
			return data, n, handled, status
		}
		if !retry {
			return nil, 0, false, gofuse.OK
		}
		if time.Now().After(deadline) {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN)
		}
		time.Sleep(samePathDirtyWaitInterval)
	}
}

// A newer committed inode mutation suppresses the old fd truncate while its
// handle is still open. Bypass that handle's pinned shadow and stale caches;
// a newer local pending image remains fail-closed until it can be selected.
func (fs *Dat9FS) readSupersededFtruncateRange(ctx context.Context, reader *FileHandle, readerPath string, readerSeq uint64, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status) {
	ino := reader.Ino
	if !fs.openHandles.HasVisibleTruncate(ino) {
		return nil, 0, false, gofuse.OK
	}
	committedSeq := fs.ftruncateCommittedSeq(ino)
	if committedSeq == 0 {
		return nil, 0, false, gofuse.OK
	}
	entry, ok := fs.inodes.GetEntry(ino)
	if !ok || entry.Unlinked {
		return nil, 0, false, gofuse.OK
	}
	if _, linked := entry.Paths[readerPath]; !linked {
		return nil, 0, false, gofuse.OK
	}
	fs.dirtyMu.Lock()
	dirty := fs.dirtyInodes[ino]
	fs.dirtyMu.Unlock()
	if entry.Size == 0 && dirty.seq > committedSeq && dirty.size == 0 {
		return nil, 0, true, gofuse.OK
	}
	if dirty.seq > committedSeq {
		return fs.readSupersededFtruncateOwnRange(reader, readerPath, readerSeq, dirty.seq, committedSeq, offset, reqSize)
	}
	var chmodOnlyGens map[string]uint64
	for path := range entry.Paths {
		if fs.pendingIndex != nil && fs.pendingIndex.HasPending(path) {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN)
		}
		if fs.writeBack != nil {
			if meta, pending := fs.writeBack.GetMeta(path); pending {
				if meta.Kind != PendingChmod {
					return nil, 0, false, gofuse.Status(syscall.EAGAIN)
				}
				if chmodOnlyGens == nil {
					chmodOnlyGens = make(map[string]uint64)
				}
				chmodOnlyGens[path] = meta.Generation
			}
		}
	}
	if fs.layerEnabled() {
		return nil, 0, false, gofuse.Status(syscall.EAGAIN)
	}
	data, err := fs.client.ReadAtCtx(ctx, fs.remotePath(readerPath), offset, int64(reqSize))
	if err != nil {
		return nil, 0, false, httpToFuseStatus(err)
	}
	current, ok := fs.inodes.GetEntry(ino)
	if !ok || current.Unlinked {
		return nil, 0, false, gofuse.Status(syscall.EAGAIN)
	}
	if _, linked := current.Paths[readerPath]; !linked {
		return nil, 0, false, gofuse.Status(syscall.EAGAIN)
	}
	fs.dirtyMu.Lock()
	newerDirty := fs.dirtyInodes[ino].seq > fs.mutationInodes[ino].committedSeq
	fs.dirtyMu.Unlock()
	if newerDirty {
		return nil, 0, false, gofuse.Status(syscall.EAGAIN)
	}
	for path := range current.Paths {
		if fs.pendingIndex != nil && fs.pendingIndex.HasPending(path) {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN)
		}
		if fs.writeBack != nil {
			if meta, pending := fs.writeBack.GetMeta(path); pending {
				if gen, known := chmodOnlyGens[path]; !known || meta.Kind != PendingChmod || meta.Generation != gen {
					return nil, 0, false, gofuse.Status(syscall.EAGAIN)
				}
			}
		}
	}
	return data, len(data), true, gofuse.OK
}

// A newer dirty reader can use only bytes it wrote after the committed
// mutation. Uncovered bytes may still belong to the superseded truncate.
func (fs *Dat9FS) readSupersededFtruncateOwnRange(reader *FileHandle, readerPath string, readerSeq, dirtySeq, committedSeq uint64, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status) {
	failClosed := gofuse.Status(syscall.EAGAIN)
	if readerSeq == 0 || readerSeq != dirtySeq || readerSeq <= committedSeq || !reader.TryLock() {
		return nil, 0, false, failClosed
	}
	defer reader.Unlock()
	if reader.Dirty == nil || reader.DirtySeq != readerSeq || reader.Path != readerPath ||
		reader.Unlinked || reader.UnlinkedSnapshot || reader.UnlinkedData != nil {
		return nil, 0, false, failClosed
	}
	entry, ok := fs.inodes.GetEntry(reader.Ino)
	if !ok || entry.Unlinked {
		return nil, 0, false, failClosed
	}
	if _, linked := entry.Paths[readerPath]; !linked {
		return nil, 0, false, failClosed
	}
	end := offset + int64(reqSize)
	if end < offset {
		return nil, 0, false, gofuse.EINVAL
	}
	if end > reader.Dirty.Size() || !reader.coversFtruncateWrites(offset, end, committedSeq) {
		return nil, 0, false, failClosed
	}
	stillCurrent := func() bool {
		fs.dirtyMu.Lock()
		defer fs.dirtyMu.Unlock()
		return fs.dirtyInodes[reader.Ino].seq == readerSeq &&
			fs.mutationInodes[reader.Ino].committedSeq == committedSeq
	}
	if !stillCurrent() {
		return nil, 0, false, failClosed
	}
	data, n, handled, status, retry := fs.readPendingFtruncateHandleLocked(reader, offset, reqSize, false)
	if retry || !handled || !stillCurrent() {
		return nil, 0, false, failClosed
	}
	return data, n, true, status
}

func (fs *Dat9FS) readPendingFtruncateVisibleRangeOnce(ctx context.Context, reader *FileHandle, entry *InodeEntry, readerSeq uint64, readerMarked bool, latestSeq uint64, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status, bool) {
	var newest *FileHandle
	var newestSeq uint64
	marked := readerMarked && readerSeq != 0
	markedSeq := uint64(0)
	if marked {
		markedSeq = readerSeq
	}
	busy := false
	for _, src := range fs.openHandles.SnapshotInode(reader.Ino) {
		if src == nil || src == reader {
			continue
		}
		if !src.TryLock() {
			busy = busy || src.Flags&syscall.O_ACCMODE != syscall.O_RDONLY
			continue
		}
		_, linked := entry.Paths[src.Path]
		valid := linked && src.Ino == reader.Ino && src.Dirty != nil && src.DirtySeq != 0 &&
			!src.Unlinked && !src.UnlinkedSnapshot && src.UnlinkedData == nil
		if valid {
			marked = marked || src.Dirty.visibleTruncate
			if src.Dirty.visibleTruncate && src.DirtySeq > markedSeq {
				markedSeq = src.DirtySeq
			}
			if src.DirtySeq > newestSeq {
				newest, newestSeq = src, src.DirtySeq
			}
		}
		src.Unlock()
	}
	if !marked {
		return nil, 0, false, gofuse.OK, busy
	}
	if busy {
		return nil, 0, false, gofuse.OK, true
	}
	latestSeq = max(latestSeq, readerSeq, newestSeq)
	if readerMarked && readerSeq != 0 && readerSeq == latestSeq {
		return nil, 0, false, gofuse.OK, false // The caller's Dirty path is current.
	}
	if readerSeq != 0 && readerSeq == latestSeq && !readerMarked {
		if !reader.TryLock() {
			return nil, 0, false, gofuse.OK, true
		}
		defer reader.Unlock()
		if reader.Dirty == nil || reader.DirtySeq != readerSeq || reader.Unlinked ||
			reader.UnlinkedSnapshot || reader.UnlinkedData != nil {
			return nil, 0, false, gofuse.OK, true
		}
		if _, linked := entry.Paths[reader.Path]; !linked || reader.Ino != entry.Ino {
			return nil, 0, false, gofuse.OK, true
		}
		if currentSeq, pending := fs.pendingFtruncateSeq(reader.Ino); !pending || currentSeq > readerSeq {
			return nil, 0, false, gofuse.OK, true
		}
		end := offset + int64(reqSize)
		if end < offset {
			return nil, 0, false, gofuse.EINVAL, false
		}
		end = min(end, reader.Dirty.Size())
		if offset < end && !reader.coversFtruncateWrites(offset, end, markedSeq) {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
		}
		return fs.readPendingFtruncateHandleLocked(reader, offset, reqSize, false)
	}
	if readerSeq > newestSeq {
		if readerMarked && readerSeq == latestSeq {
			return nil, 0, false, gofuse.OK, false
		}
		return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
	}
	if newest == nil || newestSeq != latestSeq {
		return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
	}
	if !newest.TryLock() {
		return nil, 0, false, gofuse.OK, true
	}
	defer newest.Unlock()
	if newest.Dirty == nil || newest.DirtySeq != newestSeq || newest.Unlinked ||
		newest.UnlinkedSnapshot || newest.UnlinkedData != nil {
		return nil, 0, false, gofuse.OK, true
	}
	if newestSeq <= fs.ftruncateCommittedSeq(reader.Ino) {
		return nil, 0, false, gofuse.OK, true
	}
	if !newest.Dirty.visibleTruncate {
		return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
	}
	if _, linked := entry.Paths[newest.Path]; !linked {
		return nil, 0, false, gofuse.OK, true
	}
	end := offset + int64(reqSize)
	if end < offset {
		return nil, 0, false, gofuse.EINVAL, false
	}
	end = min(end, newest.Dirty.Size())
	var loadedRemoteParts []int
	shadowReadable := fs.shadowStore != nil && (newest.ShadowReady || newest.ShadowSpill) && newest.ShadowStageGen != 0
	if end > offset && newest.Dirty.LoadPart != nil && !shadowReadable {
		partSize := newest.Dirty.PartSize()
		for part := int(offset / partSize); part <= int((end-1)/partSize); part++ {
			if int64(part)*partSize < newest.Dirty.remoteSize && !newest.Dirty.IsPartLoaded(part) {
				if newest.Dirty.dirtyParts[part] {
					return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
				}
				loadedRemoteParts = append(loadedRemoteParts, part)
			}
		}
	}
	data, n, handled, status, retry := fs.readPendingFtruncateHandleLocked(newest, offset, reqSize, true)
	if !handled || status != gofuse.OK {
		discardFtruncateLoadedParts(newest.Dirty, loadedRemoteParts)
		return data, n, handled, status, retry
	}
	current, ok := fs.inodes.GetEntry(reader.Ino)
	currentSeq, pending := fs.pendingFtruncateSeq(reader.Ino)
	if !ok || current.Unlinked || currentSeq != newestSeq || !pending {
		discardFtruncateLoadedParts(newest.Dirty, loadedRemoteParts)
		return nil, 0, false, gofuse.OK, true
	}
	if _, linked := current.Paths[newest.Path]; !linked {
		discardFtruncateLoadedParts(newest.Dirty, loadedRemoteParts)
		return nil, 0, false, gofuse.OK, true
	}
	if len(loadedRemoteParts) > 0 {
		stat, err := fs.client.StatCtx(ctx, fs.remotePath(newest.Path))
		if err != nil || stat == nil || newest.BaseRev <= 0 || stat.Revision != newest.BaseRev {
			discardFtruncateLoadedParts(newest.Dirty, loadedRemoteParts)
			return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
		}
		current, ok = fs.inodes.GetEntry(reader.Ino)
		currentSeq, pending = fs.pendingFtruncateSeq(reader.Ino)
		if !ok || current.Unlinked || currentSeq != newestSeq || !pending {
			discardFtruncateLoadedParts(newest.Dirty, loadedRemoteParts)
			return nil, 0, false, gofuse.OK, true
		}
		if _, linked := current.Paths[newest.Path]; !linked {
			discardFtruncateLoadedParts(newest.Dirty, loadedRemoteParts)
			return nil, 0, false, gofuse.OK, true
		}
	}
	return data, n, true, gofuse.OK, false
}

// discardFtruncateLoadedParts removes only clean parts first loaded by the
// current read, so a failed revision check cannot poison later reads.
// The source handle is locked and these parts were checked clean above.
func discardFtruncateLoadedParts(wb *WriteBuffer, parts []int) {
	for _, part := range parts {
		if data, ok := wb.parts[part]; ok {
			wb.curMemory -= int64(len(data))
			delete(wb.parts, part)
		}
	}
}

func (fs *Dat9FS) readPendingFtruncateHandleLocked(src *FileHandle, offset int64, reqSize uint32, allowRemoteLoad bool) ([]byte, int, bool, gofuse.Status, bool) {
	size := src.Dirty.Size()
	if offset >= size {
		return nil, 0, true, gofuse.OK, false
	}
	end := offset + int64(reqSize)
	if end < offset {
		return nil, 0, false, gofuse.EINVAL, false
	}
	if end > size {
		end = size
	}
	data := make([]byte, end-offset)
	if fs.shadowStore != nil && (src.ShadowReady || src.ShadowSpill) && src.ShadowStageGen != 0 {
		// ShadowStageGen is a content mutation generation, not the pin token
		// accepted by ReadAtGen. Read the active path and verify that no
		// sibling replaced its contents around this read.
		if fs.shadowStore.ActiveGeneration(src.Path) != src.ShadowStageGen {
			return nil, 0, false, gofuse.OK, true
		}
		n, err := fs.shadowStore.ReadAt(src.Path, offset, data)
		if fs.shadowStore.ActiveGeneration(src.Path) != src.ShadowStageGen {
			return nil, 0, false, gofuse.OK, true
		}
		if (err != nil && !errors.Is(err, io.EOF)) || n != len(data) {
			return nil, 0, false, gofuse.EIO, false
		}
		return data, n, true, gofuse.OK, false
	}
	if src.ShadowSpill {
		return nil, 0, false, gofuse.EIO, false
	}
	first := int(offset / src.Dirty.PartSize())
	last := int((end - 1) / src.Dirty.PartSize())
	evicted := src.Dirty.StreamedPartIndices()
	for part := first; part <= last; part++ {
		if evicted[part] && !src.Dirty.IsPartLoaded(part) {
			return nil, 0, false, gofuse.EIO, false
		}
		if !allowRemoteLoad && !src.Dirty.IsPartLoaded(part) {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
		}
		if err := src.Dirty.EnsureLoaded(part); err != nil {
			return nil, 0, false, gofuse.EIO, false
		}
	}
	if n := src.Dirty.ReadAt(offset, data); n != len(data) {
		return nil, 0, false, gofuse.EIO, false
	}
	return data, len(data), true, gofuse.OK, false
}
