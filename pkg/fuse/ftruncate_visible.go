package fuse

import (
	"errors"
	"io"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

// readPendingFtruncateVisibleRange serves an fd truncate's uncommitted image
// before an older shadow, cache, or remote image can win a sibling read.
// The caller must not hold any FileHandle lock while scanning siblings.
func (fs *Dat9FS) readPendingFtruncateVisibleRange(reader *FileHandle, readerPath string, readerSeq uint64, readerMarked bool, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status) {
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
			return nil, 0, false, gofuse.OK
		}
		entry, ok := fs.inodes.GetEntry(reader.Ino)
		if !ok || entry.Unlinked {
			return nil, 0, false, gofuse.OK
		}
		if _, linked := entry.Paths[readerPath]; !linked {
			return nil, 0, false, gofuse.OK
		}
		if readerMarked && readerSeq == latestSeq {
			return nil, 0, false, gofuse.OK // The caller's own Dirty path is current.
		}
		data, n, handled, status, retry := fs.readPendingFtruncateVisibleRangeOnce(reader, entry, readerSeq, readerMarked, latestSeq, offset, reqSize)
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

func (fs *Dat9FS) readPendingFtruncateVisibleRangeOnce(reader *FileHandle, entry *InodeEntry, readerSeq uint64, readerMarked bool, latestSeq uint64, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status, bool) {
	var newest *FileHandle
	var newestSeq uint64
	marked := readerMarked && readerSeq != 0
	for _, src := range fs.openHandles.SnapshotInode(reader.Ino) {
		if src == nil || src == reader {
			continue
		}
		if !src.TryLock() {
			return nil, 0, false, gofuse.OK, true
		}
		_, linked := entry.Paths[src.Path]
		valid := linked && src.Ino == reader.Ino && src.Dirty != nil && src.DirtySeq != 0 &&
			!src.Unlinked && !src.UnlinkedSnapshot && src.UnlinkedData == nil
		if valid {
			marked = marked || src.Dirty.visibleTruncate
			if src.DirtySeq > newestSeq {
				newest, newestSeq = src, src.DirtySeq
			}
		}
		src.Unlock()
	}
	if !marked {
		return nil, 0, false, gofuse.OK, false
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
	if !newest.Dirty.visibleTruncate {
		return nil, 0, false, gofuse.Status(syscall.EAGAIN), false
	}
	if _, linked := entry.Paths[newest.Path]; !linked {
		return nil, 0, false, gofuse.OK, true
	}
	return fs.readPendingFtruncateHandleLocked(newest, offset, reqSize)
}

func (fs *Dat9FS) readPendingFtruncateHandleLocked(src *FileHandle, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status, bool) {
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
		if err := src.Dirty.EnsureLoaded(part); err != nil {
			return nil, 0, false, gofuse.EIO, false
		}
	}
	if n := src.Dirty.ReadAt(offset, data); n != len(data) {
		return nil, 0, false, gofuse.EIO, false
	}
	return data, len(data), true, gofuse.OK, false
}
