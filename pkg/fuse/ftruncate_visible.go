package fuse

import (
	"context"
	"errors"
	"io"
	"sort"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pingcap/failpoint"
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
	fs.dirtyMu.Lock()
	committed := fs.mutationInodes[ino]
	dirty := fs.dirtyInodes[ino]
	fs.dirtyMu.Unlock()
	committedSeq := committed.committedSeq
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
	if entry.Size == 0 && dirty.seq > committedSeq && dirty.size == 0 {
		return nil, 0, true, gofuse.OK
	}
	if dirty.seq > committedSeq {
		return fs.readSupersededFtruncateOwnRange(ctx, reader, readerPath, readerSeq, dirty.seq, committedSeq, offset, reqSize)
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
	var data []byte
	cacheHit := false
	cacheView := fs.mountViewGeneration.Load()
	if fs.readCache != nil && fs.statCacheVerified() && !fs.bypassStableRemoteReadCaches(readerPath) &&
		committed.committedRevision > 0 && entry.Revision == committed.committedRevision &&
		entry.Size == committed.committedSize {
		if cached, ok := fs.readCache.Get(readerPath, committed.committedRevision); ok && int64(len(cached)) == committed.committedSize {
			end := offset + int64(reqSize)
			if end < offset {
				return nil, 0, false, gofuse.EINVAL
			}
			if offset < int64(len(cached)) {
				data = cached[offset:min(end, int64(len(cached)))]
			}
			cacheHit = true
			failpoint.InjectCall("ftruncateCommittedCacheBeforeValidate", fs, ino, readerPath)
		}
	}
	if !cacheHit {
		var err error
		data, err = fs.client.ReadAtCtx(ctx, fs.remotePath(readerPath), offset, int64(reqSize))
		if err != nil {
			return nil, 0, false, httpToFuseStatus(err)
		}
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
	if cacheHit {
		// A cache hit must still name the same committed image after all staging
		// checks. A newer commit can finish (and clear its dirty state) meanwhile.
		if !fs.lockMountViewRead(cacheView) {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN)
		}
		defer fs.mountViewMu.RUnlock()
		current, ok := fs.inodes.GetEntry(ino)
		linkedIno, linked := fs.inodes.GetInode(readerPath)
		if !ok || current.Unlinked || !linked || linkedIno != ino || current.ResourceID != entry.ResourceID ||
			current.Revision != committed.committedRevision || current.Size != committed.committedSize ||
			!fs.statCacheVerified() || fs.bypassStableRemoteReadCaches(readerPath) {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN)
		}
		fs.dirtyMu.Lock()
		latest := fs.mutationInodes[ino]
		unchanged := latest.committedSeq == committedSeq && latest.committedRevision == committed.committedRevision &&
			latest.committedSize == committed.committedSize && fs.dirtyInodes[ino].seq <= committedSeq
		fs.dirtyMu.Unlock()
		if !unchanged {
			return nil, 0, false, gofuse.Status(syscall.EAGAIN)
		}
	}
	return data, len(data), true, gofuse.OK
}

// A newer dirty reader can use its later writes, or a verified inherited
// image whose base is compatible with the current committed mutation.
func (fs *Dat9FS) readSupersededFtruncateOwnRange(ctx context.Context, reader *FileHandle, readerPath string, readerSeq, dirtySeq, committedSeq uint64, offset int64, reqSize uint32) ([]byte, int, bool, gofuse.Status) {
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
	// Only bytes before EOF need coverage; the range reader handles short reads.
	end = min(end, reader.Dirty.Size())
	if !reader.coversFtruncateWrites(offset, end, committedSeq) {
		data, status := fs.readInheritedFtruncateOwnRangeLocked(ctx, reader, offset, reqSize)
		return data, len(data), true, status
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

// readInheritedFtruncateOwnRangeLocked borrows this handle's current image
// when write-only coverage omits its validated base. The caller holds reader.mu
// and has checked the request range. No proof survives this call.
func (fs *Dat9FS) readInheritedFtruncateOwnRangeLocked(ctx context.Context, reader *FileHandle, offset int64, size uint32) ([]byte, gofuse.Status) {
	fail := gofuse.Status(syscall.EAGAIN)
	event := reader.pendingFtruncate.Load()
	if reader.Dirty == nil || !fs.ftruncateParticipates(reader) || event == nil || event.id == "" ||
		event.ino != reader.Ino || event.path != reader.Path || event.view != fs.mountViewGeneration.Load() ||
		!fs.ftruncateAliasLinked(reader) || !reader.LineageTrusted || reader.ftruncateInherited != event.id || !ftruncateDescends(reader, event.id) ||
		ftruncateSnapshotID(reader) == "" || reader.DirtySeq <= event.seq {
		return nil, fail
	}
	fs.dirtyMu.Lock()
	dirty, committed := fs.dirtyInodes[reader.Ino], fs.mutationInodes[reader.Ino]
	fs.dirtyMu.Unlock()
	if dirty.seq != reader.DirtySeq || reader.DirtySeq <= committed.committedSeq {
		return nil, fail
	}
	entry, exists := fs.inodes.GetEntry(reader.Ino)
	if !exists || entry.Unlinked {
		return nil, fail
	}
	revision := max(entry.Revision, fs.ftruncateAliasRevision(reader))
	// Pending root: a later unrelated known revision cannot supply this base.
	if committed.committedSeq < event.seq {
		if reader.BaseRev != event.revision || revision > event.revision {
			return nil, fail
		}
	} else {
		if committed.committedRevision <= 0 || revision != committed.committedRevision || reader.BaseRev > revision {
			return nil, fail
		}
		// A successful mutation on the currently bound revision preserves its
		// loaded base. A root committing later is identified by its exact sequence.
		related := reader.BaseRev == revision || committed.committedSeq == event.seq
		if !related {
			p := fs.ftruncateAliasProof(reader)
			related = p.rev == committed.committedRevision && p.size == committed.committedSize && ftruncateDescends(reader, p.snapshotID)
		}
		if !related {
			return nil, fail
		}
	}
	buffer, version, seq, path, base, id := reader.Dirty, reader.Dirty.contentVersion, reader.DirtySeq, reader.Path, reader.BaseRev, ftruncateSnapshotID(reader)
	shadow := reader.ShadowStageGen
	var data []byte
	var loaded []int
	valid := false
	defer func() {
		if !valid {
			discardFtruncateLoadedParts(buffer, loaded)
		}
	}()
	if shadow != 0 && (reader.ShadowReady || reader.ShadowSpill) {
		// Borrow the existing generation-locked local range reader. This
		// stack-only size descriptor is not published and owns no staging.
		var err error
		data, err = fs.readFtruncateStagedRange(&aliasStageProof{path: path, shadowGen: shadow, meta: &WriteBackMeta{Size: buffer.Size()}}, offset, size)
		if err != nil {
			return nil, fail
		}
	} else {
		var err error
		data, loaded, err = fs.readFtruncateLiveRangeLocked(ctx, reader, offset, size)
		if err != nil {
			if code, ok := err.(syscall.Errno); ok {
				return nil, gofuse.Status(code)
			}
			return nil, gofuse.EIO
		}
	}

	failpoint.InjectCall("ftruncateSelfReadBeforeValidate", fs, reader)
	if !fs.lockMountViewRead(event.view) {
		return nil, fail
	}
	defer fs.mountViewMu.RUnlock()
	nowEntry, exists := fs.inodes.GetEntry(reader.Ino)
	fs.dirtyMu.Lock()
	nowDirty, nowCommit := fs.dirtyInodes[reader.Ino], fs.mutationInodes[reader.Ino]
	fs.dirtyMu.Unlock()
	if !exists || !fs.ftruncateAliasLinked(reader) || nowEntry.ResourceID != entry.ResourceID || nowEntry.Revision != entry.Revision ||
		reader.pendingFtruncate.Load() != event || reader.Dirty != buffer || buffer.contentVersion != version || reader.DirtySeq != seq || reader.Path != path ||
		reader.BaseRev != base || ftruncateSnapshotID(reader) != id || nowDirty != dirty || nowCommit.committedSeq != committed.committedSeq || nowCommit.committedRevision != committed.committedRevision || nowCommit.committedSize != committed.committedSize ||
		max(nowEntry.Revision, fs.ftruncateAliasRevision(reader)) != revision || reader.ShadowStageGen != shadow ||
		(shadow != 0 && (fs.shadowStore == nil || fs.shadowStore.ActiveGeneration(path) != shadow)) {
		return nil, fail
	}
	valid = true
	return data, gofuse.OK
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
			data, status := fs.readInheritedFtruncateOwnRangeLocked(ctx, reader, offset, reqSize)
			return data, len(data), true, status, false
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
			if hasRetiredRead(src) && errors.Is(err, syscall.EAGAIN) {
				return nil, 0, false, gofuse.EAGAIN, false
			}
			return nil, 0, false, gofuse.EIO, false
		}
	}
	if n := src.Dirty.ReadAt(offset, data); n != len(data) {
		return nil, 0, false, gofuse.EIO, false
	}
	return data, len(data), true, gofuse.OK, false
}
