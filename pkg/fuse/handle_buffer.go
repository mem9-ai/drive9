package fuse

import (
	"errors"
	"fmt"
	"io"
	"syscall"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
)

func (fs *Dat9FS) stageShadowLocked(fh *FileHandle, durable bool) error {
	if !fs.canStageShadowFastLocked(fh) {
		return syscall.ENOTSUP
	}
	fs.adoptCommittedRevisionLocked(fh)

	size := fh.Dirty.Size()

	if fh.ShadowReady && fh.ShadowSpill {
		if err := fs.shadowStore.Truncate(fh.Path, size, fh.BaseRev); err != nil {
			return err
		}
	} else {
		if err := fs.shadowStore.WriteFull(fh.Path, fh.Dirty.bytesView(), fh.BaseRev); err != nil {
			return err
		}
		fh.ShadowReady = true
	}
	// The shadow mutation above advances its content generation. Capture it
	// immediately so every later fallback (including one caused by a pending
	// index write failure) reads the exact bytes this handle just staged.
	// Waiting until after pending-index publication leaves ShadowSpill's
	// synchronous fallback pinned to the previous generation and turns a
	// recoverable metadata error into EIO.
	fh.ShadowStageGen = fs.shadowStore.ActiveGeneration(fh.Path)
	fh.ShadowStageSeq = fh.DirtySeq

	if durable {
		if err := fs.shadowStore.Sync(fh.Path); err != nil {
			return err
		}
	}
	mode, hasMode := fs.modeForPendingHandle(fh)
	snapshotID, parentSnapshotID := ensureStagedSnapshotLineageLocked(fh)
	if fh.ShadowSpill {
		gen, err := fs.pendingIndex.PutShadowSpillWithModeAndLineage(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev, mode, hasMode, snapshotID, parentSnapshotID, fh.StagedLineageTrusted, fh.stagedAncestors...)
		if err != nil {
			return fmt.Errorf("pending index put %s: %w", fh.Path, err)
		}
		fh.PendingIndexGen = gen
	} else {
		gen, err := fs.pendingIndex.PutWithBaseRevAndModeAndLineage(fh.Path, size, fs.pendingKindForHandle(fh), fh.BaseRev, mode, hasMode, snapshotID, parentSnapshotID, fh.StagedLineageTrusted, fh.stagedAncestors...)
		if err != nil {
			return fmt.Errorf("pending index put %s: %w", fh.Path, err)
		}
		fh.PendingIndexGen = gen
	}
	if fs.ftruncateParticipates(fh) {
		fs.pendingIndex.mu.Lock()
		if meta := fs.pendingIndex.items[fh.Path]; meta != nil && meta.Generation == fh.PendingIndexGen {
			meta.ownedStagingKnown = true
			meta.ownedStagingGens = fs.captureHandleStagingGensLocked(fh)
			meta.Inode, meta.MutationSeq = fh.Ino, fh.DirtySeq
		}
		fs.pendingIndex.mu.Unlock()
	}
	publishStagedSnapshotLineageLocked(fh)
	return nil
}

func (fs *Dat9FS) loadWritableHandleFromShadowLocked(fh *FileHandle, meta *WriteBackMeta) error {
	if fs.shadowStore == nil || fh == nil || meta == nil {
		return syscall.ENOENT
	}

	shadowGen := fs.shadowStore.ActiveGeneration(fh.Path)
	var (
		data []byte
		err  error
	)
	if shadowGen != 0 {
		data, err = fs.shadowStore.ReadAllIfGeneration(fh.Path, shadowGen)
	} else {
		data, err = fs.shadowStore.ReadAll(fh.Path)
	}
	if err != nil {
		return err
	}
	zeroOverwrite := !meta.LayerClean && meta.Kind == PendingOverwrite && meta.Size == 0 && len(data) == 0

	wb := fs.newWriteBuffer(fh.Path, maxPreloadSize, 0)
	if len(data) > 0 {
		if _, err := wb.Write(0, data); err != nil {
			return err
		}
		wb.ClearDirty()
	} else if zeroOverwrite {
		if err := wb.Truncate(0); err != nil {
			return err
		}
	} else {
		wb.totalSize = meta.Size
	}

	fh.Dirty = wb
	fh.ShadowReady = true
	fh.ShadowStageGen = shadowGen
	fh.PendingIndexGen = meta.Generation
	fh.ContentSnapshotID = meta.SnapshotID
	fh.contentAncestors = processLocalMetaAncestors(meta)
	fh.LineageTrusted = meta.lineageTrusted
	fh.IsNew = meta.Kind == PendingNew
	fh.ZeroBase = zeroOverwrite
	fh.OrigSize = meta.Size
	if meta.BaseRev > 0 {
		fh.BaseRev = meta.BaseRev
	} else if rev := fs.shadowStore.BaseRev(fh.Path); rev > 0 {
		fh.BaseRev = rev
	}
	if zeroOverwrite {
		fh.DirtySeq = fs.markDirtySizeRestore(fh.Ino, 0)
		fh.StagedSnapshotID = meta.SnapshotID
		fh.StagedParentSnapshotID = meta.ParentSnapshotID
		fh.stagedAncestors = processLocalMetaAncestors(meta)
		fh.StagedSnapshotSeq = fh.DirtySeq
		fh.StagedLineageTrusted = meta.lineageTrusted
	}
	if meta.HasMode {
		mode := meta.Mode & posixPermissionModeMask
		if !meta.LayerClean {
			fs.setPendingModeLocked(fh, mode, 0)
		}
		fs.inodes.UpdateMode(fh.Ino, mode)
	}
	return nil
}

// readHandleBufferLocked serves private writable buffers and shadows before
// Read falls through to remote I/O. It always releases fh.mu; handled reports
// whether it produced a response (including an error or EOF).
func (fs *Dat9FS) readHandleBufferLocked(fh *FileHandle, input *gofuse.ReadIn) (gofuse.ReadResult, gofuse.Status, string, int, bool) {
	var source string
	var bytesRead int
	if fh.ShadowSpill && fs.shadowStore != nil && fh.Dirty != nil && isSQLitePersistentJournalPath(fh.Path) && fh.Dirty.Size() == 0 && !fh.Dirty.hasDirtyPartMarks() && !fh.ZeroBase && fh.Flags&syscall.O_TRUNC == 0 {
		handlePath := fh.Path
		baseRev := fh.BaseRev
		fh.Unlock()
		if data, n, ok, st, src := fs.readSQLitePersistentJournalVisibleRange(handlePath, fh, baseRev, int64(input.Offset), input.Size); ok || st != gofuse.OK {
			source = src
			bytesRead = n
			if st != gofuse.OK {
				return nil, st, source, bytesRead, true
			}
			return gofuse.ReadResultData(data), gofuse.OK, source, bytesRead, true
		}
		source = "sqlite-sidecar-shadow-empty-eof"
		bytesRead = 0
		return gofuse.ReadResultData(nil), gofuse.OK, source, bytesRead, true
	}

	// ShadowSpill: read from shadow file (the authoritative data source).
	// Dirty has evicted parts so ReadAt would return incomplete data.
	if fh.ShadowSpill && fs.shadowStore != nil {
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		// Open-unlinked handle: read from the pinned private backing (a
		// retired shadow generation), never from the live path — that may
		// belong to a same-path replacement or be already removed.
		shadowGen := uint64(0)
		if fh.Unlinked && fh.UnlinkedShadowGen != 0 {
			shadowGen = fh.UnlinkedShadowGen
			size = fh.UnlinkedSize
		}
		if offset >= size {
			fh.Unlock()
			source = "shadow-spill-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK, source, bytesRead, true
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}
		fh.Unlock()
		result := make([]byte, end-offset)
		var n int
		var err error
		if shadowGen != 0 {
			n, err = fs.shadowStore.ReadAtGen(shadowGen, offset, result)
		} else {
			n, err = fs.shadowStore.ReadAt(fh.Path, offset, result)
		}
		if err != nil && !errors.Is(err, io.EOF) {
			source = "shadow-spill-error"
			return nil, gofuse.EIO, source, bytesRead, true
		}
		source = "shadow-spill"
		bytesRead = n
		return gofuse.ReadResultData(result[:n]), gofuse.OK, source, bytesRead, true
	}

	// If there's a dirty buffer (even empty — e.g. after Create or truncate-to-zero),
	// read from it so we don't go back to the server and see stale/non-existent data.
	// Uses ReadAt to avoid materializing the entire sparse buffer.
	//
	// However, if the handle has evicted (streaming-uploaded) parts, we cannot
	// serve reads from those ranges — the data is on S3 but not in memory.
	// For such ranges we fall through to the server read path.
	if fh.Dirty != nil && isSQLitePersistentJournalPath(fh.Path) && fh.DirtySeq == 0 && !fh.Dirty.HasDirtyParts() {
		handlePath := fh.Path
		baseRev := fh.BaseRev
		cleanEmptyEOF := fh.Dirty.Size() == 0 && (fh.IsNew || fh.BaseRev == 0 || fh.OrigSize == 0)
		fh.Unlock()
		if data, n, ok, st, src := fs.readSQLitePersistentJournalVisibleRange(handlePath, fh, baseRev, int64(input.Offset), input.Size); ok || st != gofuse.OK {
			source = src
			bytesRead = n
			if st != gofuse.OK {
				return nil, st, source, bytesRead, true
			}
			return gofuse.ReadResultData(data), gofuse.OK, source, bytesRead, true
		}
		if cleanEmptyEOF {
			source = "sqlite-sidecar-clean-empty-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK, source, bytesRead, true
		}
		source = "sqlite-sidecar-clean-remote"
		// Clean O_RDWR SQLite sidecar handles are reader snapshots. Do not
		// serve their preloaded WAL/journal bytes from the writable buffer:
		// the shared -shm can point readers at a newer fsync-committed sidecar
		// extent, and a stale clean buffer would produce short reads.
	} else if fh.Dirty != nil && fh.Dirty.HasDirtyParts() {
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			source = "dirty-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK, source, bytesRead, true
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}

		// Check if the read range touches any evicted part.
		// If so, we cannot serve this read from memory — fall through to server.
		touchesEvicted := false
		if evicted := fh.Dirty.StreamedPartIndices(); len(evicted) > 0 {
			ps := fh.Dirty.PartSize()
			firstPart := int(offset / ps)
			lastPart := int((end - 1) / ps)
			for p := firstPart; p <= lastPart; p++ {
				if evicted[p] && !fh.Dirty.IsPartLoaded(p) {
					touchesEvicted = true
					break
				}
			}
		}

		if !touchesEvicted {
			// Ensure parts touched by this read are loaded from the server
			// before calling ReadAt. Without this, ReadAt returns zeros for
			// unloaded parts in lazily-loaded files.
			ps := fh.Dirty.PartSize()
			firstPart := int(offset / ps)
			lastPart := int((end - 1) / ps)
			for p := firstPart; p <= lastPart; p++ {
				if !fh.Dirty.IsPartLoaded(p) {
					if err := fh.Dirty.EnsureLoaded(p); err != nil {
						fh.Unlock()
						source = "dirty-load-error"
						return nil, gofuse.EIO, source, bytesRead, true
					}
				}
			}

			result := make([]byte, end-offset)
			fh.Dirty.ReadAt(offset, result)
			fh.Unlock()
			source = "dirty-buffer"
			bytesRead = len(result)
			return gofuse.ReadResultData(result), gofuse.OK, source, bytesRead, true
		}
		// touchesEvicted: for new files (remoteSize == 0), the multipart
		// upload has not been completed yet — the object doesn't exist on the
		// server, so ReadStreamRange would fail. Return EIO; sequential writers
		// (cp, dd, ffmpeg) never read back evicted data in practice.
		if fh.Dirty.remoteSize == 0 {
			fh.Unlock()
			source = "dirty-evicted-new"
			return nil, gofuse.EIO, source, bytesRead, true
		}
		// Existing file with evicted parts: the original object still exists
		// on the server, so fall through to ReadStreamRange.
		source = "dirty-evicted-remote"
		fh.Unlock()
	} else if fh.Dirty != nil && fh.ShadowReady {
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			source = "dirty-shadow-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK, source, bytesRead, true
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}
		result := make([]byte, end-offset)
		fh.Dirty.ReadAt(offset, result)
		fh.Unlock()
		source = "dirty-shadow"
		bytesRead = len(result)
		return gofuse.ReadResultData(result), gofuse.OK, source, bytesRead, true
	} else if fh.Dirty != nil && fh.Dirty.Size() > 0 && !fh.Dirty.HasDirtyParts() {
		// Writable handle with lazy-loaded buffer (no dirty parts yet) —
		// serve already-loaded ranges from memory and fall back to the server
		// only when the requested range still has unloaded parts.
		offset := int64(input.Offset)
		size := fh.Dirty.Size()
		if offset >= size {
			fh.Unlock()
			source = "dirty-clean-eof"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK, source, bytesRead, true
		}
		end := offset + int64(input.Size)
		if end > size {
			end = size
		}
		if end <= offset {
			fh.Unlock()
			source = "dirty-clean-empty"
			bytesRead = 0
			return gofuse.ReadResultData(nil), gofuse.OK, source, bytesRead, true
		}
		ps := fh.Dirty.PartSize()
		firstPart := int(offset / ps)
		lastPart := int((end - 1) / ps)
		fullyLoaded := true
		for p := firstPart; p <= lastPart; p++ {
			if !fh.Dirty.IsPartLoaded(p) {
				fullyLoaded = false
				break
			}
		}
		if fullyLoaded {
			result := make([]byte, end-offset)
			fh.Dirty.ReadAt(offset, result)
			fh.Unlock()
			source = "dirty-clean-buffer"
			bytesRead = len(result)
			return gofuse.ReadResultData(result), gofuse.OK, source, bytesRead, true
		}
		source = "dirty-clean-remote"
		fh.Unlock()
		// Fall through to server read below
	} else {
		fh.Unlock()
	}
	return nil, gofuse.OK, source, bytesRead, false
}
