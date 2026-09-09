package fuse

import (
	"context"
	"errors"
	"net/http"
	"path"
	"strings"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

type byteRange struct {
	start int64
	end   int64
}

type extentDirtySet struct {
	ranges []byteRange
}

func (d *extentDirtySet) add(start, end int64) {
	if d == nil || end <= start {
		return
	}
	d.ranges = append(d.ranges, byteRange{start, end})
	d.merge()
}

func (d *extentDirtySet) merge() {
	if len(d.ranges) < 2 {
		return
	}
	for i := 1; i < len(d.ranges); i++ {
		j := i
		for j > 0 && d.ranges[j].start < d.ranges[j-1].start {
			d.ranges[j], d.ranges[j-1] = d.ranges[j-1], d.ranges[j]
			j--
		}
	}
	out := d.ranges[:0]
	cur := d.ranges[0]
	for i := 1; i < len(d.ranges); i++ {
		r := d.ranges[i]
		if r.start <= cur.end {
			if r.end > cur.end {
				cur.end = r.end
			}
			continue
		}
		out = append(out, cur)
		cur = r
	}
	d.ranges = append(out, cur)
}

func (d *extentDirtySet) clip(size int64) {
	if d == nil {
		return
	}
	out := d.ranges[:0]
	for _, r := range d.ranges {
		if r.start >= size {
			continue
		}
		if r.end > size {
			r.end = size
		}
		if r.end > r.start {
			out = append(out, r)
		}
	}
	d.ranges = out
}

func (d *extentDirtySet) empty() bool {
	return d == nil || len(d.ranges) == 0
}

func (d *extentDirtySet) punch(start, end int64) {
	if d == nil || end <= start {
		return
	}
	out := d.ranges[:0]
	for _, r := range d.ranges {
		if r.end <= start || r.start >= end {
			out = append(out, r)
			continue
		}
		if r.start < start {
			out = append(out, byteRange{r.start, start})
		}
		if r.end > end {
			out = append(out, byteRange{end, r.end})
		}
	}
	d.ranges = out
}

func (d *extentDirtySet) cloneRanges() []byteRange {
	if d == nil || len(d.ranges) == 0 {
		return nil
	}
	out := make([]byteRange, len(d.ranges))
	copy(out, d.ranges)
	return out
}

func (fh *FileHandle) isExtent() bool {
	return fh != nil && fh.ContentLayout == client.ContentLayoutExtent
}

func (fs *Dat9FS) extentLayoutForCreate(localPath string) client.ContentLayout {
	if fs == nil {
		return ""
	}
	if fs.shouldUseExtentPath(localPath) {
		return client.ContentLayoutExtent
	}
	return client.ContentLayoutSingle
}

func (fs *Dat9FS) shouldUseExtentPath(localPath string) bool {
	if fs == nil || fs.opts == nil {
		return false
	}
	for _, pat := range fs.opts.ExtentPaths {
		if matchExtentPattern(pat, localPath) {
			return true
		}
	}
	return false
}

func matchExtentPattern(pattern, filePath string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	name := path.Base(filePath)
	if strings.HasPrefix(pattern, "*.") {
		return strings.HasSuffix(strings.ToLower(name), strings.ToLower(pattern[1:]))
	}
	if strings.Contains(pattern, "*") {
		ok, _ := path.Match(path.Base(pattern), name)
		return ok
	}
	return strings.EqualFold(name, pattern)
}

func copyExtentPayloads(wb *WriteBuffer, ranges []byteRange) []client.ExtentPayload {
	if wb == nil {
		return nil
	}
	var out []client.ExtentPayload
	for _, r := range ranges {
		if r.end <= r.start {
			continue
		}
		buf := make([]byte, r.end-r.start)
		n := wb.ReadAt(r.start, buf)
		if n <= 0 {
			continue
		}
		// WriteBuffer.ReadAt zero-fills unloaded parts. JuiceFS never
		// commits a data slice of grow zeros (those are hole slices).
		if allZero(buf[:n]) {
			continue
		}
		out = append(out, client.ExtentPayload{FileOff: r.start, Data: buf[:n]})
	}
	return out
}

func allZero(buf []byte) bool {
	for _, b := range buf {
		if b != 0 {
			return false
		}
	}
	return len(buf) > 0
}

func (fs *Dat9FS) seedHandleLayoutFromStat(ctx context.Context, fh *FileHandle) {
	if fs == nil || fh == nil || fs.client == nil || fh.ContentLayout != "" {
		return
	}
	if !fs.shouldUseExtentPath(fh.Path) {
		return
	}
	stat, err := fs.client.StatCtx(ctx, fs.remotePath(fh.Path))
	if err != nil || stat == nil {
		fh.ContentLayout = client.ContentLayoutExtent
		return
	}
	fh.ContentLayout = stat.ContentLayout
	fh.SliceGeneration = stat.SliceGeneration
	if stat.Revision > 0 && fh.BaseRev == 0 {
		fh.BaseRev = stat.Revision
	}
	if fh.isExtent() {
		fh.OrigSize = stat.Size
		if fh.extentDirty == nil {
			fh.extentDirty = &extentDirtySet{}
		}
		fs.attachExtentWriter(fh)
		go func() {
			fs.prefetchExtentSlices(context.Background(), fs.remotePath(fh.Path), stat.Revision, stat.Size)
			if snap, ok := fs.extentCache.slicesFor(fs.remotePath(fh.Path), stat.Revision); ok {
				fs.cacheOpenSlices(fh, snap.revision, snap.generation, snap.size, snap.rows)
			}
		}()
	}
}

func (fs *Dat9FS) prefetchExtentSlices(ctx context.Context, remote string, rev, size int64) {
	if fs == nil || fs.client == nil || fs.extentCache == nil || remote == "" {
		return
	}
	if _, ok := fs.extentCache.slicesFor(remote, rev); ok {
		return
	}
	rows, gotRev, gen, err := fs.client.GetSlices(ctx, remote)
	if err != nil {
		return
	}
	end := size
	for _, row := range rows {
		if row.FileOff+row.Len > end {
			end = row.FileOff + row.Len
		}
	}
	fs.extentCache.putSlices(remote, gotRev, gen, end, rows)
}

func (fs *Dat9FS) cacheOpenSlices(fh *FileHandle, rev, gen, size int64, rows []client.SliceRow) {
	if fh != nil && fh.extentOpen != nil {
		fh.extentOpen.putSlices(rev, gen, size, rows)
	}
	if fs.extentCache != nil && fh != nil {
		fs.extentCache.putSlices(fs.remotePath(fh.Path), rev, gen, size, rows)
	}
}

// ensureExtentRemoteFileLocked creates the remote inode for a brand-new extent
// file (or a new file whose profile glob selects extent). Mid-write freeze
// flushes must call this before prepare-blocks/commit-slices; otherwise the
// server 404s and FUSE surfaces ENOENT/EIO (dd close, crash-recovery write).
func (fs *Dat9FS) ensureExtentRemoteFileLocked(ctx context.Context, fh *FileHandle) (isExtent bool, status gofuse.Status) {
	if fh == nil {
		return false, gofuse.OK
	}
	wantCreate := fh.IsNew && fs.extentLayoutForCreate(fh.Path) == client.ContentLayoutExtent
	if !fh.isExtent() && !wantCreate {
		return false, gofuse.OK
	}
	if !fh.IsNew && fh.BaseRev > 0 && fh.isExtent() {
		return true, gofuse.OK
	}
	if fs.client == nil {
		return true, gofuse.EIO
	}
	if fh.IsNew || fh.BaseRev == 0 || (!fh.isExtent() && wantCreate) {
		layout := fh.ContentLayout
		if layout == "" && fh.IsNew {
			layout = fs.extentLayoutForCreate(fh.Path)
		}
		rev, err := fs.client.CreateFileWithLayout(ctx, fs.remotePath(fh.Path), string(layout))
		if err != nil {
			var se *client.StatusError
			if errors.As(err, &se) && se.StatusCode == 400 && strings.Contains(se.Message, "extent_disabled") {
				fh.ContentLayout = client.ContentLayoutSingle
				return false, gofuse.OK
			}
			if errors.As(err, &se) && se.StatusCode == http.StatusConflict {
				rev = 0
			} else {
				return true, httpToFuseStatus(err)
			}
		}
		fh.IsNew = false
		if rev > 0 {
			fh.BaseRev = rev
		}
		stat, statErr := fs.client.StatCtx(ctx, fs.remotePath(fh.Path))
		if statErr == nil && stat != nil {
			fh.ContentLayout = stat.ContentLayout
			fh.SliceGeneration = stat.SliceGeneration
			fh.BaseRev = stat.Revision
			fh.OrigSize = stat.Size
			if stat.ContentLayout != client.ContentLayoutExtent {
				return false, gofuse.OK
			}
		}
	}
	if !fh.isExtent() {
		return false, gofuse.OK
	}
	if fh.extentWriter == nil {
		fs.attachExtentWriter(fh)
	}
	return true, gofuse.OK
}

func (fs *Dat9FS) syncOpenExtentHandlesToSize(localPath string, newSize int64) gofuse.Status {
	return fs.clipOpenExtentHandlesToSize(localPath, newSize, true)
}

func (fs *Dat9FS) clipOpenExtentHandlesToSize(localPath string, newSize int64, updateOrig bool) gofuse.Status {
	if fs == nil || fs.openHandles == nil {
		return gofuse.OK
	}
	for _, fh := range fs.openHandles.SnapshotPath(localPath) {
		if fh == nil {
			continue
		}
		fh.Lock()
		if !fh.isExtent() {
			fh.Unlock()
			continue
		}
		if fh.Dirty != nil {
			if newSize < fh.Dirty.Size() {
				_ = fh.Dirty.Truncate(newSize)
			} else if err := fh.Dirty.SetSizeOnly(newSize); err != nil {
				fh.Unlock()
				return gofuse.Status(syscall.EFBIG)
			}
		}
		if fh.extentWriter != nil {
			fh.extentWriter.clip(newSize)
		}
		if fh.extentDirty != nil {
			fh.extentDirty.clip(newSize)
		}
		if fh.extentOpen != nil {
			if newSize < fh.extentOpen.logicalSize() {
				fh.extentOpen.markZeroFrom(newSize)
			}
			fh.extentOpen.clipSize(newSize)
			if newSize == 0 {
				fh.extentOpen.invalidateSlices()
			}
		}
		if updateOrig {
			fh.OrigSize = newSize
		}
		fh.Unlock()
	}
	return gofuse.OK
}

func extentHandleLogicalSize(fh *FileHandle) int64 {
	if fh == nil {
		return 0
	}
	n := int64(0)
	if fh.Dirty != nil {
		n = fh.Dirty.Size()
	}
	if fh.extentOpen != nil {
		fh.extentOpen.mu.Lock()
		if fh.extentOpen.size > n {
			n = fh.extentOpen.size
		}
		fh.extentOpen.mu.Unlock()
	}
	return n
}

func (fs *Dat9FS) flushExtentHandle(ctx context.Context, fh *FileHandle) (handled bool, status gofuse.Status) {
	if fh == nil || fh.Dirty == nil {
		return true, gofuse.OK
	}
	isExtent, st := fs.ensureExtentRemoteFileLocked(ctx, fh)
	if st != gofuse.OK {
		return true, st
	}
	if !isExtent {
		return false, gofuse.OK
	}

	if fh.extentWriter != nil && fh.extentWriter.bound() {
		waitCtx, waitCF := fs.extentFlushWaitContext(fh)
		parked := fh.RemoteCommitUnlock
		fh.RemoteCommitUnlock = nil
		writer := fh.extentWriter
		// Hold flushwaiting across holes so Write waits (JuiceFS flushwaiting).
		writer.beginFlush()
		writer.freezeAll()
		localSize := extentHandleLogicalSize(fh)
		remoteSize := fh.OrigSize
		needHoles := fh.extentNeedTruncate && extentOpenZeroFrom(fh) >= 0 && localSize > extentOpenZeroFrom(fh)
		var trunc *int64
		// JuiceFS fileWriter.Flush only commits slices. Size grow is
		// meta.Write max(old, slice_end). Truncate holes belong to
		// VFS.Truncate (extentNeedTruncate), not every fsync/Read flush.
		if fh.extentNeedTruncate && localSize != remoteSize {
			to := localSize
			trunc = &to
		}
		fh.Unlock()
		if parked != nil {
			parked()
		}
		// JuiceFS fileWriter.flush does not take a commit mutex. Holding
		// commitMu here blocks commitLoop (commitSlice) for every Read/Fsync
		// even when there are no holes.
		if trunc != nil || needHoles {
			writer.commitMu.Lock()
			fh.Lock()
			if parked != nil {
				fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
			}
			st := fs.commitExtentPayloadsMaybeCommitMu(waitCtx, fh, nil, trunc, true, false)
			if parked != nil {
				parked := fh.RemoteCommitUnlock
				fh.RemoteCommitUnlock = nil
				fh.Unlock()
				if parked != nil {
					parked()
				}
			} else {
				fh.Unlock()
			}
			if st != gofuse.OK {
				writer.commitMu.Unlock()
				writer.endFlush()
				waitCF()
				fh.Lock()
				if parked != nil {
					fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
				}
				return true, st
			}
			writer.commitMu.Unlock()
		}
		st := writer.flush(waitCtx)
		if st != gofuse.OK {
			writer.endFlush()
			waitCF()
			fh.Lock()
			if parked != nil {
				fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
			}
			return true, st
		}
		waitCF()
		fh.Lock()
		if parked != nil {
			fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
		}
		fh.adoptExtentOpenRev()
		defer writer.endFlush()
	} else if fh.extentWriter != nil && fh.extentWriter.hasInflight() {
		waitCtx, waitCF := fs.extentFlushContext(fh)
		st := fs.waitExtentInflightLocked(waitCtx, fh)
		waitCF()
		if st != gofuse.OK {
			return true, st
		}
	}
	localSize := extentHandleLogicalSize(fh)
	remoteSize := fh.OrigSize
	dirtyEmpty := fh.extentDirty.empty() && (fh.extentWriter == nil || fh.extentWriter.empty())
	needHoles := fh.extentNeedTruncate && extentOpenZeroFrom(fh) >= 0 && localSize > extentOpenZeroFrom(fh)
	if dirtyEmpty && localSize == remoteSize && !needHoles {
		fs.clearExtentHandleDirtyLocked(fh)
		return true, gofuse.OK
	}
	if localSize == 0 && remoteSize == 0 && dirtyEmpty && !needHoles {
		fs.clearExtentHandleDirtyLocked(fh)
		return true, gofuse.OK
	}

	var held []*extentSliceWriter
	var payloads []client.ExtentPayload
	if fh.extentWriter != nil && fh.extentWriter.bound() {
		// JuiceFS flush already froze+committed remaining slices.
	} else if fh.extentWriter != nil {
		held = fh.extentWriter.detachAll()
		payloads = payloadsFromSlices(held)
	} else {
		// JuiceFS grow is hole slices, not a zero-filled data object.
		payloads = copyExtentPayloads(fh.Dirty, fh.extentDirty.cloneRanges())
	}
	var truncateTo *int64
	if fh.extentNeedTruncate && localSize != remoteSize {
		to := localSize
		truncateTo = &to
	}
	if len(payloads) == 0 && truncateTo == nil && !needHoles {
		fs.clearExtentHandleDirtyLocked(fh)
		return true, gofuse.OK
	}
	putCtx, putCF := fs.extentFlushContext(fh)
	st = fs.commitExtentPayloadsLocked(putCtx, fh, payloads, truncateTo, true)
	putCF()
	if fh.extentWriter != nil && len(held) > 0 {
		if st != gofuse.OK {
			fh.extentWriter.restoreFrozen(held)
		} else {
			fh.extentWriter.dropInflight(held)
		}
	}
	return true, st
}

func (fh *FileHandle) adoptExtentOpenRev() {
	if fh == nil || fh.extentOpen == nil {
		return
	}
	of := fh.extentOpen
	of.mu.Lock()
	if of.rev > fh.BaseRev {
		fh.BaseRev = of.rev
		fh.SliceGeneration = of.gen
	}
	if of.committed > fh.OrigSize {
		fh.OrigSize = of.committed
	}
	if of.committed > 0 {
		fh.IsNew = false
		fh.ZeroBase = false
	}
	of.mu.Unlock()
}

func (fs *Dat9FS) extentFlushWaitContext(fh *FileHandle) (context.Context, context.CancelFunc) {
	wait := extentFlushWait
	if fh != nil {
		n := extentHandleLogicalSize(fh)
		if d := releaseTimeout(n); d > wait {
			wait = d
		}
	}
	return context.WithTimeout(context.Background(), wait)
}

func (fs *Dat9FS) clearExtentHandleDirtyLocked(fh *FileHandle) {
	if fh == nil {
		return
	}
	if fh.extentWriter != nil && fh.extentWriter.hasLocalDirty() {
		return
	}
	if fh.extentDirty != nil {
		fh.extentDirty = &extentDirtySet{}
	}
	if fh.Dirty != nil {
		fh.Dirty.ClearDirty()
	}
	if fs != nil {
		fs.clearDirtySize(fh.Ino, fh.DirtySeq)
	}
	fh.DirtySeq = 0
	fh.ZeroBase = false
	fh.extentNeedTruncate = false
}

func (fs *Dat9FS) commitExtentPayloadsLocked(ctx context.Context, fh *FileHandle, payloads []client.ExtentPayload, truncateTo *int64, dropFrozenOnly bool) gofuse.Status {
	return fs.commitExtentPayloadsMaybeCommitMu(ctx, fh, payloads, truncateTo, dropFrozenOnly, true)
}

func (fs *Dat9FS) commitExtentPayloadsMaybeCommitMu(ctx context.Context, fh *FileHandle, payloads []client.ExtentPayload, truncateTo *int64, dropFrozenOnly, takeCommitMu bool) gofuse.Status {
	remotePath := fs.remotePath(fh.Path)
	snapSeq := fh.DirtySeq
	baseRev := fh.BaseRev
	gen := fh.SliceGeneration
	localPath := fh.Path
	localSize := extentHandleLogicalSize(fh)
	holes := extentHolesForZeroFrom(fh, localSize)
	zeroFrom := extentOpenZeroFrom(fh)

	parkedCommit := fh.RemoteCommitUnlock
	fh.RemoteCommitUnlock = nil
	writer := fh.extentWriter
	fh.Unlock()
	if parkedCommit != nil {
		parkedCommit()
	}
	if writer != nil && takeCommitMu {
		writer.commitMu.Lock()
	}
	result, err := fs.client.FlushExtent(ctx, client.FlushExtentRequest{
		Path:               remotePath,
		ExpectedRevision:   baseRev,
		ExpectedGeneration: gen,
		Payloads:           payloads,
		Holes:              holes,
		TruncateTo:         truncateTo,
		Append:             true,
		Writeback:          false,
		AfterPut: func(blocks []client.LandedBlock) {
			for _, blk := range blocks {
				fs.promoteExtentStaging(blk.Op.BlockKey, blk.Data, blk.Op.BlockOff, blk.Op.Len)
			}
		},
		AfterLanded: func(opID string, ops []client.SliceOp, expectedRev, expectedGen int64, attemptTruncate *int64) error {
			if fs.journal == nil {
				return nil
			}
			entry := JournalEntry{
				Op:               JournalFsync,
				Path:             localPath,
				Length:           localSize,
				BaseRev:          expectedRev,
				Extent:           true,
				ExtentOpID:       opID,
				ExtentOps:        ops,
				ExtentTruncateTo: attemptTruncate,
				ExtentGeneration: expectedGen,
			}
			_ = fs.journal.Append(entry)
			_ = fs.journal.Fsync()
			return nil
		},
	})
	if writer != nil && takeCommitMu {
		writer.commitMu.Unlock()
	}
	fh.Lock()
	if parkedCommit != nil {
		fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
	}
	if err != nil {
		safeLogPrintf("extent flush failed path=%s: %v", remotePath, err)
		return httpToFuseStatus(err)
	}
	if fs.journal != nil {
		_ = fs.journal.Append(JournalEntry{Op: JournalCommit, Path: localPath, BaseRev: result.Revision})
	}
	fh.BaseRev = result.Revision
	fh.SliceGeneration = result.Generation
	fh.OrigSize = result.SizeBytes
	fh.ZeroBase = false
	logical := extentHandleLogicalSize(fh)
	if !(fh.extentNeedTruncate && logical != result.SizeBytes) {
		fh.extentNeedTruncate = false
	}
	if fs.extentCache != nil {
		fs.extentCache.invalidatePath(remotePath)
	}
	if fh.extentOpen != nil {
		if logical < result.SizeBytes {
			fh.extentOpen.clipSize(logical)
		} else {
			fh.extentOpen.setSize(logical)
		}
		fh.extentOpen.noteCommit(result.Revision, result.Generation, result.SizeBytes)
		fs.applyExtentZeroFromAfterCommit(fh, zeroFrom, localSize, result.SizeBytes, len(holes) > 0)
	}
	fs.refreshExtentHandlesAfterCommit(localPath, fh, result.Revision, result.Generation, result.SizeBytes)
	fs.scheduleExtentWriteCompact(remotePath, result.ChunkRows)
	if fh.extentDirty != nil {
		for _, p := range payloads {
			fh.extentDirty.punch(p.FileOff, p.FileOff+int64(len(p.Data)))
		}
		fh.extentDirty.clip(logical)
		if !dropFrozenOnly {
			fh.extentDirty = &extentDirtySet{}
		}
	}
	if fh.DirtySeq == snapSeq && (fh.extentWriter == nil || !fh.extentWriter.hasLocalDirty()) && !fh.extentNeedTruncate {
		fs.clearExtentHandleDirtyLocked(fh)
	}
	visible := logical
	if !fh.extentNeedTruncate && result.SizeBytes > visible {
		visible = result.SizeBytes
	}
	if fh.Dirty != nil && fh.Dirty.Size() > visible {
		visible = fh.Dirty.Size()
	}
	fs.inodes.UpdateSize(fh.Ino, visible)
	fs.cacheFileForPath(fh.Path, visible, time.Now(), result.Revision)
	return gofuse.OK
}

const extentJournalPendingExpired = "pending_expired"

func (fs *Dat9FS) replayExtentJournal(ctx context.Context) error {
	if fs == nil || fs.journal == nil || fs.client == nil {
		return nil
	}
	latest := make(map[string]JournalEntry)
	done := make(map[string]uint64)
	if err := fs.journal.Replay(func(e JournalEntry) {
		switch e.Op {
		case JournalCommit, JournalUnlink:
			if e.Seq > done[e.Path] {
				done[e.Path] = e.Seq
			}
		case JournalFsync:
			if !e.Extent {
				return
			}
			if prev, ok := latest[e.Path]; !ok || e.Seq > prev.Seq {
				latest[e.Path] = e
			}
		}
	}); err != nil {
		return err
	}
	for p, e := range latest {
		if doneSeq, ok := done[p]; ok && doneSeq > e.Seq {
			continue
		}
		if e.ExtentOpID == "" {
			continue
		}
		remote := fs.remotePath(p)
		_, err := fs.client.CommitSlicesStaged(ctx, remote, e.ExtentOpID, e.BaseRev, e.ExtentGeneration, e.ExtentOps, e.ExtentTruncateTo, false, true)
		if err != nil {
			var ce *client.ExtentCommitError
			if errors.As(err, &ce) && (ce.StatusCode == http.StatusGone || ce.Code == extentJournalPendingExpired) {
				stat, statErr := fs.client.StatCtx(ctx, remote)
				if statErr != nil || stat == nil || stat.Revision != e.BaseRev {
					safeLogPrintf("extent journal replay skipped expired op %s path=%s", e.ExtentOpID, p)
					continue
				}
				// Same op_id, staged: recover if the object is still in S3
				// after pending_blocks expired. Never mint a new op_id — that
				// would overlay whatever is now at BaseRev.
				_, err = fs.client.CommitSlicesStaged(ctx, remote, e.ExtentOpID, e.BaseRev, e.ExtentGeneration, e.ExtentOps, e.ExtentTruncateTo, true, true)
				if err != nil {
					safeLogPrintf("extent journal replay expired retry failed path=%s: %v", p, err)
					continue
				}
			} else if errors.As(err, &ce) && ce.StatusCode == http.StatusConflict && ce.Code != "block_not_landed" {
				safeLogPrintf("extent journal replay skipped cas conflict path=%s rev=%d", p, ce.Revision)
				continue
			} else {
				safeLogPrintf("extent journal replay failed path=%s: %v", p, err)
				continue
			}
		}
		_ = fs.journal.Append(JournalEntry{Op: JournalCommit, Path: p})
	}
	return nil
}
