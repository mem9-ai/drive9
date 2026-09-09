package fuse

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

const (
	extentPageSize     = 64 << 10
	extentReadaheadMax = 4 << 20
	extentFlushScan    = 100 * time.Millisecond
	extentFlushIdle    = time.Second
	extentFlushAge     = 5 * time.Second
	extentFlushMaxRuns = 800
	extentFlushWait    = 5 * time.Minute // JuiceFS fileWriter.flush minimum wait
	extentPageIdle     = 30 * time.Second
)

type extentReadAhead struct {
	lastOff   int64
	readahead int64
	total     int64
	atime     time.Time
}

type extentSliceSnap struct {
	revision   int64
	generation int64
	size       int64
	rows       []client.SliceRow
	complete   bool
}

type extentMemPage struct {
	data   []byte
	valid0 int
	valid1 int
	sum    [sha256.Size]byte
	atime  time.Time
}

type extentReadCache struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	pages    map[string]*extentMemPage
	order    []string
	slices   map[string]extentSliceSnap
}

func newExtentReadCache(maxBytes int64) *extentReadCache {
	if maxBytes <= 0 {
		maxBytes = defaultReadCacheMaxSize
	}
	return &extentReadCache{
		maxBytes: maxBytes,
		pages:    make(map[string]*extentMemPage),
		slices:   make(map[string]extentSliceSnap),
	}
}

func extentPageKey(blockKey string, pageOff int64) string {
	return fmt.Sprintf("%s:%d", blockKey, pageOff)
}

func (p *extentMemPage) checksumOK() bool {
	if p == nil || p.valid1 <= p.valid0 || p.valid0 < 0 || p.valid1 > len(p.data) {
		return false
	}
	sum := sha256.Sum256(p.data[p.valid0:p.valid1])
	return subtle.ConstantTimeCompare(sum[:], p.sum[:]) == 1
}

func (p *extentMemPage) refreshSum() {
	if p == nil || p.valid1 <= p.valid0 || p.valid0 < 0 || p.valid1 > len(p.data) {
		p.sum = [sha256.Size]byte{}
		return
	}
	p.sum = sha256.Sum256(p.data[p.valid0:p.valid1])
}

func (c *extentReadCache) getBlock(blockKey string, blockOff, length int64) ([]byte, bool) {
	if c == nil || length <= 0 {
		return nil, false
	}
	out := make([]byte, length)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.copyPagesLocked(blockKey, blockOff, length, out) {
		return nil, false
	}
	return out, true
}

func (c *extentReadCache) copyPagesLocked(blockKey string, blockOff, length int64, dest []byte) bool {
	if int64(len(dest)) < length {
		return false
	}
	end := blockOff + length
	pos := blockOff
	now := time.Now()
	for pos < end {
		pageOff := pos - pos%extentPageSize
		key := extentPageKey(blockKey, pageOff)
		p, ok := c.pages[key]
		if !ok {
			return false
		}
		if !p.checksumOK() {
			c.curBytes -= int64(len(p.data))
			delete(c.pages, key)
			return false
		}
		rel := int(pos - pageOff)
		if rel < p.valid0 || rel >= p.valid1 {
			return false
		}
		n := p.valid1 - rel
		remain := int(end - pos)
		if n > remain {
			n = remain
		}
		copy(dest[pos-blockOff:pos-blockOff+int64(n)], p.data[rel:rel+n])
		p.atime = now
		pos += int64(n)
	}
	return true
}

func (c *extentReadCache) putBlock(blockKey string, blockOff, length int64, data []byte) {
	if c == nil || len(data) == 0 || int64(len(data)) != length || length <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	off := blockOff
	src := 0
	now := time.Now()
	for src < len(data) {
		pageOff := off - off%extentPageSize
		rel := int(off - pageOff)
		room := int(extentPageSize) - rel
		take := len(data) - src
		if take > room {
			take = room
		}
		key := extentPageKey(blockKey, pageOff)
		p := c.pages[key]
		needLen := rel + take
		if p == nil {
			p = &extentMemPage{data: make([]byte, needLen), valid0: rel, valid1: rel + take}
			c.pages[key] = p
			c.order = append(c.order, key)
			c.curBytes += int64(needLen)
		} else {
			if needLen > len(p.data) {
				c.curBytes += int64(needLen - len(p.data))
				p.data = append(p.data, make([]byte, needLen-len(p.data))...)
			}
			if p.valid1 <= p.valid0 {
				p.valid0, p.valid1 = rel, rel+take
			} else if rel+take < p.valid0 || rel > p.valid1 {
				p.valid0, p.valid1 = rel, rel+take
			} else {
				if rel < p.valid0 {
					p.valid0 = rel
				}
				if rel+take > p.valid1 {
					p.valid1 = rel + take
				}
			}
		}
		copy(p.data[rel:rel+take], data[src:src+take])
		p.refreshSum()
		p.atime = now
		src += take
		off += int64(take)
	}
	firstKey := extentPageKey(blockKey, blockOff-blockOff%extentPageSize)
	for c.curBytes > c.maxBytes && len(c.order) > 1 {
		evict := c.order[0]
		c.order = c.order[1:]
		if evict == firstKey {
			c.order = append(c.order, evict)
			continue
		}
		if old, ok := c.pages[evict]; ok {
			c.curBytes -= int64(len(old.data))
			delete(c.pages, evict)
		}
	}
}

func (c *extentReadCache) reclaimIdle(now time.Time) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	kept := c.order[:0]
	for _, key := range c.order {
		p, ok := c.pages[key]
		if !ok {
			continue
		}
		if now.Sub(p.atime) >= extentPageIdle {
			c.curBytes -= int64(len(p.data))
			delete(c.pages, key)
			continue
		}
		kept = append(kept, key)
	}
	c.order = kept
}

func (c *extentReadCache) putSlices(path string, rev, gen, size int64, rows []client.SliceRow) {
	c.putSliceSnap(path, rev, gen, size, rows, true)
}

func (c *extentReadCache) mergePlanSlices(path string, rev, gen, size int64, rows []client.SliceRow) {
	c.putSliceSnap(path, rev, gen, size, rows, false)
}

func (c *extentReadCache) putSliceSnap(path string, rev, gen, size int64, rows []client.SliceRow, complete bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cur, ok := c.slices[path]
	// JuiceFS InvalidateChunk: compact bumps generation without revision.
	// A complete snap at the old generation must not hide the new layout.
	if ok && cur.revision == rev && cur.generation == gen {
		if cur.complete && !complete {
			return
		}
		if !complete {
			rows = mergeSliceRows(cur.rows, rows)
		}
		complete = cur.complete || complete
		if size < cur.size {
			size = cur.size
		}
	}
	copied := append([]client.SliceRow(nil), rows...)
	c.slices[path] = extentSliceSnap{revision: rev, generation: gen, size: size, rows: copied, complete: complete}
}

func mergeSliceRows(existing, add []client.SliceRow) []client.SliceRow {
	rows := append([]client.SliceRow(nil), existing...)
	for _, op := range add {
		if op.Len <= 0 {
			continue
		}
		next := make([]client.SliceRow, 0, len(rows)+1)
		a, b := op.FileOff, op.FileOff+op.Len
		for _, row := range rows {
			r0, r1 := row.FileOff, row.FileOff+row.Len
			if r1 <= a || r0 >= b {
				next = append(next, row)
				continue
			}
			if r0 < a {
				left := row
				left.Len = a - r0
				next = append(next, left)
			}
			if r1 > b {
				right := row
				skip := b - r0
				right.FileOff = b
				right.BlockOff = row.BlockOff + skip
				right.Len = r1 - b
				next = append(next, right)
			}
		}
		next = append(next, op)
		rows = next
	}
	return rows
}

func (c *extentReadCache) slicesFor(path string, rev int64) (extentSliceSnap, bool) {
	return c.slicesForGen(path, rev, 0)
}

func (c *extentReadCache) slicesForGen(path string, rev, gen int64) (extentSliceSnap, bool) {
	if c == nil {
		return extentSliceSnap{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	snap, ok := c.slices[path]
	if !ok || (rev > 0 && snap.revision != rev) {
		return extentSliceSnap{}, false
	}
	if gen > 0 && snap.generation != gen {
		return extentSliceSnap{}, false
	}
	return snap, true
}

func (c *extentReadCache) invalidatePath(path string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.slices, path)
}

// testHookExtentReadAfterUnlock runs after VFS.Read drops the handle lock and
// before Plan. Tests use it to inject a sibling write that JuiceFS would not
// overlay onto this Read.
var testHookExtentReadAfterUnlock func(fh *FileHandle)

func (fs *Dat9FS) readExtentRange(ctx context.Context, fh *FileHandle, offset, size int64) ([]byte, error) {
	return fs.readExtentRangeMaybeLocked(ctx, fh, offset, size, false)
}

func (fs *Dat9FS) readExtentRangeLocked(ctx context.Context, fh *FileHandle, offset, size int64) ([]byte, error) {
	return fs.readExtentRangeMaybeLocked(ctx, fh, offset, size, true)
}

func (fs *Dat9FS) readExtentRangeMaybeLocked(ctx context.Context, fh *FileHandle, offset, size int64, held bool) ([]byte, error) {
	if fs == nil || fs.client == nil || fh == nil || size <= 0 {
		return nil, nil
	}
	// JuiceFS VFS.Read: writer.Flush then reader.Read. Flush waits for
	// in-flight PUTs plus commitThread. reader.Read has no writer overlay;
	// a concurrent Write after Flush is stale-ok, not mixed into this Read.
	if !held {
		fh.Lock()
	}
	usePlanOnly := false
	if fh.extentWriter != nil && fh.extentWriter.bound() {
		if !fh.extentWriter.empty() {
			writer := fh.extentWriter
			if !held {
				fh.Unlock()
			}
			st := writer.flush(ctx)
			if !held {
				fh.Lock()
			}
			fh.adoptExtentOpenRev()
			if st == 0 {
				usePlanOnly = true
			}
		} else {
			usePlanOnly = true
		}
	}
	localSize := extentHandleLogicalSize(fh)
	if fh.OrigSize > localSize {
		localSize = fh.OrigSize
	}
	committed := fh.OrigSize
	if fh.extentOpen != nil {
		fh.extentOpen.mu.Lock()
		if fh.extentOpen.size > localSize {
			localSize = fh.extentOpen.size
		}
		if fh.extentOpen.committed > committed {
			committed = fh.extentOpen.committed
		}
		fh.extentOpen.mu.Unlock()
	}
	if fs.inodes != nil {
		if entry, ok := fs.inodes.GetEntry(fh.Ino); ok {
			if live := fs.liveFileSize(entry); live > localSize {
				localSize = live
			}
		}
	}
	isLocalOnly := (fh.IsNew || fh.BaseRev == 0 || fh.ZeroBase) && committed == 0
	if isLocalOnly {
		committed = 0
	}
	// JuiceFS has no zeroFrom: after Flush, meta.Read sees hole slices.
	// Clip remote reads only while a truncate is still uncommitted.
	if fh.extentNeedTruncate {
		if z := extentOpenZeroFrom(fh); z >= 0 && z < committed {
			committed = z
		}
	}
	if offset >= localSize {
		if !held {
			fh.Unlock()
		}
		return nil, nil
	}
	remote := fs.remotePath(fh.Path)
	if offset+size > localSize {
		size = localSize - offset
	}
	if !held {
		fh.Unlock()
	}
	if hook := testHookExtentReadAfterUnlock; hook != nil {
		hook(fh)
	}
	pad := func(remoteData []byte) []byte {
		out := make([]byte, size)
		copy(out, remoteData)
		if !usePlanOnly {
			fs.overlayExtentDirty(fh, offset, out)
		}
		return out
	}
	if isLocalOnly && !usePlanOnly && (fh.extentWriter == nil || !fh.extentWriter.bound()) {
		return pad(nil), nil
	}
	// JuiceFS meta.Read uses inode length + hole slices. A stale committed
	// watermark must not zero live pages (btreeInitPage on 0x00 flags).
	remoteLen := size
	if !usePlanOnly {
		if offset >= committed {
			remoteLen = 0
		} else if offset+remoteLen > committed {
			remoteLen = committed - offset
		}
	}
	if remoteLen <= 0 {
		return pad(nil), nil
	}
	// JuiceFS meta.Read: of.ReadChunk hit skips doRead. Write/compact
	// InvalidateChunk. Compact is in-process (scheduleExtentCompact) and
	// already drops this snap.
	if data, ok := fs.extentCachedWindow(remote, 0, offset, remoteLen); ok {
		return pad(data), nil
	}
	plan, err := fs.client.PlanExtentRead(ctx, remote, offset, remoteLen)
	if err != nil {
		return nil, err
	}
	// JuiceFS meta.Read: go compactChunk when len(ss) >= 5.
	if plan != nil && len(plan.ChunkRows) > 0 {
		fs.scheduleExtentCompact(remote, plan.ChunkRows)
	}
	if plan != nil && fh.extentOpen != nil && plan.Generation > 0 {
		of := fh.extentOpen
		of.mu.Lock()
		bumped := plan.Generation > of.gen
		if bumped {
			of.gen = plan.Generation
			of.slices = nil
		}
		if plan.Revision > of.rev {
			of.rev = plan.Revision
		}
		of.mu.Unlock()
		if bumped && fs.extentCache != nil {
			// JuiceFS compact InvalidateChunk is userspace only; KeepCache
			// stays until Open sees a new mtime/revision.
			fs.extentCache.invalidatePath(remote)
		}
	}
	if fs.extentCache != nil {
		if len(plan.Layout) > 0 {
			fs.extentCache.putSlices(remote, plan.Revision, plan.Generation, plan.SizeBytes, plan.Layout)
			fs.cacheOpenSlices(fh, plan.Revision, plan.Generation, plan.SizeBytes, plan.Layout)
		} else {
			rows := make([]client.SliceRow, 0, len(plan.Parts))
			for _, p := range plan.Parts {
				rows = append(rows, client.SliceRow{
					FileOff: p.FileOff, Len: p.Len, BlockKey: p.BlockKey, BlockOff: p.BlockOff,
				})
			}
			fullFile := plan.SizeBytes > 0 && offset <= 0 && offset+remoteLen >= plan.SizeBytes
			if fullFile {
				fs.extentCache.putSlices(remote, plan.Revision, plan.Generation, plan.SizeBytes, rows)
				fs.cacheOpenSlices(fh, plan.Revision, plan.Generation, plan.SizeBytes, rows)
			} else {
				fs.extentCache.mergePlanSlices(remote, plan.Revision, plan.Generation, plan.SizeBytes, rows)
			}
		}
	}
	var missing []client.ExtentReadPart
	for _, p := range plan.Parts {
		if p.BlockKey == "" || p.GetURL == "" {
			continue
		}
		if _, ok := fs.extentCache.getBlock(p.BlockKey, p.BlockOff, p.Len); ok {
			continue
		}
		if data, ok := fs.extentDiskCacheGetBlock(p.BlockKey, p.BlockOff, p.Len); ok {
			fs.extentCache.putBlock(p.BlockKey, p.BlockOff, p.Len, data)
			continue
		}
		missing = append(missing, p)
	}
	if len(missing) > 0 {
		type fetched struct {
			part client.ExtentReadPart
			data []byte
			err  error
		}
		ch := make(chan fetched, len(missing))
		sem := make(chan struct{}, 16)
		var wg sync.WaitGroup
		for _, p := range missing {
			p := p
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					ch <- fetched{part: p, err: ctx.Err()}
					return
				}
				data, err := fs.client.GetPresignedRetry(ctx, p.GetURL, p.Headers)
				if err == nil {
					data, err = client.ExtentPartPayload(p, data)
				}
				ch <- fetched{part: p, data: data, err: err}
			}()
		}
		go func() {
			wg.Wait()
			close(ch)
		}()
		for item := range ch {
			if item.err != nil {
				return nil, item.err
			}
			fs.extentCache.putBlock(item.part.BlockKey, item.part.BlockOff, item.part.Len, item.data)
			fs.extentDiskCachePut(item.part.BlockKey, item.part.BlockOff, item.part.Len, item.data)
		}
	}
	// JuiceFS doRead: assemble this Read from the slices meta just returned,
	// not a merged historical layout at the same revision.
	if data, ok := fs.extentWindowFromParts(plan.Parts, offset, remoteLen); ok {
		return pad(data), nil
	}
	data, err := fs.client.FetchExtentPlan(ctx, plan, offset, remoteLen)
	if err != nil {
		return nil, err
	}
	return pad(data), nil
}

func (fs *Dat9FS) extentWindowFromParts(parts []client.ExtentReadPart, offset, size int64) ([]byte, bool) {
	if fs == nil || fs.extentCache == nil || size <= 0 {
		return nil, false
	}
	out := make([]byte, size)
	end := offset + size
	pos := offset
	for _, p := range parts {
		p0, p1 := p.FileOff, p.FileOff+p.Len
		if p1 <= offset || p0 >= end || p.Len <= 0 {
			continue
		}
		if p0 > pos {
			// JuiceFS buildSlice never leaves an unlabeled gap; a missing
			// data part must not be served as KEEP_CACHE zeros.
			return nil, false
		}
		from := offset
		if p0 > from {
			from = p0
		}
		if pos > from {
			from = pos
		}
		to := end
		if p1 < to {
			to = p1
		}
		if to <= from {
			if p1 > pos {
				pos = p1
			}
			continue
		}
		if p.BlockKey == "" {
			pos = to
			continue
		}
		need := to - from
		srcOff := p.BlockOff + (from - p0)
		data, hit := fs.extentCache.getBlock(p.BlockKey, srcOff, need)
		if !hit {
			if disk, ok := fs.extentDiskCacheGetBlock(p.BlockKey, p.BlockOff, p.Len); ok {
				fs.extentCache.putBlock(p.BlockKey, p.BlockOff, p.Len, disk)
				data, hit = fs.extentCache.getBlock(p.BlockKey, srcOff, need)
			}
		}
		if !hit || int64(len(data)) < need {
			return nil, false
		}
		copy(out[from-offset:from-offset+need], data[:need])
		pos = to
	}
	if pos < end {
		return nil, false
	}
	return out, true
}

func (fs *Dat9FS) overlayExtentDirty(fh *FileHandle, offset int64, data []byte) {
	if fh == nil || fh.extentWriter == nil || len(data) == 0 {
		return
	}
	fh.extentWriter.readAt(offset, data)
}

func (fs *Dat9FS) extentCachedWindow(path string, rev, offset, size int64) ([]byte, bool) {
	if fs.extentCache == nil || size <= 0 {
		return nil, false
	}
	end := offset + size
	snap, ok := fs.extentCache.slicesFor(path, rev)
	rows := snap.rows
	if !ok {
		return nil, false
	}
	sorted := flattenClientSliceRows(rows)
	out := make([]byte, size)
	pos := offset
	for _, row := range sorted {
		r0, r1 := row.FileOff, row.FileOff+row.Len
		if r1 <= offset || r0 >= end {
			continue
		}
		if r0 > pos {
			if !snap.complete {
				return nil, false
			}
			pos = r0
		}
		if row.Kind == "hole" || row.BlockKey == "" {
			if r1 > pos {
				pos = r1
			}
			continue
		}
		from := offset
		if r0 > from {
			from = r0
		}
		to := end
		if r1 < to {
			to = r1
		}
		need := to - from
		srcOff := row.BlockOff + (from - r0)
		data, hit := fs.extentCache.getBlock(row.BlockKey, srcOff, need)
		if !hit {
			if disk, ok := fs.extentDiskCacheGetBlock(row.BlockKey, row.BlockOff, row.Len); ok {
				fs.extentCache.putBlock(row.BlockKey, row.BlockOff, row.Len, disk)
				data, hit = fs.extentCache.getBlock(row.BlockKey, srcOff, need)
			}
		}
		if !hit || int64(len(data)) < need {
			return nil, false
		}
		dst := from - offset
		copy(out[dst:dst+need], data[:need])
		if r1 > pos {
			pos = r1
		}
	}
	if pos < end {
		if !snap.complete {
			return nil, false
		}
		if snap.size > 0 && snap.size < end {
			return nil, false
		}
	}
	return out, true
}

func (fs *Dat9FS) expandExtentReadahead(fh *FileHandle, offset, size, fileSize int64) (int64, int64) {
	if fh == nil || size <= 0 {
		return offset, size
	}
	end := offset + size
	idx := 0
	best := int64(-1)
	for i, s := range fh.extentRA {
		if s.lastOff <= offset && offset <= s.lastOff+s.readahead+extentPageSize && s.lastOff >= best {
			idx = i
			best = s.lastOff
		}
	}
	ses := &fh.extentRA[idx]
	used := int64(0)
	if fs.extentCache != nil {
		used = fs.extentCache.curBytes
	}
	used += fs.extentTotalDirtyBytes()
	budget := fs.extentWriteBufferCap()
	if best >= 0 && offset >= ses.lastOff {
		if end > ses.lastOff {
			ses.total += end - ses.lastOff
		}
		if ses.readahead == 0 && (offset == 0 || ses.total > size) {
			ses.readahead = extentPageSize
		} else if ses.readahead < extentReadaheadMax && ses.total >= ses.readahead && budget > used+ses.readahead*4 {
			ses.readahead *= 2
			if ses.readahead > extentReadaheadMax {
				ses.readahead = extentReadaheadMax
			}
		} else if ses.readahead >= extentPageSize && (budget < used+ses.readahead/2 || ses.total < ses.readahead/4) {
			ses.readahead /= 2
		}
		want := offset + ses.readahead
		if want > end {
			end = want
		}
	} else {
		ses.readahead = extentPageSize
		ses.total = size
	}
	ses.lastOff = offset + size
	ses.atime = time.Now()
	const lastBS = 32 << 10
	if fileSize > lastBS && offset+size > fileSize-lastBS {
		if fileSize-lastBS < offset {
			// already covering tail
		} else if end < fileSize {
			end = fileSize
		}
	}
	if end > fileSize && fileSize > offset {
		end = fileSize
	}
	// JuiceFS checkReadahead only extends forward from the request; do not
	// snap the start backwards (that mixed SQLite 4KiB pages into a 64KiB
	// window and produced btreeInitPage corruption).
	if end < offset+size {
		end = offset + size
		if fileSize > offset && end > fileSize {
			end = fileSize
		}
	}
	return offset, end - offset
}

func flattenClientSliceRows(rows []client.SliceRow) []client.SliceRow {
	if len(rows) == 0 {
		return nil
	}
	ds := make([]datastore.SliceRow, 0, len(rows))
	for _, row := range rows {
		ds = append(ds, datastore.SliceRow{
			Chunk: row.Chunk, Seq: row.Seq, FileOff: row.FileOff, Len: row.Len,
			BlockKey: row.BlockKey, BlockOff: row.BlockOff, BlockLen: row.BlockLen,
			ChecksumSHA256: row.ChecksumSHA256, Kind: row.Kind, BornGen: row.BornGen,
		})
	}
	flat := datastore.FlattenBySeq(ds)
	out := make([]client.SliceRow, 0, len(flat))
	for _, row := range flat {
		out = append(out, client.SliceRow{
			Chunk: row.Chunk, Seq: row.Seq, FileOff: row.FileOff, Len: row.Len,
			BlockKey: row.BlockKey, BlockOff: row.BlockOff, BlockLen: row.BlockLen,
			ChecksumSHA256: row.ChecksumSHA256, Kind: row.Kind, BornGen: row.BornGen,
		})
	}
	return out
}

func (fs *Dat9FS) extentCacheDirList() []string {
	if fs == nil || fs.opts == nil || fs.opts.CacheDir == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(fs.opts.CacheDir, ":") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (fs *Dat9FS) extentCacheDir() string {
	dirs := fs.extentCacheDirList()
	if len(dirs) == 0 {
		return ""
	}
	return filepath.Join(dirs[0], "extent")
}

func (fs *Dat9FS) extentRawDirs() []string {
	var out []string
	for _, d := range fs.extentCacheDirList() {
		out = append(out, filepath.Join(d, "extent", "raw"))
	}
	return out
}

func (fs *Dat9FS) extentDiskCacheGetBlock(blockKey string, blockOff, length int64) ([]byte, bool) {
	c := fs.ensureExtentDiskCache()
	if c == nil {
		return nil, false
	}
	return c.get(blockKey, blockOff, length)
}

func (fs *Dat9FS) extentDiskCachePut(blockKey string, blockOff, length int64, data []byte) {
	c := fs.ensureExtentDiskCache()
	if c == nil {
		return
	}
	c.put(blockKey, blockOff, length, data)
}
