package fuse

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

type extentCompactScheduler struct {
	mu       sync.Mutex
	inflight map[string]struct{}
}

func newExtentCompactScheduler() *extentCompactScheduler {
	return &extentCompactScheduler{
		inflight: make(map[string]struct{}),
	}
}

func (fs *Dat9FS) ensureExtentCompact() *extentCompactScheduler {
	if fs == nil {
		return nil
	}
	if fs.extentCompact != nil {
		return fs.extentCompact
	}
	fs.extentCompact = newExtentCompactScheduler()
	return fs.extentCompact
}

func extentCompactKey(remotePath string, chunk int64) string {
	return fmt.Sprintf("%s:%d", remotePath, chunk)
}

// scheduleExtentCompact is JuiceFS meta.Read: go compactChunk when a chunk
// has >=5 slices. Compact copies the visible range from the local chunk
// cache (writeback Finish), not S3.
func (fs *Dat9FS) scheduleExtentCompact(remotePath string, chunkRows []client.ChunkRowCount) {
	if fs == nil || fs.client == nil || remotePath == "" {
		return
	}
	for _, cr := range chunkRows {
		if cr.Rows < datastore.ExtentCompactReadRows {
			continue
		}
		fs.kickExtentCompact(remotePath, cr.Chunk, false, 0)
	}
}

// scheduleExtentWriteCompact is JuiceFS meta.Write after doWrite: async
// compact at 99/350, sync compactChunk(once) at >=2500.
func (fs *Dat9FS) scheduleExtentWriteCompact(remotePath string, chunkRows []client.ChunkRowCount) {
	if fs == nil || fs.client == nil || remotePath == "" {
		return
	}
	for _, cr := range chunkRows {
		if datastore.ShouldSyncCompactOnWrite(cr.Rows) {
			fs.kickExtentCompact(remotePath, cr.Chunk, true, datastore.ExtentCompactMaxSlices)
			continue
		}
		if datastore.ShouldCompactOnWrite(cr.Rows) {
			fs.kickExtentCompact(remotePath, cr.Chunk, false, 0)
		}
	}
}

func (fs *Dat9FS) kickExtentCompact(remotePath string, chunk int64, wait bool, minRows int) {
	s := fs.ensureExtentCompact()
	if s == nil {
		return
	}
	key := extentCompactKey(remotePath, chunk)
	for {
		s.mu.Lock()
		_, busy := s.inflight[key]
		if wait {
			if busy {
				s.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				continue
			}
		} else if busy || len(s.inflight) > 10 {
			s.mu.Unlock()
			return
		}
		s.inflight[key] = struct{}{}
		s.mu.Unlock()
		break
	}
	run := func() {
		_ = fs.compactExtentChunk(context.Background(), remotePath, chunk, minRows)
		s.mu.Lock()
		delete(s.inflight, key)
		s.mu.Unlock()
	}
	if wait {
		run()
		return
	}
	go run()
}

func (fs *Dat9FS) compactExtentChunk(ctx context.Context, remotePath string, chunk int64, minRows int) error {
	rows, _, _, err := fs.client.GetSlices(ctx, remotePath)
	if err != nil {
		return err
	}
	var chunkRows []client.SliceRow
	for _, row := range rows {
		if row.Chunk != chunk {
			continue
		}
		chunkRows = append(chunkRows, row)
	}
	if minRows > 0 && len(chunkRows) < minRows {
		return nil
	}
	if len(chunkRows) < 2 {
		return nil
	}
	dsRows := make([]datastore.SliceRow, 0, len(chunkRows))
	for _, row := range chunkRows {
		dsRows = append(dsRows, datastore.SliceRow{
			Chunk: row.Chunk, Seq: row.Seq, FileOff: row.FileOff, Len: row.Len,
			BlockKey: row.BlockKey, BlockOff: row.BlockOff, BlockLen: row.BlockLen,
			Kind: row.Kind,
		})
	}
	skipped := datastore.SkipSome(dsRows)
	if skipped > 0 && skipped < len(dsRows) && datastore.CompactRangeOverlapsSkipped(dsRows[:skipped], dsRows[skipped:]) {
		skipped = 0
	}
	compacted := dsRows[skipped:]
	if len(compacted) < 2 {
		return nil
	}
	// JuiceFS compactChunk: one new slice covering the visible range, holes
	// as zeros inside that object. PackCompactRuns split/flatten here
	// last-write-wins overlayed btree overflow pages (wal-multiwrite).
	pos, size, vis := datastore.CompactChunkVisible(compacted)
	if size <= 0 || len(vis) == 0 {
		return nil
	}
	buf := make([]byte, size)
	for _, row := range vis {
		if row.Kind == datastore.SliceKindHole || row.BlockKey == "" {
			continue
		}
		piece, ok := fs.readExtentRowFromCache(row.BlockKey, row.BlockOff, row.Len)
		if !ok {
			// JuiceFS compact reads chunk cache. A miss means writeback
			// data is not local; skip rather than GET 404 / Plan holes.
			return nil
		}
		off := row.FileOff - pos
		if off < 0 || off+row.Len > size {
			return nil
		}
		copy(buf[off:off+row.Len], piece)
	}
	payloads := []client.ExtentPayload{{FileOff: pos, Data: buf}}
	landed, err := fs.client.CompactExtentChunk(ctx, remotePath, chunk, chunkRows, payloads)
	if err != nil {
		return err
	}
	for _, blk := range landed {
		if fs.extentCache != nil {
			fs.extentCache.putBlock(blk.Op.BlockKey, blk.Op.BlockOff, blk.Op.Len, blk.Data)
		}
		fs.extentDiskCachePut(blk.Op.BlockKey, blk.Op.BlockOff, blk.Op.Len, blk.Data)
	}
	if fs.extentCache != nil {
		fs.extentCache.invalidatePath(remotePath)
	}
	return nil
}

func (fs *Dat9FS) readExtentRowFromCache(blockKey string, blockOff, length int64) ([]byte, bool) {
	if length <= 0 || blockKey == "" {
		return nil, false
	}
	if data, ok := fs.extentCache.getBlock(blockKey, blockOff, length); ok && int64(len(data)) >= length {
		return data[:length], true
	}
	if data, ok := fs.extentDiskCacheGetBlock(blockKey, blockOff, length); ok && int64(len(data)) >= length {
		if fs.extentCache != nil {
			fs.extentCache.putBlock(blockKey, blockOff, length, data)
		}
		return data[:length], true
	}
	return nil, false
}
