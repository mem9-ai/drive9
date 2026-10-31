package writeback

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/mem9-ai/drive9/pkg/fusecomp/writebuffer"
)

// ByteRange is a half-open [Start, End) file range.
type ByteRange struct {
	Start int64
	End   int64
}

// dirtyPartRanges converts the write buffer's dirty part numbers into
// coalesced file ranges clipped to size. Part numbers are 1-based (the same
// convention as ShadowStore.WriteExtents). Called before the handle's
// ClearDirty so the set is exactly this commit's delta.
func dirtyPartRanges(wb *writebuffer.WriteBuffer, size int64) []ByteRange {
	if wb == nil {
		return nil
	}
	nums := wb.DirtyPartNumbers()
	if len(nums) == 0 {
		return nil
	}
	sort.Ints(nums)
	part := wb.PartSize()
	var out []ByteRange
	for _, n := range nums {
		start := int64(n-1) * part
		end := start + part
		if start >= size {
			continue
		}
		if end > size {
			end = size
		}
		if len(out) > 0 && out[len(out)-1].End == start {
			out[len(out)-1].End = end
		} else {
			out = append(out, ByteRange{Start: start, End: end})
		}
	}
	return out
}

// uploadExtentEntry routes an entry through the extent block plane when the
// target is, or will become, an extent-layout file:
//
//   - existing extent target with dirty ranges → incremental PATCH (only
//     the delta is uploaded — the append/log case);
//   - existing extent target without usable dirty ranges (spilled entries
//     lose per-part dirty tracking) or a fresh large file → whole-image
//     sequential extent session (initiate layout=extent; legacy servers
//     silently answer a classic plan and the same session proceeds
//     classically);
//   - existing classic targets are never converted — they keep the classic
//     multipart path.
//
// handled=false falls through to the classic direct-PUT/multipart routing.
func (cq *CommitQueue) uploadExtentEntry(ctx context.Context, entry *CommitEntry, apiPath string, expectedRevision int64) (int64, bool, error) {
	if !cq.extentTakeover {
		return 0, false, nil
	}
	threshold := cq.directPutThreshold()
	if entry.Size > 0 && threshold > 0 && entry.Size < threshold {
		return 0, false, nil // below the inline cutoff: classic direct PUT
	}
	if cq.client == nil {
		return 0, false, nil
	}
	st, statErr := cq.client.StatCtx(ctx, apiPath)
	if statErr == nil && st.ContentLayout != client.ContentLayoutExtent {
		return 0, false, nil // existing classic target: never converted
	}
	if statErr != nil && !client.IsNotFound(statErr) {
		// Unknown target state (transient stat failure): keep classic.
		return 0, false, nil
	}
	extentTarget := statErr == nil

	ra, err := cq.entryReaderAt(ctx, entry)
	if err != nil {
		return 0, true, err
	}
	start := time.Now()
	var upErr error
	if extentTarget && len(entry.DirtyRanges) > 0 {
		upErr = cq.commitExtentPatch(ctx, apiPath, entry, ra, st.Size)
	} else {
		upErr = cq.client.WriteExtentStreamConditional(ctx, apiPath, ra, entry.Size, nil, expectedRevision)
	}
	if cq.perf != nil {
		cq.perf.recordRemoteOp(perfRemoteWrite, upErr, time.Since(start), uint64(entry.Size))
	}
	if upErr != nil {
		return 0, true, upErr
	}
	return 0, true, nil
}

// commitExtentPatch uploads the entry's dirty ranges as extent blocks and
// commits with a revision CAS.
func (cq *CommitQueue) commitExtentPatch(ctx context.Context, apiPath string, entry *CommitEntry, ra io.ReaderAt, remoteSize int64) error {
	blocks, err := extentDirtyBlocks(ra, entry.DirtyRanges, entry.Size)
	if err != nil {
		return err
	}
	if len(blocks) == 0 && entry.Size > remoteSize {
		// Protocol workaround: PATCH requires at least one dirty block, so a
		// pure truncate-grow commits one zero block at the old end. The hole
		// beyond it stays implicit; content addressing dedupes the zeros.
		blocks = append(blocks, zeroGrowBlock(remoteSize, entry.Size))
	}
	if len(blocks) == 0 {
		return fmt.Errorf("extent patch for %s has no dirty blocks", entry.Path)
	}
	return cq.client.CommitExtentPatch(ctx, apiPath, entry.Size, blocks, entry.BaseRev)
}

// extentDirtyBlocks cuts dirty ranges into extent blocks read from the
// staged image. Ranges of at least one block size are cut on the 4 MiB grid
// (aligned blocks dedupe and compact well); smaller ranges pass through at
// their natural bounds so an append stays proportional to the bytes
// written. Fragmentation beyond that is the server compaction's problem.
func extentDirtyBlocks(ra io.ReaderAt, ranges []ByteRange, size int64) ([]client.ExtentBlockWrite, error) {
	byKey := make(map[uint64]client.ExtentBlockWrite)
	var order []uint64
	for _, r := range ranges {
		if r.Start < 0 {
			r.Start = 0
		}
		if r.End > size {
			r.End = size
		}
		for s := r.Start; s < r.End; {
			blkStart, blkEnd := s, r.End
			if r.End-s >= client.ExtentBlockSize {
				blkStart = s - s%client.ExtentBlockSize
				if e := blkStart + client.ExtentBlockSize; e < blkEnd {
					blkEnd = e
				}
			}
			// A block never crosses a 64 MiB chunk boundary, whatever its
			// size: slices live inside one chunk.
			if chunkEnd := (blkStart/client.ExtentChunkSize + 1) * client.ExtentChunkSize; blkEnd > chunkEnd {
				blkEnd = chunkEnd
			}
			if blkEnd > size {
				blkEnd = size
			}
			data := make([]byte, blkEnd-blkStart)
			if _, err := ra.ReadAt(data, blkStart); err != nil && err != io.EOF {
				return nil, fmt.Errorf("read staged bytes [%d,%d): %w", blkStart, blkEnd, err)
			}
			chunk, off, ok := client.ChunkOffset(blkStart)
			if !ok {
				return nil, fmt.Errorf("invalid block offset %d", blkStart)
			}
			key := uint64(chunk)<<32 | uint64(off)
			if _, dup := byKey[key]; !dup {
				byKey[key] = client.ExtentBlockWrite{Chunk: chunk, Off: off, Data: data}
				order = append(order, key)
			}
			s = blkEnd
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	blocks := make([]client.ExtentBlockWrite, 0, len(order))
	for _, k := range order {
		blocks = append(blocks, byKey[k])
	}
	return blocks, nil
}

// zeroGrowBlock synthesizes one zero block at the old remote end so a
// truncate-only grow satisfies the PATCH protocol's dirty-block minimum.
func zeroGrowBlock(oldSize, newSize int64) client.ExtentBlockWrite {
	n := int64(4096)
	if rem := newSize - oldSize; rem < n {
		n = rem
	}
	chunk, off, _ := client.ChunkOffset(oldSize)
	return client.ExtentBlockWrite{Chunk: chunk, Off: off, Data: make([]byte, n)}
}

// entryReaderAt exposes the entry's staged image as an io.ReaderAt:
// materialized payload when it fits in memory, the shadow generation
// otherwise (multi-GiB spill files stream block-by-block through the same
// interface).
func (cq *CommitQueue) entryReaderAt(ctx context.Context, entry *CommitEntry) (io.ReaderAt, error) {
	if !entry.ShadowSpill {
		data, err := cq.readEntryPayloadCtx(ctx, entry)
		if err != nil {
			return nil, err
		}
		return bytes.NewReader(data), nil
	}
	return &shadowGenReaderAt{shadows: cq.shadows, gen: entry.ShadowGen, size: entry.Size}, nil
}

type shadowGenReaderAt struct {
	shadows *staging.ShadowStore
	gen     uint64
	size    int64
}

func (r *shadowGenReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative read offset")
	}
	if off >= r.size {
		return 0, io.EOF
	}
	if off+int64(len(p)) > r.size {
		p = p[:r.size-off]
	}
	n, err := r.shadows.ReadAtGen(r.gen, off, p)
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
