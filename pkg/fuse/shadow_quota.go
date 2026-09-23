package fuse

import (
	"io"
	"os"
	"syscall"

	"github.com/google/btree"
)

// writeShadowAt preserves the actual byte count on partial I/O failures.
// Go 1.25's os.File.WriteAt discards the count when its internal pwrite returns
// both bytes and an error. SyscallConn keeps the descriptor alive during I/O.
func writeShadowAt(file *os.File, data []byte, offset int64) (int, error) {
	conn, err := file.SyscallConn()
	if err != nil {
		return 0, err
	}
	var total int
	var writeErr error
	err = conn.Write(func(fd uintptr) bool {
		for total < len(data) {
			n, err := syscall.Pwrite(int(fd), data[total:], offset+int64(total))
			if n > 0 {
				total += n
			}
			if err == syscall.EINTR {
				continue
			}
			if err != nil {
				writeErr = err
				break
			}
			if n == 0 {
				writeErr = io.ErrShortWrite
				break
			}
		}
		return true
	})
	if err != nil {
		return total, err
	}
	return total, writeErr
}

// shadowWrittenRanges is the ordered union of bytes written in this process.
// Adjacent ranges are merged. The B-tree keeps sparse, fragmented writes from
// scanning every earlier range; no ranges are persisted or recovered.
type shadowWrittenRanges struct {
	tree  *btree.BTreeG[DirtyExtent]
	total int64
}

func newShadowWrittenTree() *btree.BTreeG[DirtyExtent] {
	return btree.NewG[DirtyExtent](32, func(a, b DirtyExtent) bool {
		return a.Offset < b.Offset
	})
}

func (r shadowWrittenRanges) clone() shadowWrittenRanges {
	if r.tree == nil {
		return r
	}
	return shadowWrittenRanges{tree: r.tree.Clone(), total: r.total}
}

func (r shadowWrittenRanges) addedBytes(offset, length int64) int64 {
	if length <= 0 || r.tree == nil {
		return length
	}
	end, added := offset+length, length
	r.tree.DescendLessOrEqual(DirtyExtent{Offset: offset}, func(e DirtyExtent) bool {
		if e.Offset < offset && e.Offset+e.Length > offset {
			added -= min(end, e.Offset+e.Length) - offset
		}
		return false
	})
	r.tree.AscendGreaterOrEqual(DirtyExtent{Offset: offset}, func(e DirtyExtent) bool {
		if e.Offset >= end {
			return false
		}
		added -= min(end, e.Offset+e.Length) - e.Offset
		return true
	})
	return added
}

func (r *shadowWrittenRanges) add(offset, length int64) {
	if length <= 0 {
		return
	}
	r.total += r.addedBytes(offset, length)
	if r.tree == nil {
		r.tree = newShadowWrittenTree()
	}
	end := offset + length
	start := offset
	var removed []DirtyExtent
	r.tree.DescendLessOrEqual(DirtyExtent{Offset: offset}, func(e DirtyExtent) bool {
		if e.Offset < offset && e.Offset+e.Length >= offset {
			start = e.Offset
			end = max(end, e.Offset+e.Length)
			removed = append(removed, e)
		}
		return false
	})
	r.tree.AscendGreaterOrEqual(DirtyExtent{Offset: offset}, func(e DirtyExtent) bool {
		if e.Offset > end {
			return false
		}
		end = max(end, e.Offset+e.Length)
		removed = append(removed, e)
		return true
	})
	for _, e := range removed {
		r.tree.Delete(e)
	}
	r.tree.ReplaceOrInsert(DirtyExtent{Offset: start, Length: end - start})
}

func (r *shadowWrittenRanges) truncate(size int64) int64 {
	if size <= 0 {
		r.tree = nil
		r.total = 0
		return 0
	}
	if r.tree == nil {
		return 0
	}
	var clipped DirtyExtent
	r.tree.DescendLessOrEqual(DirtyExtent{Offset: size}, func(e DirtyExtent) bool {
		if e.Offset < size && e.Offset+e.Length > size {
			clipped = e
		}
		return false
	})
	if clipped.Length > 0 {
		r.tree.Delete(clipped)
		r.total -= clipped.Offset + clipped.Length - size
		clipped.Length = size - clipped.Offset
		r.tree.ReplaceOrInsert(clipped)
	}
	var removed []DirtyExtent
	r.tree.AscendGreaterOrEqual(DirtyExtent{Offset: size}, func(e DirtyExtent) bool {
		removed = append(removed, e)
		return true
	})
	for _, e := range removed {
		r.tree.Delete(e)
		r.total -= e.Length
	}
	return r.total
}

// recordWrite accounts actual I/O, even when a partial write also returned an
// error. The caller holds the path lock; s.mu protects readers of shadow state.
func (s *ShadowStore) recordWrite(remotePath string, sf *ShadowFile, offset int64, n int, baseRev int64) {
	if n <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	added := sf.written.addedBytes(offset, int64(n))
	sf.written.add(offset, int64(n))
	sf.writtenBytes += added
	s.quotaBytes.Add(added)
	newSize := max(sf.size, offset+int64(n))
	s.pendingBytes.Add(newSize - sf.size)
	sf.size = newSize
	if baseRev != 0 {
		sf.baseRev = baseRev
	}
	s.bumpWriteGenLocked(remotePath)
}

// resizeWrittenLocked clips runtime coverage but never charges newly created
// holes. Call after a successful truncate with s.mu held.
func (s *ShadowStore) resizeWrittenLocked(sf *ShadowFile, size int64) {
	written := sf.written.truncate(size)
	s.quotaBytes.Add(written - sf.writtenBytes)
	sf.writtenBytes = written
}
