package fuse

// shadowWrittenRanges is the sorted, non-overlapping union of bytes written by
// this process. Adjacent ranges are merged, so sequential writes use one entry.
// It is never recovered from logical file sizes or persisted to disk.
type shadowWrittenRanges []DirtyExtent

func (r shadowWrittenRanges) addedBytes(offset, length int64) int64 {
	end, added := offset+length, length
	for _, e := range r {
		if e.Offset >= end {
			break
		}
		if overlap := min(end, e.Offset+e.Length) - max(offset, e.Offset); overlap > 0 {
			added -= overlap
		}
	}
	return added
}

func (r *shadowWrittenRanges) add(offset, length int64) {
	if length <= 0 {
		return
	}
	end := offset + length
	i := 0
	for i < len(*r) && (*r)[i].Offset+(*r)[i].Length < offset {
		i++
	}
	j := i
	for j < len(*r) && (*r)[j].Offset <= end {
		offset = min(offset, (*r)[j].Offset)
		end = max(end, (*r)[j].Offset+(*r)[j].Length)
		j++
	}
	if i == j {
		*r = append(*r, DirtyExtent{})
		copy((*r)[i+1:], (*r)[i:])
	} else {
		copy((*r)[i+1:], (*r)[j:])
		*r = (*r)[:len(*r)-(j-i)+1]
	}
	(*r)[i] = DirtyExtent{Offset: offset, Length: end - offset}
}

func (r *shadowWrittenRanges) truncate(size int64) int64 {
	var total int64
	n := 0
	for _, e := range *r {
		if e.Offset >= size {
			break
		}
		e.Length = min(e.Length, size-e.Offset)
		(*r)[n] = e
		total += e.Length
		n++
	}
	*r = (*r)[:n]
	return total
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
