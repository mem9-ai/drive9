package extent

import (
	"encoding/binary"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

const sliceBytes = 24

type rawSlice struct {
	pos         uint32
	id          uint64
	size        uint32
	off         uint32
	len         uint32
	left, right *rawSlice
}

func parseRawSlices(buf []byte) []*rawSlice {
	if len(buf)%sliceBytes != 0 {
		return nil
	}
	n := len(buf) / sliceBytes
	out := make([]*rawSlice, n)
	base := make([]rawSlice, n)
	for i := 0; i < n; i++ {
		b := buf[i*sliceBytes:]
		base[i] = rawSlice{
			pos:  binary.BigEndian.Uint32(b[0:4]),
			id:   binary.BigEndian.Uint64(b[4:12]),
			size: binary.BigEndian.Uint32(b[12:16]),
			off:  binary.BigEndian.Uint32(b[16:20]),
			len:  binary.BigEndian.Uint32(b[20:24]),
		}
		out[i] = &base[i]
	}
	return out
}

func newRawSlice(pos uint32, id uint64, cleng, off, length uint32) *rawSlice {
	if length == 0 {
		return nil
	}
	return &rawSlice{pos: pos, id: id, size: cleng, off: off, len: length}
}

func (s *rawSlice) cut(pos uint32) (left, right *rawSlice) {
	if s == nil {
		return nil, nil
	}
	if pos <= s.pos {
		if s.left == nil {
			s.left = newRawSlice(pos, 0, 0, 0, s.pos-pos)
		}
		left, s.left = s.left.cut(pos)
		return left, s
	} else if pos < s.pos+s.len {
		l := pos - s.pos
		right = newRawSlice(pos, s.id, s.size, s.off+l, s.len-l)
		right.right = s.right
		s.len = l
		s.right = nil
		return s, right
	}
	if s.right == nil {
		s.right = newRawSlice(s.pos+s.len, 0, 0, 0, pos-s.pos-s.len)
	}
	s.right, right = s.right.cut(pos)
	return s, right
}

func (s *rawSlice) visit(f func(*rawSlice)) {
	if s == nil {
		return
	}
	s.left.visit(f)
	right := s.right
	f(s)
	right.visit(f)
}

func buildOverlay(ss []*rawSlice) []jfsmeta.Slice {
	var root *rawSlice
	for i := range ss {
		s := new(rawSlice)
		*s = *ss[i]
		s.left, s.right = nil, nil
		var right *rawSlice
		s.left, right = root.cut(s.pos)
		_, s.right = right.cut(s.pos + s.len)
		root = s
	}
	var pos uint32
	var chunk []jfsmeta.Slice
	root.visit(func(s *rawSlice) {
		if s.pos > pos {
			chunk = append(chunk, jfsmeta.Slice{Size: s.pos - pos, Len: s.pos - pos})
			pos = s.pos
		}
		chunk = append(chunk, jfsmeta.Slice{Id: s.id, Size: s.size, Off: s.off, Len: s.len})
		pos += s.len
	})
	return chunk
}

func compactOverlay(ss []*rawSlice) (pos uint32, size uint32, chunk []jfsmeta.Slice) {
	chunk = buildOverlay(ss)
	n := len(chunk)
	for n > 1 {
		if chunk[0].Id == 0 {
			pos += chunk[0].Len
			chunk = chunk[1:]
			n--
		} else if chunk[n-1].Id == 0 {
			chunk = chunk[:n-1]
			n--
		} else {
			break
		}
	}
	if n == 1 && chunk[0].Id == 0 {
		chunk[0].Len = 1
	}
	for _, c := range chunk {
		size += c.Len
	}
	return pos, size, chunk
}

func skipSome(chunk []*rawSlice) int {
	var skipped int
	total := len(chunk)
OUT:
	for skipped < total {
		ss := chunk[skipped:]
		pos, size, c := compactOverlay(ss)
		first := ss[0]
		if first.len < (1<<20) || first.len*5 < size || size == 0 {
			break
		}
		if pos != first.pos || c[0].Id != first.id || c[0].Off != first.off || c[0].Len != first.len {
			break
		}
		for _, s := range ss[1:] {
			if s.pos == first.pos && s.id == first.id && s.off == first.off && s.len == first.len {
				break OUT
			}
		}
		skipped++
	}
	return skipped
}
