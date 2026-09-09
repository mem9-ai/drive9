package fuse

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"syscall"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
)

// extentFileWriter is the JuiceFS fileWriter analogue: one open extent file
// fans writes into per-chunk sliceWriters that own the dirty bytes.
type extentFileWriter struct {
	mu       sync.Mutex
	seq      uint64
	chunks   map[int64]*extentChunkWriter
	inflight []*extentSliceWriter
	pending  []*extentSliceWriter // freeze order; JuiceFS chunk.slices FIFO

	fs       *Dat9FS
	of       *extentOpenFile
	ino      uint64
	path     string
	fileID   string // server inode id; block keys are blocks/{fileID}/...
	unlinked bool   // JuiceFS Unlink is name/nlink; do not Meta.Create the name again

	err          gofuse.Status
	flushwaiting uint16
	writewaiting uint16
	committing   bool

	flushcond  *sync.Cond // wait for pending==nil (JuiceFS flushcond)
	writecond  *sync.Cond // wait for flushwaiting==0
	commitcond *sync.Cond // PUT done / slice committed
	commitMu   sync.Mutex // Drive9-must: file_slices CAS is per-inode
}

type extentChunkWriter struct {
	file    *extentFileWriter
	indx    int64
	current *extentSliceWriter
	frozen  []*extentSliceWriter
}

type extentSliceWriter struct {
	chunk   *extentChunkWriter
	fileOff int64
	buf     []byte
	seq     uint64
	started time.Time
	lastMod time.Time
	frozen  bool

	done      bool
	committed bool
	uploading bool
	err       error
	landed    []client.LandedBlock
	dep       *extentSliceWriter
	growing   bool
}

func newExtentFileWriter() *extentFileWriter {
	w := &extentFileWriter{chunks: make(map[int64]*extentChunkWriter)}
	w.flushcond = sync.NewCond(&w.mu)
	w.writecond = sync.NewCond(&w.mu)
	w.commitcond = sync.NewCond(&w.mu)
	return w
}

func (w *extentFileWriter) bind(fs *Dat9FS, path string, ino uint64, of *extentOpenFile) {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.fs = fs
	w.path = path
	w.ino = ino
	w.of = of
	w.mu.Unlock()
}

func (w *extentFileWriter) bound() bool {
	return w != nil && w.fs != nil && w.fs.client != nil
}

func (w *extentFileWriter) markUnlinked() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.unlinked = true
	w.mu.Unlock()
}

func (w *extentFileWriter) writeAt(off int64, data []byte) (frozeFull bool) {
	if w == nil || len(data) == 0 {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// JuiceFS fileWriter.Write: wait while flushwaiting>0.
	w.writewaiting++
	for w.flushwaiting > 0 {
		w.writecond.Wait()
	}
	w.writewaiting--
	w.seq++
	seq := w.seq
	for len(data) > 0 {
		chunk := off / client.ExtentChunkSize
		chunkEnd := (chunk + 1) * client.ExtentChunkSize
		n := int64(len(data))
		if off+n > chunkEnd {
			n = chunkEnd - off
		}
		cw := w.chunkLocked(chunk)
		used, full := cw.writeAt(off, data[:n], seq)
		if full {
			frozeFull = true
		}
		off += used
		data = data[used:]
	}
	return frozeFull
}

func (w *extentFileWriter) status() gofuse.Status {
	if w == nil {
		return gofuse.OK
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *extentFileWriter) chunkLocked(indx int64) *extentChunkWriter {
	cw := w.chunks[indx]
	if cw == nil {
		cw = &extentChunkWriter{file: w, indx: indx}
		w.chunks[indx] = cw
	}
	return cw
}

func (cw *extentChunkWriter) writeAt(off int64, data []byte, seq uint64) (int64, bool) {
	n := int64(len(data))
	if n == 0 {
		return 0, false
	}
	s := cw.findWritableSlice(off, n)
	if s != nil {
		start := s.fileOff
		rel := off - start
		// JuiceFS findWritableSlice: pos <= off+slen. A write that would
		// skip ahead (rel > slen) must start a new slice, not zero-fill.
		if rel > int64(len(s.buf)) {
			s = nil
		} else {
			remain := client.ExtentMaxBlockSize - rel
			if remain > 0 {
				take := n
				if take > remain {
					take = remain
				}
				need := rel + take
				if need > int64(len(s.buf)) {
					s.buf = append(s.buf, make([]byte, need-int64(len(s.buf)))...)
				}
				copy(s.buf[rel:rel+take], data[:take])
				s.lastMod = time.Now()
				s.seq = seq
				full := int64(len(s.buf)) >= client.ExtentMaxBlockSize
				if full {
					cw.freezeCurrent()
				}
				return take, full
			}
		}
	}
	if cw.current != nil {
		cw.freezeCurrent()
	}
	now := time.Now()
	cw.current = &extentSliceWriter{fileOff: off, started: now, lastMod: now, seq: seq}
	take := n
	if take > client.ExtentMaxBlockSize {
		take = client.ExtentMaxBlockSize
	}
	cw.current.buf = append(cw.current.buf, data[:take]...)
	full := int64(len(cw.current.buf)) >= client.ExtentMaxBlockSize
	if full {
		cw.freezeCurrent()
	}
	return take, full
}

// findWritableSlice is JuiceFS chunkWriter.findWritableSlice: newest unfrozen
// slice accepts a write in [off, off+slen]; any overlap with another slice
// starts a new slice (last-write-wins append).
func (cw *extentChunkWriter) findWritableSlice(off, size int64) *extentSliceWriter {
	if cw.current != nil && !cw.current.frozen {
		start := cw.current.fileOff
		end := start + int64(len(cw.current.buf))
		if off >= start && off <= end {
			return cw.current
		}
	}
	end := off + size
	if cw.current != nil && !cw.current.frozen {
		s0, s1 := cw.current.fileOff, cw.current.fileOff+int64(len(cw.current.buf))
		if off < s1 && s0 < end {
			return nil
		}
	}
	for i := len(cw.frozen) - 1; i >= 0; i-- {
		s := cw.frozen[i]
		if s == nil {
			continue
		}
		s0, s1 := s.fileOff, s.fileOff+int64(len(s.buf))
		if off < s1 && s0 < end {
			return nil
		}
	}
	return nil
}

func (cw *extentChunkWriter) freezeCurrent() {
	if cw.current == nil || len(cw.current.buf) == 0 {
		cw.current = nil
		return
	}
	s := cw.current
	cw.current = nil
	// JuiceFS sliceWriter.flushData uploads slen as written, including
	// interior zeros. Splitting those out made KEEP_CACHE serve zeros
	// that meta no longer covered (wal-multiwrite btreeInitPage).
	s.frozen = true
	s.chunk = cw
	cw.frozen = append(cw.frozen, s)
	w := cw.file
	if w == nil {
		return
	}
	w.pending = append(w.pending, s)
	if w.bound() && !s.uploading {
		s.uploading = true
		w.startCommitLoopLocked()
		go s.flushData()
	}
}

func (w *extentFileWriter) freezeDue(now time.Time, tooMany bool) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	lastBit := now.UnixNano() & 1
	i := 0
	for _, cw := range w.chunks {
		if cw.current == nil {
			i++
			continue
		}
		s := cw.current
		idle := now.Sub(s.lastMod) > extentFlushIdle && now.Sub(s.started) > extentFlushIdle
		aged := now.Sub(s.started) > extentFlushAge
		half := tooMany && int64(i)%2 == lastBit
		if idle || aged || half {
			cw.freezeCurrent()
		}
		i++
	}
}

func (w *extentFileWriter) freezeAll() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cw := range w.chunks {
		cw.freezeCurrent()
	}
}

func (w *extentFileWriter) freezeAllPayloads() []client.ExtentPayload {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cw := range w.chunks {
		cw.freezeCurrent()
	}
	var out []client.ExtentPayload
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			if len(s.buf) > 0 {
				out = append(out, payloadFromSlice(s))
			}
		}
	}
	return out
}

func (w *extentFileWriter) sliceCount() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, cw := range w.chunks {
		n += len(cw.frozen)
		if cw.current != nil && len(cw.current.buf) > 0 {
			n++
		}
	}
	return n
}

func (w *extentFileWriter) frozenCount() int {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for _, cw := range w.chunks {
		n += len(cw.frozen)
	}
	return n
}

func payloadFromSlice(s *extentSliceWriter) client.ExtentPayload {
	return client.ExtentPayload{FileOff: s.fileOff, Data: append([]byte(nil), s.buf...)}
}

func (w *extentFileWriter) frozenPayloads() []client.ExtentPayload {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []client.ExtentPayload
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			if len(s.buf) > 0 {
				out = append(out, payloadFromSlice(s))
			}
		}
	}
	return out
}

func (w *extentFileWriter) allPayloads() []client.ExtentPayload {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []client.ExtentPayload
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			if len(s.buf) > 0 {
				out = append(out, payloadFromSlice(s))
			}
		}
		if cw.current != nil && len(cw.current.buf) > 0 {
			out = append(out, payloadFromSlice(cw.current))
		}
	}
	return out
}

func (w *extentFileWriter) dropFrozen() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.dropFrozenLocked()
}

func (w *extentFileWriter) dropFrozenLocked() {
	for indx, cw := range w.chunks {
		cw.frozen = cw.frozen[:0]
		if cw.current == nil || len(cw.current.buf) == 0 {
			delete(w.chunks, indx)
		}
	}
}

func (w *extentFileWriter) dropAllSlices() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.chunks = make(map[int64]*extentChunkWriter)
	w.inflight = nil
	w.pending = nil
	w.commitcond.Broadcast()
	w.flushcond.Broadcast()
}

// detachFrozen removes frozen slices from the writer so a concurrent write
// that freezes more data during commit I/O cannot be dropped with them.
func (w *extentFileWriter) detachFrozen() []*extentSliceWriter {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.detachFrozenLocked()
}

// detachAll freezes the current slice, then detaches every frozen slice.
func (w *extentFileWriter) detachAll() []*extentSliceWriter {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cw := range w.chunks {
		cw.freezeCurrent()
	}
	return w.detachFrozenLocked()
}

func (w *extentFileWriter) detachFrozenLocked() []*extentSliceWriter {
	var held []*extentSliceWriter
	for indx, cw := range w.chunks {
		if len(cw.frozen) > 0 {
			held = append(held, cw.frozen...)
			w.inflight = append(w.inflight, cw.frozen...)
			cw.frozen = nil
		}
		if cw.current == nil || len(cw.current.buf) == 0 {
			delete(w.chunks, indx)
		}
	}
	w.pending = subtractSliceWriters(w.pending, held)
	return held
}

func (w *extentFileWriter) dropInflight(held []*extentSliceWriter) {
	if w == nil || len(held) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight = subtractSliceWriters(w.inflight, held)
}

func (w *extentFileWriter) restoreFrozen(held []*extentSliceWriter) {
	if w == nil || len(held) == 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight = subtractSliceWriters(w.inflight, held)
	for _, s := range held {
		if s == nil || len(s.buf) == 0 {
			continue
		}
		cw := w.chunkLocked(s.fileOff / client.ExtentChunkSize)
		cw.frozen = append(cw.frozen, s)
	}
}

func subtractSliceWriters(all, remove []*extentSliceWriter) []*extentSliceWriter {
	if len(remove) == 0 {
		return all
	}
	drop := make(map[*extentSliceWriter]struct{}, len(remove))
	for _, s := range remove {
		drop[s] = struct{}{}
	}
	out := all[:0]
	for _, s := range all {
		if _, ok := drop[s]; !ok {
			out = append(out, s)
		}
	}
	return out
}

func payloadsFromSlices(held []*extentSliceWriter) []client.ExtentPayload {
	if len(held) == 0 {
		return nil
	}
	out := make([]client.ExtentPayload, 0, len(held))
	for _, s := range held {
		if s != nil && len(s.buf) > 0 {
			out = append(out, payloadFromSlice(s))
		}
	}
	return out
}

func overlaySlice(s *extentSliceWriter, off, end int64, dest []byte) {
	if s == nil || len(s.buf) == 0 {
		return
	}
	s0, s1 := s.fileOff, s.fileOff+int64(len(s.buf))
	if s1 <= off || s0 >= end {
		return
	}
	from := off
	if s0 > from {
		from = s0
	}
	to := end
	if s1 < to {
		to = s1
	}
	copy(dest[from-off:to-off], s.buf[from-s0:to-s0])
}

func (w *extentFileWriter) readAt(off int64, dest []byte) {
	if w == nil || len(dest) == 0 {
		return
	}
	end := off + int64(len(dest))
	w.mu.Lock()
	defer w.mu.Unlock()
	type hit struct {
		seq uint64
		s   *extentSliceWriter
	}
	var hits []hit
	for _, s := range w.inflight {
		hits = append(hits, hit{s.seq, s})
	}
	for _, cw := range w.chunks {
		if cw.current != nil {
			hits = append(hits, hit{cw.current.seq, cw.current})
		}
		for _, s := range cw.frozen {
			hits = append(hits, hit{s.seq, s})
		}
	}
	for i := 1; i < len(hits); i++ {
		j := i
		for j > 0 && hits[j].seq < hits[j-1].seq {
			hits[j], hits[j-1] = hits[j-1], hits[j]
			j--
		}
	}
	for _, h := range hits {
		overlaySlice(h.s, off, end, dest)
	}
}

func (w *extentFileWriter) hasCurrent() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, cw := range w.chunks {
		if cw.current != nil && len(cw.current.buf) > 0 {
			return true
		}
	}
	return false
}

func (w *extentFileWriter) dirtyBytes() int64 {
	if w == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var n int64
	for _, s := range w.inflight {
		n += int64(len(s.buf))
	}
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			n += int64(len(s.buf))
		}
		if cw.current != nil {
			n += int64(len(cw.current.buf))
		}
	}
	return n
}

func (w *extentFileWriter) hasInflight() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.inflight) > 0 || len(w.pending) > 0 {
		return true
	}
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			if s != nil && !s.committed {
				return true
			}
		}
	}
	return false
}

func (w *extentFileWriter) hasLocalDirty() bool {
	if w == nil {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.pending) > 0 {
		return true
	}
	for _, cw := range w.chunks {
		if len(cw.frozen) > 0 || (cw.current != nil && len(cw.current.buf) > 0) {
			return true
		}
	}
	return false
}

func (w *extentFileWriter) empty() bool {
	if w == nil {
		return true
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.inflight) > 0 || len(w.pending) > 0 {
		return false
	}
	for _, cw := range w.chunks {
		if len(cw.frozen) > 0 || (cw.current != nil && len(cw.current.buf) > 0) {
			return false
		}
	}
	return true
}

func (w *extentFileWriter) clip(size int64) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	// JuiceFS writer.Truncate only sets length; frozen/inflight slices keep
	// their buffers and meta.Truncate holes overlay the tail. Mutating a
	// frozen buf while flushData copies it tears the PUT (VACUUM / ftruncate).
	for indx, cw := range w.chunks {
		if cw.current != nil {
			cw.current = clipSlice(cw.current, size)
		}
		if cw.current == nil && len(cw.frozen) == 0 {
			delete(w.chunks, indx)
		}
	}
}

func clipSlice(s *extentSliceWriter, size int64) *extentSliceWriter {
	if s == nil || s.fileOff >= size {
		return nil
	}
	if s.fileOff+int64(len(s.buf)) > size {
		s.buf = s.buf[:size-s.fileOff]
	}
	if len(s.buf) == 0 {
		return nil
	}
	return s
}

func punchSliceList(in []*extentSliceWriter, start, end int64) []*extentSliceWriter {
	var next []*extentSliceWriter
	for _, s := range in {
		left, right := splitPunch(s, start, end)
		if left != nil {
			next = append(next, left)
		}
		if right != nil {
			next = append(next, right)
		}
	}
	return next
}

func (w *extentFileWriter) punch(start, end int64) {
	if w == nil || end <= start {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.inflight = punchSliceList(w.inflight, start, end)
	for indx, cw := range w.chunks {
		var next []*extentSliceWriter
		if cw.current != nil {
			left, right := splitPunch(cw.current, start, end)
			if left != nil {
				next = append(next, left)
			}
			if right != nil {
				next = append(next, right)
			}
			cw.current = nil
		}
		for _, s := range cw.frozen {
			left, right := splitPunch(s, start, end)
			if left != nil {
				next = append(next, left)
			}
			if right != nil {
				next = append(next, right)
			}
		}
		cw.frozen = next
		if len(cw.frozen) == 0 {
			delete(w.chunks, indx)
		}
	}
	w.syncPendingWithFrozenLocked()
}

func (w *extentFileWriter) syncPendingWithFrozenLocked() {
	live := make(map[*extentSliceWriter]struct{})
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			live[s] = struct{}{}
		}
	}
	var next []*extentSliceWriter
	seen := make(map[*extentSliceWriter]struct{})
	for _, s := range w.pending {
		if _, ok := live[s]; ok {
			next = append(next, s)
			seen[s] = struct{}{}
			continue
		}
		s.done = true
		s.buf = nil
	}
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			if _, ok := seen[s]; ok {
				continue
			}
			if s == nil || len(s.buf) == 0 {
				continue
			}
			s.chunk = cw
			next = append(next, s)
			if w.bound() && !s.uploading {
				s.uploading = true
				w.startCommitLoopLocked()
				go s.flushData()
			}
		}
	}
	w.pending = next
	w.commitcond.Broadcast()
	w.flushcond.Broadcast()
}

func splitPunch(s *extentSliceWriter, start, end int64) (left, right *extentSliceWriter) {
	if s == nil {
		return nil, nil
	}
	s0, s1 := s.fileOff, s.fileOff+int64(len(s.buf))
	if s1 <= start || s0 >= end {
		return s, nil
	}
	if s0 >= start && s1 <= end {
		return nil, nil
	}
	if s0 < start {
		left = &extentSliceWriter{
			chunk: s.chunk, fileOff: s.fileOff, buf: append([]byte(nil), s.buf[:start-s0]...),
			seq: s.seq, started: s.started, lastMod: s.lastMod, frozen: s.frozen,
		}
	}
	if s1 > end {
		right = &extentSliceWriter{
			chunk: s.chunk, fileOff: end, buf: append([]byte(nil), s.buf[end-s0:]...),
			seq: s.seq, started: s.started, lastMod: s.lastMod, frozen: s.frozen,
		}
	}
	return left, right
}

func (w *extentFileWriter) requeueFrozenLocked() {
	if w == nil {
		return
	}
	seen := make(map[*extentSliceWriter]struct{}, len(w.pending))
	for _, s := range w.pending {
		seen[s] = struct{}{}
	}
	for _, cw := range w.chunks {
		for _, s := range cw.frozen {
			if s == nil || s.committed || len(s.buf) == 0 {
				continue
			}
			if _, ok := seen[s]; ok {
				continue
			}
			s.done = false
			s.err = nil
			w.pending = append(w.pending, s)
			if w.bound() && !s.uploading {
				s.uploading = true
				go s.flushData()
			}
			seen[s] = struct{}{}
		}
	}
	if len(w.pending) > 0 {
		w.startCommitLoopLocked()
	}
}

func (w *extentFileWriter) startCommitLoopLocked() {
	if w == nil || w.committing {
		return
	}
	w.committing = true
	go w.commitLoop()
}

func (s *extentSliceWriter) markDone() {
	if s == nil || s.chunk == nil || s.chunk.file == nil {
		return
	}
	w := s.chunk.file
	w.mu.Lock()
	s.done = true
	w.commitcond.Broadcast()
	w.flushcond.Broadcast()
	w.mu.Unlock()
}

// flushData is JuiceFS sliceWriter.flushData / chunk.Writer.Finish in
// writeback mode: prepare-blocks + local stage, no S3 wait. commitLoop
// commits metadata then PUTs in the background.
func (s *extentSliceWriter) flushData() {
	defer s.markDone()
	if s == nil || len(s.buf) == 0 {
		return
	}
	if s.chunk == nil || s.chunk.file == nil {
		return
	}
	w := s.chunk.file
	if !w.bound() {
		return
	}
	fs := w.fs
	w.mu.Lock()
	payload := payloadFromSlice(s)
	w.mu.Unlock()
	ctx := context.Background()
	if err := w.ensureRemote(ctx); err != nil {
		s.err = err
		return
	}
	// JuiceFS writeback Finish is local. Block keys are allocated here
	// (blocks/{inode}/{id}); commit-slices is the meta.Write, PUT is async.
	data := append([]byte(nil), payload.Data...)
	sum := client.SHA256Hex(data)
	fileID := w.fileID
	if fileID == "" {
		fileID = "fuse"
	}
	key := "blocks/" + fileID + "/" + client.NewExtentOpID()
	landed := []client.LandedBlock{{
		Op: client.SliceOp{
			FileOff:        payload.FileOff,
			Len:            int64(len(data)),
			BlockKey:       key,
			BlockOff:       0,
			ChecksumSHA256: sum,
		},
		Data: data,
		Path: fs.remotePath(w.path),
	}}
	fs.promoteExtentStaging(key, data, 0, int64(len(data)))
	if fs.extentCache != nil {
		fs.extentCache.putBlock(key, 0, int64(len(data)), data)
	}
	s.landed = landed
}

func (w *extentFileWriter) ensureRemote(ctx context.Context) error {
	if w == nil || !w.bound() {
		return nil
	}
	w.mu.Lock()
	id, unlinked := w.fileID, w.unlinked
	w.mu.Unlock()
	if id != "" {
		return nil
	}
	// JuiceFS Unlink is Meta.Unlink (name/nlink). Slice flush stays on the
	// inode; it must not Meta.Create the directory edge again. Holding
	// remoteCommitLock across HTTP CreateFileWithLayout deadlocked
	// speedtest1-truncate (Write flushwaiting vs SetAttr).
	if unlinked {
		return nil
	}
	fs := w.fs
	remote := fs.remotePath(w.path)
	if w.of != nil {
		w.of.mu.Lock()
		rev := w.of.rev
		w.of.mu.Unlock()
		if rev > 0 {
			stat, statErr := fs.client.StatCtx(ctx, remote)
			if statErr == nil && stat != nil && stat.ResourceID != "" {
				w.fileID = stat.ResourceID
			}
			return nil
		}
	}
	rev, err := fs.client.CreateFileWithLayout(ctx, remote, string(client.ContentLayoutExtent))
	if err != nil {
		var se *client.StatusError
		if errors.As(err, &se) && se.StatusCode == http.StatusConflict {
			rev = 0
		} else {
			return err
		}
	}
	stat, statErr := fs.client.StatCtx(ctx, remote)
	if statErr == nil && stat != nil {
		rev = stat.Revision
		if stat.ResourceID != "" {
			w.fileID = stat.ResourceID
		}
		if w.of != nil {
			w.of.mu.Lock()
			if stat.Revision > w.of.rev {
				w.of.rev = stat.Revision
				w.of.gen = stat.SliceGeneration
			}
			if stat.Size > w.of.size {
				w.of.size = stat.Size
			}
			w.of.mu.Unlock()
		}
	} else if rev > 0 && w.of != nil {
		w.of.mu.Lock()
		if rev > w.of.rev {
			w.of.rev = rev
		}
		w.of.mu.Unlock()
	}
	return nil
}

func (w *extentFileWriter) commitLoop() {
	for {
		w.mu.Lock()
		for len(w.pending) > 0 && !w.pending[0].done {
			w.commitcond.Wait()
		}
		if len(w.pending) == 0 {
			w.committing = false
			w.flushcond.Broadcast()
			w.mu.Unlock()
			return
		}
		s := w.pending[0]
		putErr := s.err
		w.mu.Unlock()

		var commitErr error
		if putErr != nil {
			commitErr = putErr
		} else if len(s.buf) > 0 {
			commitErr = w.commitSlice(s)
		}

		w.mu.Lock()
		if commitErr != nil {
			// JuiceFS commitThread sets f.err and does not clear it on a
			// later success. Clearing it let Fsync return OK after a lost
			// slice, and sibling Reads then saw meta holes (zeros).
			if w.err == gofuse.OK {
				w.err = gofuse.EIO
			}
			safeLogPrintf("extent commit slice path=%s off=%d: %v", w.path, s.fileOff, commitErr)
			s.committed = false
		} else {
			s.committed = true
			w.dropCommittedFrozenLocked(s)
		}
		if len(w.pending) > 0 && w.pending[0] == s {
			w.pending = w.pending[1:]
		} else {
			w.pending = subtractSliceWriters(w.pending, []*extentSliceWriter{s})
		}
		w.flushcond.Broadcast()
		w.commitcond.Broadcast()
		if w.flushwaiting == 0 && w.writewaiting > 0 {
			w.writecond.Broadcast()
		}
		w.mu.Unlock()
	}
}

func (w *extentFileWriter) punchFrozenRangeLocked(start, end int64) {
	if w == nil || end <= start {
		return
	}
	for indx, cw := range w.chunks {
		kept := cw.frozen[:0]
		for _, s := range cw.frozen {
			left, right := splitPunch(s, start, end)
			if left != nil {
				left.chunk = cw
				kept = append(kept, left)
			}
			if right != nil {
				right.chunk = cw
				kept = append(kept, right)
			}
		}
		cw.frozen = kept
		if cw.current == nil && len(cw.frozen) == 0 {
			delete(w.chunks, indx)
		}
	}
	w.syncPendingWithFrozenLocked()
}

func (w *extentFileWriter) dropCommittedFrozenLocked(s *extentSliceWriter) {
	if s == nil || s.chunk == nil {
		return
	}
	cw := s.chunk
	kept := cw.frozen[:0]
	for _, f := range cw.frozen {
		if f != s {
			kept = append(kept, f)
		}
	}
	cw.frozen = kept
	if cw.current == nil && len(cw.frozen) == 0 {
		delete(w.chunks, cw.indx)
	}
}

func (w *extentFileWriter) commitSlice(slices ...*extentSliceWriter) error {
	if w == nil || !w.bound() || len(slices) == 0 {
		return nil
	}
	fs := w.fs
	if fs != nil && fs.openHandles != nil {
		unlinked := true
		found := false
		for _, h := range fs.openHandles.SnapshotPath(w.path) {
			if h == nil {
				continue
			}
			found = true
			if !h.Unlinked {
				unlinked = false
				break
			}
		}
		if found && unlinked {
			return nil
		}
	}
	var landed []client.LandedBlock
	for _, s := range slices {
		if s == nil || len(s.buf) == 0 {
			continue
		}
		landed = append(landed, s.landed...)
	}
	if len(landed) == 0 {
		return nil
	}
	ctx := context.Background()
	localPath := w.path
	w.commitMu.Lock()
	defer w.commitMu.Unlock()
	result, err := fs.client.FlushExtent(ctx, client.FlushExtentRequest{
		Path:               fs.remotePath(localPath),
		ExpectedRevision:   0,
		ExpectedGeneration: 0,
		Append:             true,
		Writeback:          true,
		Landed:             landed,
		AfterLanded: func(opID string, ops []client.SliceOp, expectedRev, expectedGen int64, attemptTruncate *int64) error {
			if fs.journal == nil {
				return nil
			}
			_ = fs.journal.Append(JournalEntry{
				Op:               JournalFsync,
				Path:             localPath,
				BaseRev:          expectedRev,
				Extent:           true,
				ExtentOpID:       opID,
				ExtentOps:        ops,
				ExtentTruncateTo: attemptTruncate,
				ExtentGeneration: expectedGen,
			})
			_ = fs.journal.Fsync()
			return nil
		},
		AfterPut: func(blocks []client.LandedBlock) {
			for _, blk := range blocks {
				fs.promoteExtentStaging(blk.Op.BlockKey, blk.Data, blk.Op.BlockOff, blk.Op.Len)
			}
		},
	})
	if err != nil {
		return err
	}
	w.recordCommit(slices[len(slices)-1], result)
	return nil
}

func (w *extentFileWriter) recordCommit(s *extentSliceWriter, result *client.CommitSlicesResult) {
	if w == nil || result == nil || w.fs == nil {
		return
	}
	fs := w.fs
	if fs.journal != nil {
		_ = fs.journal.Append(JournalEntry{Op: JournalCommit, Path: w.path, BaseRev: result.Revision})
	}
	if w.of != nil {
		w.of.noteCommit(result.Revision, result.Generation, result.SizeBytes)
	}
	if fs.extentCache != nil {
		// JuiceFS meta.Write: InvalidateChunk. Merging locally served a
		// stale header (walthread2 SQLITE_NOTADB).
		fs.extentCache.invalidatePath(fs.remotePath(w.path))
	}
	fs.refreshExtentHandlesAfterCommit(w.path, nil, result.Revision, result.Generation, result.SizeBytes)
	fs.scheduleExtentWriteCompact(fs.remotePath(w.path), result.ChunkRows)
	if fs.openHandles != nil && s != nil {
		end := s.fileOff + int64(len(s.buf))
		for _, h := range fs.openHandles.SnapshotPath(w.path) {
			if h == nil || !h.TryLock() {
				continue
			}
			if h.extentDirty != nil {
				h.extentDirty.punch(s.fileOff, end)
			}
			h.Unlock()
		}
	}
	if fs.inodes != nil && w.ino != 0 {
		visible := result.SizeBytes
		if w.of != nil {
			if n := w.of.logicalSize(); n > visible {
				visible = n
			}
		}
		fs.inodes.UpdateSize(w.ino, visible)
	}
	fs.cacheFileForPath(w.path, result.SizeBytes, time.Now(), result.Revision)
}

func (w *extentFileWriter) beginFlush() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.flushwaiting++
	w.mu.Unlock()
}

func (w *extentFileWriter) endFlush() {
	if w == nil {
		return
	}
	w.mu.Lock()
	if w.flushwaiting > 0 {
		w.flushwaiting--
	}
	if w.flushwaiting == 0 && w.writewaiting > 0 {
		w.writecond.Broadcast()
	}
	w.mu.Unlock()
}

// flush is JuiceFS fileWriter.flush: freeze remaining slices, wait until
// pending uploads+commits drain (meta.Background PUT, not the FUSE thread).
func (w *extentFileWriter) flush(ctx context.Context) gofuse.Status {
	if w == nil {
		return gofuse.OK
	}
	if !w.bound() {
		return gofuse.OK
	}
	start := time.Now()
	w.mu.Lock()
	defer w.mu.Unlock()
	w.flushwaiting++
	deadline := time.Now().Add(extentFlushWait)
	if ctx != nil {
		if d, ok := ctx.Deadline(); ok && d.After(deadline) {
			deadline = d
		}
	}
	// JuiceFS: timeout/EINTR is per-call, not f.err (commitThread failures).
	var err gofuse.Status
	for (len(w.pending) > 0 || w.hasCurrentLocked()) && err == gofuse.OK {
		for _, cw := range w.chunks {
			cw.freezeCurrent()
		}
		if len(w.pending) == 0 && !w.hasCurrentLocked() {
			break
		}
		if time.Now().After(deadline) {
			safeLogPrintf("extent flush timeout path=%s after %s", w.path, time.Since(start))
			err = gofuse.EIO
			break
		}
		waitExtentCondTimeout(w.flushcond, time.Second*3)
	}
	w.flushwaiting--
	if w.flushwaiting == 0 && w.writewaiting > 0 {
		w.writecond.Broadcast()
	}
	if err != gofuse.OK {
		return err
	}
	if w.err != gofuse.OK {
		return w.err
	}
	return gofuse.OK
}

func (w *extentFileWriter) hasCurrentLocked() bool {
	if w == nil {
		return false
	}
	for _, cw := range w.chunks {
		if cw.current != nil && len(cw.current.buf) > 0 {
			return true
		}
	}
	return false
}

func (w *extentFileWriter) waitFrozenCommitted(ctx context.Context) gofuse.Status {
	if w == nil || !w.bound() {
		return gofuse.OK
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	deadline := time.Now().Add(extentFlushWait)
	if ctx != nil {
		if d, ok := ctx.Deadline(); ok && d.After(deadline) {
			deadline = d
		}
	}
	for len(w.pending) > 0 && w.err == gofuse.OK {
		if time.Now().After(deadline) {
			return gofuse.EIO
		}
		waitExtentCondTimeout(w.flushcond, 100*time.Millisecond)
	}
	if w.err != gofuse.OK {
		return w.err
	}
	return gofuse.OK
}

func waitExtentCondTimeout(c *sync.Cond, d time.Duration) {
	if c == nil {
		time.Sleep(d)
		return
	}
	timer := time.AfterFunc(d, func() {
		c.Broadcast()
	})
	c.Wait()
	timer.Stop()
}

func (fs *Dat9FS) extentFlushContext(fh *FileHandle) (context.Context, context.CancelFunc) {
	n := int64(client.ExtentMaxBlockSize)
	if fh != nil && fh.extentWriter != nil {
		if d := fh.extentWriter.dirtyBytes(); d > n {
			n = d
		}
	}
	return context.WithTimeout(context.Background(), releaseTimeout(n))
}

func (fs *Dat9FS) waitExtentWriteBudget(ctx context.Context, fh *FileHandle, capn int64) gofuse.Status {
	if fs == nil || fh == nil || capn <= 0 {
		return gofuse.OK
	}
	used := fs.extentTotalDirtyBytes()
	if used <= capn {
		return gofuse.OK
	}
	if fh.extentWriter != nil {
		fh.extentWriter.freezeAll()
		if fh.extentWriter.bound() {
			if fs.extentTotalDirtyBytes() <= capn*2 {
				return gofuse.OK
			}
		} else {
			flushCtx, flushCF := fs.extentFlushContext(fh)
			st := fs.flushExtentFrozenLocked(flushCtx, fh)
			flushCF()
			if st != gofuse.OK {
				return st
			}
			if fs.extentTotalDirtyBytes() <= capn*2 {
				return gofuse.OK
			}
		}
	}
	parked := fh.RemoteCommitUnlock
	fh.RemoteCommitUnlock = nil
	fh.Unlock()
	if parked != nil {
		parked()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && fs.extentTotalDirtyBytes() > capn*2 {
		timer := time.NewTimer(100 * time.Millisecond)
		<-timer.C
	}
	fh.Lock()
	if parked != nil {
		fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
	}
	return gofuse.OK
}

func (fs *Dat9FS) waitExtentInflightLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
	if fs == nil || fh == nil || fh.extentWriter == nil || !fh.extentWriter.hasInflight() {
		return gofuse.OK
	}
	parked := fh.RemoteCommitUnlock
	fh.RemoteCommitUnlock = nil
	fh.Unlock()
	if parked != nil {
		parked()
	}
	deadline := time.Now().Add(releaseTimeout(0))
	if ctx != nil {
		if d, ok := ctx.Deadline(); ok && d.After(deadline) {
			deadline = d
		}
	}
	for time.Now().Before(deadline) && fh.extentWriter.hasInflight() {
		timer := time.NewTimer(20 * time.Millisecond)
		<-timer.C
	}
	fh.Lock()
	if parked != nil {
		fh.RemoteCommitUnlock = fs.lockWritableRemoteCommitPath(fh.Path)
	}
	if fh.extentWriter != nil && fh.extentWriter.hasInflight() {
		return gofuse.Status(syscall.EIO)
	}
	return gofuse.OK
}

func (fs *Dat9FS) startExtentBackground() {
	if fs == nil {
		return
	}
	fs.extentBGOnce.Do(func() {
		fs.extentBGStop = make(chan struct{})
		fs.extentBGDone = make(chan struct{})
		go fs.extentFlushLoop()
	})
}

func (fs *Dat9FS) stopExtentBackground() {
	if fs == nil || fs.extentBGStop == nil {
		return
	}
	select {
	case <-fs.extentBGStop:
	default:
		close(fs.extentBGStop)
	}
	if fs.extentBGDone != nil {
		<-fs.extentBGDone
	}
}

func (fs *Dat9FS) extentFlushLoop() {
	defer close(fs.extentBGDone)
	ticker := time.NewTicker(extentFlushScan)
	defer ticker.Stop()
	for {
		select {
		case <-fs.extentBGStop:
			return
		case <-ticker.C:
			fs.scanExtentWriters()
		}
	}
}

func (fs *Dat9FS) scanExtentWriters() {
	if fs.extentCache != nil {
		fs.extentCache.reclaimIdle(time.Now())
	}
	type item struct {
		id uint64
		fh *FileHandle
	}
	var handles []item
	fs.fileHandles.ForEach(func(id uint64, fh *FileHandle) {
		handles = append(handles, item{id, fh})
	})
	now := time.Now()
	seen := map[*extentFileWriter]struct{}{}
	for _, it := range handles {
		fh := it.fh
		if !fh.TryLock() {
			continue
		}
		if !fh.isExtent() || fh.Dirty == nil {
			fh.Unlock()
			continue
		}
		if fh.extentWriter != nil {
			if _, dup := seen[fh.extentWriter]; dup {
				fh.Unlock()
				continue
			}
			seen[fh.extentWriter] = struct{}{}
			tooMany := fh.extentWriter.sliceCount() > extentFlushMaxRuns
			// JuiceFS dataWriter.flushAll: freeze idle/aged slices and
			// go flushData(); commitThread uploads meta in the background.
			fh.extentWriter.freezeDue(now, tooMany)
		}
		fh.Unlock()
	}
}

func (fs *Dat9FS) flushExtentFrozenLocked(ctx context.Context, fh *FileHandle) gofuse.Status {
	if fh != nil && fh.extentWriter != nil && fh.extentWriter.bound() {
		st := fh.extentWriter.waitFrozenCommitted(ctx)
		fh.adoptExtentOpenRev()
		if st != gofuse.OK {
			return st
		}
		logical := extentHandleLogicalSize(fh)
		var truncateTo *int64
		if fh.extentNeedTruncate && logical != fh.OrigSize && (fh.extentWriter == nil || !fh.extentWriter.hasCurrent()) {
			truncateTo = &logical
		}
		needHoles := fh.extentNeedTruncate && extentOpenZeroFrom(fh) >= 0 && logical > extentOpenZeroFrom(fh)
		if truncateTo == nil && !needHoles {
			return gofuse.OK
		}
		return fs.commitExtentPayloadsLocked(ctx, fh, nil, truncateTo, true)
	}
	var held []*extentSliceWriter
	if fh.extentWriter != nil {
		held = fh.extentWriter.detachFrozen()
	}
	payloads := payloadsFromSlices(held)
	var truncateTo *int64
	logical := extentHandleLogicalSize(fh)
	if fh.extentNeedTruncate && logical != fh.OrigSize && (fh.extentWriter == nil || !fh.extentWriter.hasCurrent()) {
		truncateTo = &logical
	}
	needHoles := fh.extentNeedTruncate && extentOpenZeroFrom(fh) >= 0 && logical > extentOpenZeroFrom(fh)
	if len(payloads) == 0 && truncateTo == nil && !needHoles {
		return gofuse.OK
	}
	isExtent, st := fs.ensureExtentRemoteFileLocked(ctx, fh)
	if st != gofuse.OK {
		if fh.extentWriter != nil {
			fh.extentWriter.restoreFrozen(held)
		}
		return st
	}
	if !isExtent {
		if fh.extentWriter != nil {
			fh.extentWriter.restoreFrozen(held)
		}
		return gofuse.OK
	}
	st = fs.commitExtentPayloadsLocked(ctx, fh, payloads, truncateTo, true)
	if fh.extentWriter != nil {
		if st != gofuse.OK {
			fh.extentWriter.restoreFrozen(held)
		} else {
			fh.extentWriter.dropInflight(held)
		}
	}
	return st
}
