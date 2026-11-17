package extent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"

	"github.com/juicedata/juicefs/pkg/object"

	"github.com/mem9-ai/drive9/pkg/client"
)

// objectStorage adapts the chunk store onto the v2 presign data plane.
//
// Writes: the engine uploads a slice object BEFORE committing its metadata
// (vfs flushes the store first, then calls meta.Write), but v2 presigns
// blocks only inside an upload session. Put therefore stages the bytes in
// memory; the real presign+PUT happens in Bridge.commit once the session
// exists (meta.Write/Close).
//
// Reads: deterministic block ids resolve through the read-plan URL
// registry; synthesized zero blocks never leave memory.
type objectStorage struct {
	bridge *Bridge
}

const maxStageBytes = 256 << 20

// maxPinnedCommitBytes bounds the staged-byte volume pending commits (and
// non-Writeback in-memory slice staging) may hold at once. Keeping this at
// 384 MiB keeps live heap + JuiceFS BufferSize well below a 3 GiB container
// memory ceiling while still pipelining multiple 64 MiB batches concurrently.
// Overridable via DRIVE9_EXTENT_PINNED_BUDGET_BYTES.
var maxPinnedCommitBytes int64 = 384 << 20

// commitBatchBytes is the pending-byte volume Bridge.Write lets accumulate
// for a file before it drives a commit. The engine hands over one 4 MiB
// slice per call; committing each one serialized a full publish round trip
// (stat → PATCH plan → PUT → complete) per 4 MiB, which paced large-file
// drains to a single block's latency. Accumulating to this threshold
// publishes the same slices in fewer, multi-block PATCH commits; the
// pinned budget above still backpressures a stalled commit pipeline, and
// flush/close publish whatever remains regardless (close-to-open is
// unchanged — mid-write publication was always best-effort). A remembered
// commit error skips the batching so every write keeps retrying recovery.
// Zero restores per-slice commits. Overridable for bench work.
var commitBatchBytes int64 = 64 << 20

func init() {
	if v := os.Getenv("DRIVE9_EXTENT_PINNED_BUDGET_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			maxPinnedCommitBytes = n
		}
	}
	if v := os.Getenv("DRIVE9_EXTENT_COMMIT_BATCH_BYTES"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			commitBatchBytes = n
		}
	}
}

func (s objectStorage) String() string { return "drive9-presign" }

func (s objectStorage) Limits() object.Limits { return object.Limits{} }

func (s objectStorage) Create(ctx context.Context) error { return nil }

// parseKey extracts the slice id and object size from a JuiceFS-derived key
// (chunks/XX/a/b/<id>_<indx>_<size>).
func parseKey(key string) (id uint64, indx int64, size int64, err error) {
	parts := strings.Split(key, "/")
	last := parts[len(parts)-1]
	fields := strings.Split(last, "_")
	if len(fields) != 3 {
		return 0, 0, 0, fmt.Errorf("unrecognized chunk key %q", key)
	}
	id, err = strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("chunk key id %q: %w", fields[0], err)
	}
	indx, err = strconv.ParseInt(fields[1], 10, 63)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("chunk key indx %q: %w", fields[1], err)
	}
	size, err = strconv.ParseInt(fields[2], 10, 63)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("chunk key size %q: %w", fields[2], err)
	}
	return id, indx, size, nil
}

func (s objectStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	id, indx, size, err := parseKey(key)
	if err != nil {
		return err
	}
	if size <= 0 || size > maxStageBytes {
		return fmt.Errorf("staged slice size %d out of bounds", size)
	}
	if s.bridge == nil {
		buf := make([]byte, size)
		_, err := io.ReadFull(in, buf)
		return err
	}
	if s.bridge.cfg.NativeWriteback && s.bridge.cfg.CacheDir == "" {
		return fmt.Errorf("extent native writeback requires CacheDir")
	}
	if err := s.bridge.blocks.admitSliceBlock(ctx, id, indx, size, maxPinnedCommitBytes, s.bridge.cfg.NativeWriteback, s.bridge.kickPendingFlush); err != nil {
		return err
	}
	preferDisk := s.bridge.cfg.NativeWriteback || s.bridge.blocks.totalStagingBudget() > maxPinnedCommitBytes*3/4
	if preferDisk && s.bridge.blocks.tryReserveDiskSpool(size) {
		if err := s.putSliceSpool(id, indx, size, in); err != nil {
			s.bridge.blocks.rollbackSliceBlock(id, size)
			return err
		}
		return nil
	}
	if err := s.putSliceMem(id, indx, size, in); err != nil {
		s.bridge.blocks.rollbackSliceBlock(id, size)
		return err
	}
	return nil
}

func (s objectStorage) putSliceMem(id uint64, indx, size int64, in io.Reader) error {
	buf := make([]byte, size)
	if _, err := io.ReadFull(in, buf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return fmt.Errorf("short staging read: %w", err)
		}
		return err
	}
	var prevLen int64
	var prevFile *os.File
	var prevSpool int64
	s.bridge.mu.Lock()
	sl := s.bridge.stagedSlices[id]
	if sl == nil {
		sl = &stagedSlice{blocks: make(map[int64][]byte)}
		s.bridge.stagedSlices[id] = sl
	}
	if prev, exists := sl.blocks[indx]; exists && prev != nil {
		prevLen = int64(len(prev))
		sl.total -= prevLen
	}
	if sl.files != nil {
		if pf, exists := sl.files[indx]; exists && pf.f != nil {
			prevFile = pf.f
			prevSpool = pf.size
			delete(sl.files, indx)
			sl.total -= prevSpool
		}
	}
	sl.blocks[indx] = buf
	sl.total += size
	s.bridge.mu.Unlock()
	if prevLen+prevSpool > 0 {
		s.bridge.blocks.refundOverwriteSliceUsed(id, prevLen+prevSpool)
	}
	if prevFile != nil {
		_ = prevFile.Close()
		s.bridge.blocks.releaseDiskSpool(prevSpool)
	}
	return nil
}

func (s objectStorage) putSliceSpool(id uint64, indx, size int64, in io.Reader) error {
	dir := s.bridge.cfg.CacheDir
	if dir == "" {
		s.bridge.blocks.releaseDiskSpool(size)
		return fmt.Errorf("missing extent cache directory for disk spool")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.bridge.blocks.releaseDiskSpool(size)
		return err
	}
	f, err := os.CreateTemp(dir, "extent-spill-*")
	if err != nil {
		s.bridge.blocks.releaseDiskSpool(size)
		return err
	}
	_ = os.Remove(f.Name())
	n, copyErr := io.Copy(f, io.LimitReader(in, size))
	if copyErr != nil || n != size {
		_ = f.Close()
		s.bridge.blocks.releaseDiskSpool(size)
		if copyErr != nil {
			return copyErr
		}
		return fmt.Errorf("short staging read: %d of %d", n, size)
	}
	var prevLen int64
	var prevFile *os.File
	var prevSpool int64
	s.bridge.mu.Lock()
	sl := s.bridge.stagedSlices[id]
	if sl == nil {
		sl = &stagedSlice{blocks: make(map[int64][]byte)}
		s.bridge.stagedSlices[id] = sl
	}
	if prev, exists := sl.blocks[indx]; exists && prev != nil {
		prevLen = int64(len(prev))
		delete(sl.blocks, indx)
		sl.total -= prevLen
	}
	if sl.files == nil {
		sl.files = make(map[int64]spooledBlock)
	}
	if pf, exists := sl.files[indx]; exists && pf.f != nil {
		prevFile = pf.f
		prevSpool = pf.size
		sl.total -= prevSpool
	}
	sl.files[indx] = spooledBlock{f: f, size: size}
	sl.total += size
	s.bridge.mu.Unlock()
	if prevLen+prevSpool > 0 {
		s.bridge.blocks.refundOverwriteSliceUsed(id, prevLen+prevSpool)
	}
	if prevFile != nil {
		_ = prevFile.Close()
		s.bridge.blocks.releaseDiskSpool(prevSpool)
	}
	return nil
}

type spooledBlock struct {
	f    *os.File
	size int64
}

// stagedSlice accumulates the blocks of one not-yet-committed slice object.
type stagedSlice struct {
	blocks map[int64][]byte
	files  map[int64]spooledBlock
	total  int64
}

// closeAndSpoolBytes closes any open spooled block descriptors and returns the
// total spooled byte count to release back to blockRegistry.
func (sl *stagedSlice) closeAndSpoolBytes() int64 {
	var spooled int64
	for k, sb := range sl.files {
		if sb.f != nil {
			_ = sb.f.Close()
		}
		spooled += sb.size
		delete(sl.files, k)
	}
	return spooled
}

// join returns the concatenated slice object bytes.
func (sl *stagedSlice) join() []byte {
	if len(sl.files) == 0 && len(sl.blocks) == 1 {
		if b, ok := sl.blocks[0]; ok && int64(len(b)) == sl.total {
			return b
		}
	}
	out := make([]byte, 0, sl.total)
	for i := int64(0); int64(len(out)) < sl.total; i++ {
		if b, ok := sl.blocks[i]; ok && b != nil {
			out = append(out, b...)
			continue
		}
		if sb, ok := sl.files[i]; ok && sb.f != nil {
			buf := make([]byte, sb.size)
			if _, err := sb.f.ReadAt(buf, 0); err != nil {
				return nil
			}
			out = append(out, buf...)
			continue
		}
		break
	}
	return out
}

func (s objectStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	id, indx, size, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	// The chunk store addresses offsets WITHIN one 4 MiB block; the key's
	// block index selects which slice of a multi-block backing object is
	// being read (engine slices and server-compacted blocks span up to
	// 64 MiB). Into the joined object the byte position is therefore
	// indx*blockSize + off — using off alone made every block read the
	// object's first 4 MiB.
	objOff := int64(indx)*client.ExtentBlockSize + off
	if limit < 0 {
		limit = size - off
	}
	if limit <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	s.bridge.mu.Lock()
	sl := s.bridge.stagedSlices[id]
	var staged []byte
	if sl != nil {
		staged = sl.join()
	}
	s.bridge.mu.Unlock()
	if sl != nil {
		if objOff > int64(len(staged)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		end := objOff + limit
		if end > int64(len(staged)) {
			end = int64(len(staged))
		}
		return io.NopCloser(bytes.NewReader(staged[objOff:end])), nil
	}
	return s.bridge.blocks.fetch(ctx, s.bridge.cfg.Client, id, objOff, limit)
}

// simpleObject implements object.Object for Head replies.
type simpleObject struct {
	key   string
	size  int64
	mtime time.Time
}

func (o simpleObject) Key() string          { return o.key }
func (o simpleObject) Size() int64          { return o.size }
func (o simpleObject) Mtime() time.Time     { return o.mtime }
func (o simpleObject) IsDir() bool          { return false }
func (o simpleObject) IsSymlink() bool      { return false }
func (o simpleObject) StorageClass() string { return "" }
func (o simpleObject) Status() string       { return "" }

func (s objectStorage) Head(ctx context.Context, key string) (object.Object, error) {
	_, _, size, err := parseKey(key)
	if err != nil {
		return nil, err
	}
	return simpleObject{key: key, size: size, mtime: time.Now()}, nil
}

func (s objectStorage) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	// Committed block deletion is server-owned (tenant-worker GC), but an
	// aborted sliceWriter calls Delete on uncommitted staged blocks — release
	// any local staging buffers/spool and reservation immediately.
	if s.bridge == nil {
		return nil
	}
	id, indx, _, err := parseKey(key)
	if err != nil {
		return nil
	}
	var removedLen int64
	var removedFile *os.File
	var removedSpool int64
	var empty bool
	s.bridge.mu.Lock()
	if sl := s.bridge.stagedSlices[id]; sl != nil {
		if prev, ok := sl.blocks[indx]; ok && prev != nil {
			removedLen = int64(len(prev))
			delete(sl.blocks, indx)
			sl.total -= removedLen
		}
		if sl.files != nil {
			if pf, ok := sl.files[indx]; ok && pf.f != nil {
				removedFile = pf.f
				removedSpool = pf.size
				delete(sl.files, indx)
				sl.total -= removedSpool
			}
		}
		if len(sl.blocks) == 0 && len(sl.files) == 0 {
			delete(s.bridge.stagedSlices, id)
			empty = true
		}
	} else {
		empty = true
	}
	s.bridge.mu.Unlock()
	if removedFile != nil {
		_ = removedFile.Close()
		s.bridge.blocks.releaseDiskSpool(removedSpool)
	}
	if empty {
		s.bridge.blocks.retireSliceMem(id)
	} else if removedLen+removedSpool > 0 {
		s.bridge.blocks.refundOverwriteSliceUsed(id, removedLen+removedSpool)
	}
	return nil
}

func (s objectStorage) Copy(ctx context.Context, dst, src string) error {
	return fmt.Errorf("copy is not supported by the drive9 presign plane")
}

func (s objectStorage) List(ctx context.Context, prefix, startAfter, token, delimiter string, limit int64, followLink bool) ([]object.Object, bool, string, error) {
	return nil, false, "", fmt.Errorf("list is not supported by the drive9 presign plane")
}

func (s objectStorage) ListAll(ctx context.Context, prefix, marker string, followLink bool) (<-chan object.Object, error) {
	return nil, fmt.Errorf("list is not supported by the drive9 presign plane")
}

func (s objectStorage) CreateMultipartUpload(ctx context.Context, key string) (*object.MultipartUpload, error) {
	return nil, fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) UploadPart(ctx context.Context, key string, uploadID string, num int, body []byte) (*object.Part, error) {
	return nil, fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) UploadPartCopy(ctx context.Context, key string, uploadID string, num int, srcKey string, off, size int64) (*object.Part, error) {
	return nil, fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) AbortUpload(ctx context.Context, key string, uploadID string) {}

func (s objectStorage) CompleteUpload(ctx context.Context, key string, uploadID string, parts []*object.Part) error {
	return fmt.Errorf("multipart is not supported by the drive9 presign plane")
}

func (s objectStorage) ListUploads(ctx context.Context, marker string) ([]*object.PendingPart, string, error) {
	return nil, "", nil
}

func (s objectStorage) Restore(ctx context.Context, key string, days int32) error {
	return fmt.Errorf("restore is not supported by the drive9 presign plane")
}

// ---- registry implementation ----

const urlTTL = 8 * time.Minute

func (r *blockRegistry) register(sha, url string, length int64, path string, fileOffset int64, ino meta.Ino) uint64 {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return 0
	}
	id := detID(sum)
	r.mu.Lock()
	// Refresh URL/provenance but PRESERVE cached bytes: a pending commit may
	// still need them (a sibling staged the same checksum and its upload
	// reader has not consumed it); clearing them here made a fresh sibling
	// upload zeros for real content. Bytes retire only via
	// releaseCommitted's reference-aware path.
	if e, ok := r.m[id]; ok {
		if url != "" {
			e.url = url
			e.expires = time.Now().Add(urlTTL)
		}
		e.path = path
		e.provenanceStart = fileOffset
		e.provenanceIno = ino
		if e.size < length {
			e.size = length
		}
	} else {
		var exp time.Time
		if url != "" {
			exp = time.Now().Add(urlTTL)
		}
		r.m[id] = &blockEntry{sha256: sha, size: length, url: url, expires: exp, path: path, provenanceStart: fileOffset, provenanceIno: ino}
	}
	r.mu.Unlock()
	return id
}

// stageBytes caches block content for pending commits. Idempotent: an
// already-staged block keeps its first copy (write retries used to
// re-copy the whole slice on every attempt).
// registerLocal publishes a locally staged block (deferred write) under
// its deterministic id so the engine's reader can fetch it from staging.
func (r *blockRegistry) registerLocal(sha string, length int64) uint64 {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return 0
	}
	return detID(sum)
}

// stagePinned stores staged bytes with an optional pending-commit pin;
// pinned entries are never retired by the classic-read LRU.
func (r *blockRegistry) stagePinned(sha string, data []byte, pinned bool) uint64 {
	id := r.stageBytes(sha, data)
	if pinned {
		r.pin(sha, int64(len(data)))
	}
	return id
}

// stagePinnedAndRetireSlice atomically pins data for pending commit and retires
// sliceID's staging reservation under one lock so totalStagingBudget never
// spikes above limit during the stagedSlice -> pending transition.
func (r *blockRegistry) stagePinnedAndRetireSlice(sha string, data []byte, sliceID uint64) uint64 {
	id := r.stageBytes(sha, data)
	size := int64(len(data))
	r.mu.Lock()
	if r.pins[sha] == 0 {
		r.pinSizes[sha] = size
		r.pinnedBytes += size
	}
	r.pins[sha]++
	r.retireSliceMemLocked(sliceID)
	r.mu.Unlock()
	return id
}

func (r *blockRegistry) hasSliceAdmission(sliceID uint64) bool {
	r.mu.Lock()
	_, ok := r.sliceTarget[sliceID]
	r.mu.Unlock()
	return ok
}

// pin marks a checksum as referenced by a pending commit and accounts its
// bytes against the pinned budget on the first reference.
func (r *blockRegistry) pin(sha string, size int64) {
	r.mu.Lock()
	if r.pins[sha] == 0 {
		r.pinSizes[sha] = size
		r.pinnedBytes += size
	}
	r.pins[sha]++
	r.mu.Unlock()
}

// unpin releases a pending-commit reference; the last release retires the
// checksum's budget share and wakes any budget waiter.
func (r *blockRegistry) unpin(sha string) {
	r.mu.Lock()
	if r.pins[sha] > 0 {
		r.pins[sha]--
		if r.pins[sha] == 0 {
			r.pinnedBytes -= r.pinSizes[sha]
			delete(r.pinSizes, sha)
			close(r.budgetCh)
			r.budgetCh = make(chan struct{})
		}
	}
	r.mu.Unlock()
}

// pinnedBudget reports the staged-byte volume currently pinned by pending
// commits.
func (r *blockRegistry) pinnedBudget() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pinnedBytes
}

// totalStagingBudget reports combined pinned-commit and in-memory staged-slice
// bytes currently retained in RAM.
func (r *blockRegistry) totalStagingBudget() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pinnedBytes + r.stagedMemBytes
}

// addStagedMem adjusts the byte volume retained in Bridge.stagedSlices and
// wakes budget waiters when bytes are released.
func (r *blockRegistry) addStagedMem(delta int64) {
	if delta == 0 {
		return
	}
	r.mu.Lock()
	r.stagedMemBytes += delta
	if r.stagedMemBytes < 0 {
		r.stagedMemBytes = 0
	}
	if delta < 0 {
		close(r.budgetCh)
		r.budgetCh = make(chan struct{})
	}
	r.mu.Unlock()
}

// waitBudgetWithHook blocks while full() reports true under r.mu, invoking
// onWait whenever it parks.
func (r *blockRegistry) waitBudgetWithHook(ctx context.Context, full func() bool, onWait func()) error {
	r.mu.Lock()
	for full() {
		ch := r.budgetCh
		r.mu.Unlock()
		if onWait != nil {
			onWait()
		}
		t := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ch:
			t.Stop()
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
		r.mu.Lock()
	}
	r.mu.Unlock()
	return nil
}

// waitBudget blocks while full() reports true under r.mu.
func (r *blockRegistry) waitBudget(ctx context.Context, full func() bool) error {
	return r.waitBudgetWithHook(ctx, full, nil)
}

// waitPinnedBudget blocks while pending commits hold at least limit staged
// bytes, converting an unbounded in-memory backlog into write backpressure:
// the caller (slice ingestion) parks, the kernel queues the WRITE, and the
// writer throttles to commit speed instead of OOM-killing the worker.
func (r *blockRegistry) waitPinnedBudget(ctx context.Context, limit int64) error {
	return r.waitBudget(ctx, func() bool { return r.pinnedBytes >= limit })
}

// waitTotalStagingBudget blocks while combined stagedSlices + pinned commits
// hold at least limit bytes in RAM.
func (r *blockRegistry) waitTotalStagingBudget(ctx context.Context, limit int64) error {
	return r.waitBudget(ctx, func() bool { return r.pinnedBytes+r.stagedMemBytes >= limit })
}

func (r *blockRegistry) hasSmallerWaiterLocked(sliceID uint64) bool {
	for id, cnt := range r.waitingSlices {
		if cnt > 0 && id < sliceID {
			return true
		}
	}
	return false
}

var errNativeStagingYield = errors.New("extent native staging busy: yielding upload worker for active slice")

// waitSliceAdmission admits a staging slice in ascending slice ID order and
// reserves its estimated footprint up front so concurrent upload workers stay
// strictly bounded at or below limit without starving older slices.
func (r *blockRegistry) waitSliceAdmission(ctx context.Context, sliceID uint64, est, limit int64) error {
	return r.waitSliceAdmissionWithHook(ctx, sliceID, est, limit, false, nil)
}

func (r *blockRegistry) waitSliceAdmissionWithHook(ctx context.Context, sliceID uint64, est, limit int64, yieldWhenBusy bool, onWait func()) error {
	r.mu.Lock()
	if _, ok := r.sliceTarget[sliceID]; ok {
		r.mu.Unlock()
		return nil
	}
	if r.waitingSlices == nil {
		r.waitingSlices = make(map[uint64]int)
	}
	r.waitingSlices[sliceID]++
	admitted := false
	yielded := false
	defer func() {
		r.waitingSlices[sliceID]--
		if r.waitingSlices[sliceID] == 0 {
			delete(r.waitingSlices, sliceID)
		}
		if !admitted || limit <= 0 || r.pinnedBytes+r.stagedMemBytes < limit || r.waitingSlices[sliceID] > 0 {
			close(r.budgetCh)
			r.budgetCh = make(chan struct{})
		}
		r.mu.Unlock()
	}()
	for (limit > 0 && r.pinnedBytes+r.stagedMemBytes+est > limit && (r.pinnedBytes > 0 || r.stagedMemBytes > 0)) || r.hasSmallerWaiterLocked(sliceID) {
		if yieldWhenBusy && len(r.sliceTarget) > 0 {
			if yielded {
				return errNativeStagingYield
			}
			yielded = true
		}
		ch := r.budgetCh
		r.mu.Unlock()
		if onWait != nil {
			onWait()
		}
		waitDur := 250 * time.Millisecond
		if yieldWhenBusy {
			waitDur = 25 * time.Millisecond
		}
		t := time.NewTimer(waitDur)
		select {
		case <-ch:
			t.Stop()
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			r.mu.Lock()
			return ctx.Err()
		}
		r.mu.Lock()
		if _, ok := r.sliceTarget[sliceID]; ok {
			admitted = true
			return nil
		}
	}
	if r.sliceTarget == nil {
		r.sliceTarget = make(map[uint64]int64)
	}
	r.sliceTarget[sliceID] = est
	r.stagedMemBytes += est
	admitted = true
	return nil
}

// admitSliceBlock reserves staging capacity for one block of sliceID while
// strictly enforcing limit (never bypassed). Multi-block slices reserve their
// full 64 MiB chunk envelope on admission so subsequent blocks of an admitted
// slice never wait and never push totalStagingBudget past limit.
func (r *blockRegistry) admitSliceBlock(ctx context.Context, sliceID uint64, indx, size, limit int64, yieldWhenBusy bool, onWait func()) error {
	r.mu.Lock()
	if target, ok := r.sliceTarget[sliceID]; ok {
		if r.sliceUsed == nil {
			r.sliceUsed = make(map[uint64]int64)
		}
		if r.sliceUsed[sliceID]+size <= target {
			r.sliceUsed[sliceID] += size
			r.mu.Unlock()
			return nil
		}
		delta := r.sliceUsed[sliceID] + size - target
		for limit > 0 && r.pinnedBytes+r.stagedMemBytes+delta > limit && (r.pinnedBytes > 0 || r.stagedMemBytes > 0) {
			ch := r.budgetCh
			r.mu.Unlock()
			if onWait != nil {
				onWait()
			}
			t := time.NewTimer(250 * time.Millisecond)
			select {
			case <-ch:
				t.Stop()
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
			r.mu.Lock()
			target = r.sliceTarget[sliceID]
			if r.sliceUsed[sliceID]+size <= target {
				r.sliceUsed[sliceID] += size
				r.mu.Unlock()
				return nil
			}
			delta = r.sliceUsed[sliceID] + size - target
		}
		r.sliceTarget[sliceID] += delta
		r.sliceUsed[sliceID] += size
		r.stagedMemBytes += delta
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	est := int64(meta.ChunkSize)
	if indx == 0 && size < blockSizeMiB<<20 {
		est = size
	}
	if limit > 0 && est > limit && size <= limit {
		est = size
	}
	if err := r.waitSliceAdmissionWithHook(ctx, sliceID, est, limit, yieldWhenBusy, onWait); err != nil {
		return err
	}
	r.mu.Lock()
	if r.sliceUsed == nil {
		r.sliceUsed = make(map[uint64]int64)
	}
	if target, ok := r.sliceTarget[sliceID]; ok && r.sliceUsed[sliceID]+size > target {
		delta := r.sliceUsed[sliceID] + size - target
		r.sliceTarget[sliceID] += delta
		r.stagedMemBytes += delta
	}
	r.sliceUsed[sliceID] += size
	r.mu.Unlock()
	return nil
}

func (r *blockRegistry) refundOverwriteSliceUsed(sliceID uint64, size int64) {
	if size <= 0 {
		return
	}
	r.mu.Lock()
	if r.sliceUsed[sliceID] <= size {
		delete(r.sliceUsed, sliceID)
	} else {
		r.sliceUsed[sliceID] -= size
	}
	r.mu.Unlock()
}

func (r *blockRegistry) rollbackSliceBlock(sliceID uint64, size int64) {
	if size <= 0 {
		return
	}
	r.mu.Lock()
	if r.sliceUsed[sliceID] <= size {
		r.retireSliceMemLocked(sliceID)
	} else {
		r.sliceUsed[sliceID] -= size
	}
	r.mu.Unlock()
}

func (r *blockRegistry) checkDiskFreeRatio(size int64) bool {
	if r.freeRatio <= 0 || r.cacheDir == "" {
		return true
	}
	var st syscall.Statfs_t
	if err := syscall.Statfs(r.cacheDir, &st); err != nil {
		return true
	}
	total := int64(st.Blocks) * int64(st.Bsize)
	free := int64(st.Bavail) * int64(st.Bsize)
	if total <= 0 {
		return true
	}
	return float64(free-size)/float64(total) >= r.freeRatio
}

func (r *blockRegistry) tryReserveDiskSpool(size int64) bool {
	if r.cacheDir == "" || r.diskBudget <= 0 || size <= 0 {
		return false
	}
	if !r.checkDiskFreeRatio(size) {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stagedDiskBytes+size > r.diskBudget {
		return false
	}
	r.stagedDiskBytes += size
	return true
}

func (r *blockRegistry) releaseDiskSpool(size int64) {
	if size <= 0 {
		return
	}
	r.mu.Lock()
	r.stagedDiskBytes -= size
	if r.stagedDiskBytes < 0 {
		r.stagedDiskBytes = 0
	}
	close(r.budgetCh)
	r.budgetCh = make(chan struct{})
	r.mu.Unlock()
}

func (r *blockRegistry) retireSliceMem(sliceID uint64) {
	r.mu.Lock()
	r.retireSliceMemLocked(sliceID)
	r.mu.Unlock()
}

func (r *blockRegistry) retireSliceMemLocked(sliceID uint64) {
	contrib := r.sliceUsed[sliceID]
	if t := r.sliceTarget[sliceID]; t > contrib {
		contrib = t
	}
	delete(r.sliceTarget, sliceID)
	delete(r.sliceUsed, sliceID)
	if contrib > 0 {
		r.stagedMemBytes -= contrib
		if r.stagedMemBytes < 0 {
			r.stagedMemBytes = 0
		}
		close(r.budgetCh)
		r.budgetCh = make(chan struct{})
	}
}

func (r *blockRegistry) stageBytes(sha string, data []byte) uint64 {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	id := detID(sum)
	if e, ok := r.m[id]; ok {
		if e.cached == nil {
			e.cached = data
		}
		return id
	}
	r.m[id] = &blockEntry{sha256: sha, size: int64(len(data)), cached: data}
	return id
}

// release drops a block's staged bytes. Called after a successful commit —
// the bytes live in object storage now, and nothing bounds the registry's
// cached copies, so a write-only workload otherwise retained everything
// it ever wrote for the mount's lifetime.
func (r *blockRegistry) release(sha string) {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return
	}
	r.mu.Lock()
	if e, ok := r.m[detID(sum)]; ok {
		e.cached = nil
	}
	r.mu.Unlock()
}

func (r *blockRegistry) stagedBytes(sha string) ([]byte, bool) {
	sum, err := hex.DecodeString(sha)
	if err != nil || len(sum) < 8 {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.m[detID(sum)]; ok && e.cached != nil {
		return e.cached, true
	}
	return nil, false
}

// fetch serves a block byte range, refetching the file's read plan when the
// cached URL is missing or stale. limit < 0 means "to the end of the
// object" (the JuiceFS ObjectStorage contract).
func (r *blockRegistry) fetch(ctx context.Context, c fetchClient, id uint64, off, limit int64) (io.ReadCloser, error) {
	// Snapshot the entry under the lock; parallel fetches and stageBytes
	// mutate the same entry.
	r.mu.Lock()
	e, ok := r.m[id]
	var snap blockEntry
	if ok {
		snap = *e
	}
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("block id %d is unknown to the bridge", id)
	}
	if limit < 0 {
		limit = snap.size - off
	}
	if limit <= 0 {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if snap.url != "" && time.Now().Before(snap.expires) {
		return c.FetchExtentBlockRange(ctx, snap.url, off, limit)
	}
	if snap.cached != nil {
		if off > int64(len(snap.cached)) {
			return io.NopCloser(bytes.NewReader(nil)), nil
		}
		end := off + limit
		if end > int64(len(snap.cached)) || limit <= 0 {
			end = int64(len(snap.cached))
		}
		return io.NopCloser(bytes.NewReader(snap.cached[off:end])), nil
	}
	if snap.path == "" {
		return nil, fmt.Errorf("block %d has neither URL nor provenance", id)
	}
	path := snap.path
	if snap.provenanceIno != 0 && r.currentPath != nil {
		// The file may have been renamed or retained under an alias since
		// this plan was fetched; refresh through the inode's CURRENT
		// mapping, never the stale name.
		if cur, ok := r.currentPath(snap.provenanceIno); ok {
			path = cur
		}
	}
	plan, err := c.GetExtentReadPlan(ctx, path, snap.provenanceStart, snap.provenanceStart+snap.size)
	if err != nil {
		return nil, err
	}
	for _, blk := range plan.Blocks {
		if blk.BlockKey == snap.sha256 {
			r.mu.Lock()
			if cur, still := r.m[id]; still {
				cur.url = blk.URL
				cur.expires = time.Now().Add(urlTTL)
			}
			r.mu.Unlock()
			return c.FetchExtentBlockRange(ctx, blk.URL, off, limit)
		}
	}
	return nil, fmt.Errorf("block %s no longer present in the plan of %s", snap.sha256[:12], path)
}

// fetchClient is the subset of *client.Client the registry needs.
type fetchClient interface {
	FetchExtentBlockRange(ctx context.Context, url string, offset, length int64) (io.ReadCloser, error)
	GetExtentReadPlan(ctx context.Context, path string, start, end int64) (*client.ExtentReadPlan, error)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

var _ = sync.Mutex{} // keep sync import while the registry evolves
