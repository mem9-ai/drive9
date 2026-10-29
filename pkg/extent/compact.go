package extent

import (
	"context"
	"encoding/binary"
	"fmt"
	"syscall"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/sirupsen/logrus"
)

// ExecuteCompact merges one chunk's slices, uploads the new blob, and CAS-commits.
//
// ctx bounds the work at step boundaries: it is checked on entry and again
// before the upload, so an unmount stops the loop between steps instead of
// starting another one. A step already in flight is
// deliberately not interruptible — the transport detaches the request context
// (context.WithoutCancel in Transport.Call) so a cancelled FUSE request cannot
// kill a meta op that is about to commit, and vfs.Compact's object PUT has no
// context to cancel. The commit itself is never abandoned: the CAS is a single
// meta op that either lands or does not, and a cancelled upload leaves the
// slice unreferenced, which is what the CAS-lost path below already handles.
func ExecuteCompact(ctx context.Context, rt *Runtime, ino uint64, indx uint32) error {
	if rt == nil || rt.Meta == nil || rt.Store == nil {
		return fmt.Errorf("compact: missing runtime")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	metaCtx := jfsmeta.Background()
	origin, err := readChunkRaw(rt.Transport, metaCtx, ino, indx)
	if err != nil {
		return err
	}
	if len(origin) < sliceBytes*2 {
		return nil
	}
	origin = capJuiceCompactOrigin(origin)
	ss := parseRawSlices(origin)
	if len(ss) < 2 {
		return nil
	}
	skipped := skipSome(ss)
	if skipped >= len(ss) {
		skipped = 0
	}
	pos, size, overlay := compactOverlay(ss[skipped:])
	if len(overlay) == 0 || size == 0 {
		return nil
	}
	// Last chance to avoid an upload nobody will wait for: an unmount that
	// arrived while the chunk was read should not start a 64 MiB PUT.
	if err := ctx.Err(); err != nil {
		return err
	}
	var id uint64
	if st := rt.Meta.NewSlice(metaCtx, &id); st != 0 {
		return st
	}
	if err := vfs.Compact(rt.ChunkConf, rt.Store, overlay, id, 0); err != nil {
		return fmt.Errorf("compact data: %w", err)
	}
	st := commitCompactFn(rt.Meta, jfsmeta.Ino(ino), indx, origin, skipped, pos, id, size)
	if st == syscall.EINVAL {
		// EINVAL means the chunk is gone (drained) or the origin moved under us
		// (another executor compacted it first): both are end states where the
		// chunk needs no work, which is the same "success" this function
		// reports for a chunk that is already thin. Report it as success, or
		// both executors requeue a task that is done, burn one of its attempts
		// and log a failure that never happened.
		//
		// The blob we uploaded is referenced by nothing; JuiceFS hands it to
		// deleteSlice, so do the same or it leaks for ever (P0-2).
		enqueueSliceGC(rt, id, size)
		return nil
	}
	if st != 0 {
		return st
	}
	return nil
}

// commitCompactFn is jfsmeta.Drive9CommitCompact, replaceable in tests so the
// CAS-lost end state can be driven deterministically: reaching it otherwise
// needs two executors racing one chunk.
var commitCompactFn = jfsmeta.Drive9CommitCompact

// SetCommitCompactForTest replaces the CAS-commit call and returns a restore
// function.
func SetCommitCompactForTest(fn func(jfsmeta.Meta, jfsmeta.Ino, uint32, []byte, int, uint32, uint64, uint32) syscall.Errno) func() {
	old := commitCompactFn
	commitCompactFn = fn
	return func() { commitCompactFn = old }
}

// enqueueSliceGC asks the server to schedule block GC for a slice that will
// never be referenced. Best effort: a failure leaves the blob for a later
// orphan scan.
func enqueueSliceGC(rt *Runtime, id uint64, size uint32) {
	if rt == nil || rt.Transport == nil || id == 0 || size == 0 {
		return
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	if st := rt.Transport.Call(jfsmeta.Background(), jfsmeta.Drive9OpDeleteSlice,
		map[string]any{"id": id, "size": size}, &resp); st != 0 || resp.Errno != 0 {
		logrus.Warnf("extent compact: enqueue GC for slice %d (%d bytes) failed: status=%d errno=%d", id, size, st, resp.Errno)
	}
}

// juiceMaxCompactSlices is JuiceFS maxCompactSlices (pkg/meta/base.go).
const juiceMaxCompactSlices = 1000

func capJuiceCompactOrigin(origin []byte) []byte {
	if n := len(origin) / sliceBytes; n > juiceMaxCompactSlices {
		return origin[:juiceMaxCompactSlices*sliceBytes]
	}
	return origin
}

func readChunkRaw(tr jfsmeta.Drive9Transport, ctx jfsmeta.Context, ino uint64, indx uint32) ([]byte, error) {
	if tr == nil {
		return nil, fmt.Errorf("compact: missing transport")
	}
	var resp struct {
		Errno  int    `json:"errno"`
		Slices []byte `json:"slices"`
	}
	st := tr.Call(ctx, jfsmeta.Drive9OpRead, map[string]any{"inode": ino, "indx": indx}, &resp)
	if st != 0 {
		return nil, st
	}
	if resp.Errno != 0 {
		return nil, syscall.Errno(resp.Errno)
	}
	return resp.Slices, nil
}

// ClaimCompact is the JSON body for op claim_compact.
type ClaimCompact struct {
	Inode  uint64 `json:"inode"`
	Indx   uint32 `json:"indx"`
	TaskID string `json:"task_id"`
	// Receipt is the lease identity the server minted for this claim. Every
	// later ack for the claimed task must echo it: the server rotates the
	// column on every reclaim, so a task_id-only ack from a worker whose
	// lease expired cannot touch the new owner's lease.
	Receipt string `json:"receipt,omitempty"`
}

func ClaimNextCompact(tr jfsmeta.Drive9Transport) (ino uint64, indx uint32, taskID, receipt string, err error) {
	if tr == nil {
		return 0, 0, "", "", fmt.Errorf("compact: missing transport")
	}
	var resp struct {
		Errno   int    `json:"errno"`
		Inode   uint64 `json:"inode"`
		Indx    uint32 `json:"indx"`
		TaskID  string `json:"task_id"`
		Receipt string `json:"receipt"`
	}
	// receipt_capable is REQUIRED: the server refuses receipt-less claims
	// outright (EINVAL), because a task_id-only client cannot be fenced
	// across a lease expiry — two such workers are indistinguishable. A
	// refused client simply finds no claimable task and idles; compaction
	// degrades until upgrade, it never corrupts.
	st := tr.Call(jfsmeta.Background(), "claim_compact", map[string]any{"receipt_capable": true}, &resp)
	if st != 0 {
		return 0, 0, "", "", st
	}
	if resp.Errno != 0 {
		return 0, 0, "", "", syscall.Errno(resp.Errno)
	}
	return resp.Inode, resp.Indx, resp.TaskID, resp.Receipt, nil
}

// CompleteCompact acknowledges a claimed task the executor is done with. It is
// called both for a chunk that was rewritten and for one that no longer needs
// work (drained, or already thin): leaving the row LEASED there meant the lease
// expired, the task was reclaimed, it no-opped again, and one row per
// (inode, chunk) stayed in the table for the life of the tenant.
func CompleteCompact(tr jfsmeta.Drive9Transport, taskID, receipt string) error {
	if tr == nil || taskID == "" {
		return nil
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	body := map[string]any{"task_id": taskID}
	if receipt != "" {
		// The receipt proves this ack targets the lease the claim minted;
		// after an expiry and reclaim the server rotated the column and the
		// ack answers EINVAL instead of touching the new owner.
		body["receipt"] = receipt
	}
	st := tr.Call(jfsmeta.Background(), "complete_compact", body, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}

// RequeueCompact returns a task to the queue after a failed attempt. It counts
// the attempt, so a chunk that keeps failing reaches max_attempts and parks as
// FAILED instead of being retried forever by every mount.
func RequeueCompact(tr jfsmeta.Drive9Transport, taskID, receipt string, cause error) error {
	if tr == nil || taskID == "" {
		return nil
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	body := map[string]any{"task_id": taskID, "error": msg}
	if receipt != "" {
		body["receipt"] = receipt
	}
	st := tr.Call(jfsmeta.Background(), "requeue_compact", body, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}

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
