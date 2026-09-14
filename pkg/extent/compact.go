package extent

import (
	"context"
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
}

func ClaimNextCompact(tr jfsmeta.Drive9Transport) (ino uint64, indx uint32, taskID string, err error) {
	if tr == nil {
		return 0, 0, "", fmt.Errorf("compact: missing transport")
	}
	var resp struct {
		Errno  int    `json:"errno"`
		Inode  uint64 `json:"inode"`
		Indx   uint32 `json:"indx"`
		TaskID string `json:"task_id"`
	}
	st := tr.Call(jfsmeta.Background(), "claim_compact", map[string]any{}, &resp)
	if st != 0 {
		return 0, 0, "", st
	}
	if resp.Errno != 0 {
		return 0, 0, "", syscall.Errno(resp.Errno)
	}
	return resp.Inode, resp.Indx, resp.TaskID, nil
}

// CompleteCompact acknowledges a claimed task the executor is done with. It is
// called both for a chunk that was rewritten and for one that no longer needs
// work (drained, or already thin): leaving the row LEASED there meant the lease
// expired, the task was reclaimed, it no-opped again, and one row per
// (inode, chunk) stayed in the table for the life of the tenant.
func CompleteCompact(tr jfsmeta.Drive9Transport, taskID string) error {
	if tr == nil || taskID == "" {
		return nil
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	st := tr.Call(jfsmeta.Background(), "complete_compact", map[string]any{"task_id": taskID}, &resp)
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
func RequeueCompact(tr jfsmeta.Drive9Transport, taskID string, cause error) error {
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
	st := tr.Call(jfsmeta.Background(), "requeue_compact", map[string]any{"task_id": taskID, "error": msg}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}
