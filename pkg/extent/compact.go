package extent

import (
	"fmt"
	"syscall"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/sirupsen/logrus"
)

// ExecuteCompact merges one chunk's slices, uploads the new blob, and CAS-commits.
func ExecuteCompact(rt *Runtime, ino uint64, indx uint32) error {
	if rt == nil || rt.Meta == nil || rt.Store == nil {
		return fmt.Errorf("compact: missing runtime")
	}
	ctx := jfsmeta.Background()
	origin, err := readChunkRaw(rt.Transport, ctx, ino, indx)
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
	var id uint64
	if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
		return st
	}
	if err := vfs.Compact(rt.ChunkConf, rt.Store, overlay, id, 0); err != nil {
		return fmt.Errorf("compact data: %w", err)
	}
	st := jfsmeta.Drive9CommitCompact(unwrapDrive9Meta(rt.Meta), jfsmeta.Ino(ino), indx, origin, skipped, pos, id, size)
	if st == syscall.EINVAL {
		// CAS lost: another executor compacted the same chunk first, so the
		// blob we just uploaded is referenced by nothing. JuiceFS hands it to
		// deleteSlice; do the same or it leaks forever (P0-2).
		enqueueSliceGC(rt, id, size)
	}
	if st != 0 {
		return st
	}
	return nil
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

func unwrapDrive9Meta(m jfsmeta.Meta) jfsmeta.Meta {
	if w, ok := m.(*memLockMeta); ok && w != nil && w.Meta != nil {
		return w.Meta
	}
	return m
}

func RequeueCompact(tr jfsmeta.Drive9Transport, taskID string) error {
	if tr == nil || taskID == "" {
		return nil
	}
	var resp struct {
		Errno int `json:"errno"`
	}
	st := tr.Call(jfsmeta.Background(), "requeue_compact", map[string]any{"task_id": taskID}, &resp)
	if st != 0 {
		return st
	}
	if resp.Errno != 0 {
		return syscall.Errno(resp.Errno)
	}
	return nil
}
