package datastore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/extent"
)

func newExtentRuntime(t *testing.T, s *Store, delay time.Duration) *extent.Runtime {
	t.Helper()
	dir := t.TempDir()
	tr := extent.NewTransport(s.RunExtentMetaOp)
	st, err := extent.OpenStorage(&extent.Credential{
		Scheme:   extent.SchemeFile,
		Endpoint: dir,
		Prefix:   "t/test/",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := extent.NewRuntime(extent.RuntimeConfig{
		Transport: tr,
		Storage:   st,
	})
	if err != nil {
		t.Fatal(err)
	}
	tr.Delay = delay
	return rt
}

func TestUncommittedSequentialWritesDoNotMetaHTTP(t *testing.T) {
	s := newTestStore(t)
	var mu sync.Mutex
	counts := map[string]int{}
	dir := t.TempDir()
	tr := extent.NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		mu.Lock()
		counts[op]++
		mu.Unlock()
		return s.RunExtentMetaOp(ctx, op, raw)
	})
	st, err := extent.OpenStorage(&extent.Credential{Scheme: extent.SchemeFile, Endpoint: dir, Prefix: "t/test/"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := extent.NewRuntime(extent.RuntimeConfig{Transport: tr, Storage: st})
	if err != nil {
		t.Fatal(err)
	}
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/walspill.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "walspill.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	mu.Lock()
	counts = map[string]int{}
	mu.Unlock()
	w := rt.Writer.Open(ino, 0, 0)
	page := bytes.Repeat([]byte("w"), 4096)
	for i := 0; i < 2000; i++ {
		if st := w.Write(ctx, uint64(i*4096), page); st != 0 {
			t.Fatalf("write %d: %v", i, st)
		}
	}
	mu.Lock()
	gotWrite, gotIncr, gotAll := counts["write"], counts["incr_counter"], copyCounts(counts)
	mu.Unlock()
	if gotWrite != 0 {
		t.Fatalf("uncommitted sequential Write HTTP write=%d ops=%v; JuiceFS Meta.Write is freeze/flush only", gotWrite, gotAll)
	}
	if gotIncr > 2 {
		t.Fatalf("uncommitted NewSlice incr_counter=%d, want <=2 (JuiceFS sliceIdBatch=4096)", gotIncr)
	}
}

func copyCounts(m map[string]int) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func TestExtentDataWriterReaderHarness(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	runExtentIOHarness(t, s, rt)
}

func TestExtentDataWriterReaderDelayedMeta(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 30*time.Millisecond)
	runExtentIOHarness(t, s, rt)
}

func runExtentIOHarness(t *testing.T, s *Store, rt *extent.Runtime) {
	t.Helper()
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/harness.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "harness.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	w := rt.Writer.Open(ino, 0, 0)
	r := rt.Reader.Open(ino, 0)
	defer func() {
		_ = w.Close(ctx)
		r.Close(ctx)
	}()
	payload := bytes.Repeat([]byte("A"), 1024)
	for i := 0; i < 8; i++ {
		off := uint64(i * 512)
		if st := w.Write(ctx, off, payload); st != 0 {
			t.Fatalf("write %d: %v", i, st)
		}
	}
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush: %v", st)
	}
	w.Truncate(2048)
	rt.Writer.Truncate(ino, 2048)
	rt.Reader.Truncate(ino, 2048)
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush after truncate: %v", st)
	}
	if got := w.GetLength(); got != 2048 {
		t.Fatalf("writer length=%d, want 2048", got)
	}
	if st := rt.Meta.GetAttr(ctx, ino, &attr); st != 0 {
		t.Fatalf("getattr: %v", st)
	}
	if attr.Length != 2048 && w.GetLength() != 2048 {
		t.Fatalf("attr length=%d writer=%d", attr.Length, w.GetLength())
	}

	_ = w.Close(ctx)
	r.Close(ctx)
	w = rt.Writer.Open(ino, w.GetLength(), 0)
	r = rt.Reader.Open(ino, w.GetLength())
	buf := make([]byte, 1024)
	n, st := r.Read(ctx, 0, buf)
	if st != 0 {
		t.Fatalf("reopen read: %v", st)
	}
	if n == 0 || buf[0] != 'A' {
		shown := n
		if shown > 8 {
			shown = 8
		}
		t.Fatalf("reopen read n=%d buf0=%q", n, buf[:shown])
	}
	rc, err := rt.OpenRead(uint64(ino), 2048, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatalf("OpenRead: %v", err)
	}
	if len(got) != 2048 || got[0] != 'A' {
		t.Fatalf("OpenRead len=%d buf0=%q", len(got), got[:min(8, len(got))])
	}

	proj, err := s.GetExtentProjection(context.Background(), "/harness.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != uint64(ino) {
		t.Fatalf("projection ino=%d want %d", proj.ExtentIno, ino)
	}
}

func TestJfsMknodPartsOneTxn(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	var id uint64 = 42
	body, errno, err := s.RunExtentMetaOp(ctx, "mknod", mustJSON(map[string]any{
		"parent": 1, "name": "j.db-journal", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/j.db-journal", "uid": 0, "gid": 0,
		"indx": 0,
		"parts": []map[string]any{
			{"off": 0, "slice": map[string]any{"Id": id, "Size": 4, "Off": 0, "Len": 4}},
		},
	}))
	if err != nil || errno != 0 {
		t.Fatalf("mknod+parts errno=%d err=%v body=%s", errno, err, body)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jfs_chunk_ref WHERE chunkid = ?`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("chunk_ref=%d, want 1 (mknod+write in one JuiceFS-shaped txn)", n)
	}
	var ino uint64
	if err := s.db.QueryRow(`SELECT inode FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("j.db-journal")).Scan(&ino); err != nil {
		t.Fatal(err)
	}
	if ino != 9 {
		t.Fatalf("edge inode=%d, want 9", ino)
	}
}

func TestJfsUnlinkDoesNotWalkChunkRefs(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/journal.db-journal")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "journal.db-journal", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	wp := rt.Meta.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	parts := make([]jfsmeta.WritePart, 8)
	for i := range parts {
		var id uint64
		if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice %d: %v", i, st)
		}
		parts[i] = jfsmeta.WritePart{Off: uint32(i * 4), Slice: jfsmeta.Slice{Id: id, Size: 4, Len: 4}}
	}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	unlinkCtx := ctx.WithValue(jfsmeta.Drive9OpenedKey, false)
	if st := rt.Meta.Unlink(unlinkCtx, jfsmeta.RootInode, "journal.db-journal"); st != 0 {
		t.Fatalf("unlink: %v", st)
	}
	var refs int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jfs_chunk_ref`).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if refs != 8 {
		t.Fatalf("chunk_ref rows=%d after unlink, want 8 (JuiceFS MaxDeletes=0: unlink must not walk slices)", refs)
	}
}

func TestJfsWritePartsBatchesChunkRefs(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/parts.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "parts.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := rt.Meta.Open(ctx, ino, 2, &attr); st != 0 {
		t.Fatalf("open: %v", st)
	}
	wp, ok := rt.Meta.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	if !ok {
		t.Fatal("Meta must expose WriteParts")
	}
	parts := make([]jfsmeta.WritePart, 3)
	for i := range parts {
		var id uint64
		if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice %d: %v", i, st)
		}
		parts[i] = jfsmeta.WritePart{Off: uint32(i * 4), Slice: jfsmeta.Slice{Id: id, Size: 4, Len: 4}}
	}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jfs_chunk_ref`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("chunk_ref rows=%d, want 3 (one multi-value INSERT per WriteParts)", n)
	}
}

func TestStatOverlaysJfsNodeLength(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/statsize.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "statsize.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	payload := bytes.Repeat([]byte("A"), 1024)
	w := rt.Writer.Open(ino, 0, 0)
	if st := w.Write(ctx, 0, payload); st != 0 {
		t.Fatalf("write: %v", st)
	}
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush: %v", st)
	}
	_ = w.Close(ctx)

	bg := context.Background()
	var inodeSize int64
	err := s.db.QueryRow(`SELECT i.size_bytes FROM inodes i JOIN file_nodes f ON f.inode_id = i.inode_id WHERE f.extent_ino = ?`, uint64(ino)).Scan(&inodeSize)
	if err != nil {
		t.Fatal(err)
	}
	if inodeSize != 0 {
		t.Fatalf("inode size_bytes=%d after Write, want 0", inodeSize)
	}

	nf, err := s.Stat(bg, "/statsize.db")
	if err != nil {
		t.Fatal(err)
	}
	if nf.File == nil {
		t.Fatal("Stat file is nil")
	}
	if nf.File.SizeBytes != int64(len(payload)) {
		t.Fatalf("Stat size=%d, want %d (jfs_node.length overlay)", nf.File.SizeBytes, len(payload))
	}
	lite, err := s.StatLite(bg, "/statsize.db")
	if err != nil {
		t.Fatal(err)
	}
	if lite.File == nil || lite.File.SizeBytes != int64(len(payload)) {
		t.Fatalf("StatLite size=%v, want %d", lite.File, len(payload))
	}
}

func TestJfsWriteDoesNotDualWriteInodeSize(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/size.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "size.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	w := rt.Writer.Open(ino, 0, 0)
	if st := w.Write(ctx, 0, bytes.Repeat([]byte("A"), 1024)); st != 0 {
		t.Fatalf("write: %v", st)
	}
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush: %v", st)
	}
	_ = w.Close(ctx)
	var size int64
	err := s.db.QueryRow(`SELECT i.size_bytes FROM inodes i JOIN file_nodes f ON f.inode_id = i.inode_id WHERE f.extent_ino = ?`, uint64(ino)).Scan(&size)
	if err != nil {
		t.Fatal(err)
	}
	if size != 0 {
		t.Fatalf("inode size_bytes=%d after Write, want 0 (JuiceFS doWrite updates jfs_node only)", size)
	}
	if st := rt.Meta.GetAttr(ctx, ino, &attr); st != 0 {
		t.Fatalf("getattr: %v", st)
	}
	if attr.Length == 0 {
		t.Fatal("jfs_node length must still track the write")
	}
}

func TestJfsWriteDoesNotEnqueueCompact(t *testing.T) {
	s := newTestStore(t)
	restore := SetCompactThresholdForTest(3)
	t.Cleanup(restore)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/nocompact.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "nocompact.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	wp := rt.Meta.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	parts := make([]jfsmeta.WritePart, 6)
	for i := range parts {
		var id uint64
		if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice %d: %v", i, st)
		}
		parts[i] = jfsmeta.WritePart{Off: uint32(i * 4), Slice: jfsmeta.Slice{Id: id, Size: 4, Len: 4}}
	}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	var queued int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM slice_compact_tasks`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("write enqueued compact tasks=%d, want 0 (JuiceFS doWrite does not INSERT compact on the write txn)", queued)
	}
	cino, _, taskID, err := s.ClaimCompactTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if taskID == "" || cino != uint64(ino) {
		t.Fatalf("claim ino=%d task=%q, want ino=%d (discover fat chunks off the write path)", cino, taskID, ino)
	}
}

func TestJfsWritePartsReportsSliceCount(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/nslices.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "nslices.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	wp := rt.Meta.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	parts := make([]jfsmeta.WritePart, 6)
	for i := range parts {
		var id uint64
		if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice %d: %v", i, st)
		}
		parts[i] = jfsmeta.WritePart{Off: uint32(i * 4), Slice: jfsmeta.Slice{Id: id, Size: 4, Len: 4}}
	}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	body, errno, err := s.RunExtentMetaOp(context.Background(), "write", mustJSON(map[string]any{
		"inode": uint64(ino), "indx": 0, "off": 24,
		"slice": map[string]any{"Id": uint64(99), "Size": 4, "Off": 0, "Len": 4},
		"mtime": time.Now(),
	}))
	if err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v", errno, err)
	}
	var got struct {
		NumSlices int `json:"num_slices"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if got.NumSlices != 7 {
		t.Fatalf("num_slices=%d, want 7 (JuiceFS doWrite returns len(slices)/24)", got.NumSlices)
	}
}

func TestClaimCompactSkipsRunawayBlob(t *testing.T) {
	s := newTestStore(t)
	restore := SetCompactThresholdForTest(3)
	t.Cleanup(restore)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/ok.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "ok.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	wp := rt.Meta.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	parts := make([]jfsmeta.WritePart, 6)
	for i := range parts {
		var id uint64
		if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice %d: %v", i, st)
		}
		parts[i] = jfsmeta.WritePart{Off: uint32(i * 4), Slice: jfsmeta.Slice{Id: id, Size: 4, Len: 4}}
	}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	huge := make([]byte, 2<<20)
	if _, err := s.db.Exec(`INSERT INTO jfs_chunk (inode, indx, slices) VALUES (?, 0, ?)
		ON DUPLICATE KEY UPDATE slices = VALUES(slices)`, uint64(ino)+1000, huge); err != nil {
		t.Fatal(err)
	}
	cino, _, taskID, err := s.ClaimCompactTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if taskID == "" || cino != uint64(ino) {
		t.Fatalf("claim ino=%d task=%q, want live chunk ino=%d not the 2MiB runaway", cino, taskID, ino)
	}
}

func TestExtentCompactClaimCAS(t *testing.T) {
	s := newTestStore(t)
	restore := SetCompactThresholdForTest(3)
	t.Cleanup(restore)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/compact.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "compact.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	w := rt.Writer.Open(ino, 0, 0)
	for i := 0; i < 6; i++ {
		buf := bytes.Repeat([]byte{byte('A' + i)}, 1024)
		if st := w.Write(ctx, uint64(i*1024), buf); st != 0 {
			t.Fatalf("write %d: %v", i, st)
		}
		if st := w.Flush(ctx); st != 0 {
			t.Fatalf("flush %d: %v", i, st)
		}
	}
	_ = w.Close(ctx)

	cino, indx, taskID, err := s.ClaimCompactTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if taskID == "" || cino == 0 {
		t.Fatalf("expected compact task, got ino=%d task=%q", cino, taskID)
	}
	beforeBody, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": cino, "indx": indx}))
	if err != nil || errno != 0 {
		t.Fatalf("read before compact errno=%d err=%v", errno, err)
	}
	var before struct {
		Slices []byte `json:"slices"`
	}
	if err := json.Unmarshal(beforeBody, &before); err != nil {
		t.Fatal(err)
	}
	nBefore := len(before.Slices) / 24
	if nBefore < 3 {
		t.Fatalf("need multiple slices to compact, got %d", nBefore)
	}

	if err := extent.ExecuteCompact(rt, cino, indx); err != nil {
		t.Fatalf("execute compact: %v", err)
	}
	body, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": cino, "indx": indx}))
	if err != nil || errno != 0 {
		t.Fatalf("read after compact errno=%d err=%v", errno, err)
	}
	var out struct {
		Slices []byte `json:"slices"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	nAfter := len(out.Slices) / 24
	if nAfter == 0 {
		t.Fatal("compact left no slices")
	}
	if nAfter >= nBefore {
		t.Fatalf("compact did not reduce slices: before=%d after=%d", nBefore, nAfter)
	}
	keys, err := s.ListPendingBlockGC(context.Background(), 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("compact did not enqueue superseded slices for block GC")
	}
}

func TestJfsCompactCASTxnDoesNotEnqueueGC(t *testing.T) {
	s := newTestStore(t)
	restore := SetCompactThresholdForTest(3)
	t.Cleanup(restore)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/cas-gc.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "cas-gc.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	wp := rt.Meta.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	parts := make([]jfsmeta.WritePart, 6)
	for i := range parts {
		var id uint64
		if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("newslice %d: %v", i, st)
		}
		parts[i] = jfsmeta.WritePart{Off: uint32(i * 4), Slice: jfsmeta.Slice{Id: id, Size: 4, Len: 4}}
	}
	if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	body, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": uint64(ino), "indx": 0}))
	if err != nil || errno != 0 {
		t.Fatalf("read errno=%d err=%v", errno, err)
	}
	var got struct {
		Slices []byte `json:"slices"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Slices) < 2*24 {
		t.Fatalf("need multiple slices, got %d bytes", len(got.Slices))
	}
	var newID uint64
	if st := rt.Meta.NewSlice(ctx, &newID); st != 0 {
		t.Fatalf("compact slice: %v", st)
	}
	var gcBefore int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM block_gc_tasks`).Scan(&gcBefore); err != nil {
		t.Fatal(err)
	}
	var olds []extentSliceRef
	err = s.InTx(context.Background(), func(tx *sql.Tx) error {
		eno, refs, err := s.jfsCompactTx(tx, uint64(ino), 0, got.Slices, 0, 0, newID, 24)
		if err != nil {
			return err
		}
		if eno != 0 {
			return syscall.Errno(eno)
		}
		olds = refs
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(olds) == 0 {
		t.Fatal("compact CAS returned no superseded refs")
	}
	var gcAfterCAS int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM block_gc_tasks`).Scan(&gcAfterCAS); err != nil {
		t.Fatal(err)
	}
	if gcAfterCAS != gcBefore {
		t.Fatalf("compact CAS txn enqueued GC before=%d after=%d (JuiceFS deleteSlice is after doCompactChunk txn)", gcBefore, gcAfterCAS)
	}
	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.jfsEnqueueDeadSliceGCTx(tx, uint64(ino), olds)
	}); err != nil {
		t.Fatal(err)
	}
	var gcAfter int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM block_gc_tasks`).Scan(&gcAfter); err != nil {
		t.Fatal(err)
	}
	if gcAfter <= gcAfterCAS {
		t.Fatal("deleteSlice-after-CAS did not enqueue block GC")
	}
}

func TestJfsWriteOpPartsOneTxn(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/parts.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "parts.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	parts := []map[string]any{
		{"off": 0, "slice": ExtentSlice{Id: 21, Size: 4, Off: 0, Len: 4}},
		{"off": 4, "slice": ExtentSlice{Id: 22, Size: 4, Off: 0, Len: 4}},
		{"off": 8, "slice": ExtentSlice{Id: 23, Size: 4, Off: 0, Len: 4}},
	}
	body, errno, err := s.RunExtentMetaOp(context.Background(), "write", mustJSON(map[string]any{
		"inode": ino, "indx": 0, "mtime": time.Now().UTC(), "parts": parts,
	}))
	if err != nil || errno != 0 {
		t.Fatalf("write parts errno=%d err=%v body=%s", errno, err, body)
	}
	readBody, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": ino, "indx": 0}))
	if err != nil || errno != 0 {
		t.Fatalf("read errno=%d err=%v", errno, err)
	}
	var out struct {
		Slices []byte `json:"slices"`
	}
	if err := json.Unmarshal(readBody, &out); err != nil {
		t.Fatal(err)
	}
	if got := len(out.Slices) / 24; got != 3 {
		t.Fatalf("chunk slices=%d, want 3 from one write txn", got)
	}
}

func TestExtentSessionSweepWithoutClient(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, errno, err := s.RunExtentMetaOp(ctx, "new_session", mustJSON(map[string]any{
		"sid": 99, "expire": time.Now().Unix() - 60, "info": []byte("{}"),
	})); err != nil || errno != 0 {
		t.Fatalf("new_session errno=%d err=%v", errno, err)
	}
	n, err := s.SweepStaleExtentSessions(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("swept %d sessions, want 1", n)
	}
}

func TestExtentUnlinkEnqueuesBlockGC(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "gc.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/gc.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 99, Size: 4, Off: 0, Len: 4},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "write", write); err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v", errno, err)
	}
	if err := s.UnlinkExtentPath(ctx, "/gc.db", false); err != nil {
		t.Fatal(err)
	}
	keys, err := s.ListPendingBlockGC(ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("unlink of a sliced extent file enqueued no block_gc_tasks")
	}
	found := false
	for _, k := range keys {
		if strings.Contains(k, "99_") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected GC key for slice 99, got %v", keys)
	}
}

func TestExtentDirRenameSubtreeProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if err := s.InsertNode(ctx, &FileNode{
		NodeID: "dir-d", Path: "/d/", ParentPath: "/", Name: "d",
		IsDirectory: true, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	mkdir, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "d", "type": 2, "mode": 0755,
		"inode": 10, "proj_path": "/d/",
		"attr": ExtentAttr{Typ: 2, Mode: 0755, Nlink: 2, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", mkdir); err != nil || errno != 0 {
		t.Fatalf("mkdir errno=%d err=%v", errno, err)
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 10, "name": "a.db", "type": 1, "mode": 0644,
		"inode": 11, "proj_path": "/d/a.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 10, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if _, err := s.RenameDir(ctx, "/d/", "/e/"); err != nil {
		t.Fatal(err)
	}
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", mustJSON(map[string]any{
		"src_parent": 1, "src_name": "d", "dst_parent": 1, "dst_name": "e",
	})); err != nil || errno != 0 {
		t.Fatalf("jfs dir edge rename errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/d/a.db"); err == nil {
		t.Fatal("child projection still at old path after dir rename")
	}
	proj, err := s.GetExtentProjection(ctx, "/e/a.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 11 {
		t.Fatalf("child extent_ino=%d, want 11", proj.ExtentIno)
	}
}

func TestExtentPrefixIsolation(t *testing.T) {
	dir := t.TempDir()
	st, err := extent.OpenStorage(&extent.Credential{
		Scheme:   extent.SchemeFile,
		Endpoint: dir,
		Prefix:   "t/tenant-a/",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Put(context.Background(), "chunks/1/1/1_0_4", bytes.NewReader([]byte("abcd"))); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "t/tenant-a/chunks/1/1/1_0_4")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("prefixed object missing at %s: %v", want, err)
	}
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
