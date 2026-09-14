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
	tr := extent.NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		return s.RunExtentMetaOp(ctx, op, raw, nil)
	})
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
		return s.RunExtentMetaOp(ctx, op, raw, nil)
	})
	st, err := extent.OpenStorage(&extent.Credential{Scheme: extent.SchemeFile, Endpoint: dir, Prefix: "t/test/"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := extent.NewRuntime(extent.RuntimeConfig{Transport: tr, Storage: st})
	if err != nil {
		t.Fatal(err)
	}
	// Quiesce the runtime before the TempDir cleanup removes the storage root:
	// t.Cleanup runs LIFO, so this closes the runtime (and its upload workers)
	// ahead of the RemoveAll that t.TempDir registered first. Without it the
	// asynchronous slice uploads race the cleanup and the test fails with
	// "directory not empty" on a loaded host.
	t.Cleanup(func() { _ = extent.CloseRuntime(rt) })
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
	rc, err := rt.OpenRead(context.Background(), uint64(ino), 2048, 0, 0)
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
	}), nil)
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
	// The unlink *transaction* must not walk slices (JuiceFS MaxDeletes=0): it
	// only records one jfs_delfile row, and RunExtentMetaOp drains it right
	// after the commit. The drain drops each slice's reference count, queues
	// the blocks whose count reached zero, and deletes the now-dead
	// jfs_chunk_ref rows so overwrite-heavy workloads cannot grow that table
	// without bound.
	var delFile, refs, gcTasks int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jfs_delfile`).Scan(&delFile); err != nil {
		t.Fatal(err)
	}
	if delFile != 0 {
		t.Fatalf("jfs_delfile rows=%d after unlink, want 0 (drained post-commit)", delFile)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jfs_chunk_ref`).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if refs != 0 {
		t.Fatalf("chunk_ref rows=%d after unlink, want 0 (drained and reclaimed)", refs)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM block_gc_tasks`).Scan(&gcTasks); err != nil {
		t.Fatal(err)
	}
	if gcTasks != 8 {
		t.Fatalf("block_gc_tasks=%d after unlink, want 8 (one per unreferenced slice)", gcTasks)
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
	// The extent write path leaves the projection at 0 on purpose (keeping it
	// in sync there cost +40-50% on extent_write), so the overlay is what makes
	// Stat answer with the authoritative jfs length.
	var inodeSize int64
	err := s.db.QueryRow(`SELECT i.size_bytes FROM inodes i JOIN file_nodes f ON f.inode_id = i.inode_id WHERE f.extent_ino = ?`, uint64(ino)).Scan(&inodeSize)
	if err != nil {
		t.Fatal(err)
	}
	if inodeSize != 0 {
		t.Fatalf("inode size_bytes=%d after forcing it stale, want 0", inodeSize)
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

// The extent write path deliberately leaves the projection's size at 0 (see
// jfsWritePartsTx: keeping it in sync there cost +40-50% on extent_write), so
// every reader that needs a real size must take it from jfs_node. Search reads
// inodes directly and joins it: without that join `fs find` printed 0, dropped
// the file for --min-size, and kept it for --max-size whatever its real size.
func TestExtentFileSizeInSearchFollowsJfsNode(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/sizesearch.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "sizesearch.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	const n = 1024
	w := rt.Writer.Open(ino, 0, 0)
	if st := w.Write(ctx, 0, bytes.Repeat([]byte("A"), n)); st != 0 {
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
		t.Fatalf("inode size_bytes=%d, want the projection to stay 0", size)
	}
	if st := rt.Meta.GetAttr(ctx, ino, &attr); st != 0 {
		t.Fatalf("getattr: %v", st)
	}
	if int64(attr.Length) != n {
		t.Fatalf("jfs_node length=%d, want %d", attr.Length, n)
	}
	check := func(min, max int64, wantFound bool) {
		t.Helper()
		res, err := s.Find(ctx, &FindFilter{PathPrefix: "/", MinSize: min, MaxSize: max})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range res {
			if r.Path == "/sizesearch.db" {
				found = true
				if r.SizeBytes != n {
					t.Fatalf("find reported size_bytes=%d, want %d", r.SizeBytes, n)
				}
			}
		}
		if found != wantFound {
			t.Fatalf("find min=%d max=%d found=%v, want %v (%+v)", min, max, found, wantFound, res)
		}
	}
	check(0, 0, true)    // no filter: reported with the real size
	check(n, 0, true)    // min == size: kept
	check(n+1, 0, false) // min > size: dropped
	check(0, n, true)    // max == size: kept
	check(0, n-1, false) // max < size: dropped (0 <= max used to keep it)
}

// A write below the threshold queues nothing; the write that reaches it queues
// exactly one task, after the write transaction has committed. This is the only
// source of compact tasks now that the self-feeding jfs_chunk scan is gone.
func TestJfsWriteEnqueuesCompactWhenFat(t *testing.T) {
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
	wp := rt.Meta.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	write := func(n int) {
		t.Helper()
		parts := make([]jfsmeta.WritePart, n)
		for i := range parts {
			var id uint64
			if st := rt.Meta.NewSlice(ctx, &id); st != 0 {
				t.Fatalf("newslice: %v", st)
			}
			parts[i] = jfsmeta.WritePart{Off: uint32(i * 4), Slice: jfsmeta.Slice{Id: id, Size: 4, Len: 4}}
		}
		if st := wp.WriteParts(ctx, ino, 0, parts, time.Now()); st != 0 {
			t.Fatalf("WriteParts: %v", st)
		}
	}
	write(2)
	var queued int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM slice_compact_tasks`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued != 0 {
		t.Fatalf("write below the threshold queued %d compact tasks, want 0", queued)
	}
	write(1)
	if err := s.db.QueryRow(`SELECT COUNT(*), MIN(status) FROM slice_compact_tasks`).Scan(&queued, new(string)); err != nil {
		t.Fatal(err)
	}
	if queued != 1 {
		t.Fatalf("write at the threshold queued %d compact tasks, want 1", queued)
	}
	cino, cindx, taskID, err := s.ClaimCompactTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if taskID == "" || cino != uint64(ino) || cindx != 0 {
		t.Fatalf("claim ino=%d indx=%d task=%q, want ino=%d indx=0", cino, cindx, taskID, ino)
	}
}

// Two executors racing for one queued task: exactly one gets it, and the lease
// is not stealable until it expires.
func TestClaimCompactTaskLeasesOnce(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.db.Exec(`INSERT INTO slice_compact_tasks (task_id, extent_ino, chunk, status, max_attempts)
		VALUES ('queued-1', 77, 0, 'PENDING', 8)`); err != nil {
		t.Fatal(err)
	}
	type claim struct {
		task string
		ino  uint64
	}
	results := make(chan claim, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ino, _, taskID, err := s.ClaimCompactTask(ctx)
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			results <- claim{task: taskID, ino: ino}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	won := 0
	for c := range results {
		if c.task != "" {
			won++
			if c.ino != 77 {
				t.Fatalf("winner claimed ino=%d, want 77", c.ino)
			}
		}
	}
	if won != 1 {
		t.Fatalf("%d concurrent claims won, want exactly 1", won)
	}
	if _, _, taskID, err := s.ClaimCompactTask(ctx); err != nil || taskID != "" {
		t.Fatalf("claim while leased = (%q, %v), want no task", taskID, err)
	}
	// An expired lease is claimable again.
	if _, err := s.db.Exec(`UPDATE slice_compact_tasks SET lease_until = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE task_id = 'queued-1'`); err != nil {
		t.Fatal(err)
	}
	ino, _, taskID, err := s.ClaimCompactTask(ctx)
	if err != nil || taskID != "queued-1" || ino != 77 {
		t.Fatalf("claim after expiry = (%d, %q, %v), want (77, queued-1, nil)", ino, taskID, err)
	}
}

// Reviving a finished task must never take the lease away from an executor that
// is still working on it.
func TestCompactEnqueueKeepsLiveLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	write := func() {
		t.Helper()
		if _, err := s.db.Exec(`INSERT INTO slice_compact_tasks (task_id, extent_ino, chunk, status, max_attempts, available_at)
			VALUES ('w1', 88, 1, 'PENDING', 8, CURRENT_TIMESTAMP(3))
			ON DUPLICATE KEY UPDATE status = IF(status IN ('COMPLETED','FAILED'), 'PENDING', status)`); err != nil {
			t.Fatal(err)
		}
	}
	write()
	ino, _, taskID, err := s.ClaimCompactTask(ctx)
	if err != nil || taskID != "w1" || ino != 88 {
		t.Fatalf("claim = (%d, %q, %v), want (88, w1, nil)", ino, taskID, err)
	}
	write()
	var status, holder string
	if err := s.db.QueryRow(`SELECT status, task_id FROM slice_compact_tasks WHERE extent_ino = 88 AND chunk = 1`).
		Scan(&status, &holder); err != nil {
		t.Fatal(err)
	}
	if status != "LEASED" || holder != "w1" {
		t.Fatalf("a write stole the live lease: status=%s task_id=%s, want LEASED/w1", status, holder)
	}
	if _, err := s.db.Exec(`UPDATE slice_compact_tasks SET status = 'COMPLETED' WHERE extent_ino = 88 AND chunk = 1`); err != nil {
		t.Fatal(err)
	}
	write()
	if err := s.db.QueryRow(`SELECT status FROM slice_compact_tasks WHERE extent_ino = 88 AND chunk = 1`).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if status != "PENDING" {
		t.Fatalf("a completed task was not revived: status=%s, want PENDING", status)
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
	}), nil)
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
	beforeBody, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": cino, "indx": indx}), nil)
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

	if err := extent.ExecuteCompact(context.Background(), rt, cino, indx); err != nil {
		t.Fatalf("execute compact: %v", err)
	}

	body, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": cino, "indx": indx}), nil)
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
	expireBlockGCGrace(t, s)
	keys, err := s.ListPendingBlockGC(context.Background(), 32)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) == 0 {
		t.Fatal("compact did not enqueue superseded slices for block GC")
	}
}

// expireBlockGCGrace makes queued block deletions visible to
// ListPendingBlockGC without waiting out the production grace period. Every
// test that asserts "the object was queued" has to call it first: the grace is
// exactly what keeps a reader on another mount from hitting a deleted block.
func expireBlockGCGrace(t *testing.T, s *Store) {
	t.Helper()
	if _, err := s.db.Exec(`UPDATE block_gc_tasks SET available_at = DATE_SUB(CURRENT_TIMESTAMP(3), INTERVAL 1 SECOND)
		WHERE status = 'PENDING'`); err != nil {
		t.Fatal(err)
	}
}

func TestBlockGCGraceHidesFreshTask(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, errno, err := s.RunExtentMetaOp(ctx, "delete_slice", []byte(`{"id":1234,"size":4}`), nil); err != nil || errno != 0 {
		t.Fatalf("delete_slice errno=%d err=%v", errno, err)
	}
	var key string
	if err := s.db.QueryRow(`SELECT block_key FROM block_gc_tasks ORDER BY created_at LIMIT 1`).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if keys, err := s.ListPendingBlockGC(ctx, 8); err != nil || len(keys) != 0 {
		t.Fatalf("fresh block GC task is visible: keys=%v err=%v", keys, err)
	}
	expireBlockGCGrace(t, s)
	keys, err := s.ListPendingBlockGC(ctx, 8)
	if err != nil || len(keys) != 1 {
		t.Fatalf("keys=%v err=%v after the grace period, want exactly the queued block", keys, err)
	}
	// A failed delete is retried, and parked once max_attempts is exhausted.
	for i := 1; i <= 8; i++ {
		retry, err := s.RequeueBlockGC(ctx, key, io.ErrUnexpectedEOF)
		if err != nil {
			t.Fatal(err)
		}
		if !retry && i < 8 {
			t.Fatalf("attempt %d parked before max_attempts", i)
		}
		if retry && i == 8 {
			t.Fatal("attempt 8 still retryable, want FAILED")
		}
	}
	status, err := s.BlockGCTaskStatus(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if status != "FAILED" {
		t.Fatalf("status=%s after max_attempts, want FAILED", status)
	}
	if keys, err := s.ListPendingBlockGC(ctx, 8); err != nil || len(keys) != 0 {
		t.Fatalf("parked task is still listed: keys=%v err=%v", keys, err)
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
	body, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": uint64(ino), "indx": 0}), nil)
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
	}), nil)
	if err != nil || errno != 0 {
		t.Fatalf("write parts errno=%d err=%v body=%s", errno, err, body)
	}
	readBody, errno, err := s.RunExtentMetaOp(context.Background(), "read", mustJSON(map[string]any{"inode": ino, "indx": 0}), nil)
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
	}), nil); err != nil || errno != 0 {
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
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 99, Size: 4, Off: 0, Len: 4},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil); err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v", errno, err)
	}
	if err := s.UnlinkExtentPath(ctx, "/gc.db", false); err != nil {
		t.Fatal(err)
	}
	expireBlockGCGrace(t, s)
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

func TestDrainDeletedFileReclaimsSharedSlices(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	// Two extent files share one slice (77, 4 bytes). The unlink transaction
	// only records jfs_delfile; DrainDeletedFile drops the refcount and queues
	// the block for object deletion once the last reference is gone.
	for i, spec := range []struct {
		inode uint64
		path  string
	}{
		{2, "/drain-a.db"},
		{3, "/drain-b.db"},
	} {
		create, _ := json.Marshal(map[string]any{
			"parent": 1, "name": strings.TrimPrefix(spec.path, "/"), "type": 1, "mode": 0644,
			"inode": spec.inode, "proj_path": spec.path,
			"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
			t.Fatalf("mknod %d errno=%d err=%v", i, errno, err)
		}
		write, _ := json.Marshal(map[string]any{
			"inode": spec.inode, "indx": 0, "off": 0,
			"slice": ExtentSlice{Id: 77, Size: 4, Off: 0, Len: 4},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil); err != nil || errno != 0 {
			t.Fatalf("write %d errno=%d err=%v", i, errno, err)
		}
	}
	if err := s.UnlinkExtentPath(ctx, "/drain-a.db", false); err != nil {
		t.Fatal(err)
	}
	// The slice is still referenced by the second file: no object deletion yet.
	var refs int
	if err := s.db.QueryRow(`SELECT refs FROM jfs_chunk_ref WHERE chunkid = 77 AND size = 4`).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if refs != 1 {
		t.Fatalf("refs=%d after unlinking one of two owners, want 1", refs)
	}
	if keys, err := s.ListPendingBlockGC(ctx, 32); err != nil || len(keys) != 0 {
		t.Fatalf("shared slice queued for GC early: keys=%v err=%v", keys, err)
	}
	if err := s.UnlinkExtentPath(ctx, "/drain-b.db", false); err != nil {
		t.Fatal(err)
	}
	expireBlockGCGrace(t, s)
	keys, err := s.ListPendingBlockGC(ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, k := range keys {
		if strings.Contains(k, "77_0_4") {
			found = true
		}
	}
	if !found {
		t.Fatalf("last owner unlink did not queue the block: %v", keys)
	}
	for _, q := range []string{
		`SELECT COUNT(*) FROM jfs_delfile WHERE inode IN (2,3)`,
		`SELECT COUNT(*) FROM jfs_chunk WHERE inode IN (2,3)`,
	} {
		var n int
		if err := s.db.QueryRow(q).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Fatalf("drained rows left over for %s: %d", q, n)
		}
	}
	// Draining an already-drained inode must stay a no-op.
	if err := s.DrainDeletedFile(ctx, 2); err != nil {
		t.Fatal(err)
	}
}

func TestServerDirOpsTouchJfsTree(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	insertDir := func(path, parent, name string) {
		t.Helper()
		if err := s.InsertNode(ctx, &FileNode{
			NodeID: "dir-" + name, Path: path, ParentPath: parent, Name: name,
			IsDirectory: true, CreatedAt: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	mkdirJfs := func(parent uint64, name, projPath string) uint64 {
		t.Helper()
		req, _ := json.Marshal(map[string]any{
			"parent": parent, "name": name, "type": 2, "mode": 0755, "proj_path": projPath,
		})
		body, errno, err := s.RunExtentMetaOp(ctx, "mknod", req, nil)
		if err != nil || errno != 0 {
			t.Fatalf("mkdir %s errno=%d err=%v", name, errno, err)
		}
		var resp struct {
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		return resp.Inode
	}
	mknodFile := func(parent uint64, name, projPath string, slice uint64) {
		t.Helper()
		req, _ := json.Marshal(map[string]any{
			"parent": parent, "name": name, "type": 1, "mode": 0644, "proj_path": projPath,
			"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: parent, Full: true},
		})
		body, errno, err := s.RunExtentMetaOp(ctx, "mknod", req, nil)
		if err != nil || errno != 0 {
			t.Fatalf("mknod %s errno=%d err=%v", name, errno, err)
		}
		var resp struct {
			Inode uint64 `json:"inode"`
		}
		if err := json.Unmarshal(body, &resp); err != nil {
			t.Fatal(err)
		}
		write, _ := json.Marshal(map[string]any{
			"inode": resp.Inode, "indx": 0, "off": 0,
			"slice": ExtentSlice{Id: slice, Size: 4, Off: 0, Len: 4},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "write", write, nil); err != nil || errno != 0 {
			t.Fatalf("write %s errno=%d err=%v", name, errno, err)
		}
	}
	jfsEdgeNames := func() map[string]bool {
		t.Helper()
		rows, err := s.db.Query(`SELECT name FROM jfs_edge`)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = rows.Close() }()
		out := map[string]bool{}
		for rows.Next() {
			var name []byte
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
			}
			out[string(name)] = true
		}
		return out
	}

	insertDir("/cli-rm/", "/", "cli-rm")
	insertDir("/cli-rm/sub/", "/cli-rm/", "sub")
	cliIno := mkdirJfs(jfsRootIno, "cli-rm", "/cli-rm/")
	subIno := mkdirJfs(cliIno, "sub", "/cli-rm/sub/")
	mknodFile(cliIno, "a.db", "/cli-rm/a.db", 55)
	mknodFile(subIno, "c.db", "/cli-rm/sub/c.db", 56)
	for _, name := range []string{"cli-rm", "sub", "a.db", "c.db"} {
		if !jfsEdgeNames()[name] {
			t.Fatalf("fixture missing jfs edge %q", name)
		}
	}

	// fs rm -r: the server-side delete must take the jfs subtree with it.
	if _, err := s.DeleteDirRecursive(ctx, "/cli-rm/"); err != nil {
		t.Fatal(err)
	}
	edges := jfsEdgeNames()
	for _, name := range []string{"cli-rm", "sub", "a.db", "c.db"} {
		if edges[name] {
			t.Fatalf("jfs edge %q survived fs rm -r", name)
		}
	}
	var nodes int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM jfs_node`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 {
		t.Fatalf("jfs_node count=%d after fs rm -r, want only the root", nodes)
	}
	expireBlockGCGrace(t, s)
	keys, err := s.ListPendingBlockGC(ctx, 32)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"55_0_4", "56_0_4"} {
		found := false
		for _, k := range keys {
			if strings.Contains(k, want) {
				found = true
			}
		}
		if !found {
			t.Fatalf("fs rm -r did not queue block %s: %v", want, keys)
		}
	}

	// fs mv: one edge update moves the subtree, and the old name disappears.
	insertDir("/ren-a/", "/", "ren-a")
	renIno := mkdirJfs(jfsRootIno, "ren-a", "/ren-a/")
	mknodFile(renIno, "keep.db", "/ren-a/keep.db", 57)
	if _, err := s.RenameDir(ctx, "/ren-a/", "/ren-b/"); err != nil {
		t.Fatal(err)
	}
	edges = jfsEdgeNames()
	if edges["ren-a"] {
		t.Fatal("jfs edge ren-a survived the rename")
	}
	if !edges["ren-b"] || !edges["keep.db"] {
		t.Fatalf("rename lost jfs edges: %v", edges)
	}

	// rmdir of an empty directory must drop its own jfs edge, so recreating
	// the same name and removing it again works (case G).
	insertDir("/gone/", "/", "gone")
	goneIno := mkdirJfs(jfsRootIno, "gone", "/gone/")
	if goneIno == 0 {
		t.Fatal("mkdir returned inode 0")
	}
	if err := s.DeleteEmptyDir(ctx, "/gone/"); err != nil {
		t.Fatal(err)
	}
	if jfsEdgeNames()["gone"] {
		t.Fatal("jfs edge gone survived rmdir")
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
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", mkdir, nil); err != nil || errno != 0 {
		t.Fatalf("mkdir errno=%d err=%v", errno, err)
	}
	create, _ := json.Marshal(map[string]any{
		"parent": 10, "name": "a.db", "type": 1, "mode": 0644,
		"inode": 11, "proj_path": "/d/a.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 10, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if _, err := s.RenameDir(ctx, "/d/", "/e/"); err != nil {
		t.Fatal(err)
	}
	// P0-3: the server-side directory rename moves the jfs edge in the same
	// transaction, so the old name must be gone before any FUSE-side step.
	var edgeParent uint64
	var edgeName []byte
	if err := s.db.QueryRow(`SELECT parent, name FROM jfs_edge WHERE inode = 10`).Scan(&edgeParent, &edgeName); err != nil {
		t.Fatal(err)
	}
	if edgeParent != jfsRootIno || string(edgeName) != "e" {
		t.Fatalf("jfs edge parent=%d name=%q after rename, want (1,e)", edgeParent, edgeName)
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

func TestExtentWriteHonorsQuotaLimits(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	body, errno, err := s.RunExtentMetaOp(ctx, "mknod", mustJSON(map[string]any{
		"parent": 1, "name": "quota.db", "type": 1, "mode": 0644,
		"inode": 5, "proj_path": "/quota.db", "uid": 0, "gid": 0,
		"indx": 0,
		"parts": []map[string]any{
			{"off": 0, "slice": map[string]any{"Id": 77, "Size": 4, "Off": 0, "Len": 4}},
		},
	}), nil)
	if err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v body=%s", errno, err, body)
	}
	write := func(off uint32, sliceID uint64, quota *ExtentQuotaLimit) int {
		t.Helper()
		_, errno, err := s.RunExtentMetaOp(ctx, "write", mustJSON(map[string]any{
			"inode": 5, "indx": 0, "off": off,
			"slice": map[string]any{"Id": sliceID, "Size": 4, "Off": 0, "Len": 4},
			"mtime": time.Now(),
		}), quota)
		if err != nil {
			t.Fatalf("write err=%v", err)
		}
		return errno
	}
	// Growing the file to 12 bytes (off 8 + 4) past the per-file limit is
	// refused before the slice is recorded.
	if got := write(8, 78, &ExtentQuotaLimit{MaxFileSizeBytes: 6}); got != int(syscall.EFBIG) {
		t.Fatalf("file-size-limited write errno=%d, want EFBIG", got)
	}
	// Growth past the tenant storage limit is refused too: the file is 4 bytes
	// and the write adds 8, so used + growth (8) exceeds the 6-byte limit.
	if got := write(8, 79, &ExtentQuotaLimit{MaxStorageBytes: 6, UsedBytes: 0}); got != int(syscall.EDQUOT) {
		t.Fatalf("storage-limited write errno=%d, want EDQUOT", got)
	}
	// A write that stays inside both limits succeeds, and an overwrite (no
	// growth) is not charged again.
	if got := write(8, 80, &ExtentQuotaLimit{MaxFileSizeBytes: 64, MaxStorageBytes: 64}); got != 0 {
		t.Fatalf("admitted write errno=%d, want 0", got)
	}
	if got := write(0, 81, &ExtentQuotaLimit{MaxFileSizeBytes: 4, MaxStorageBytes: 4}); got != 0 {
		t.Fatalf("overwrite errno=%d, want 0 (no growth, no admission)", got)
	}
}

func TestExtentUsageBytesAndDelta(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", mustJSON(map[string]any{
		"parent": 1, "name": "usage.db", "type": 1, "mode": 0644,
		"inode": 6, "proj_path": "/usage.db", "uid": 0, "gid": 0,
		"indx": 0,
		"parts": []map[string]any{
			{"off": 0, "slice": map[string]any{"Id": 90, "Size": 10, "Off": 0, "Len": 10}},
		},
	}), nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// Nothing has been reported yet, so the whole total is unreported — the term
	// a write admission adds to the central counters.
	unreported, err := s.ExtentUnreportedUsageBytes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if unreported != 10 {
		t.Fatalf("unreported extent usage=%d, want 10 (the jfs length, not the 0 projection size)", unreported)
	}
	// One more name for the same bytes is still one file's bytes, and a mirrored
	// directory holds none: counting file_nodes rows would charge the hardlink
	// twice and every mirrored directory an extra 4096, inflating both the
	// admission limit and the central counters the worker reports into.
	if err := s.LinkFileNode(ctx, "/usage.db", "/alias.db", "/", "alias.db", "link-usage", time.Now()); err != nil {
		t.Fatalf("hardlink: %v", err)
	}
	if err := s.InsertNode(ctx, &FileNode{
		NodeID: "dir-usage", Path: "/dusage/", ParentPath: "/", Name: "dusage",
		IsDirectory: true, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", mustJSON(map[string]any{
		"parent": 1, "name": "dusage", "type": 2, "mode": 0755, "proj_path": "/dusage/",
	}), nil); err != nil || errno != 0 {
		t.Fatalf("mkdir errno=%d err=%v", errno, err)
	}
	s.invalidateCachedExtentUsage()
	if got, err := s.ExtentUnreportedUsageBytes(ctx); err != nil {
		t.Fatal(err)
	} else if got != 10 {
		t.Fatalf("unreported usage with a hardlink alias and a mirrored directory = %d, want 10", got)
	}
	// Peek does not advance the marker, so the same delta stays pending until
	// the caller's report lands and Commit records the total. That is what
	// makes a failed report retryable instead of silently lost.
	total, delta, err := s.PeekExtentUsageDelta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if delta != 10 {
		t.Fatalf("first delta=%d, want 10", delta)
	}
	if _, again, err := s.PeekExtentUsageDelta(ctx); err != nil {
		t.Fatal(err)
	} else if again != delta {
		t.Fatalf("uncommitted delta=%d, want %d (still pending)", again, delta)
	}
	if err := s.CommitExtentUsageDelta(ctx, total, delta); err != nil {
		t.Fatal(err)
	}
	if _, again, err := s.PeekExtentUsageDelta(ctx); err != nil {
		t.Fatal(err)
	} else if again != 0 {
		t.Fatalf("delta after commit=%d, want 0 (already reported)", again)
	}
	// The admission term follows the same marker: once the bytes are reported
	// the central counters carry them, so the unreported remainder is zero and
	// adding the total would count them twice.
	if got, err := s.ExtentUnreportedUsageBytes(ctx); err != nil {
		t.Fatal(err)
	} else if got != 0 {
		t.Fatalf("unreported usage after commit=%d, want 0", got)
	}
	// A delete moves the term the other way: the marker still holds the bytes
	// the central counters were told about, so the admission subtracts them
	// until the worker reports the negative delta. That cancels the central
	// over-count instead of charging a tenant for a file it no longer has.
	if _, err := s.DB().Exec(`DELETE FROM file_nodes WHERE path IN (?, ?)`, "/usage.db", "/alias.db"); err != nil {
		t.Fatal(err)
	}
	// The admission term is cached for extentUsageCacheTTL, so let the entry that
	// the commit above stored expire before asking again.
	time.Sleep(extentUsageCacheTTL + 50*time.Millisecond)
	if got, err := s.ExtentUnreportedUsageBytes(ctx); err != nil {
		t.Fatal(err)
	} else if got != -10 {
		t.Fatalf("unreported usage after the file went away=%d, want -10", got)
	}
}

// The marker advance must not be an unconditional upsert: a reporter that peeked
// an older marker (a shard-ring transition handing the tenant to another pod
// mid-report) would then write its stale total over the newer one and make the
// difference look unreported for ever.
func TestCommitExtentUsageDeltaKeepsNewerMarker(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	marker := func() int64 {
		t.Helper()
		var got int64
		if err := s.DB().QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, extentUsageReportedKey).Scan(&got); err != nil {
			t.Fatal(err)
		}
		return got
	}
	// A reporter that peeked nothing at all (fresh tenant) creates the marker.
	if err := s.CommitExtentUsageDelta(ctx, 100, 100); err != nil {
		t.Fatal(err)
	}
	if got := marker(); got != 100 {
		t.Fatalf("marker after the first report = %d, want 100", got)
	}
	// A second reporter that read (total 0, delta 0) before the first one landed
	// must leave the newer marker alone.
	if err := s.CommitExtentUsageDelta(ctx, 0, 0); err != nil {
		t.Fatal(err)
	}
	if got := marker(); got != 100 {
		t.Fatalf("marker after a stale report = %d, want 100 (the newer total must survive)", got)
	}
	// A reporter whose own peek is still current advances it.
	if err := s.CommitExtentUsageDelta(ctx, 300, 200); err != nil {
		t.Fatal(err)
	}
	if got := marker(); got != 300 {
		t.Fatalf("marker after a current report = %d, want 300", got)
	}
}

// The shard-ownership gate keeps one owner per tenant, but ownership is a hash
// ring: a transition can hand the tenant over while a report is in flight, and
// two pods would then push the same delta into relative central counters that
// nothing recomputes. The lease is the cross-pod serializer for that window.
func TestExtentUsageReportLeaseSerializesReporters(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	token, ok, err := s.ClaimExtentUsageReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || token == 0 {
		t.Fatalf("first claim = (%d, %v), want a token", token, ok)
	}
	// A second reporter — another pod against the same tenant — must be refused
	// while the lease is live.
	if _, ok, err := s.ClaimExtentUsageReport(ctx); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("a second reporter claimed the lease while it was held")
	}
	if err := s.ReleaseExtentUsageReport(ctx, token); err != nil {
		t.Fatal(err)
	}
	next, ok, err := s.ClaimExtentUsageReport(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("the lease was not claimable after its release")
	}
	// A stale release must not clear a lease that has since been taken over.
	if err := s.ReleaseExtentUsageReport(ctx, token); err != nil {
		t.Fatal(err)
	}
	var held int64
	if err := s.DB().QueryRow(`SELECT value FROM jfs_counter WHERE name = ?`, extentQuotaReportLeaseKey).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held != next {
		t.Fatalf("a stale release cleared the live lease: value=%d, want %d", held, next)
	}
	// An expired lease is takeable again: a reporter that died mid-report must
	// not block the tenant's quota reporting for ever.
	if _, err := s.DB().Exec(`UPDATE jfs_counter SET value = 1 WHERE name = ?`, extentQuotaReportLeaseKey); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := s.ClaimExtentUsageReport(ctx); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("an expired lease must be claimable")
	}
}

func TestExtentLocksAreAuthoritativeAcrossRuntimes(t *testing.T) {
	s := newTestStore(t)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/locked.db")

	a := newExtentRuntime(t, s, 0)
	b := newExtentRuntime(t, s, 0)
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := a.Meta.Create(ctx, jfsmeta.RootInode, "locked.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}

	// Two runtimes are two mounts of the same filesystem: a record lock taken
	// by one must exclude the other through the metadata service, not through
	// a per-process map.
	if st := a.Meta.Setlk(ctx, ino, 1, false, syscall.F_WRLCK, 0, 100, 42); st != 0 {
		t.Fatalf("A setlk write: %v", st)
	}
	if st := b.Meta.Setlk(ctx, ino, 1, false, syscall.F_WRLCK, 0, 100, 43); st != syscall.EAGAIN {
		t.Fatalf("B setlk write on A's lock = %v, want EAGAIN", st)
	}
	if st := b.Meta.Setlk(ctx, ino, 1, false, syscall.F_RDLCK, 50, 60, 43); st != syscall.EAGAIN {
		t.Fatalf("B setlk read overlapping A's write = %v, want EAGAIN", st)
	}
	// A disjoint range is free.
	if st := b.Meta.Setlk(ctx, ino, 2, false, syscall.F_RDLCK, 200, 300, 43); st != 0 {
		t.Fatalf("B setlk read disjoint range: %v", st)
	}
	// Getlk reports A's conflicting lock to B.
	ltype, start, end := uint32(syscall.F_WRLCK), uint64(0), uint64(100)
	var pid uint32
	if st := b.Meta.Getlk(ctx, ino, 9, &ltype, &start, &end, &pid); st != 0 {
		t.Fatalf("B getlk: %v", st)
	}
	if ltype != syscall.F_WRLCK || start != 0 || end != 100 {
		t.Fatalf("getlk = type %d [%d,%d], want write [0,100]", ltype, start, end)
	}
	// Flock excludes across runtimes too.
	if st := a.Meta.Flock(ctx, ino, 1, syscall.F_WRLCK, false); st != 0 {
		t.Fatalf("A flock write: %v", st)
	}
	if st := b.Meta.Flock(ctx, ino, 1, syscall.F_WRLCK, false); st != syscall.EAGAIN {
		t.Fatalf("B flock write on A's flock = %v, want EAGAIN", st)
	}
	// After A releases both, B can take them.
	if st := a.Meta.Setlk(ctx, ino, 1, false, syscall.F_UNLCK, 0, 100, 42); st != 0 {
		t.Fatalf("A unlock: %v", st)
	}
	if st := a.Meta.Flock(ctx, ino, 1, syscall.F_UNLCK, false); st != 0 {
		t.Fatalf("A funlock: %v", st)
	}
	if st := b.Meta.Setlk(ctx, ino, 1, false, syscall.F_WRLCK, 0, 100, 43); st != 0 {
		t.Fatalf("B setlk after A unlock: %v", st)
	}
	if st := b.Meta.Flock(ctx, ino, 1, syscall.F_WRLCK, false); st != 0 {
		t.Fatalf("B flock after A funlock: %v", st)
	}
}

// A chunk that only ever grows through truncates must reach compaction too. The
// enqueue used to be write-path only, so a log/journal file truncated
// repeatedly accumulated zero slices in one chunk forever, and every read had
// to parse an ever-longer slice list.
func TestTruncateEnqueuesCompactWhenChunkGrows(t *testing.T) {
	s := newTestStore(t)
	restore := SetCompactThresholdForTest(3)
	t.Cleanup(restore)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "log.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/log.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	truncate := func(length uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"inode": 2, "length": length})
		if _, errno, err := s.RunExtentMetaOp(ctx, "truncate", body, nil); err != nil || errno != 0 {
			t.Fatalf("truncate to %d errno=%d err=%v", length, errno, err)
		}
	}
	// Grow and shrink across one chunk boundary repeatedly: each step appends
	// zero slices to chunk 0 until it crosses the threshold.
	for i := uint64(0); i < 4; i++ {
		truncate(2 * ExtentChunkSize)
		truncate(1)
	}
	var queued int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM slice_compact_tasks WHERE extent_ino = 2 AND chunk = 0`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued == 0 {
		t.Fatal("no compact task queued for a chunk grown past the threshold by truncates")
	}
}

// A size change can arrive through setattr rather than truncate (a direct meta
// call with set&8, or the VFS's SetAttr that follows Truncate). The reply must
// carry the chunks the post-commit enqueue inspects, or a chunk grown past the
// threshold that way is never scheduled for compaction — the only valve there
// is.
func TestSetattrSizeEnqueuesCompactWhenChunkGrows(t *testing.T) {
	s := newTestStore(t)
	restore := SetCompactThresholdForTest(3)
	t.Cleanup(restore)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "sa.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/sa.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// FATTR_SIZE == 1<<3, the setattr bit jfsSetAttrTx routes through truncate.
	truncate := func(length uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{"inode": 2, "length": length})
		if _, errno, err := s.RunExtentMetaOp(ctx, "truncate", body, nil); err != nil || errno != 0 {
			t.Fatalf("truncate errno=%d err=%v", errno, err)
		}
	}
	setattrSize := func(length uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"inode": 2, "set": uint16(1 << 3),
			"attr": ExtentAttr{Typ: 1, Length: length, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "setattr", body, nil); err != nil || errno != 0 {
			t.Fatalf("setattr size=%d errno=%d err=%v", length, errno, err)
		}
	}
	// Every other step is a setattr, so only its enqueue can queue the task.
	for i := uint64(0); i < 4; i++ {
		setattrSize(2 * ExtentChunkSize)
		setattrSize(1)
	}
	_ = truncate
	var queued int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM slice_compact_tasks WHERE extent_ino = 2 AND chunk = 0`).Scan(&queued); err != nil {
		t.Fatal(err)
	}
	if queued == 0 {
		t.Fatal("no compact task queued for a chunk grown past the threshold through setattr")
	}
}

// A compact executor that loses the CAS is in the same end state as the winner:
// the origin it collected no longer matches because the chunk is already
// compacted. That is success, not a failure — reporting EINVAL made both
// executors requeue a finished task, burn one of its attempts and log a failure
// that never happened.
func TestExtentCompactCASLostIsSuccess(t *testing.T) {
	s := newTestStore(t)
	restore := SetCompactThresholdForTest(3)
	t.Cleanup(restore)
	rt := newExtentRuntime(t, s, 0)
	ctx := jfsmeta.NewContext(1, 0, []uint32{0})
	ctx = ctx.WithValue(jfsmeta.Drive9PathKey, "/caslost.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "caslost.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	w := rt.Writer.Open(ino, 0, 0)
	for i := 0; i < 6; i++ {
		if st := w.Write(ctx, uint64(i*1024), bytes.Repeat([]byte{byte('A' + i)}, 1024)); st != 0 {
			t.Fatalf("write %d: %v", i, st)
		}
		if st := w.Flush(ctx); st != 0 {
			t.Fatalf("flush %d: %v", i, st)
		}
	}
	_ = w.Close(ctx)

	cino, indx, taskID, err := s.ClaimCompactTask(context.Background())
	if err != nil || taskID == "" {
		t.Fatalf("claim compact task: ino=%d task=%q err=%v", cino, taskID, err)
	}
	restoreCommit := extent.SetCommitCompactForTest(func(jfsmeta.Meta, jfsmeta.Ino, uint32, []byte, int, uint32, uint64, uint32) syscall.Errno {
		return syscall.EINVAL
	})
	t.Cleanup(restoreCommit)

	if err := extent.ExecuteCompact(context.Background(), rt, cino, indx); err != nil {
		t.Fatalf("a CAS-lost compact must report success, got %v", err)
	}
}
