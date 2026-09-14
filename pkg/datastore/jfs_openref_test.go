package datastore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/extent"
)

// Cross-runtime unlink of a file another runtime holds open must keep the jfs
// node and its blocks alive until that handle closes. "Still open" is decided
// from the metadata-side holder rows (open_ref on first open), not from the
// unlinking runtime's local handle table: before that, the unlinking runtime's
// view was the only one consulted, the node was reclaimed under the holder's
// fd, and every later Fsync on that fd returned EBADF.
//
// This drives two real JuiceFS runtimes (two sessions) against one store, the
// same shape as a FUSE mount deleting a file a different mount holds open.
func TestCrossRuntimeUnlinkKeepsOpenHandleAlive(t *testing.T) {
	s := newTestStore(t)
	rtA := newExtentRuntime(t, s, 0)
	rtB := newExtentRuntime(t, s, 0)
	t.Cleanup(func() { _ = extent.CloseRuntime(rtA) })
	t.Cleanup(func() { _ = extent.CloseRuntime(rtB) })

	ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/cross.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rtA.Meta.Create(ctx, jfsmeta.RootInode, "cross.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	w := rtA.Writer.Open(ino, 0, 0)
	if st := w.Write(ctx, 0, bytes.Repeat([]byte("x"), 512)); st != 0 {
		t.Fatalf("write: %v", st)
	}
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush: %v", st)
	}
	// RELEASE of the create-time handle, as the kernel sends it: baseMeta
	// marks a created inode open, and this close balances that.
	if st := rtA.Meta.Close(ctx, ino); st != 0 {
		t.Fatalf("close of the create handle on A: %v", st)
	}

	// Runtime A opens a handle. The engine registers the holder server-side;
	// the row is what a foreign unlink must see.
	if st := rtA.Meta.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("open on A: %v", st)
	}
	if holders := holderRows(t, s, uint64(ino)); holders != 1 {
		t.Fatalf("holder rows after open = %d, want 1", holders)
	}

	// Runtime B — a different session, its own handle table empty — unlinks.
	// A local-handle answer to "is it open" would be no; the metadata answer
	// must be yes.
	bctx := jfsmeta.NewContext(2, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/cross.db")
	if st := rtB.Meta.Unlink(bctx, jfsmeta.RootInode, "cross.db"); st != 0 {
		t.Fatalf("cross-runtime unlink: %v", st)
	}
	var nodes int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 {
		t.Fatalf("jfs_node rows after cross-runtime unlink = %d, want 1 (the holder keeps the data)", nodes)
	}
	// nlink straight from the row: the engine's open-file attr cache (1s)
	// would still serve the pre-unlink attribute for an inode this runtime
	// holds open, which is benign and not what this assertion is about.
	var nlink uint32
	if err := s.DB().QueryRow(`SELECT nlink FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nlink); err != nil {
		t.Fatal(err)
	}
	if nlink != 0 {
		t.Fatalf("nlink after foreign unlink = %d, want 0 (name gone, inode sustained)", nlink)
	}

	// The open handle still commits data — the EBADF symptom of the bug was a
	// Fsync on exactly this state.
	if st := w.Write(ctx, 4096, bytes.Repeat([]byte("y"), 512)); st != 0 {
		t.Fatalf("write through the held handle: %v", st)
	}
	if st := w.Flush(ctx); st != 0 {
		t.Fatalf("flush (fsync) through the held handle: %v", st)
	}
	_ = w.Close(ctx)

	// The last close releases the holder, and the inode is reclaimed then:
	// node gone, blocks queued. Reclamation before this point is the bug;
	// never reclaiming is only a leak.
	if st := rtA.Meta.Close(ctx, ino); st != 0 {
		t.Fatalf("close on A: %v", st)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 {
		t.Fatalf("jfs_node rows after the last close = %d, want 0", nodes)
	}
	// The delete_sustained op drains the reclamation it queued inline
	// (delfile row consumed in the same pass), so the durable evidence is the
	// block GC queue holding this inode's now-unreferenced slices.
	var blocks int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM block_gc_tasks WHERE extent_ino = ?`, uint64(ino)).Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if blocks == 0 {
		t.Fatal("block_gc_tasks rows after the last close = 0, want the inode's unreferenced blocks queued")
	}
}

// A second holder extends the lifetime past the first close: each session's
// row is independent, and only the last one may reclaim.
func TestCrossRuntimeLastCloseWinsAcrossSessions(t *testing.T) {
	s := newTestStore(t)
	rtA := newExtentRuntime(t, s, 0)
	rtB := newExtentRuntime(t, s, 0)
	t.Cleanup(func() { _ = extent.CloseRuntime(rtA) })
	t.Cleanup(func() { _ = extent.CloseRuntime(rtB) })

	actx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/two.db")
	bctx := jfsmeta.NewContext(2, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/two.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rtA.Meta.Create(actx, jfsmeta.RootInode, "two.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	if st := rtA.Meta.Close(actx, ino); st != 0 {
		t.Fatalf("close of the create handle on A: %v", st)
	}
	var aAttr jfsmeta.Attr
	if st := rtA.Meta.Open(actx, ino, syscall.O_RDONLY, &aAttr); st != 0 {
		t.Fatalf("open on A: %v", st)
	}
	var bAttr jfsmeta.Attr
	if st := rtB.Meta.Open(bctx, ino, syscall.O_RDONLY, &bAttr); st != 0 {
		t.Fatalf("open on B: %v", st)
	}
	if st := rtB.Meta.Unlink(bctx, jfsmeta.RootInode, "two.db"); st != 0 {
		t.Fatalf("unlink on B: %v", st)
	}

	// A closes; B still holds, so the node must survive A's close.
	if st := rtA.Meta.Close(actx, ino); st != 0 {
		t.Fatalf("close on A: %v", st)
	}
	var nodes int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 {
		t.Fatalf("jfs_node rows after the first holder closed = %d, want 1 (one holder left)", nodes)
	}

	// The last close reclaims.
	if st := rtB.Meta.Close(bctx, ino); st != 0 {
		t.Fatalf("close on B: %v", st)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 {
		t.Fatalf("jfs_node rows after the last holder closed = %d, want 0", nodes)
	}
}

// The control for the holder behavior: with nobody holding the inode, an
// unlink reclaims immediately (node gone, blocks queued) — the holder rows
// must not turn every delete into a session-lifetime leak.
func TestUnlinkWithoutHoldersStillReclaimsImmediately(t *testing.T) {
	s := newTestStore(t)
	rt := newExtentRuntime(t, s, 0)
	t.Cleanup(func() { _ = extent.CloseRuntime(rt) })

	ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/cold.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "cold.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	// Nobody holds the file: the create handle was released (the kernel's
	// RELEASE), so the unlink must reclaim immediately.
	if st := rt.Meta.Close(ctx, ino); st != 0 {
		t.Fatalf("close of the create handle: %v", st)
	}
	if st := rt.Meta.Unlink(ctx, jfsmeta.RootInode, "cold.db"); st != 0 {
		t.Fatalf("unlink: %v", st)
	}
	var nodes int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 {
		t.Fatalf("jfs_node rows after an unheld unlink = %d, want 0 (immediate reclaim)", nodes)
	}
}

// A crashed holder is the stale-session sweep's case, not a leak: when the
// session expires, the sweep removes its rows and reclaims what they held.
// This drives the sweep's transaction helpers directly (jfsCleanStaleSessionTx)
// the way SweepStaleExtentSessions does.
func TestStaleSessionSweepReleasesDeadHolders(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "orphan.db", "type": 1, "mode": 0644,
		"inode": 77, "proj_path": "/orphan.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// A dead session's holder row plus a session2 row that is already stale.
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT IGNORE INTO jfs_sustained (sid, inode) VALUES (?, ?)`, 4242, 77); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jfs_session2 (sid, expire, info) VALUES (?, ?, NULL)`, 4242, time.Now().Add(-time.Hour).Unix())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// The holder keeps the inode alive through an unlink with no local handle.
	if err := s.UnlinkExtentPath(ctx, "/orphan.db", false); err != nil {
		t.Fatalf("unlink with a foreign holder: %v", err)
	}
	var nodes int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 77`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 1 {
		t.Fatalf("jfs_node rows while a (dead) holder row exists = %d, want 1", nodes)
	}
	// The sweep ends the dead session and reclaims the sustained inode.
	if _, err := s.SweepStaleExtentSessions(ctx, 16); err != nil {
		t.Fatal(err)
	}
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 77`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 {
		t.Fatalf("jfs_node rows after the stale-session sweep = %d, want 0 (dead holders must not leak)", nodes)
	}
}

// Two sessions holding the same inode must not both skip reclaim when they
// close concurrently: a snapshot count lets each transaction see the other's
// row survive, both commit without reclaiming, and a nlink=0 node with no
// edge, no sustained row and no delfile record is stranded — nothing walks it
// again. The FOR UPDATE count serializes the closers (one blocks on the
// other's deleted row, then sees zero and reclaims); the loser may hit a
// deadlock, which the engine's retry loop resolves and this test mirrors.
func TestConcurrentLastClosesReclaimExactlyOnce(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "race.db", "type": 1, "mode": 0644,
		"inode": 909, "proj_path": "/race.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// Two foreign holders and an unlink that defers to them: nlink=0 with
	// two sustained rows.
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		for _, sid := range []uint64{501, 502} {
			if eno, err := s.jfsOpenRefTx(tx, sid, 909); err != nil || eno != 0 {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.UnlinkExtentPath(ctx, "/race.db", false); err != nil {
		t.Fatalf("unlink: %v", err)
	}

	closeAs := func(sid uint64) error {
		for attempt := 0; ; attempt++ {
			err := s.InTx(ctx, func(tx *sql.Tx) error {
				return s.jfsDeleteSustainedTx(tx, sid, 909)
			})
			if err == nil {
				return nil
			}
			if attempt < 8 && (strings.Contains(err.Error(), "Deadlock") ||
				strings.Contains(err.Error(), "40001") || strings.Contains(err.Error(), "1213")) {
				continue
			}
			return err
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, sid := range []uint64{501, 502} {
		wg.Add(1)
		go func(sid uint64) {
			defer wg.Done()
			errs <- closeAs(sid)
		}(sid)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent close: %v", err)
		}
	}
	var nodes, delfile int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = 909`).Scan(&nodes); err != nil {
		t.Fatal(err)
	}
	sustained := holderRows(t, s, 909)
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_delfile WHERE inode = 909`).Scan(&delfile); err != nil {
		t.Fatal(err)
	}
	if nodes != 0 || sustained != 0 {
		t.Fatalf("after concurrent last closes: nodes=%d sustained=%d, want 0/0 (no stranded inode)", nodes, sustained)
	}
	if delfile != 1 {
		t.Fatalf("delfile rows after concurrent last closes = %d, want 1 (reclaimed exactly once)", delfile)
	}
}

// deleteSustainedFault models the two indistinguishable failures of a holder
// release: the RPC can fail before the server transaction commits, or the
// transaction can commit and the response get lost. The client cannot tell
// them apart, which is exactly what the release path must survive.
type deleteSustainedFault struct {
	mu              sync.Mutex
	mode            int    // 0 none, 1 fail-before-commit, 2 commit-then-drop-response
	deliveredByKind [2]int // faults actually delivered, per kind, so the churn test can prove it exercised both
}

const (
	faultNone             = 0
	faultFailBeforeCommit = 1
	faultDropAfterCommit  = 2
)

func (f *deleteSustainedFault) take() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	mode := f.mode
	f.mode = faultNone // one-shot, like a real transient fault
	if mode != faultNone {
		f.deliveredByKind[mode-1]++
	}
	return mode
}

func (f *deleteSustainedFault) deliveredOfKind(mode int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deliveredByKind[mode-1]
}

func (f *deleteSustainedFault) arm(mode int) {
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
}

// newFaultExtentRuntime is newExtentRuntime with delete_sustained responses
// subject to the armed fault.
func newFaultExtentRuntime(t *testing.T, s *Store, fault *deleteSustainedFault) *extent.Runtime {
	t.Helper()
	dir := t.TempDir()
	tr := extent.NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		if op == "delete_sustained" {
			switch fault.take() {
			case faultFailBeforeCommit:
				return nil, int(syscall.EIO), errors.New("simulated failure before commit")
			case faultDropAfterCommit:
				// The server transaction runs and commits; only the response
				// is lost — the client's error does not mean the row survived.
				// If the server op itself failed, that failure propagates as
				// itself (nothing committed, nothing to drop).
				if _, eno, rerr := s.RunExtentMetaOp(ctx, op, raw, nil); rerr != nil || eno != 0 {
					t.Logf("dropAfterCommit: server op failed before committing (eno=%d err=%v); propagating the real failure", eno, rerr)
					return nil, eno, rerr
				}
				return nil, int(syscall.EIO), errors.New("simulated response loss after commit")
			}
		}
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
	rt, err := extent.NewRuntime(extent.RuntimeConfig{Transport: tr, Storage: st})
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func holderRows(t *testing.T, s *Store, ino uint64) int {
	t.Helper()
	var n int
	if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_sustained WHERE inode = ?`, ino).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The three close outcomes a release must survive. The response-lost case is
// the P1: the client used to cache "registered" across a failed release, the
// re-open skipped its registration, and the inode was reclaimed under the
// live fd (Fsync → EBADF). After the fix, a failed release invalidates the
// cached state and the next Open re-registers, so the server-side holder is
// valid again in all three outcomes.
func TestReleaseUncertaintyKeepsHolderValidOnReopen(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode int
		// rowsAfterClose: what the server really holds once the faulted
		// close finishes (1 when the delete never committed, 0 when it did).
		rowsAfterClose int
	}{
		{"normal release", faultNone, 0},
		{"failure before commit", faultFailBeforeCommit, 1},
		{"response lost after commit", faultDropAfterCommit, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)
			fault := &deleteSustainedFault{}
			rtA := newFaultExtentRuntime(t, s, fault)
			rtB := newExtentRuntime(t, s, 0)
			t.Cleanup(func() { _ = extent.CloseRuntime(rtA) })
			t.Cleanup(func() { _ = extent.CloseRuntime(rtB) })

			ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/uncertain.db")
			var ino jfsmeta.Ino
			var attr jfsmeta.Attr
			if st := rtA.Meta.Create(ctx, jfsmeta.RootInode, "uncertain.db", 0644, 0, 0, &ino, &attr); st != 0 {
				t.Fatalf("create: %v", st)
			}
			// RELEASE of the create-time handle, faulted per the scenario.
			if tc.mode != faultNone {
				fault.arm(tc.mode)
			}
			if st := rtA.Meta.Close(ctx, ino); st != 0 {
				t.Fatalf("close of the create handle: %v", st)
			}
			if got := holderRows(t, s, uint64(ino)); got != tc.rowsAfterClose {
				t.Fatalf("holder rows after the faulted close = %d, want %d", got, tc.rowsAfterClose)
			}

			// The re-open must end with a valid server-side holder whatever
			// the release really did — this is the step the stale cache broke.
			if st := rtA.Meta.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
				t.Fatalf("re-open: %v", st)
			}
			if got := holderRows(t, s, uint64(ino)); got != 1 {
				t.Fatalf("holder rows after the re-open = %d, want 1 (registration must not be skipped)", got)
			}

			// And the holder must actually protect: a foreign unlink defers,
			// the open fd keeps committing, the last close reclaims.
			bctx := jfsmeta.NewContext(2, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/uncertain.db")
			if st := rtB.Meta.Unlink(bctx, jfsmeta.RootInode, "uncertain.db"); st != 0 {
				t.Fatalf("foreign unlink: %v", st)
			}
			var nodes int
			if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nodes); err != nil {
				t.Fatal(err)
			}
			if nodes != 1 {
				t.Fatalf("jfs_node rows after foreign unlink = %d, want 1 (the holder protects)", nodes)
			}
			w := rtA.Writer.Open(ino, 0, 0)
			if st := w.Write(ctx, 0, bytes.Repeat([]byte("z"), 64)); st != 0 {
				t.Fatalf("write through the held handle: %v", st)
			}
			if st := w.Flush(ctx); st != 0 {
				t.Fatalf("flush (fsync) through the held handle: %v", st)
			}
			_ = w.Close(ctx)
			if st := rtA.Meta.Close(ctx, ino); st != 0 {
				t.Fatalf("last close: %v", st)
			}
			if err := s.DB().QueryRow(`SELECT COUNT(*) FROM jfs_node WHERE inode = ?`, uint64(ino)).Scan(&nodes); err != nil {
				t.Fatal(err)
			}
			if nodes != 0 {
				t.Fatalf("jfs_node rows after the last close = %d, want 0", nodes)
			}
		})
	}
}

// Close/Open churn with intermittent release faults must never end with a
// live fd and no server-side holder.
func TestConcurrentCloseOpenWithReleaseFaults(t *testing.T) {
	s := newTestStore(t)
	fault := &deleteSustainedFault{}
	rt := newFaultExtentRuntime(t, s, fault)
	t.Cleanup(func() { _ = extent.CloseRuntime(rt) })

	ctx := jfsmeta.NewContext(1, 0, []uint32{0}).WithValue(jfsmeta.Drive9PathKey, "/churn.db")
	var ino jfsmeta.Ino
	var attr jfsmeta.Attr
	if st := rt.Meta.Create(ctx, jfsmeta.RootInode, "churn.db", 0644, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("create: %v", st)
	}
	// RELEASE of the create-time handle: without it the refcount never
	// reaches zero and no churn close is ever a last close.
	if st := rt.Meta.Close(ctx, ino); st != 0 {
		t.Fatalf("close of the create handle: %v", st)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	armorDone := make(chan struct{})
	// A fault armorer: alternate the two fault kinds while the churn runs. It
	// waits on its own channel — joining the workers' WaitGroup would only be
	// stoppable after Wait returned, and Wait would wait on it forever.
	go func() {
		defer close(armorDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				fault.arm(faultFailBeforeCommit)
			} else {
				fault.arm(faultDropAfterCommit)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var a jfsmeta.Attr
			for i := 0; i < 25; i++ {
				if st := rt.Meta.Open(ctx, ino, syscall.O_RDWR, &a); st != 0 {
					t.Errorf("open during churn: %v", st)
					return
				}
				if st := rt.Meta.Close(ctx, ino); st != 0 {
					t.Errorf("close during churn: %v", st)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(stop)
	<-armorDone
	for _, mode := range []int{faultFailBeforeCommit, faultDropAfterCommit} {
		if got := fault.deliveredOfKind(mode); got < 1 {
			t.Fatalf("faults of kind %d delivered during churn = %d, want >= 1 (the churn must exercise both failure shapes)", mode, got)
		}
	}
	// Disarm the fault injector first: an armed fault consumed by the final
	// close would legitimately leave the row (unknown outcome), which is the
	// safe direction, not a bug.
	fault.arm(faultNone)
	// The final open must carry a valid holder.
	if st := rt.Meta.Open(ctx, ino, syscall.O_RDWR, &attr); st != 0 {
		t.Fatalf("final open: %v", st)
	}
	if got := holderRows(t, s, uint64(ino)); got != 1 {
		t.Fatalf("holder rows after churn = %d, want 1 (final open registered)", got)
	}
	if st := rt.Meta.Close(ctx, ino); st != 0 {
		t.Fatalf("final close: %v", st)
	}
	if got := holderRows(t, s, uint64(ino)); got != 0 {
		t.Fatalf("holder rows after the final clean close = %d, want 0", got)
	}
}
