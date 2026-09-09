package datastore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestExtentMetaConcurrentParentCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	const n = 8
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("f%d.db", i)
			body, _ := json.Marshal(map[string]any{
				"parent": 999, "name": name, "type": 1, "mode": 0644,
				"proj_path": "/run/" + name,
				"attr":      ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
			})
			_, errno, err := s.RunExtentMetaOp(ctx, "mknod", body)
			if err != nil {
				errCh <- err
				return
			}
			if errno != 0 {
				errCh <- fmt.Errorf("mknod %s errno=%d", name, errno)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		path := fmt.Sprintf("/run/f%d.db", i)
		if _, err := s.GetExtentProjection(ctx, path); err != nil {
			t.Fatalf("projection %s: %v", path, err)
		}
	}
}

func TestExtentMetaCreateUnlinkRecreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, err := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "type": 1, "mode": 0644, "cumask": 0,
		"inode": 2, "proj_path": "/foo.db", "uid": 0, "gid": 0,
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	body, errno, err := s.RunExtentMetaOp(ctx, "mknod", create)
	if err != nil {
		t.Fatal(err)
	}
	if errno != 0 {
		t.Fatalf("mknod errno=%d body=%s", errno, body)
	}
	proj, err := s.GetExtentProjection(ctx, "/foo.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ContentLayout != ContentLayoutExtent || proj.ExtentIno != 2 {
		t.Fatalf("projection = %+v", proj)
	}

	unlink, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "proj_path": "/foo.db", "opened": false,
	})
	if _, errno, err = s.RunExtentMetaOp(ctx, "unlink", unlink); err != nil || errno != 0 {
		t.Fatalf("unlink errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/foo.db"); err == nil {
		t.Fatal("projection still present after unlink")
	}

	create2, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "foo.db", "type": 1, "mode": 0644, "cumask": 0,
		"inode": 3, "proj_path": "/foo.db", "uid": 0, "gid": 0,
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err = s.RunExtentMetaOp(ctx, "mknod", create2); err != nil || errno != 0 {
		t.Fatalf("recreate errno=%d err=%v", errno, err)
	}
	proj, err = s.GetExtentProjection(ctx, "/foo.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 3 {
		t.Fatalf("recreate ino = %d, want 3", proj.ExtentIno)
	}

	if _, errno, err = s.RunExtentMetaOp(ctx, "mknod", create2); err != nil {
		t.Fatal(err)
	}
	if errno != int(syscall.EEXIST) {
		t.Fatalf("exclusive recreate errno=%d, want EEXIST", errno)
	}
}

func TestExtentMetaWriteUpdatesLength(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "blob", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/blob",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	write, _ := json.Marshal(map[string]any{
		"inode": 2, "indx": 0, "off": 0,
		"slice": ExtentSlice{Id: 1, Size: 4, Off: 0, Len: 4},
	})
	body, errno, err := s.RunExtentMetaOp(ctx, "write", write)
	if err != nil || errno != 0 {
		t.Fatalf("write errno=%d err=%v body=%s", errno, err, body)
	}
	getattr, _ := json.Marshal(map[string]any{"inode": 2})
	body, errno, err = s.RunExtentMetaOp(ctx, "getattr", getattr)
	if err != nil || errno != 0 {
		t.Fatalf("getattr errno=%d err=%v", errno, err)
	}
	var out struct {
		Attr ExtentAttr `json:"attr"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Attr.Length != 4 {
		t.Fatalf("length=%d, want 4", out.Attr.Length)
	}
}

func TestExtentMetaRollbackBetweenTrees(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	jfsTestFailBeforeProjection = func() error {
		return syscall.EIO
	}
	t.Cleanup(func() { jfsTestFailBeforeProjection = nil })
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "roll.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/roll.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	_, errno, err := s.RunExtentMetaOp(ctx, "mknod", create)
	if err == nil && errno == 0 {
		t.Fatal("expected rollback error")
	}
	if _, err := s.GetExtentProjection(ctx, "/roll.db"); err == nil {
		t.Fatal("projection committed after injected failure")
	}
	lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "roll.db"})
	body, errno, err := s.RunExtentMetaOp(ctx, "lookup", lookup)
	if err != nil {
		t.Fatal(err)
	}
	if errno != int(syscall.ENOENT) {
		t.Fatalf("lookup after rollback errno=%d body=%s, want ENOENT", errno, body)
	}
}

func TestExtentMetaRenameReplacesDestination(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mknod := func(name, path string, inode uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"parent": 1, "name": name, "type": 1, "mode": 0644,
			"inode": inode, "proj_path": path,
			"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", body); err != nil || errno != 0 {
			t.Fatalf("mknod %s errno=%d err=%v", name, errno, err)
		}
	}
	mknod("src.db", "/src.db", 2)
	mknod("dst.db", "/dst.db", 3)
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "src.db",
		"dst_parent": 1, "dst_name": "dst.db",
		"src_path": "/src.db", "dst_path": "/dst.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename); err != nil || errno != 0 {
		t.Fatalf("rename-replace errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/src.db"); err == nil {
		t.Fatal("source projection still present")
	}
	proj, err := s.GetExtentProjection(ctx, "/dst.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("replaced dest ino=%d, want 2", proj.ExtentIno)
	}
	lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "dst.db"})
	body, errno, err := s.RunExtentMetaOp(ctx, "lookup", lookup)
	if err != nil || errno != 0 {
		t.Fatalf("lookup dest errno=%d err=%v", errno, err)
	}
	var out struct {
		Inode uint64 `json:"inode"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Inode != 2 {
		t.Fatalf("lookup dest ino=%d, want 2", out.Inode)
	}
}

func TestExtentMetaRenameReplaceWhenDestHasProjectionButNoEdge(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	mknod := func(name, path string, inode uint64) {
		t.Helper()
		body, _ := json.Marshal(map[string]any{
			"parent": 1, "name": name, "type": 1, "mode": 0644,
			"inode": inode, "proj_path": path,
			"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
		})
		if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", body); err != nil || errno != 0 {
			t.Fatalf("mknod %s errno=%d err=%v", name, errno, err)
		}
	}
	mknod("src.db", "/src.db", 2)
	mknod("dst.db", "/dst.db", 3)
	if _, err := s.DB().Exec(`DELETE FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("dst.db")); err != nil {
		t.Fatal(err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "src.db",
		"dst_parent": 1, "dst_name": "dst.db",
		"src_path": "/src.db", "dst_path": "/dst.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename); err != nil || errno != 0 {
		t.Fatalf("rename-replace leftover dest projection errno=%d err=%v, want 0 (pjdfstest rename/09 dest without juicefs edge)", errno, err)
	}
	proj, err := s.GetExtentProjection(ctx, "/dst.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("replaced dest ino=%d, want 2", proj.ExtentIno)
	}
}

func TestExtentMetaRenameMovesProjectionByExtentIno(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "old.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/old.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "old.db",
		"dst_parent": 1, "dst_name": "new.db",
		"src_path": "/not-the-real-src.db", "dst_path": "/new.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename); err != nil || errno != 0 {
		t.Fatalf("rename with mismatched src_path errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/old.db"); err == nil {
		t.Fatal("old projection still present after extent_ino rename")
	}
	proj, err := s.GetExtentProjection(ctx, "/new.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("renamed ino=%d, want 2", proj.ExtentIno)
	}
}

func TestExtentMetaRenameUpdatesProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "old.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/old.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "old.db",
		"dst_parent": 1, "dst_name": "new.db",
		"src_path": "/old.db", "dst_path": "/new.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename); err != nil || errno != 0 {
		t.Fatalf("rename errno=%d err=%v", errno, err)
	}
	if _, err := s.GetExtentProjection(ctx, "/old.db"); err == nil {
		t.Fatal("old projection still present")
	}
	proj, err := s.GetExtentProjection(ctx, "/new.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("renamed ino=%d, want 2", proj.ExtentIno)
	}
	lookup, _ := json.Marshal(map[string]any{"parent": 1, "name": "new.db"})
	body, errno, err := s.RunExtentMetaOp(ctx, "lookup", lookup)
	if err != nil || errno != 0 {
		t.Fatalf("lookup errno=%d err=%v", errno, err)
	}
	var out struct {
		Inode uint64 `json:"inode"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Inode != 2 {
		t.Fatalf("lookup ino=%d", out.Inode)
	}
}

func TestExtentMetaFlockAndSetlkContend(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "lock.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/lock.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	flock, _ := json.Marshal(map[string]any{"inode": 2, "sid": 1, "owner": 11, "ltype": uint32('W')})
	if _, errno, err := s.RunExtentMetaOp(ctx, "flock", flock); err != nil || errno != 0 {
		t.Fatalf("flock errno=%d err=%v", errno, err)
	}
	flock2, _ := json.Marshal(map[string]any{"inode": 2, "sid": 1, "owner": 12, "ltype": uint32('W')})
	if _, errno, err := s.RunExtentMetaOp(ctx, "flock", flock2); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.EAGAIN) {
		t.Fatalf("second flock errno=%d, want EAGAIN", errno)
	}
	setlk, _ := json.Marshal(map[string]any{
		"inode": 2, "sid": 1, "owner": 21, "ltype": uint32('W'),
		"start": 0, "end": 100, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", setlk); err != nil || errno != 0 {
		t.Fatalf("setlk errno=%d err=%v", errno, err)
	}
	setlk2, _ := json.Marshal(map[string]any{
		"inode": 2, "sid": 1, "owner": 22, "ltype": uint32('W'),
		"start": 50, "end": 150, "pid": 2,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", setlk2); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.EAGAIN) {
		t.Fatalf("overlapping setlk errno=%d, want EAGAIN", errno)
	}
}

func TestExtentSetlkConcurrentExclusive(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "race.db", "type": 1, "mode": 0644,
		"inode": 4, "proj_path": "/race.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	const n = 8
	var wg sync.WaitGroup
	got := make([]int, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			raw, _ := json.Marshal(map[string]any{
				"inode": 4, "sid": 1, "owner": uint64(100 + i),
				"ltype": uint32(syscall.F_WRLCK),
				"start": 0x40000000, "end": 0x40000000, "pid": uint32(i + 1),
			})
			_, errno, err := s.RunExtentMetaOp(ctx, "setlk", raw)
			if err != nil {
				got[i] = -1
				return
			}
			got[i] = errno
		}()
	}
	wg.Wait()
	ok, busy, other := 0, 0, 0
	for _, errno := range got {
		switch errno {
		case 0:
			ok++
		case int(syscall.EAGAIN):
			busy++
		default:
			other++
		}
	}
	if ok != 1 || busy != n-1 || other != 0 {
		t.Fatalf("concurrent exclusive setlk ok=%d busy=%d other=%d got=%v (want 1 grant, rest EAGAIN)", ok, busy, other, got)
	}
}

func TestExtentSetlkBlockWaitsForUnlock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "block.db", "type": 1, "mode": 0644,
		"inode": 5, "proj_path": "/block.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	lock, _ := json.Marshal(map[string]any{
		"inode": 5, "sid": 1, "owner": 1, "ltype": uint32(syscall.F_WRLCK),
		"start": 0, "end": 0xFFFF, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", lock); err != nil || errno != 0 {
		t.Fatalf("holder setlk errno=%d err=%v", errno, err)
	}
	contend, _ := json.Marshal(map[string]any{
		"inode": 5, "sid": 1, "owner": 2, "ltype": uint32(syscall.F_WRLCK),
		"start": 0, "end": 0xFFFF, "pid": 2, "block": true,
	})
	started := make(chan struct{})
	got := make(chan int, 1)
	go func() {
		close(started)
		_, errno, err := s.RunExtentMetaOp(ctx, "setlk", contend)
		if err != nil {
			got <- -1
			return
		}
		got <- errno
	}()
	<-started
	select {
	case errno := <-got:
		t.Fatalf("blocking setlk returned errno=%d before unlock", errno)
	case <-time.After(80 * time.Millisecond):
	}
	unlock, _ := json.Marshal(map[string]any{
		"inode": 5, "sid": 1, "owner": 1, "ltype": uint32(syscall.F_UNLCK),
		"start": 0, "end": 0xFFFF, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", unlock); err != nil || errno != 0 {
		t.Fatalf("unlock errno=%d err=%v", errno, err)
	}
	select {
	case errno := <-got:
		if errno != 0 {
			t.Fatalf("blocking setlk after unlock errno=%d, want 0", errno)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocking setlk did not return after unlock")
	}
}

func TestExtentSetlkBlockCanceledReturnsEINTR(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "intr.db", "type": 1, "mode": 0644,
		"inode": 6, "proj_path": "/intr.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	lock, _ := json.Marshal(map[string]any{
		"inode": 6, "sid": 1, "owner": 1, "ltype": uint32(syscall.F_WRLCK),
		"start": 0, "end": 1, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", lock); err != nil || errno != 0 {
		t.Fatalf("holder setlk errno=%d err=%v", errno, err)
	}
	blockCtx, cancel := context.WithCancel(ctx)
	contend, _ := json.Marshal(map[string]any{
		"inode": 6, "sid": 1, "owner": 2, "ltype": uint32(syscall.F_WRLCK),
		"start": 0, "end": 1, "pid": 2, "block": true,
	})
	got := make(chan int, 1)
	go func() {
		_, errno, err := s.RunExtentMetaOp(blockCtx, "setlk", contend)
		if err != nil {
			got <- -1
			return
		}
		got <- errno
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case errno := <-got:
		if errno != int(syscall.EINTR) {
			t.Fatalf("canceled blocking setlk errno=%d, want EINTR", errno)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled blocking setlk did not return")
	}
}

func TestJfsUpdateLocksSingleByteUnlock(t *testing.T) {
	held := []jfsPLock{{Type: uint32(syscall.F_WRLCK), Start: 0x40000000, End: 0x40000000, Pid: 1}}
	got := jfsUpdateLocks(held, jfsPLock{Type: uint32(syscall.F_UNLCK), Start: 0x40000000, End: 0x40000000, Pid: 1})
	if len(got) != 0 {
		t.Fatalf("unlock left %+v", got)
	}
}

func TestExtentSetlkSingleByteUnlock(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "wal.db", "type": 1, "mode": 0644,
		"inode": 3, "proj_path": "/wal.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// SQLite WAL pending/reserved bytes are single-byte inclusive ranges.
	lock, _ := json.Marshal(map[string]any{
		"inode": 3, "sid": 1, "owner": 7, "ltype": uint32(syscall.F_WRLCK),
		"start": 0x40000000, "end": 0x40000000, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", lock); err != nil || errno != 0 {
		t.Fatalf("single-byte setlk errno=%d err=%v", errno, err)
	}
	contend, _ := json.Marshal(map[string]any{
		"inode": 3, "sid": 1, "owner": 8, "ltype": uint32(syscall.F_WRLCK),
		"start": 0x40000000, "end": 0x40000000, "pid": 2,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", contend); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.EAGAIN) {
		t.Fatalf("single-byte conflict errno=%d, want EAGAIN", errno)
	}
	unlock, _ := json.Marshal(map[string]any{
		"inode": 3, "sid": 1, "owner": 7, "ltype": uint32(syscall.F_UNLCK),
		"start": 0x40000000, "end": 0x40000000, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", unlock); err != nil || errno != 0 {
		t.Fatalf("single-byte unlock errno=%d err=%v", errno, err)
	}
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", contend); err != nil || errno != 0 {
		t.Fatalf("after unlock errno=%d err=%v, want 0", errno, err)
	}
}

func TestExtentUnlinkPathAndHTTPDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "gone.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/gone.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if _, err := s.DeleteFileWithRefCheck(ctx, "/gone.db"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetExtentProjection(ctx, "/gone.db"); err == nil {
		t.Fatal("projection survived HTTP delete")
	}
}

func TestExtentUnlinkMissingEdgeDeletesProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "orphan.db", "type": 1, "mode": 0644,
		"inode": 4, "proj_path": "/orphan.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if _, err := s.DB().Exec(`DELETE FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("orphan.db")); err != nil {
		t.Fatal(err)
	}
	unlink, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "orphan.db", "proj_path": "/orphan.db",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "unlink", unlink); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.ENOENT) {
		t.Fatalf("unlink missing edge errno=%d, want ENOENT", errno)
	}
	if _, err := s.GetExtentProjection(ctx, "/orphan.db"); err == nil {
		t.Fatal("projection survived unlink of missing juicefs edge")
	}
}

func TestLinkFileNodeCopiesExtentProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "src.db", "type": 1, "mode": 0644,
		"inode": 9, "proj_path": "/src.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	if err := s.LinkFileNode(ctx, "/src.db", "/alias.db", "/", "alias.db", "link-1", time.Now()); err != nil {
		t.Fatalf("LinkFileNode: %v", err)
	}
	proj, err := s.GetExtentProjection(ctx, "/alias.db")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ContentLayout != ContentLayoutExtent || proj.ExtentIno != 9 {
		t.Fatalf("hardlink projection = %+v", proj)
	}

	// The hardlink must also extend the JuiceFS inode: a projection row alone
	// leaves nlink=1 and a stale ctime on the extent inode (pjdfstest link/00
	// "successful link updates ctime").
	var before int64
	if err := s.DB().QueryRow(`SELECT ctime FROM jfs_node WHERE inode = 9`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := s.InTx(ctx, func(tx *sql.Tx) error {
		return s.LinkFileNodeTx(ctx, tx, "/src.db", "/alias2.db", "/", "alias2.db", "link-2", time.Now())
	}); err != nil {
		t.Fatalf("LinkFileNodeTx alias2: %v", err)
	}
	var nlink int
	var ctime int64
	if err := s.DB().QueryRow(`SELECT nlink, ctime FROM jfs_node WHERE inode = 9`).Scan(&nlink, &ctime); err != nil {
		t.Fatal(err)
	}
	if nlink != 3 {
		t.Fatalf("extent inode nlink = %d, want 3", nlink)
	}
	if ctime <= before {
		t.Fatalf("extent inode ctime = %d, want > %d after hardlink", ctime, before)
	}
	var edgeIno uint64
	if err := s.DB().QueryRow(`SELECT inode FROM jfs_edge WHERE parent = 1 AND name = ?`, []byte("alias2.db")).Scan(&edgeIno); err != nil {
		t.Fatalf("jfs edge for alias2: %v", err)
	}
	if edgeIno != 9 {
		t.Fatalf("jfs edge inode = %d, want 9", edgeIno)
	}
}

func TestExtentMetaMknodFifoCreatesProjection(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "pipe", "type": 4, "mode": 0644,
		"inode": 5, "proj_path": "/pipe",
		"attr": ExtentAttr{Typ: 4, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod fifo errno=%d err=%v", errno, err)
	}
	proj, err := s.GetExtentProjection(ctx, "/pipe")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 5 {
		t.Fatalf("fifo projection ino=%d, want 5", proj.ExtentIno)
	}
	getattr, _ := json.Marshal(map[string]any{"inode": 5})
	body, errno, err := s.RunExtentMetaOp(ctx, "getattr", getattr)
	if err != nil || errno != 0 {
		t.Fatalf("getattr fifo errno=%d err=%v", errno, err)
	}
	var out struct {
		Attr ExtentAttr `json:"attr"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Attr.Typ != 4 {
		t.Fatalf("fifo type=%d, want 4", out.Attr.Typ)
	}
}

func TestExtentMetaSymlinkReadlink(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "link", "type": 3, "mode": 0777,
		"inode": 6, "proj_path": "/link", "path": "target",
		"attr": ExtentAttr{Typ: 3, Mode: 0777, Nlink: 1, Length: 6, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create); err != nil || errno != 0 {
		t.Fatalf("mknod symlink errno=%d err=%v", errno, err)
	}
	readlink, _ := json.Marshal(map[string]any{"inode": 6})
	body, errno, err := s.RunExtentMetaOp(ctx, "readlink", readlink)
	if err != nil || errno != 0 {
		t.Fatalf("readlink errno=%d err=%v body=%s", errno, err, body)
	}
	var out struct {
		Target []byte `json:"target"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Target) != "target" {
		t.Fatalf("readlink %q, want target", out.Target)
	}
}

func TestExtentMetaRenameFifoOntoRegular(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	reg, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "file", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/file",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", reg); err != nil || errno != 0 {
		t.Fatalf("mknod file errno=%d err=%v", errno, err)
	}
	fifo, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "pipe", "type": 4, "mode": 0644,
		"inode": 3, "proj_path": "/pipe",
		"attr": ExtentAttr{Typ: 4, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", fifo); err != nil || errno != 0 {
		t.Fatalf("mknod fifo errno=%d err=%v", errno, err)
	}
	rename, _ := json.Marshal(map[string]any{
		"src_parent": 1, "src_name": "file",
		"dst_parent": 1, "dst_name": "pipe",
		"src_path": "/file", "dst_path": "/pipe",
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "rename", rename); err != nil || errno != 0 {
		t.Fatalf("rename file onto fifo errno=%d err=%v", errno, err)
	}
	proj, err := s.GetExtentProjection(ctx, "/pipe")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ExtentIno != 2 {
		t.Fatalf("replaced dest ino=%d, want 2", proj.ExtentIno)
	}
}
