package datastore

import (
	"context"
	"encoding/json"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestExtentMetaFlockAndSetlkContend(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	create, _ := json.Marshal(map[string]any{
		"parent": 1, "name": "lock.db", "type": 1, "mode": 0644,
		"inode": 2, "proj_path": "/lock.db",
		"attr": ExtentAttr{Typ: 1, Mode: 0644, Nlink: 1, Parent: 1, Full: true},
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	flock, _ := json.Marshal(map[string]any{"inode": 2, "sid": 1, "owner": 11, "ltype": uint32('W')})
	if _, errno, err := s.RunExtentMetaOp(ctx, "flock", flock, nil); err != nil || errno != 0 {
		t.Fatalf("flock errno=%d err=%v", errno, err)
	}
	flock2, _ := json.Marshal(map[string]any{"inode": 2, "sid": 1, "owner": 12, "ltype": uint32('W')})
	if _, errno, err := s.RunExtentMetaOp(ctx, "flock", flock2, nil); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.EAGAIN) {
		t.Fatalf("second flock errno=%d, want EAGAIN", errno)
	}
	setlk, _ := json.Marshal(map[string]any{
		"inode": 2, "sid": 1, "owner": 21, "ltype": uint32('W'),
		"start": 0, "end": 100, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", setlk, nil); err != nil || errno != 0 {
		t.Fatalf("setlk errno=%d err=%v", errno, err)
	}
	setlk2, _ := json.Marshal(map[string]any{
		"inode": 2, "sid": 1, "owner": 22, "ltype": uint32('W'),
		"start": 50, "end": 150, "pid": 2,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", setlk2, nil); err != nil {
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
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
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
			_, errno, err := s.RunExtentMetaOp(ctx, "setlk", raw, nil)
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
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	lock, _ := json.Marshal(map[string]any{
		"inode": 5, "sid": 1, "owner": 1, "ltype": uint32(syscall.F_WRLCK),
		"start": 0, "end": 0xFFFF, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", lock, nil); err != nil || errno != 0 {
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
		_, errno, err := s.RunExtentMetaOp(ctx, "setlk", contend, nil)
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
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", unlock, nil); err != nil || errno != 0 {
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
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	lock, _ := json.Marshal(map[string]any{
		"inode": 6, "sid": 1, "owner": 1, "ltype": uint32(syscall.F_WRLCK),
		"start": 0, "end": 1, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", lock, nil); err != nil || errno != 0 {
		t.Fatalf("holder setlk errno=%d err=%v", errno, err)
	}
	blockCtx, cancel := context.WithCancel(ctx)
	contend, _ := json.Marshal(map[string]any{
		"inode": 6, "sid": 1, "owner": 2, "ltype": uint32(syscall.F_WRLCK),
		"start": 0, "end": 1, "pid": 2, "block": true,
	})
	got := make(chan int, 1)
	go func() {
		_, errno, err := s.RunExtentMetaOp(blockCtx, "setlk", contend, nil)
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
	if _, errno, err := s.RunExtentMetaOp(ctx, "mknod", create, nil); err != nil || errno != 0 {
		t.Fatalf("mknod errno=%d err=%v", errno, err)
	}
	// SQLite WAL pending/reserved bytes are single-byte inclusive ranges.
	lock, _ := json.Marshal(map[string]any{
		"inode": 3, "sid": 1, "owner": 7, "ltype": uint32(syscall.F_WRLCK),
		"start": 0x40000000, "end": 0x40000000, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", lock, nil); err != nil || errno != 0 {
		t.Fatalf("single-byte setlk errno=%d err=%v", errno, err)
	}
	contend, _ := json.Marshal(map[string]any{
		"inode": 3, "sid": 1, "owner": 8, "ltype": uint32(syscall.F_WRLCK),
		"start": 0x40000000, "end": 0x40000000, "pid": 2,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", contend, nil); err != nil {
		t.Fatal(err)
	} else if errno != int(syscall.EAGAIN) {
		t.Fatalf("single-byte conflict errno=%d, want EAGAIN", errno)
	}
	unlock, _ := json.Marshal(map[string]any{
		"inode": 3, "sid": 1, "owner": 7, "ltype": uint32(syscall.F_UNLCK),
		"start": 0x40000000, "end": 0x40000000, "pid": 1,
	})
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", unlock, nil); err != nil || errno != 0 {
		t.Fatalf("single-byte unlock errno=%d err=%v", errno, err)
	}
	if _, errno, err := s.RunExtentMetaOp(ctx, "setlk", contend, nil); err != nil || errno != 0 {
		t.Fatalf("after unlock errno=%d err=%v, want 0", errno, err)
	}
}
