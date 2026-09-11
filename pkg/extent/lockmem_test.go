package extent

import (
	"context"
	"encoding/json"
	"syscall"
	"testing"
	"time"

	jfsmeta "github.com/juicedata/juicefs/pkg/meta"
)

func TestMemUpdateLocksSingleByteUnlock(t *testing.T) {
	held := []memPLock{{typ: uint32(syscall.F_WRLCK), start: 0x40000000, end: 0x40000000, pid: 1}}
	got := memUpdateLocks(held, memPLock{typ: uint32(syscall.F_UNLCK), start: 0x40000000, end: 0x40000000, pid: 1})
	if len(got) != 0 {
		t.Fatalf("unlock left %+v", got)
	}
}

func TestMemUpdateLocksSortsLikeJuiceFS(t *testing.T) {
	// sqlite unix-locking.h: PENDING 0x40000000, SHARED 0x40000002..0x400000FF.
	// JuiceFS updateLocks appends splits then sort.Slice by start.
	held := []memPLock{{typ: uint32(syscall.F_RDLCK), start: 0x40000002, end: 0x400000FF, pid: 1}}
	held = memUpdateLocks(held, memPLock{typ: uint32(syscall.F_WRLCK), start: 0x40000000, end: 0x40000000, pid: 1})
	got := memUpdateLocks(held, memPLock{typ: uint32(syscall.F_UNLCK), start: 0x40000000, end: 0x400000FF, pid: 1})
	if len(got) != 0 {
		t.Fatalf("sqlite DELETE exclusive UNLCK left %+v, want empty (JuiceFS updateLocks sorts before merge)", got)
	}
}

func TestMemLockMetaExclusiveWait(t *testing.T) {
	m := wrapLockMeta(nil).(*memLockMeta)
	nl := memPLock{typ: uint32(syscall.F_WRLCK), start: 0, end: 1, pid: 1}
	m.plockApply(7, 1, nl)
	if _, busy := m.plockConflict(7, 2, nl); !busy {
		t.Fatal("second exclusive must conflict")
	}
	m.plockApply(7, 1, memPLock{typ: uint32(syscall.F_UNLCK), start: 0, end: 1, pid: 1})
	if _, busy := m.plockConflict(7, 2, nl); busy {
		t.Fatal("unlock must clear conflict")
	}
}

func TestMemLockMetaSetlkLocalOnly(t *testing.T) {
	m := wrapLockMeta(nil).(*memLockMeta)
	ctx := jfsmeta.Background()
	if st := m.Setlk(ctx, 7, 1, false, uint32(syscall.F_WRLCK), 0, 1, 1); st != 0 {
		t.Fatalf("first lock: %v", st)
	}
	if st := m.Setlk(ctx, 7, 2, false, uint32(syscall.F_WRLCK), 0, 1, 2); st != syscall.EAGAIN {
		t.Fatalf("conflict: %v, want EAGAIN", st)
	}
	if st := m.Setlk(ctx, 7, 1, false, uint32(syscall.F_UNLCK), 0, 1, 1); st != 0 {
		t.Fatalf("unlock: %v", st)
	}
	if st := m.Setlk(ctx, 7, 2, false, uint32(syscall.F_WRLCK), 0, 1, 2); st != 0 {
		t.Fatalf("after unlock: %v", st)
	}
}

type countWriteParts struct {
	jfsmeta.Meta
	n, parts int
}

func (c *countWriteParts) WriteParts(_ jfsmeta.Context, _ jfsmeta.Ino, _ uint32, parts []jfsmeta.WritePart, _ time.Time) syscall.Errno {
	c.n++
	c.parts = len(parts)
	return 0
}

func TestUnwrapDrive9Meta(t *testing.T) {
	inner := jfsmeta.NewDrive9Meta(jfsmeta.DefaultConf(), NewTransport(func(context.Context, string, json.RawMessage) (json.RawMessage, int, error) {
		return []byte(`{"errno":0}`), 0, nil
	}))
	wrapped := wrapLockMeta(inner)
	got := unwrapDrive9Meta(wrapped)
	if got != inner {
		t.Fatal("Drive9CommitCompact must see *drive9Meta under memLockMeta")
	}
}

func TestMemLockMetaWritePartsOneCall(t *testing.T) {
	inner := &countWriteParts{}
	m := wrapLockMeta(inner).(*memLockMeta)
	parts := []jfsmeta.WritePart{
		{Off: 0, Slice: jfsmeta.Slice{Id: 1, Size: 4, Len: 4}},
		{Off: 4, Slice: jfsmeta.Slice{Id: 2, Size: 4, Len: 4}},
		{Off: 8, Slice: jfsmeta.Slice{Id: 3, Size: 4, Len: 4}},
	}
	if st := m.WriteParts(jfsmeta.Background(), 9, 0, parts, time.Time{}); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	if inner.n != 1 {
		t.Fatalf("WriteParts calls=%d, want 1 (JuiceFS commitThread batches slices into one meta txn)", inner.n)
	}
	if inner.parts != 3 {
		t.Fatalf("parts=%d, want 3", inner.parts)
	}
}

func TestDrive9WritePartsOneHTTP(t *testing.T) {
	var writes int
	var partN int
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		if op == jfsmeta.Drive9OpWrite {
			writes++
			var obj struct {
				Parts []json.RawMessage `json:"parts"`
			}
			_ = json.Unmarshal(raw, &obj)
			partN = len(obj.Parts)
		}
		return []byte(`{"errno":0,"num_slices":3}`), 0, nil
	})
	inner := jfsmeta.NewDrive9Meta(jfsmeta.DefaultConf(), tr)
	if err := inner.Init(&jfsmeta.Format{Name: "t", UUID: "u", Storage: "file", BlockSize: 4096}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	m := wrapLockMeta(inner)
	wp, ok := m.(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	if !ok {
		t.Fatal("extent Meta must expose WriteParts for VFS Flush batching")
	}
	parts := []jfsmeta.WritePart{
		{Off: 0, Slice: jfsmeta.Slice{Id: 1, Size: 4, Len: 4}},
		{Off: 4, Slice: jfsmeta.Slice{Id: 2, Size: 4, Len: 4}},
		{Off: 8, Slice: jfsmeta.Slice{Id: 3, Size: 4, Len: 4}},
	}
	writes, partN = 0, 0
	if st := wp.WriteParts(jfsmeta.Background(), 9, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	if writes != 1 {
		t.Fatalf("HTTP write RPCs=%d, want 1", writes)
	}
	if partN != 3 {
		t.Fatalf("JSON parts=%d, want 3", partN)
	}
}

func TestMemLockMetaForwardsWaitWrites(t *testing.T) {
	inner := jfsmeta.NewDrive9Meta(jfsmeta.DefaultConf(), NewTransport(func(context.Context, string, json.RawMessage) (json.RawMessage, int, error) {
		return []byte(`{"errno":0}`), 0, nil
	}))
	m := wrapLockMeta(inner)
	w, ok := m.(interface{ WaitWrites(jfsmeta.Ino) syscall.Errno })
	if !ok {
		t.Fatal("memLockMeta must forward WaitWrites so VFS Flush drains HTTP writes")
	}
	if st := w.WaitWrites(9); st != 0 {
		t.Fatalf("WaitWrites: %v", st)
	}
}

func TestMemLockMetaForwardsQueueWriteParts(t *testing.T) {
	inner := &countWriteParts{}
	m := wrapLockMeta(inner)
	q, ok := m.(interface {
		QueueWriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	if !ok {
		t.Fatal("memLockMeta must forward QueueWriteParts so commitThread can batch HTTP")
	}
	parts := []jfsmeta.WritePart{{Off: 0, Slice: jfsmeta.Slice{Id: 1, Size: 4, Len: 4}}}
	if st := q.QueueWriteParts(jfsmeta.Background(), 9, 0, parts, time.Time{}); st != 0 {
		t.Fatalf("QueueWriteParts: %v", st)
	}
	if inner.n != 1 {
		t.Fatalf("inner WriteParts calls=%d, want 1 (fallback when inner has no queue)", inner.n)
	}
}

func TestDrive9WritePartsSkipsInlineCompact(t *testing.T) {
	var compact int
	tr := NewTransport(func(ctx context.Context, op string, raw json.RawMessage) (json.RawMessage, int, error) {
		if op == jfsmeta.Drive9OpCompact {
			compact++
		}
		return []byte(`{"errno":0,"num_slices":400}`), 0, nil
	})
	conf := jfsmeta.DefaultConf()
	conf.NoBGJob = true
	inner := jfsmeta.NewDrive9Meta(conf, tr)
	if err := inner.Init(&jfsmeta.Format{Name: "t", UUID: "u", Storage: "file", BlockSize: 4096}, true); err != nil {
		t.Fatalf("init: %v", err)
	}
	wp := wrapLockMeta(inner).(interface {
		WriteParts(jfsmeta.Context, jfsmeta.Ino, uint32, []jfsmeta.WritePart, time.Time) syscall.Errno
	})
	parts := []jfsmeta.WritePart{
		{Off: 0, Slice: jfsmeta.Slice{Id: 1, Size: 4, Len: 4}},
		{Off: 4, Slice: jfsmeta.Slice{Id: 2, Size: 4, Len: 4}},
	}
	if st := wp.WriteParts(jfsmeta.Background(), 9, 0, parts, time.Now()); st != 0 {
		t.Fatalf("WriteParts: %v", st)
	}
	if compact != 0 {
		t.Fatalf("inline compact RPCs=%d, want 0 (compact CAS is not inside WriteParts HTTP)", compact)
	}
}

func TestMemLockMetaBlockingSetlkWakes(t *testing.T) {
	m := wrapLockMeta(nil).(*memLockMeta)
	ctx := jfsmeta.Background()
	if st := m.Setlk(ctx, 9, 1, false, uint32(syscall.F_WRLCK), 0, 1, 1); st != 0 {
		t.Fatalf("holder: %v", st)
	}
	done := make(chan syscall.Errno, 1)
	go func() {
		done <- m.Setlk(ctx, 9, 2, true, uint32(syscall.F_WRLCK), 0, 1, 2)
	}()
	deadline := time.After(2 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case st := <-done:
			if st != 0 {
				t.Fatalf("waiter: %v", st)
			}
			return
		case <-deadline:
			t.Fatal("blocking Setlk did not wake after local unlock")
		case <-tick.C:
			_ = m.Setlk(ctx, 9, 1, false, uint32(syscall.F_UNLCK), 0, 1, 1)
		}
	}
}
