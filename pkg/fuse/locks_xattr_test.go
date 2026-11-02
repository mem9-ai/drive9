package fuse

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/extensions/xattr"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newXattrFS(t *testing.T) *FS {
	t.Helper()
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("x"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	terminal, _ := core.New(cl, core.Options{})
	xattrExt, err := xattr.New(xattr.Options{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := New(Options{Extensions: []fs.Handler{xattrExt, terminal}})
	if err != nil {
		t.Fatal(err)
	}
	f.Init(nil)
	return f
}

func TestFacadeXattrRoundTrip(t *testing.T) {
	f := newXattrFS(t)
	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &lout); st != gofuse.OK {
		t.Fatal(st)
	}
	h := &gofuse.InHeader{NodeId: lout.NodeId}

	si := &gofuse.SetXAttrIn{}
	si.NodeId = lout.NodeId
	if st := f.SetXAttr(nil, si, "user.k", []byte("value")); st != gofuse.OK {
		t.Fatalf("set: %v", st)
	}
	if st := f.SetXAttr(nil, si, "user.k", []byte("v2")); st != gofuse.OK {
		t.Fatalf("replace: %v", st)
	}
	if n, st := f.GetXAttr(nil, h, "user.k", make([]byte, 1)); st != gofuse.Status(syscall.ERANGE) || n != 2 {
		t.Fatalf("small dest = %d %v, want ERANGE 2", n, st)
	}
	if n, st := f.GetXAttr(nil, h, "user.k", make([]byte, 8)); st != gofuse.OK || n != 2 {
		t.Fatalf("get = %d %v", n, st)
	}
	if n, st := f.ListXAttr(nil, h, make([]byte, 1)); st != gofuse.Status(syscall.ERANGE) {
		t.Fatalf("list probe = %d %v", n, st)
	}
	if n, st := f.ListXAttr(nil, h, make([]byte, 64)); st != gofuse.OK || n != 7 { // "user.k\0"
		t.Fatalf("list = %d %v", n, st)
	}
	if st := f.RemoveXAttr(nil, h, "user.k"); st != gofuse.OK {
		t.Fatalf("remove: %v", st)
	}
	if _, st := f.GetXAttr(nil, h, "user.k", nil); st != gofuse.ENOATTR {
		t.Fatalf("get after remove = %v, want ENOATTR", st)
	}
}

func TestLockTableSplitAndRelease(t *testing.T) {
	tt := newFuseLockTable()
	lk := gofuse.FileLock{Start: 0, End: 99, Typ: uint32(syscall.F_WRLCK)}
	if st := tt.set(nil, 1, 10, 5, lk, false); st != gofuse.OK {
		t.Fatal(st)
	}
	// same owner: unlock a middle range splits the held lock
	un := gofuse.FileLock{Start: 40, End: 59, Typ: uint32(syscall.F_UNLCK)}
	if st := tt.set(nil, 1, 10, 5, un, false); st != gofuse.OK {
		t.Fatal(st)
	}
	probe := gofuse.FileLock{Start: 50, End: 50, Typ: uint32(syscall.F_RDLCK)}
	if _, ok := tt.conflict(1, 20, probe); ok {
		t.Fatal("hole still conflicts")
	}
	probe.Start, probe.End = 0, 0
	if _, ok := tt.conflict(1, 20, probe); !ok {
		t.Fatal("left fragment lost")
	}
	probe.Start, probe.End = 99, 99
	if _, ok := tt.conflict(1, 20, probe); !ok {
		t.Fatal("right fragment lost")
	}
	tt.release(1, 10)
	probe.Start, probe.End = 0, 99
	if _, ok := tt.conflict(1, 20, probe); ok {
		t.Fatal("release left locks")
	}
}

func TestLockTableBlockingWakesOnRelease(t *testing.T) {
	tt := newFuseLockTable()
	wr := gofuse.FileLock{Start: 0, End: 9, Typ: uint32(syscall.F_WRLCK)}
	if st := tt.set(nil, 1, 1, 1, wr, false); st != gofuse.OK {
		t.Fatal(st)
	}
	cancel := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if st := tt.set(cancel, 1, 2, 2, wr, true); st != gofuse.OK {
			t.Errorf("blocked set: %v", st)
		}
	}()
	tt.release(1, 1)
	wg.Wait()
}
