// v1 dat9fs_test.go kernel-bypass marker rows: the sweeper reclaims
// expired markers without a reopen, a stale sweep deadline spares a renewed
// marker, rearms replace (not duplicate) the marker while keeping one
// coalesced sweep deadline, sweep-vs-rearm races never clear a renewed
// marker, and at the facade the marker is inode-scoped (a recreated file
// gets a clean inode) but follows a renamed one.
package fuse

import (
	"context"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/extensions/core"
	"github.com/mem9-ai/drive9/pkg/fs"
)

func newBypassFacade(t *testing.T) *FS {
	t.Helper()
	b := testbackend.NewBackend()
	b.Seed("/f", []byte("data"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	cl := client.New(ts.URL, "")
	cl.Warm(context.Background())
	terminal, _ := core.New(cl, core.Options{})
	f, err := New(Options{Extensions: []fs.Handler{terminal}})
	if err != nil {
		t.Fatal(err)
	}
	f.Init(nil)
	return f
}

// v1 TestKernelCacheBypassMarkerExpiresAndClears /
// TestKernelCacheBypassMarkersExpireWithoutReopen: the coalesced sweeper
// reclaims an expired marker on its own timer, with no intervening open.
func TestPortBypassSweepReclaimsExpiredWithoutReopen(t *testing.T) {
	tt := newKernelCacheBypassTable()
	t.Cleanup(tt.clear)
	tt.armUntil(1, "/a", 1, 1, "test", time.Now().Add(30*time.Millisecond))
	tt.armUntil(2, "/b", 1, 1, "test", time.Now().Add(1*time.Minute))

	deadline := time.Now().Add(3 * time.Second)
	for tt.active(1) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if tt.active(1) {
		t.Fatal("expired marker survived the sweeper without a reopen")
	}
	if !tt.active(2) {
		t.Fatal("sweeper reclaimed a live marker")
	}
}

// v1 TestKernelCacheBypassStaleSweepDoesNotClearRenewedMarker: renewing a
// marker keeps the earlier sweep deadline, and that stale sweep must not
// clear the renewed marker.
func TestPortBypassStaleSweepSparesRenewedMarker(t *testing.T) {
	tt := newKernelCacheBypassTable()
	t.Cleanup(tt.clear)
	tt.armUntil(7, "/f", 1, 8, "test", time.Now().Add(50*time.Millisecond))
	tt.armUntil(7, "/f", 2, 9, "test", time.Now().Add(1*time.Minute))

	time.Sleep(300 * time.Millisecond) // the stale deadline fired and swept
	if !tt.active(7) {
		t.Fatal("stale sweep cleared the renewed marker")
	}
	tt.mu.Lock()
	rev := tt.markers[7].revision
	tt.mu.Unlock()
	if rev != 2 {
		t.Fatalf("renewed marker revision = %d, want the rearm to replace the entry", rev)
	}
}

// v1 TestKernelCacheBypassRearmKeepsSingleSweepTimerAndOneMarker: rearms
// replace the marker (one per inode) and the sweep deadline tracks the
// earliest expiry across all markers.
func TestPortBypassRearmReplacesMarkerAndKeepsEarliestDeadline(t *testing.T) {
	tt := newKernelCacheBypassTable()
	t.Cleanup(tt.clear)
	now := time.Now()
	tt.armUntil(7, "/f", 1, 1, "test", now.Add(time.Second))
	tt.armUntil(7, "/f", 2, 2, "test", now.Add(2*time.Second))
	earliest := now.Add(500 * time.Millisecond)
	tt.armUntil(8, "/g", 1, 1, "test", earliest)
	tt.armUntil(9, "/h", 1, 1, "test", now.Add(3*time.Second))

	tt.mu.Lock()
	markers := len(tt.markers)
	rev := tt.markers[7].revision
	tt.mu.Unlock()
	if markers != 3 {
		t.Fatalf("markers = %d, want one per inode", markers)
	}
	if rev != 2 {
		t.Fatalf("marker revision = %d, want the rearm to replace it", rev)
	}
	if got := tt.sweepWhen.Load(); got != earliest.UnixNano() {
		t.Fatalf("sweep deadline = %d, want the earliest expiry %d", got, earliest.UnixNano())
	}
}

// v1 TestKernelCacheBypassTimerFireConcurrentRearmKeepsRenewedMarker: a
// sweep racing a rearm can never end with the renewed marker gone — the
// sweeper only drops markers that are expired at sweep time.
func TestPortBypassConcurrentSweepAndRearmKeepRenewedMarker(t *testing.T) {
	tt := newKernelCacheBypassTable()
	t.Cleanup(tt.clear)
	var wg sync.WaitGroup
	for range 50 {
		tt.armUntil(7, "/f", 1, 1, "test", time.Now().Add(-time.Second))
		wg.Add(1)
		go func() {
			defer wg.Done()
			tt.sweep()
		}()
		tt.armUntil(7, "/f", 2, 2, "test", time.Now().Add(time.Minute))
		wg.Wait()
		if !tt.active(7) {
			t.Fatal("renewed marker lost to a racing sweep")
		}
	}
}

// v1 TestKernelCacheBypassMarkerIsInodeScopedAcrossRecreate: deleting and
// recreating the path binds a fresh inode; the old marker must not bleed
// onto the new incarnation.
func TestPortBypassMarkerScopedToInodeAcrossRecreate(t *testing.T) {
	f := newBypassFacade(t)

	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &lout); st != gofuse.OK {
		t.Fatal(st)
	}
	oldIno := lout.NodeId
	f.ArmBypass("/f", 1, 4, "test")

	if st := f.Unlink(nil, &gofuse.InHeader{NodeId: 1}, "f"); st != gofuse.OK {
		t.Fatalf("unlink: %v", st)
	}
	var cout gofuse.CreateOut
	ci := &gofuse.CreateIn{Flags: uint32(syscall.O_CREAT | syscall.O_RDWR), Mode: 0o644}
	ci.NodeId = 1
	if st := f.Create(nil, ci, "f", &cout); st != gofuse.OK {
		t.Fatalf("recreate: %v", st)
	}
	if cout.NodeId == oldIno {
		t.Fatal("recreate reused the old inode")
	}
	var oout gofuse.OpenOut
	oi := &gofuse.OpenIn{}
	oi.NodeId = cout.NodeId
	if st := f.Open(nil, oi, &oout); st != gofuse.OK {
		t.Fatalf("open: %v", st)
	}
	if oout.OpenFlags&gofuse.FOPEN_DIRECT_IO != 0 {
		t.Fatal("old marker bled onto the recreated inode")
	}
}

// v1 TestKernelCacheBypassMarkerFollowsRenamedInode: rename rebinds the
// same inode under the new name and the marker follows it.
func TestPortBypassMarkerFollowsRenamedInode(t *testing.T) {
	f := newBypassFacade(t)

	var lout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "f", &lout); st != gofuse.OK {
		t.Fatal(st)
	}
	ino := lout.NodeId
	f.ArmBypass("/f", 1, 4, "test")

	ri := &gofuse.RenameIn{Newdir: 1}
	ri.NodeId = 1
	if st := f.Rename(nil, ri, "f", "g"); st != gofuse.OK {
		t.Fatalf("rename: %v", st)
	}
	var gout gofuse.EntryOut
	if st := f.Lookup(nil, &gofuse.InHeader{NodeId: 1}, "g", &gout); st != gofuse.OK {
		t.Fatal(st)
	}
	if gout.NodeId != ino {
		t.Fatalf("renamed inode = %d, want the rebound %d", gout.NodeId, ino)
	}
	var oout gofuse.OpenOut
	oi := &gofuse.OpenIn{}
	oi.NodeId = gout.NodeId
	if st := f.Open(nil, oi, &oout); st != gofuse.OK {
		t.Fatalf("open: %v", st)
	}
	if oout.OpenFlags&gofuse.FOPEN_DIRECT_IO == 0 {
		t.Fatal("marker did not follow the renamed inode")
	}
}
