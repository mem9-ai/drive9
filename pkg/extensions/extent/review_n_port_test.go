package extent

// qiffang twenty-second round: a directory rename migrates the descendant
// path→inode cache (and live-handle registry) instead of leaving stale
// mappings that resolve through a recreated old directory.

import "testing"

func TestPortRenameMigratesDescendantInoCache(t *testing.T) {
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	e.rememberExtentIno("/d/f.db", 77)
	e.rememberExtentIno("/d/s", 78)
	e.rememberExtentIno("/other.db", 5)

	e.migrateInoPrefix("/d", "/moved")

	if ino, ok := e.cachedIno("/moved/f.db"); !ok || ino != 77 {
		t.Fatalf("/moved/f.db = %d ok=%v, want 77", ino, ok)
	}
	if ino, ok := e.cachedIno("/moved/s"); !ok || ino != 78 {
		t.Fatalf("/moved/s = %d ok=%v, want 78", ino, ok)
	}
	if _, ok := e.cachedIno("/d/f.db"); ok {
		t.Fatal("old descendant mapping survived the rename")
	}
	if ino, ok := e.cachedIno("/other.db"); !ok || ino != 5 {
		t.Fatalf("/other.db = %d ok=%v, want untouched 5", ino, ok)
	}

	e.trackLive("/d/f.db")
	e.trackLive("/d/f.db")
	e.retargetLivePrefix("/d", "/moved")
	if !e.hasLive("/moved/f.db") || e.hasLive("/d/f.db") {
		t.Fatal("live-handle registry did not follow the directory rename")
	}
	e.untrackLive("/moved/f.db")
	e.untrackLive("/moved/f.db")
}

// Rename and the last Release operate on one registry protocol: whichever
// wins, the old AND new paths end with no live count.
func TestPortExtentRenameReleaseRace(t *testing.T) {
	e, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		h := &extentHandle{e: e, path: "/d/f"}
		e.trackLiveHandle(h)
		done := make(chan struct{})
		go func() {
			e.retargetLivePrefix("/d", "/m")
			close(done)
		}()
		e.untrackLiveHandle(h)
		<-done
		if e.hasLive("/d/f") || e.hasLive("/m/f") {
			t.Fatalf("iteration %d: live counts survived (old=%v new=%v)", i, e.hasLive("/d/f"), e.hasLive("/m/f"))
		}
	}
}
