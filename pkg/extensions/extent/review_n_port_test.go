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
	e.paths.rememberIno("/d/f.db", 77)
	e.paths.rememberIno("/d/s", 78)
	e.paths.rememberIno("/other.db", 5)

	e.paths.migrateInoPrefix("/d", "/moved")

	if ino, ok := e.paths.cachedIno("/moved/f.db"); !ok || ino != 77 {
		t.Fatalf("/moved/f.db = %d ok=%v, want 77", ino, ok)
	}
	if ino, ok := e.paths.cachedIno("/moved/s"); !ok || ino != 78 {
		t.Fatalf("/moved/s = %d ok=%v, want 78", ino, ok)
	}
	if _, ok := e.paths.cachedIno("/d/f.db"); ok {
		t.Fatal("old descendant mapping survived the rename")
	}
	if ino, ok := e.paths.cachedIno("/other.db"); !ok || ino != 5 {
		t.Fatalf("/other.db = %d ok=%v, want untouched 5", ino, ok)
	}

	e.paths.trackLive("/d/f.db")
	e.paths.trackLive("/d/f.db")
	e.paths.retargetLivePrefix("/d", "/moved")
	if !e.paths.hasLive("/moved/f.db") || e.paths.hasLive("/d/f.db") {
		t.Fatal("live-handle registry did not follow the directory rename")
	}
	e.paths.untrackLive("/moved/f.db")
	e.paths.untrackLive("/moved/f.db")
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
		e.paths.trackLiveHandle(h)
		done := make(chan struct{})
		go func() {
			e.paths.retargetLivePrefix("/d", "/m")
			close(done)
		}()
		e.paths.untrackLiveHandle(h)
		<-done
		if e.paths.hasLive("/d/f") || e.paths.hasLive("/m/f") {
			t.Fatalf("iteration %d: live counts survived (old=%v new=%v)", i, e.paths.hasLive("/d/f"), e.paths.hasLive("/m/f"))
		}
	}
}
