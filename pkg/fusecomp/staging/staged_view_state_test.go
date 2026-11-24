package staging

import (
	"os"
	"testing"
	"time"
)

// TestGetMetaAndViewStateDistinguishesMissFromTornPair pins the reason a
// staged read produced nothing: callers that treat a miss as success (the
// write-back durability barrier) must not do that for a pruned pair.
func TestGetMetaAndViewStateDistinguishesMissFromTornPair(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const path = "/torn.txt"
	payload := []byte("staged image")
	if err := c.Put(path, payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatalf("put: %v", err)
	}

	meta, data, state, _ := c.GetMetaAndViewState(path)
	if state != StagedViewOK {
		t.Fatalf("state = %v, want StagedViewOK", state)
	}
	if meta == nil || string(data) != string(payload) {
		t.Fatalf("meta/data mismatch: %+v %q", meta, data)
	}

	// Drop the in-memory copy so the next view load reads the .dat file, then
	// rewrite it so its checksum no longer matches the metadata binding: the
	// view load prunes the pair.
	c.DropInMemoryPayload(path)
	if err := os.WriteFile(c.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatalf("corrupt data: %v", err)
	}
	if _, _, state, err := c.GetMetaAndViewState(path); state != StagedViewNoData || err == nil {
		t.Fatalf("state after corrupting the data = %v (err %v), want StagedViewNoData with a reason", state, err)
	}

	// The pruned pair is gone, and the cache keeps the loss on record: a later
	// lookup must report "data gone", never "nothing staged", or a barrier would
	// read it as an upload someone else completed.
	if _, _, state, err := c.GetMetaAndViewState(path); state != StagedViewNoData || err == nil {
		t.Fatalf("state after the pair was pruned = %v (err %v), want StagedViewNoData with the recorded loss", state, err)
	}
}

// TestPendingChmodIsNotPrunedAsATornPair pins the state that keeps a
// chmod-only record alive: its .dat is absent by design, and reading the view
// would delete a healthy entry and report the barrier lost.
func TestPendingChmodIsNotPrunedAsATornPair(t *testing.T) {
	const path = "/chmod-only.txt"
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	meta := WriteBackMeta{
		Path:       path,
		Size:       7,
		Kind:       PendingChmod,
		Generation: 42,
	}
	c.PublishMeta(meta)

	got, data, state, err := c.GetMetaAndViewState(path)
	if err != nil || state != StagedViewPendingChmod {
		t.Fatalf("state = %v (err %v), want StagedViewPendingChmod", state, err)
	}
	if got == nil || got.Kind != PendingChmod || len(data) != 0 {
		t.Fatalf("meta = %+v data=%q, want the chmod record and no payload", got, data)
	}
	if !c.HasMeta(path) {
		t.Fatal("PendingChmod record was pruned by the meta+view read")
	}
}

// TestPrunedPairReportsLossToTheOwner pins the plain-read choke point: a read
// that prunes a damaged pair must tell the cache owner, because that is the
// only place a later barrier can learn the bytes never landed.
func TestPrunedPairReportsLossToTheOwner(t *testing.T) {
	const path = "/lost.txt"
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var lostPath string
	var lostErr error
	c.OnViewLost = func(p string, err error) { lostPath, lostErr = p, err }

	payload := []byte("staged image")
	if err := c.Put(path, payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	c.DropInMemoryPayload(path)
	if err := os.WriteFile(c.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A plain read (not the uploader) hits the torn pair first.
	if _, ok := c.Get(path); ok {
		t.Fatal("Get served a torn pair")
	}
	if lostPath != path {
		t.Fatalf("OnViewLost path = %q, want %q", lostPath, path)
	}
	if lostErr == nil {
		t.Fatal("OnViewLost carried no reason")
	}
}

// TestGetReportsTheLostPairBeforeItReturns pins the ordering a durability
// barrier depends on: the prune and its report happen inside the same path
// lock window, so a barrier that takes the lock next cannot read the missing
// record as "another upload consumed it". Releasing the lock before the report
// reopens exactly that window.
func TestGetReportsTheLostPairBeforeItReturns(t *testing.T) {
	const path = "/in-flight-report.txt"
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	c.OnViewLost = func(string, error) {
		close(entered)
		<-release
	}

	payload := []byte("staged image")
	if err := c.Put(path, payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	c.DropInMemoryPayload(path)
	if err := os.WriteFile(c.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}

	read := make(chan struct{})
	go func() {
		defer close(read)
		if _, ok := c.Get(path); ok {
			t.Error("Get served a torn pair")
		}
	}()
	<-entered // the hook is running, so the prune has happened

	// The barrier that follows the prune must not get to the path until the
	// loss has been reported: with the report outside the path lock it would
	// take the lock here, find no record, and report a durable file.
	type result struct {
		state StagedViewState
		err   error
	}
	barrier := make(chan result, 1)
	go func() {
		_, _, state, err := c.GetMetaAndViewState(path)
		barrier <- result{state: state, err: err}
	}()
	select {
	case got := <-barrier:
		t.Fatalf("a barrier ran while the loss report was in flight (state %v, err %v)", got.state, got.err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)
	<-read
	got := <-barrier
	if got.state != StagedViewNoData || got.err == nil {
		t.Fatalf("barrier state after the prune = %v (err %v), want StagedViewNoData with the recorded loss", got.state, got.err)
	}
}

// TestGetViewReportsPrunedPair pins the read-path variant: a view load that
// prunes a pair must report it before returning, or the barrier that follows
// has no way to tell a lost image from a consumed one.
func TestGetViewReportsPrunedPair(t *testing.T) {
	const path = "/view-lost.txt"
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var lostPath string
	var lostErr error
	c.OnViewLost = func(p string, err error) { lostPath, lostErr = p, err }

	payload := []byte("staged image")
	if err := c.Put(path, payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	c.DropInMemoryPayload(path)
	if err := os.WriteFile(c.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}

	if _, ok := c.GetView(path); ok {
		t.Fatal("GetView served a torn pair")
	}
	if lostPath != path {
		t.Fatalf("OnViewLost path = %q, want %q", lostPath, path)
	}
	if lostErr == nil {
		t.Fatal("OnViewLost carried no reason")
	}
	if c.HasMeta(path) {
		t.Fatal("the pruned pair is still in the pending index")
	}
}

// TestViewLossMarkerSurvivesTheRestart pins the durable half: the record is a
// file next to the staged pairs, so a process that comes up after a crash still
// fails the barrier instead of reading the pruned image as a completed upload.
func TestViewLossMarkerSurvivesTheRestart(t *testing.T) {
	dir := t.TempDir()
	const path = "/restart-lost.txt"
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("staged image")
	if err := c.Put(path, payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	c.DropInMemoryPayload(path)
	if err := os.WriteFile(c.DatFile(path), []byte("different bytes!"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get(path); ok {
		t.Fatal("Get served a torn pair")
	}
	if _, err := os.Stat(c.lostFile(path)); err != nil {
		t.Fatalf("loss marker missing: %v", err)
	}

	restarted, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Rebuild the on-disk metadata index the way recovery does.
	_ = restarted.ListPending()
	if _, _, state, err := restarted.GetMetaAndViewState(path); state != StagedViewNoData || err == nil {
		t.Fatalf("barrier after restart = %v (err %v), want StagedViewNoData with the recorded loss", state, err)
	}
	// A fresh staging supersedes the record.
	if err := restarted.Put(path, payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(restarted.lostFile(path)); !os.IsNotExist(err) {
		t.Fatalf("loss marker survived a fresh staging: %v", err)
	}
}

// TestViewLossMarkerIsGenerationScoped pins the clearing rule: an older upload
// landing must not erase the record of a newer image that was lost, while a
// landing at or above the lost generation does.
func TestViewLossMarkerIsGenerationScoped(t *testing.T) {
	const path = "/gen-scoped.txt"
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c.writeViewLoss(path, 5, os.ErrNotExist)
	c.ClearViewLossIfGeneration(path, 4)
	if _, err := os.Stat(c.lostFile(path)); err != nil {
		t.Fatalf("a landing below the lost generation cleared the record: %v", err)
	}
	c.ClearViewLossIfGeneration(path, 5)
	if _, err := os.Stat(c.lostFile(path)); !os.IsNotExist(err) {
		t.Fatalf("a landing at the lost generation kept the record: %v", err)
	}
}

// TestViewLossMarkerIsClearedByNonPutPublications pins the lifecycle hole the
// review found: an image that lands through the queue, an inline commit or a
// rename replaces the path's content without a wbCache.Put, and a surviving
// marker would then fail every later barrier on the name.
func TestViewLossMarkerIsClearedByNonPutPublications(t *testing.T) {
	const (
		staged = "/queued.txt"
		inline = "/inline.txt"
		src    = "/moved.txt"
		dst    = "/target.txt"
	)
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{staged, inline, src, dst} {
		c.writeViewLoss(p, 7, os.ErrNotExist)
	}
	// A queue staging/inline commit publishes the newest image for the name.
	c.ClearViewLoss(staged)
	c.ClearViewLoss(inline)
	// A rename replaces both names' incarnations.
	c.ClearViewLoss(src)
	c.ClearViewLoss(dst)
	for _, p := range []string{staged, inline, src, dst} {
		if err := c.ViewLoss(p); err != nil {
			t.Fatalf("loss record for %s survived its supersession: %v", p, err)
		}
		if _, err := os.Stat(c.lostFile(p)); !os.IsNotExist(err) {
			t.Fatalf("loss marker for %s survived: %v", p, err)
		}
	}
}

// TestViewLossPrefixClearsSubtreeRecords pins the subtree rule: removal and
// directory rename cannot see .lost markers through the .meta scan, so the
// prefix clear must reach them.
func TestViewLossPrefixClearsSubtreeRecords(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c.writeViewLoss("/tree/a.txt", 3, os.ErrNotExist)
	c.writeViewLoss("/tree/sub/b.txt", 4, os.ErrNotExist)
	c.writeViewLoss("/other.txt", 5, os.ErrNotExist)
	c.ClearViewLossPrefix("/tree/")
	if err := c.ViewLoss("/tree/a.txt"); err != nil {
		t.Fatalf("subtree record survived the prefix clear: %v", err)
	}
	if err := c.ViewLoss("/tree/sub/b.txt"); err != nil {
		t.Fatalf("nested subtree record survived the prefix clear: %v", err)
	}
	if err := c.ViewLoss("/other.txt"); err == nil {
		t.Fatal("unrelated record was cleared by the prefix clear")
	}
}

// TestViewLossMarkerSeedsTheGenerationCounter pins the restart rule: a marker
// left by a previous process must lift the cache's generation counter above the
// lost generation, or the next staging reuses it and the clear cannot tell the
// replacement from the loss.
func TestViewLossMarkerSeedsTheGenerationCounter(t *testing.T) {
	dir := t.TempDir()
	const path = "/seeded.txt"
	first, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	first.writeViewLoss(path, 42, os.ErrNotExist)

	restarted, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ViewLoss(path); err == nil {
		t.Fatal("marker not honored after a restart")
	}
	if got := restarted.nextGen.Load(); got < 42 {
		t.Fatalf("generation counter after restart = %d, want at least the lost generation 42", got)
	}
	// A fresh staging gets a generation above the lost one, so clearing by it
	// works.
	if err := restarted.Put(path, []byte("new"), 3, PendingNew); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ViewLoss(path); err != nil {
		t.Fatalf("a fresh staging left the recovered marker: %v", err)
	}
}

// TestViewLossRekeysOnRename pins the move rule: a loss follows the name it
// belongs to, so a renamed subtree's descendants still fail their barriers at
// the destination instead of silently succeeding.
func TestViewLossRekeysOnRename(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c.writeViewLoss("/src/a.txt", 9, os.ErrNotExist)
	c.writeViewLoss("/src/sub/b.txt", 10, os.ErrNotExist)
	if !c.HasViewLossPrefix("/src/") {
		t.Fatal("prefix check missed a recorded loss")
	}
	c.RekeyViewLossPrefix("/src/", "/dst/")
	if err := c.ViewLoss("/src/a.txt"); err != nil {
		t.Fatal("the source name kept its loss record after the move")
	}
	if err := c.ViewLoss("/dst/a.txt"); err == nil {
		t.Fatal("the loss record did not follow the rename")
	}
	if err := c.ViewLoss("/dst/sub/b.txt"); err == nil {
		t.Fatal("the nested loss record did not follow the rename")
	}
	// A single-path rekey moves the in-memory record too.
	c.RekeyViewLoss("/dst/a.txt", "/dst/c.txt")
	if err := c.ViewLoss("/dst/c.txt"); err == nil {
		t.Fatal("single-path rekey dropped the record")
	}
	if err := c.ViewLoss("/dst/a.txt"); err != nil {
		t.Fatal("single-path rekey left the old name recorded")
	}
}

// TestListPendingKeepsEntriesOnTransientReadErrors pins the distinction the
// view loader already makes: only a missing .dat file means the image is gone.
func TestListPendingKeepsEntriesOnTransientReadErrors(t *testing.T) {
	const path = "/transient.txt"
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("staged image")
	if err := c.Put(path, payload, int64(len(payload)), PendingNew); err != nil {
		t.Fatal(err)
	}
	// A directory at the .dat path makes ReadFile fail with EISDIR: a transient
	// error that is not evidence the image is gone.
	if err := os.Remove(c.DatFile(path)); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(c.DatFile(path), 0o755); err != nil {
		t.Fatal(err)
	}
	_ = c.ListPending()
	if !c.HasMeta(path) {
		t.Fatal("a transient read error pruned the entry")
	}
	if err := c.ViewLoss(path); err != nil {
		t.Fatal("a transient read error recorded a loss")
	}
}

// TestClearViewLossDoesNotFsyncWhenNothingIsRecorded pins the hot path: a
// healthy mount has no markers, and a clear runs on every Put, so it must not
// sync the cache directory just to discover there is nothing to remove.
func TestClearViewLossDoesNotFsyncWhenNothingIsRecorded(t *testing.T) {
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	// A read-only cache directory makes any attempted fsync/removal fail loudly:
	// the clear must not even try when no marker exists.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	c.ClearViewLossIfGeneration("/healthy.txt", 5)
	c.ClearViewLoss("/healthy.txt")
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatal(err)
	}
}

// TestRenamePendingCarriesTheLossRecord pins the pair-move rule: the marker
// moves with the .dat/.meta pair, and the destination's replaced marker is gone.
func TestRenamePendingCarriesTheLossRecord(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put("/old.txt", []byte("payload"), 7, PendingNew); err != nil {
		t.Fatal(err)
	}
	c.writeViewLoss("/old.txt", 7, os.ErrNotExist)
	c.writeViewLoss("/new.txt", 3, os.ErrNotExist)
	if !c.RenamePending("/old.txt", "/new.txt") {
		t.Fatal("rename did not move the staged pair")
	}
	if err := c.ViewLoss("/new.txt"); err == nil {
		t.Fatal("the loss record did not follow the renamed pair")
	}
	if err := c.ViewLoss("/old.txt"); err != nil {
		t.Fatal("the old name kept its loss record")
	}
}

// TestHasViewLossPrefixFailsClosed pins the read-side rule: an unreadable marker
// must report a loss (true) so a directory rename cannot move over it.
func TestHasViewLossPrefixFailsClosed(t *testing.T) {
	dir := t.TempDir()
	first, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(first.lostFile("/corrupt.txt"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	first.writeViewLoss("/tree/lost.txt", 4, os.ErrNotExist)

	// Reopen: the prefix helpers answer from the in-process mirror, which the
	// load builds from disk — including the marker it could not decode.
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if !c.HasViewLossPrefix("/tree/") {
		t.Fatal("a recorded loss under the prefix was not reported")
	}
	if !c.HasViewLossPrefix("/corrupt.txt") {
		t.Fatal("an unreadable marker did not fail closed")
	}
	// Removing the attributable loss must not silence the unreadable one.
	c.ClearViewLossPrefix("/tree/")
	if !c.HasViewLossPrefix("/corrupt.txt") {
		t.Fatal("clearing a subtree dropped the unreadable marker's fail-closed answer")
	}
}

// TestViewLossCountIsCheapAndFailsClosed pins both halves of the drain counter:
// it is O(1) (the in-process mirror, no directory scan per syncfs) and it counts
// a marker that could not be decoded, because a drain must not certify a mount
// whose records it cannot read.
func TestViewLossCountIsCheapAndFailsClosed(t *testing.T) {
	dir := t.TempDir()
	const path = "/counted.txt"
	first, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	first.writeViewLoss(path, 3, os.ErrNotExist)
	if err := os.WriteFile(first.lostFile("/corrupt.txt"), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.ViewLossCount(); got < 2 {
		t.Fatalf("ViewLossCount = %d, want the recorded loss and the unreadable marker", got)
	}
	// Clearing the recorded loss keeps the unreadable marker counted.
	restarted.ClearViewLoss(path)
	if got := restarted.ViewLossCount(); got != 1 {
		t.Fatalf("ViewLossCount after clearing = %d, want 1 (the unreadable marker)", got)
	}
	// A fresh staging over the corrupt marker's own hash clears the unknown
	// bookkeeping: the marker is replaced by a readable one.
	if err := restarted.Put("/corrupt.txt", []byte("x"), 1, PendingNew); err != nil {
		t.Fatal(err)
	}
	if got := restarted.ViewLossCount(); got != 0 {
		t.Fatalf("ViewLossCount after restaging over the corrupt marker = %d, want 0", got)
	}
}

// TestRenamePendingMovesChmodOnlyEntry pins the blocker the review found: a
// chmod-only record has no .dat by design, so the rename must move the metadata
// without treating the missing payload as a lost image (which would drop the
// pending mode and wedge every later drain).
func TestRenamePendingMovesChmodOnlyEntry(t *testing.T) {
	const (
		src = "/chmod-src.txt"
		dst = "/chmod-dst.txt"
	)
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c.PublishMeta(WriteBackMeta{Path: src, Size: 7, Kind: PendingChmod, Generation: 11})
	if !c.RenamePending(src, dst) {
		t.Fatal("a chmod-only record did not move")
	}
	meta, ok := c.GetMeta(dst)
	if !ok || meta == nil || meta.Kind != PendingChmod {
		t.Fatalf("destination meta = %+v, %v; want the PendingChmod record", meta, ok)
	}
	if _, ok := c.GetMeta(src); ok {
		t.Fatal("the source record survived the rename")
	}
	if err := c.ViewLoss(dst); err != nil {
		t.Fatalf("a healthy chmod-only rename recorded a loss: %v", err)
	}
	if err := c.ViewLoss(src); err != nil {
		t.Fatalf("a healthy chmod-only rename recorded a loss on the source: %v", err)
	}
}

// TestRenamePendingRecordsMissingDataOnDestination pins the destination rule: the
// object is moving, so a payload that vanished before the rename must leave the
// loss on the destination (the callers rekey extents before this call, so a
// record on the dead source name would be stranded).
func TestRenamePendingRecordsMissingDataOnDestination(t *testing.T) {
	const (
		src = "/gone-src.txt"
		dst = "/gone-dst.txt"
	)
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Put(src, []byte("payload"), 7, PendingNew); err != nil {
		t.Fatal(err)
	}
	// Remove the payload behind the cache's back: the metadata still describes it.
	if err := os.Remove(c.DatFile(src)); err != nil {
		t.Fatal(err)
	}
	if c.RenamePending(src, dst) {
		t.Fatal("rename reported success over a missing payload")
	}
	if err := c.ViewLoss(dst); err == nil {
		t.Fatal("the missing payload was not recorded on the destination")
	}
	if _, ok := c.GetMeta(src); ok {
		t.Fatal("the orphaned source metadata survived")
	}
}

// TestRenamePendingClearsStaleDestinationPayload pins the data-corruption case:
// moving a record onto a destination that has a cached payload must drop that
// payload, or readers and the uploader serve the replaced incarnation's bytes
// under the source's metadata. It covers both a PendingChmod move (whose
// destination payload is dropped by design) and a disk-only source.
func TestRenamePendingClearsStaleDestinationPayload(t *testing.T) {
	t.Run("chmod-only onto cached destination", func(t *testing.T) {
		c, err := NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		// The destination has a real payload cached in memory.
		if err := c.Put("/d.txt", []byte("old destination"), 15, PendingNew); err != nil {
			t.Fatal(err)
		}
		c.PublishMeta(WriteBackMeta{Path: "/s.txt", Size: 4, Kind: PendingChmod, Generation: 21})
		if !c.RenamePending("/s.txt", "/d.txt") {
			t.Fatal("a chmod-only record did not move")
		}
		if _, _, state, ferr := c.GetMetaAndViewState("/d.txt"); state != StagedViewPendingChmod {
			t.Fatalf("state = %v (err %v), want StagedViewPendingChmod (no stale payload served)", state, ferr)
		}
	})

	t.Run("disk-only source onto cached destination", func(t *testing.T) {
		c, err := NewWriteBackCache(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Put("/dst2.txt", []byte("stale destination bytes"), 23, PendingNew); err != nil {
			t.Fatal(err)
		}
		// Keep the destination payload cached while the source's is evicted.
		if err := c.Put("/src2.txt", []byte("real source bytes"), 17, PendingNew); err != nil {
			t.Fatal(err)
		}
		c.DropInMemoryPayload("/src2.txt")
		if !c.RenamePending("/src2.txt", "/dst2.txt") {
			t.Fatal("rename failed")
		}
		meta, data, state, ferr := c.GetMetaAndViewState("/dst2.txt")
		if state != StagedViewOK || ferr != nil {
			t.Fatalf("state = %v (err %v), want StagedViewOK", state, ferr)
		}
		if meta.Size != 17 || string(data) != "real source bytes" {
			t.Fatalf("served size=%d data=%q, want the source image (17 bytes)", meta.Size, string(data))
		}
	})
}

// TestLossGenerationBoundNeverReturnsTheClearSentinel pins the inverted-sentinel
// bug: an unattributable marker must keep the landing from clearing it, which is
// a bound of 0 — returning ^uint64(0) would be read as "clear unconditionally".
func TestLossGenerationBoundNeverReturnsTheClearSentinel(t *testing.T) {
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	const path = "/unattributable.txt"
	if got := c.LossGenerationBound(path, 5); got != 5 {
		t.Fatalf("bound without a marker = %d, want the caller's 5", got)
	}
	// An unreadable marker for the path: the bound must refuse to clear. It is
	// only attributed at load, so reopen the cache over the same directory.
	if err := os.WriteFile(c.lostFile(path), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.LossGenerationBound(path, 5); got != 0 {
		t.Fatalf("bound over an unreadable marker = %d, want 0 (never the unconditional-clear sentinel)", got)
	}
	reopened.ClearViewLossIfGeneration(path, reopened.LossGenerationBound(path, 5))
	if err := reopened.ViewLoss(path); err == nil {
		t.Fatal("a landing cleared an unattributable marker")
	}
}

// TestGenerationCounterIgnoresTheSentinel pins the overflow guard: the
// unreadable-marker sentinel must never advance the counter (Add(1) would wrap to
// zero and violate the "generation != 0" invariant).
func TestGenerationCounterIgnoresTheSentinel(t *testing.T) {
	c, err := NewWriteBackCache(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c.advanceNextGen(^uint64(0))
	if got := c.nextGen.Load(); got == ^uint64(0) {
		t.Fatalf("counter advanced to the sentinel: %d", got)
	}
	if gen := c.nextGen.Add(1); gen == 0 {
		t.Fatal("the counter wrapped to zero after an advance to the sentinel")
	}
}

// TestUnattributableLossFollowsRekey pins the gap the review found: a marker
// that cannot be attributed must stay visible through ViewLoss (the destination
// is where the object now lives), survive a restart, and never be written as a
// finite or sentinel generation in the mirror.
func TestUnattributableLossFollowsRekey(t *testing.T) {
	dir := t.TempDir()
	const (
		src = "/unk-src.txt"
		dst = "/unk-dst.txt"
	)
	first, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(first.lostFile(src), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A fresh cache adopts the marker as unattributable.
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ViewLoss(src); err == nil {
		t.Fatal("the unreadable marker did not report a loss")
	}
	c.RekeyViewLoss(src, dst)
	if err := c.ViewLoss(dst); err == nil {
		t.Fatal("the unattributable loss did not follow the rekey")
	}
	if _, err := os.Stat(c.lostFile(dst)); err != nil {
		t.Fatalf("no durable marker at the destination: %v", err)
	}
	// A restart must still fail the destination closed.
	restarted, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.ViewLoss(dst); err == nil {
		t.Fatal("the rekeyed marker did not survive a restart")
	}
	if got := restarted.ViewLossCount(); got == 0 {
		t.Fatal("the drain counter dropped the rekeyed marker")
	}
}

// TestRecordViewLossNeverStoresNonFiniteGenerations pins the choke point: a
// failed unlink over an unreadable marker must not leave zero or the sentinel in
// the mirror, where LossGenerationBound and rekeyViewLoss would treat it as real.
func TestRecordViewLossNeverStoresNonFiniteGenerations(t *testing.T) {
	const path = "/nonfinite.txt"
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.recordViewLossLocked(path, ^uint64(0))
	c.recordViewLossLocked("/zero.txt", 0)
	c.mu.Unlock()
	for _, p := range []string{path, "/zero.txt"} {
		c.mu.Lock()
		gen, ok := c.lostGens[p]
		_, unk := c.lostUnknown[lostHash(p)]
		c.mu.Unlock()
		if ok {
			t.Fatalf("lostGens[%s] = %d, want no entry", p, gen)
		}
		if !unk {
			t.Fatalf("%s was not marked unknown", p)
		}
		if err := c.ViewLoss(p); err == nil {
			t.Fatalf("ViewLoss(%s) returned nil despite the unattributable record", p)
		}
	}
	if got := c.LossGenerationBound(path, 7); got != 0 {
		t.Fatalf("bound = %d, want 0 for an unattributable record", got)
	}
}
