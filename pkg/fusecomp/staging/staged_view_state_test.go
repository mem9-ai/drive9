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

	// The pruned pair is gone: a later lookup reports nothing staged. Reporting
	// the loss beyond that belongs to the caller that owns a durable failure
	// record (the commit queue's terminal failures), not to this cache.
	if _, _, state, _ := c.GetMetaAndViewState(path); state != StagedViewNoMeta {
		t.Fatalf("state after the pair was pruned = %v, want StagedViewNoMeta", state)
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
	if got.state != StagedViewNoMeta {
		t.Fatalf("barrier state after the prune = %v, want StagedViewNoMeta once the loss is recorded", got.state)
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
