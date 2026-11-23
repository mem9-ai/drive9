package staging

import (
	"os"
	"testing"
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
