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
	if !c.HasViewLoss(path) {
		t.Fatal("no damage witness after the pair was pruned: a retried barrier would read it as already uploaded")
	}

	// Pruning removed the record, but the witness keeps the failure visible: a
	// barrier that retries must not read "nothing staged" as success.
	if _, _, state, err := c.GetMetaAndViewState(path); state != StagedViewNoData || err == nil {
		t.Fatalf("state for a pruned pair with a witness = %v (err %v), want StagedViewNoData", state, err)
	}

	// An explicit removal supersedes the witness.
	c.Remove(path)
	if c.HasViewLoss(path) {
		t.Fatal("damage witness survived an explicit removal")
	}
	if _, _, state, _ := c.GetMetaAndViewState(path); state != StagedViewNoMeta {
		t.Fatalf("state after explicit removal = %v, want StagedViewNoMeta", state)
	}
}
