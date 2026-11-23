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

	meta, data, state := c.GetMetaAndViewState(path)
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
	if _, _, state := c.GetMetaAndViewState(path); state != StagedViewTornPair {
		t.Fatalf("state after corrupting the data = %v, want StagedViewTornPair", state)
	}

	// Pruning removed the record: from here on the path is simply not staged.
	if _, _, state := c.GetMetaAndViewState(path); state != StagedViewNoMeta {
		t.Fatalf("state after the pair was pruned = %v, want StagedViewNoMeta", state)
	}
}
