package staging

import (
	"os"
	"testing"
)

// TestTornPairPrunedNotServed pins the DataCRC binding: the batched
// dir-sync put stages .dat/.meta through two renames that a crash between
// them can mix — a surviving new .meta over a stale .dat. Such a torn pair
// must be pruned at read time, never served: uploading the stale bytes at
// the NEW base revision would silently publish old content.
func TestTornPairPrunedNotServed(t *testing.T) {
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	const path = "/f.bin"

	first := []byte("generation one")
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, first, int64(len(first)), PendingOverwrite, 4, 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	// A second staging of the SAME length (a torn pair of equal sizes
	// cannot be caught by length alone) with different content and a new
	// base revision.
	second := []byte("generation two")
	gen, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, second, int64(len(second)), PendingOverwrite, 9, 0o644, false, "", "", false)
	if err != nil {
		t.Fatal(err)
	}
	meta, data, ok := c.GetMetaAndView(path)
	if !ok || string(data) != "generation two" || meta.BaseRev != 9 {
		t.Fatalf("intact pair not served: ok=%t data=%q meta=%+v", ok, data, meta)
	}

	// Simulate the crash window: the .dat's rename is lost, leaving the
	// stale generation's bytes under the new metadata. Dropping the
	// in-memory payload mirrors the restart a real crash forces — only
	// then does the read path load the pair from disk.
	if err := os.WriteFile(c.DatFile(path), first, 0o644); err != nil {
		t.Fatal(err)
	}
	c.DropInMemoryPayload(path)
	if _, _, ok := c.GetMetaAndView(path); ok {
		t.Fatal("torn pair was served instead of pruned")
	}
	if _, ok := c.GetMeta(path); ok {
		t.Fatal("torn pair's metadata survived the prune")
	}
	if _, err := os.Stat(c.DatFile(path)); !os.IsNotExist(err) {
		t.Fatal("torn pair's data file survived the prune")
	}

	// A fresh staging after the prune works normally.
	third := []byte("generation three")
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, third, int64(len(third)), PendingOverwrite, int64(gen+1), 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	if _, data, ok := c.GetMetaAndView(path); !ok || string(data) != "generation three" {
		t.Fatalf("re-stage after prune not served: ok=%t data=%q", ok, data)
	}
}

// TestTornPairEmptyTruncationPruned pins the zero-checksum edge: an empty
// payload binds with DataCRC 0 and Size 0, so "checksum absent" must never
// be inferred from the value — a truncation-to-empty whose .dat rename was
// lost leaves the stale nonempty bytes under the new metadata, and the
// binding (length first) must prune that pair, not serve or upload the old
// contents at the truncation's base revision.
func TestTornPairEmptyTruncationPruned(t *testing.T) {
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	const path = "/empty.bin"

	old := []byte("previous contents")
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, old, int64(len(old)), PendingOverwrite, 3, 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	// The truncation snapshot: empty image, bound with CRC 0 and Size 0.
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, nil, 0, PendingOverwrite, 7, 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	// Crash window: the truncation's .dat rename is lost.
	if err := os.WriteFile(c.DatFile(path), old, 0o644); err != nil {
		t.Fatal(err)
	}
	c.DropInMemoryPayload(path)

	if _, _, ok := c.GetMetaAndView(path); ok {
		t.Fatal("torn truncation pair was served instead of pruned")
	}
	if _, ok := c.GetMeta(path); ok {
		t.Fatal("torn truncation pair's metadata survived the prune")
	}
}

// TestTornPairPrunedOnPlainReads pins the consumer choke point: the binding
// is enforced where the payload is loaded (getViewLocked), so the ordinary
// read paths — Get/GetView, the pending-image capture and writable-open
// preloads behind them — cannot observe a torn pair either; pruning must
// not depend on the uploader's checked getters running first.
func TestTornPairPrunedOnPlainReads(t *testing.T) {
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	const path = "/reads.bin"

	first := []byte("stale generation")
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, first, int64(len(first)), PendingOverwrite, 2, 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	second := []byte("freshmutation") // same length, different bytes
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, second, int64(len(second)), PendingOverwrite, 5, 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.DatFile(path), first, 0o644); err != nil {
		t.Fatal(err)
	}
	c.DropInMemoryPayload(path)

	if _, ok := c.Get(path); ok {
		t.Fatal("plain Get served a torn pair")
	}
	// The prune already ran on that Get; a later view must see nothing
	// (and the files are gone), exactly like the checked getters.
	if _, ok := c.GetView(path); ok {
		t.Fatal("GetView served a torn pair after the prune")
	}
	if _, ok := c.GetMeta(path); ok {
		t.Fatal("torn pair's metadata survived the prune")
	}
}

// TestTornPairPrunedAtRecoveryAdmission pins the recovery path: ListPending
// admits payloads into the in-memory cache itself, so a torn pair left by a
// crash must be caught THERE — once cached, the memory fast path serves the
// stale bytes unchecked and the upload republishes old contents at the new
// base. A fresh instance models the restart: no in-memory state exists, and
// the pair reaches memory only through this scan.
func TestTornPairPrunedAtRecoveryAdmission(t *testing.T) {
	dir := t.TempDir()
	c, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	const path = "/recover.bin"

	old := []byte("crash-survivor bytes")
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, old, int64(len(old)), PendingOverwrite, 3, 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	empty := []byte{}
	if _, _, err := c.PutWithBaseRevAndModeAndLineageTimingsDirSync(path, empty, 0, PendingOverwrite, 7, 0o644, false, "", "", false); err != nil {
		t.Fatal(err)
	}
	// The crash window: the truncation's .dat rename is lost.
	if err := os.WriteFile(c.DatFile(path), old, 0o644); err != nil {
		t.Fatal(err)
	}

	// Fresh instance: the constructor scans the metadata, ListPending is
	// the recovery admission of the payload.
	c2, err := NewWriteBackCache(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range c2.ListPending() {
		if e.Meta.Path == path {
			t.Fatalf("torn pair admitted by recovery: size=%d base=%d", e.Meta.Size, e.Meta.BaseRev)
		}
	}
	if _, ok := c2.GetMeta(path); ok {
		t.Fatal("torn pair's metadata survived recovery admission")
	}
	if _, err := os.Stat(c2.DatFile(path)); !os.IsNotExist(err) {
		t.Fatal("torn pair's data file survived recovery admission")
	}
}
