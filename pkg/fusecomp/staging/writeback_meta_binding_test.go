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
