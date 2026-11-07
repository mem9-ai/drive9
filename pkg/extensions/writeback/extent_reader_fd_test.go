package writeback

import (
	"context"
	"os"
	"runtime/debug"
	"testing"
)

// A spilled extent upload opens the shadow file to stream from it. That
// descriptor belongs to the upload, so it must be closed when the upload
// finishes — on success and on the size-mismatch error path alike.
//
// The GC is disabled for the measurement: a leaked *os.File is eventually
// closed by its finalizer, which hides exactly the bug this guards (the
// unfixed path leaked 42 descriptors across 40 uploads, and only a handful
// when finalizers were allowed to run).
func TestSpilledExtentUploadClosesItsReader(t *testing.T) {
	old := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(old)

	content := make([]byte, 12<<20)
	for i := range content {
		content[i] = byte(i%251) + 1
	}
	cq, _, entry, _ := newSpilledExtentQueue(t, content)

	countFDs := func() int {
		ents, err := os.ReadDir("/dev/fd")
		if err != nil {
			t.Skipf("no /dev/fd on this platform: %v", err)
		}
		return len(ents)
	}
	before := countFDs()
	const uploads = 40
	for i := 0; i < uploads; i++ {
		if _, err := cq.uploadEntry(context.Background(), entry); err != nil {
			t.Fatalf("upload %d: %v", i, err)
		}
	}
	after := countFDs()
	if after-before > 4 {
		t.Fatalf("%d descriptors leaked across %d spilled uploads (before=%d after=%d)",
			after-before, uploads, before, after)
	}
}
