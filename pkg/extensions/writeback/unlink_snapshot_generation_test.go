package writeback

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// An anonymous (unlinked) descriptor reads its own snapshot, and that
// snapshot is bound to a CONTENT generation. Reading that id through the
// pin-generation API never resolves — the two are different number spaces —
// so the branch used to fall through to the pending/shadow tiers, which
// describe whichever object now owns the reused name.
//
// This exercises the read primitive the branch depends on, because the full
// unlink path needs a live mount to drive.
func TestUnlinkSnapshotReadsThroughContentGeneration(t *testing.T) {
	shadows, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)

	const path = "/gone.bin"
	if err := shadows.WriteFull(path, []byte("snapshot-bytes"), 0); err != nil {
		t.Fatal(err)
	}
	contentGen := shadows.ActiveGeneration(path)
	if contentGen == 0 {
		t.Fatal("staged image has no content generation")
	}

	// The image the snapshot recorded is readable through its content
	// generation...
	buf := make([]byte, len("snapshot-bytes"))
	if n, rerr := shadows.ReadAtIfGeneration(path, contentGen, 0, buf); rerr != nil || string(buf[:n]) != "snapshot-bytes" {
		t.Fatalf("content-generation read = %q, err=%v", buf[:n], rerr)
	}

	// ...and after a replacement advances the generation it is NOT, which is
	// what makes the snapshot boundary meaningful.
	if err := shadows.WriteFull(path, []byte("replacement!"), 0); err != nil {
		t.Fatal(err)
	}
	if shadows.ActiveGeneration(path) == contentGen {
		t.Fatal("replacement did not advance the content generation")
	}
	if _, rerr := shadows.ReadAtIfGeneration(path, contentGen, 0, buf); rerr == nil {
		t.Fatal("stale snapshot read succeeded after the path was replaced")
	}
}
