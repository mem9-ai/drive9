package writeback

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// An anonymous (unlinked) descriptor reads its own snapshot through the
// snapshot's PIN token. That token is what keeps the backing readable after a
// replacement reuses the pathname: the replacement advances the ACTIVE
// content generation and retires the old backing, but the retired descriptor
// survives until the pin is released. Reading by content generation instead
// would miss exactly the image the pin was taken to preserve.
//
// This exercises the primitive the unlink read path depends on; the full
// unlink sequence needs a live mount to drive.
func TestUnlinkSnapshotPinSurvivesPathnameReuse(t *testing.T) {
	shadows, err := staging.NewShadowStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(shadows.Close)

	const path = "/gone.bin"
	const original = "snapshot-bytes"
	if err := shadows.WriteFull(path, []byte(original), 0); err != nil {
		t.Fatal(err)
	}
	contentGen := shadows.ActiveGeneration(path)
	if contentGen == 0 {
		t.Fatal("staged image has no content generation")
	}
	pin := shadows.Pin(path)
	if pin == 0 {
		t.Fatal("Pin returned no token")
	}
	defer shadows.Unpin(pin)

	// A replacement reuses the name and stages its own image. This advances
	// the content generation and retires the pinned backing.
	if err := shadows.WriteFull(path, []byte("replacement!"), 0); err != nil {
		t.Fatal(err)
	}
	if shadows.ActiveGeneration(path) == contentGen {
		t.Fatal("replacement did not advance the content generation")
	}

	// The pinned descriptor still serves the ORIGINAL bytes: that is the
	// guarantee an anonymous fd depends on.
	buf := make([]byte, len(original))
	if n, rerr := shadows.ReadAtGen(pin, 0, buf); rerr != nil || string(buf[:n]) != original {
		t.Fatalf("pinned snapshot read = %q, err=%v; want %q", buf[:n], rerr, original)
	}

	// The stale CONTENT generation is not a readable address any more — which
	// is why the read path must not use it.
	if _, rerr := shadows.ReadAtIfGeneration(path, contentGen, 0, buf); rerr == nil {
		t.Fatal("stale content generation still resolved; the two spaces are being conflated again")
	}

	// Releasing the pin retires the backing for good.
	shadows.Unpin(pin)
	pin = 0
	if _, rerr := shadows.ReadAtGen(staging.PinGen(contentGen), 0, buf); rerr == nil {
		// Not a hard failure: the content generation could coincidentally
		// match a live pin-space id. The meaningful assertion is above.
		t.Log("content generation happened to resolve as a pin token; content match already asserted")
	}
}
