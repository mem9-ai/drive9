package extent

import (
	"context"
	"syscall"
	"testing"
)

// The floor must be the revision the COMPLETION echoed, not the one a stat
// happens to report. This is the discriminating case: the server's stat answer
// lags the completion (exactly what a replica does), so a bridge that only
// records stats settles on the OLD value and its freshness guard can never
// fire. The echo is the authoritative value.
func TestFreshCreateFloorComesFromTheEchoNotTheStat(t *testing.T) {
	f := newFakeV2(t)
	f.setDir("/d", true)
	// The completion will echo 51; every stat answers its hardcoded 3.
	f.mu.Lock()
	f.completes = 50
	f.mu.Unlock()
	h := newTestHandler(t, f, 0)
	b := h.opts.Engine.Bridge
	ctx := context.Background()

	res, err, _ := dispatchCreate(ctx, h, "/d/a.db", syscall.O_RDWR|syscall.O_CREAT)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	eh := res.Handle.(*engineHandle)
	if err := eh.Write(ctx, 0, []byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := eh.Sync(ctx); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := b.publishedRevision(eh.ino); got != 51 {
		t.Fatalf("publishedRevision = %d, want the echoed 51; a lagging stat (3) must not be what reads are judged against", got)
	}
}
