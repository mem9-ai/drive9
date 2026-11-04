package writeback

import (
	"errors"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

// The synchronous durability policies (fsync / write-sync / close-sync) wait
// for their path's queued upload before reporting success. A path can be
// IDLE because its upload terminally failed — the queue removes the entry —
// so an idle-only wait would acknowledge durability for bytes that never
// landed. WaitPathOutcome must report that failure.
func TestWaitPathOutcomeReportsTerminalFailure(t *testing.T) {
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	cq := NewCommitQueue(newTestClient("http://127.0.0.1:1"), shadow, nil, nil, 1, 8)
	defer cq.DrainAll()

	boom := errors.New("server rejected the upload")
	cq.rememberPathCommitErr(&CommitEntry{Path: "/p.bin"}, boom)

	// The path is idle (nothing queued), so an idle-only wait returns true...
	if !cq.WaitPathDeadline("/p.bin", time.Second) {
		t.Fatal("precondition: the path must be idle")
	}
	// ...but the outcome-aware wait must surface the failure.
	if got := cq.WaitPathOutcome("/p.bin", time.Second); !errors.Is(got, boom) {
		t.Fatalf("WaitPathOutcome = %v, want the terminal upload failure", got)
	}

	// A later successful commit clears it: a stale failure must not surface
	// for a path that has since landed.
	cq.rememberPathCommitErr(&CommitEntry{Path: "/p.bin"}, nil)
	if got := cq.WaitPathOutcome("/p.bin", time.Second); got != nil {
		t.Fatalf("WaitPathOutcome after success = %v, want nil", got)
	}
}
