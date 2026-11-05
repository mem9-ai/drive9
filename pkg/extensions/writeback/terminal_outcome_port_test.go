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
	cq.RecordTerminalFailureForPath("/p.bin", boom)

	// The path is idle (nothing queued), so an idle-only wait returns true...
	if !cq.WaitPathDeadline("/p.bin", time.Second) {
		t.Fatal("precondition: the path must be idle")
	}
	// ...but the outcome-aware wait must surface the failure.
	if got := cq.WaitPathOutcome("/p.bin", time.Second); !errors.Is(got, boom) {
		t.Fatalf("WaitPathOutcome = %v, want the terminal upload failure", got)
	}
}

// A remembered failure belongs to the PUBLICATION that failed, not to the
// pathname forever: after a later successful publication of the same name, a
// Sync must not keep reporting the old error.
func TestTerminalFailureRetiredBySuccess(t *testing.T) {
	shadow, err := staging.NewShadowStoreWithQuota(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer shadow.Close()
	cq := NewCommitQueue(newTestClient("http://127.0.0.1:1"), shadow, nil, nil, 1, 8)
	defer cq.DrainAll()

	boom := errors.New("predecessor failed")
	cq.RecordTerminalFailureForPath("/q.bin", boom)
	if got := cq.WaitPathOutcome("/q.bin", time.Second); !errors.Is(got, boom) {
		t.Fatalf("precondition: outcome = %v, want the failure", got)
	}

	// A later successful publication retires it. Exercised through the REAL
	// success path (landedAt), not the clearer directly.
	wb, werr := New(Options{Policy: PolicyCloseSync, Client: newTestClient("http://127.0.0.1:1"), Staging: mustStaging(t)})
	if werr != nil {
		t.Fatal(werr)
	}
	if wb.queue == nil {
		t.Fatal("precondition: the writeback must own a commit queue")
	}
	wb.queue.RecordTerminalFailureForPath("/q.bin", boom)
	wb.landedAt("/q.bin", 5, 1024, 0, nil)
	if got := wb.queue.WaitPathOutcome("/q.bin", time.Second); got != nil {
		t.Fatalf("after a successful publication, outcome = %v, want nil", got)
	}
}
