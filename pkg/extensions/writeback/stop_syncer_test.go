package writeback

import (
	"context"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
)

func TestStopJoinsRealStagingSyncer(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		ledger, err := staging.NewStaging(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(ledger.Close)
		w, err := New(Options{Client: client.New("http://example.invalid", ""), Staging: ledger, LazyStaging: enabled, SyncWindow: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = w.Stop(context.Background()) })
		if enabled && w.syncerDone == nil {
			t.Fatal("syncer not started")
		}
		for i := 0; i < 2; i++ {
			if err := w.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		if enabled {
			select {
			case <-w.syncerDone:
			default:
				t.Fatal("Stop returned before journal syncer exited")
			}
		}
		if err := ledger.Journal.Append(staging.JournalEntry{Op: staging.JournalCommit, Path: "/still-owned"}); err != nil {
			t.Fatal("Stop prematurely closed owner staging", err)
		}
	}
}
