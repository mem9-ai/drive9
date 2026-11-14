//go:build failpoint

package writeback

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/fusecomp/staging"
	"github.com/pingcap/failpoint"
)

func TestStopWaitsForActualJournalSyncLoopExit(t *testing.T) {
	ledger, err := staging.NewStaging(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	name := "github.com/mem9-ai/drive9/pkg/extensions/writeback/writebackSyncLoopExiting"
	if err := failpoint.EnableCall(name, func() { close(entered); <-release }); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = failpoint.Disable(name) }()
	w, err := New(Options{Client: client.New("http://example.invalid", ""), Staging: ledger, LazyStaging: true, SyncWindow: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	go func() { done <- w.Stop(t.Context()) }()
	<-entered
	// Cancellation has already reached the actual SyncLoop; it has not yet
	// exited. A concurrent Stop must also wait for that same completion.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	go func() { done <- w.Stop(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("Stop returned before loop exited: %v", err)
	default:
	}
	once.Do(func() { close(release) })
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-w.syncerDone:
	default:
		t.Fatal("syncer still alive")
	}
}
