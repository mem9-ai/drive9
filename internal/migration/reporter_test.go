package migration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestEventReporterRetriesWithoutBlockingProducer(t *testing.T) {
	var calls atomic.Int32
	received := make(chan client.MigrationEvent, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		var event client.MigrationEvent
		_ = json.NewDecoder(r.Body).Decode(&event)
		w.WriteHeader(http.StatusNoContent)
		received <- event
	}))
	defer server.Close()
	reporter := newEventReporter(client.New(server.URL, "secret-key"), 1)
	ctx, cancel := context.WithCancel(context.Background())
	reporter.start(ctx)
	event := client.MigrationEvent{EventID: "event-1", SourceVersionToken: "token", SourcePath: "/path"}
	if !reporter.enqueue(event) {
		t.Fatal("first event was dropped")
	}
	select {
	case got := <-received:
		if got.EventID != event.EventID || got.SourceVersionToken != "token" {
			t.Fatalf("event=%+v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Reporter did not retry")
	}
	deadline := time.Now().Add(time.Second)
	for reporter.snapshot().Sent == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	reporter.wait()
	stats := reporter.snapshot()
	if stats.Sent != 1 || stats.Failed != 0 || calls.Load() != 2 {
		t.Fatalf("stats=%+v calls=%d", stats, calls.Load())
	}
}

func TestEventReporterBoundsQueueFailureAndRedacts(t *testing.T) {
	request := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		request <- string(body)
		http.NotFound(w, r)
	}))
	defer server.Close()
	reporter := newEventReporter(client.New(server.URL, "never-in-body"), 1)
	if !reporter.enqueue(client.MigrationEvent{EventID: "one"}) || reporter.enqueue(client.MigrationEvent{EventID: "two"}) {
		t.Fatal("bounded queue did not drop exactly the overflow")
	}
	ctx, cancel := context.WithCancel(context.Background())
	reporter.start(ctx)
	select {
	case body := <-request:
		if strings.Contains(body, "never-in-body") {
			t.Fatal("API key leaked into event body")
		}
	case <-time.After(time.Second):
		t.Fatal("Reporter did not send queued event")
	}
	deadline := time.Now().Add(time.Second)
	for reporter.snapshot().Failed == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	reporter.wait()
	stats := reporter.snapshot()
	if stats.Failed != 1 || stats.Dropped != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestDualCASReportsExactlyOneLogicalEventAndNeverBlocksRepair(t *testing.T) {
	for _, status := range []int{0, http.StatusInternalServerError} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "a"), []byte("source-data"), 0o644); err != nil {
				t.Fatal(err)
			}
			target := &memoryTarget{nodes: make(map[string]memoryTargetNode), eventStatus: status}
			now := time.Now()
			worker, server := newDualWorker(t, root, target, time.Minute, &now)
			defer server.Close()
			worker.startup = &Startup{
				Job:              Job{VolumeID: "vol-001", NodeName: "node", Target: TargetConfig{SpaceRef: "space", Prefix: "/data"}},
				acceptedTenantID: "tenant-a",
			}
			worker.reporter = newEventReporter(worker.api, 4)
			worker.apply.onCAS = worker.reportCAS
			ctx, cancel := context.WithCancel(context.Background())
			worker.reporter.start(ctx)
			defer func() { cancel(); worker.reporter.wait() }()
			if err := worker.DeepRecovery(context.Background()); err != nil {
				t.Fatal(err)
			}
			if worker.reporter.snapshot().Sent+worker.reporter.snapshot().Failed != 0 {
				t.Fatal("event emitted before a CAS attempt")
			}
			now = now.Add(time.Minute)
			if err := worker.Round(context.Background(), RoundModeFast); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				stats := worker.reporter.snapshot()
				if stats.Sent+stats.Failed == 1 || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			stats := worker.reporter.snapshot()
			if !worker.State().Conditions.CurrentConverged || stats.Sent+stats.Failed != 1 {
				t.Fatalf("repair state=%+v reporter=%+v", worker.State(), stats)
			}
			target.mu.Lock()
			events := append([]client.MigrationEvent(nil), target.events...)
			target.mu.Unlock()
			if len(events) == 0 || events[0].CASAttempt != 1 || events[0].Phase != string(PhaseDualWriteRepairing) || events[0].SpaceID != "tenant-a" || events[0].SourceVersionToken == "" || events[0].Operation != "create" || events[0].SourceChecksumSHA256 == "" || events[0].ErrorClass != "none" {
				t.Fatalf("events=%+v", events)
			}
			for _, event := range events[1:] {
				if event.EventID != events[0].EventID {
					t.Fatalf("endpoint retry created a second logical event: %+v", events)
				}
			}
		})
	}
}

func TestSyncingCASNeverReportsMigrationEvent(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	worker, server := newRoundWorker(t, root, target)
	defer server.Close()
	worker.startup = &Startup{Job: Job{VolumeID: "vol-001", Target: TargetConfig{Prefix: "/data"}}}
	worker.reporter = newEventReporter(worker.api, 1)
	worker.apply.onCAS = worker.reportCAS
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	if snapshot := worker.reporter.snapshot(); snapshot != (ReporterSnapshot{}) {
		t.Fatalf("SYNCING emitted event: %+v", snapshot)
	}
}

func TestDualCandidateConvergingBeforeCASReportsNothing(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("source"), 0o644); err != nil {
		t.Fatal(err)
	}
	target := &memoryTarget{nodes: make(map[string]memoryTargetNode)}
	now := time.Now()
	worker, server := newDualWorker(t, root, target, time.Minute, &now)
	defer server.Close()
	worker.startup = &Startup{Job: Job{VolumeID: "vol-001", Target: TargetConfig{Prefix: "/data"}}}
	worker.reporter = newEventReporter(worker.api, 1)
	worker.apply.onCAS = worker.reportCAS
	if err := worker.DeepRecovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	target.nodes["a"] = memoryTargetNode{data: []byte("source"), revision: 1, mode: 0o100644, resourceID: "business-write"}
	target.mu.Unlock()
	now = now.Add(30 * time.Second)
	if err := worker.Round(context.Background(), RoundModeFast); err != nil {
		t.Fatal(err)
	}
	if len(worker.State().Grace) != 0 || worker.reporter.snapshot() != (ReporterSnapshot{}) {
		t.Fatalf("pre-CAS convergence state=%+v reporter=%+v", worker.State(), worker.reporter.snapshot())
	}
}

func TestEventReporterHungEndpointTimesOut(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	defer func() { close(release); server.Close() }()
	reporter := newEventReporter(client.New(server.URL, "key"), 1)
	ctx, cancel := context.WithCancel(context.Background())
	reporter.start(ctx)
	if !reporter.enqueue(client.MigrationEvent{EventID: "hung"}) {
		t.Fatal("event dropped")
	}
	deadline := time.Now().Add(3 * time.Second)
	for reporter.snapshot().Failed == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	reporter.wait()
	if reporter.snapshot().Failed != 1 {
		t.Fatalf("hung stats=%+v", reporter.snapshot())
	}
}
