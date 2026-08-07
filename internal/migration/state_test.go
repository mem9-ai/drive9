package migration

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func completeTestRound(id string, converged bool) Round {
	now := time.Now()
	return Round{
		ID:           id,
		Mode:         RoundModeDeep,
		StartedAt:    now.Add(-time.Second),
		CompletedAt:  now,
		ScanComplete: true,
		Converged:    converged,
		Source: map[string]SourceEntry{
			"/file": {Path: "/file", Kind: EntryRegular, Version: SourceVersion{Device: 1, Inode: 2, Size: 3}},
		},
		Target: map[string]TargetEntry{
			"/file": {Path: "/file", Kind: EntryRegular, Revision: 4, ChecksumSHA256: strings.Repeat("a", 64)},
		},
		Findings: []Finding{{Path: "/file", Kind: FindingContent, Severity: SeverityWarning}},
	}
}

func markTestRoundReconciled(state *State, round Round) {
	for path, entry := range round.Source {
		state.MarkReconciled(path, entry.Version)
	}
}

func TestStateStartsEmptyAndRestartDoesNotRestoreWorkingData(t *testing.T) {
	state := NewState(PhaseSyncing)
	snapshot := state.Snapshot()
	if snapshot.Phase != PhaseSyncing || snapshot.Current.ID != "" || snapshot.LastComplete != nil {
		t.Fatalf("initial snapshot=%+v", snapshot)
	}
	if snapshot.Conditions.ReadyForRollout || snapshot.Conditions.CurrentConverged || snapshot.Conditions.Attention || snapshot.RecoveryComplete || snapshot.InitialCopyComplete {
		t.Fatalf("initial conditions=%+v", snapshot)
	}
	if snapshot.RepairMtimeFloor != nil || snapshot.Verification.Status != "" || len(snapshot.Grace) != 0 || len(snapshot.Retry) != 0 || len(snapshot.Observed) != 0 || len(snapshot.Reconciled) != 0 || snapshot.ActiveOperations != 0 {
		t.Fatalf("initial working data=%+v", snapshot)
	}

	state.BeginRound("one", RoundModeDeep, time.Now())
	if err := state.PublishRound(completeTestRound("one", true)); err != nil {
		t.Fatal(err)
	}
	restarted := NewState(PhaseSyncing).Snapshot()
	if restarted.LastComplete != nil || restarted.Current.ID != "" || restarted.Conditions.ReadyForRollout || len(restarted.Observed) != 0 || len(restarted.Reconciled) != 0 {
		t.Fatalf("restart inherited working data=%+v", restarted)
	}
}

func TestStateNeverPublishesIncompleteRound(t *testing.T) {
	state := NewState(PhaseSyncing)
	state.BeginRound("incomplete", RoundModeDeep, time.Now())
	round := completeTestRound("incomplete", true)
	round.ScanComplete = false
	if err := state.PublishRound(round); !errors.Is(err, ErrIncompleteRound) {
		t.Fatalf("publish error=%v", err)
	}
	snapshot := state.Snapshot()
	if snapshot.LastComplete != nil || snapshot.Current.ScanComplete || snapshot.Current.Converged || snapshot.Conditions.ReadyForRollout {
		t.Fatalf("incomplete round was published=%+v", snapshot)
	}
}

func TestStateFailureRetainsLastCompleteButClearsConvergence(t *testing.T) {
	state := NewState(PhaseSyncing)
	state.SetRecoveryComplete(true)
	state.SetInitialCopyComplete(true)
	state.BeginRound("complete", RoundModeDeep, time.Now())
	round := completeTestRound("complete", true)
	markTestRoundReconciled(state, round)
	if err := state.PublishRound(round); err != nil {
		t.Fatal(err)
	}
	if !state.Snapshot().Conditions.ReadyForRollout {
		t.Fatal("complete converged round did not set readiness")
	}
	state.BeginRound("failed", RoundModeFast, time.Now())
	state.FailRound("failed", "scan")
	snapshot := state.Snapshot()
	if snapshot.LastComplete == nil || snapshot.LastComplete.ID != "complete" {
		t.Fatalf("last complete=%+v", snapshot.LastComplete)
	}
	if snapshot.Current.ScanComplete || snapshot.Current.Converged || snapshot.Conditions.ReadyForRollout || snapshot.Conditions.CurrentConverged {
		t.Fatalf("failure retained convergence=%+v", snapshot)
	}
}

func TestStateAttentionForcesConditionsFalse(t *testing.T) {
	state := NewState(PhaseSyncing)
	state.SetRecoveryComplete(true)
	state.SetInitialCopyComplete(true)
	state.BeginRound("complete", RoundModeDeep, time.Now())
	round := completeTestRound("complete", true)
	markTestRoundReconciled(state, round)
	if err := state.PublishRound(round); err != nil {
		t.Fatal(err)
	}
	state.SetAttention(true)
	conditions := state.Snapshot().Conditions
	if !conditions.Attention || conditions.ReadyForRollout || conditions.CurrentConverged {
		t.Fatalf("attention conditions=%+v", conditions)
	}
	state.SetAttention(false)
	if !state.Snapshot().Conditions.ReadyForRollout {
		t.Fatal("rechecked cleared Attention did not restore derived readiness")
	}
}

func TestStatePublishClonesAndReleasesPriorManifest(t *testing.T) {
	state := NewState(PhaseSyncing)
	state.BeginRound("first", RoundModeDeep, time.Now())
	first := completeTestRound("first", false)
	for i := range 1000 {
		path := fmt.Sprintf("/old/%d", i)
		first.Source[path] = SourceEntry{Path: path, Kind: EntryDirectory}
	}
	if err := state.PublishRound(first); err != nil {
		t.Fatal(err)
	}
	first.Source["/mutated"] = SourceEntry{Path: "/mutated"}
	if _, ok := state.Snapshot().LastComplete.Source["/mutated"]; ok {
		t.Fatal("published state aliases caller map")
	}

	state.BeginRound("second", RoundModeDeep, time.Now())
	second := completeTestRound("second", true)
	if err := state.PublishRound(second); err != nil {
		t.Fatal(err)
	}
	snapshot := state.Snapshot()
	if snapshot.LastComplete.ID != "second" || len(snapshot.LastComplete.Source) != 1 {
		t.Fatalf("prior manifest retained=%+v", snapshot.LastComplete)
	}
}

func TestStateConcurrentSnapshotsAreRaceFree(t *testing.T) {
	state := NewState(PhaseDualWriteRepairing)
	state.SetRecoveryComplete(true)
	var wg sync.WaitGroup
	for reader := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_ = state.Snapshot()
			}
		}()
		_ = reader
	}
	for i := range 100 {
		id := fmt.Sprintf("round-%d", i)
		state.BeginRound(id, RoundModeFast, time.Now())
		if err := state.PublishRound(completeTestRound(id, i%2 == 0)); err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
}

func TestCASEventContextDoesNotCloneCompleteRound(t *testing.T) {
	state := NewState(PhaseDualWriteRepairing)
	state.BeginRound("round", RoundModeFast, time.Now())
	version := SourceVersion{Device: 1, Inode: 2, Kind: EntryRegular, Size: 3, MtimeNS: 4, CtimeNS: 5, Mode: 0o644}
	candidate := state.trackGrace("/file", version, time.Now().Add(-time.Minute))

	largeRound := &Round{Source: make(map[string]SourceEntry, 10_000), Target: make(map[string]TargetEntry, 10_000)}
	for i := range 10_000 {
		path := fmt.Sprintf("/entry/%d", i)
		largeRound.Source[path] = SourceEntry{Path: path}
		largeRound.Target[path] = TargetEntry{Path: path}
	}
	state.mu.Lock()
	state.lastComplete = largeRound
	state.mu.Unlock()

	token := sourceVersionToken(version)
	context, ok := state.eventContextForCAS("/file", token)
	if !ok || context.Phase != PhaseDualWriteRepairing || context.RoundID != "round" || context.Candidate != candidate {
		t.Fatalf("event context=%+v found=%v", context, ok)
	}
	if _, ok := state.eventContextForCAS("/missing", token); ok {
		t.Fatal("missing Grace candidate returned an event context")
	}
	if allocs := testing.AllocsPerRun(20, func() {
		_, _ = state.eventContextForCAS("/file", token)
	}); allocs > 2 {
		t.Fatalf("event context allocated %.1f objects; likely cloned process state", allocs)
	}
}

func TestStateSnapshotHasNoSecretOrFileContentField(t *testing.T) {
	state := NewState(PhaseSyncing)
	state.BeginRound("one", RoundModeDeep, time.Now())
	if err := state.PublishRound(completeTestRound("one", false)); err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(state.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"api_key", "credential", "file_content", "upload_id", "sensitive-file-bytes"} {
		if strings.Contains(string(body), forbidden) {
			t.Fatalf("snapshot contains forbidden field/value %q: %s", forbidden, body)
		}
	}
}
