package migration

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestStreamDiffClassifiesCompleteGenerations(t *testing.T) {
	ctx := context.Background()
	store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	source := []generationRecord{
		sourceTestRecord("/a", EntryRegular, "aaa", ""),
		sourceTestRecord("/b", EntryDirectory, "", ""),
		sourceTestRecord("/c", EntryRegular, "ccc", ""),
	}
	target := []generationRecord{
		targetTestRecord("/a", EntryRegular, "aaa", "inode-a", 1),
		targetTestRecord("/c", EntryRegular, "different", "inode-c", 1),
		targetTestRecord("/d", EntryRegular, "ddd", "inode-d", 1),
	}
	sourceMetadata := saveTestGeneration(t, ctx, store, "source-a", stageSource, recordSource, source)
	targetMetadata := saveTestGeneration(t, ctx, store, "target-a", stageTarget, recordTarget, target)
	builder := testStreamDiffBuilder(t, store, "diff-a")
	result, err := builder.Build(ctx, sourceMetadata.GenerationID, targetMetadata.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.SourceCount != 3 || result.Summary.TargetCount != 3 || result.Summary.BlockerCount != 3 || result.Summary.Converged {
		t.Fatalf("summary = %+v", result.Summary)
	}
	if result.Summary.WorkCounts["mkdir"] != 1 || result.Summary.WorkCounts["write"] != 1 || result.Summary.WorkCounts["delete"] != 1 {
		t.Fatalf("work counts = %v", result.Summary.WorkCounts)
	}
	if result.Metadata.WorkCounts["mkdir"] != 1 || result.Metadata.WorkCounts["write"] != 1 || result.Metadata.WorkCounts["delete"] != 1 {
		t.Fatalf("metadata work counts = %v", result.Metadata.WorkCounts)
	}
	findings := readDiffFindings(t, ctx, store, result.Metadata)
	if !slices.Equal(findings, []FindingKind{FindingContent, FindingSourceOnly, FindingTargetOnly}) {
		t.Fatalf("findings = %v", findings)
	}
	if _, err := store.LoadComplete(ctx, result.Metadata.GenerationID, result.Metadata.Identity); err != nil {
		t.Fatal(err)
	}
}

func TestStreamDiffHardlinkIdentityUsesBoundedResourceAndOwnerGroups(t *testing.T) {
	ctx := context.Background()
	store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	source := []generationRecord{
		sourceTestRecord("/alias-a", EntryRegular, "aaa", "1:2"),
		sourceTestRecord("/alias-b", EntryRegular, "aaa", "1:2"),
	}
	target := []generationRecord{
		targetTestRecord("/alias-a", EntryRegular, "aaa", "inode-a", 1),
		targetTestRecord("/alias-b", EntryRegular, "aaa", "inode-b", 1),
	}
	sourceMetadata := saveTestGeneration(t, ctx, store, "source-links", stageSource, recordSource, source)
	targetMetadata := saveTestGeneration(t, ctx, store, "target-links", stageTarget, recordTarget, target)
	result, err := testStreamDiffBuilder(t, store, "diff-links").Build(ctx, sourceMetadata.GenerationID, targetMetadata.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.FindingCounts[FindingIdentity] == 0 || result.Summary.Converged {
		t.Fatalf("summary = %+v", result.Summary)
	}
	stage := result.Metadata.Stages[stageDiff]
	operations := make(map[string]int)
	for _, descriptor := range stage.Chunks {
		reader, err := store.OpenChunk(ctx, result.Metadata.GenerationID, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		for {
			record, ok, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if record.Diff != nil {
				operations[record.Diff.Operation]++
			}
		}
	}
	if operations["link-0-primary"] != 1 || operations["link-1-alias"] != 1 {
		t.Fatalf("operations = %v", operations)
	}
}

func TestStreamDiffDualWriteTargetOnlyIsWarning(t *testing.T) {
	ctx := context.Background()
	store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	source := saveTestGeneration(t, ctx, store, "source-empty", stageSource, recordSource, nil)
	target := saveTestGeneration(t, ctx, store, "target-residue", stageTarget, recordTarget, []generationRecord{
		targetTestRecord("/residue", EntryRegular, "old", "inode-old", 1),
	})
	builder, err := newStreamDiffBuilder(store, streamDiffConfig{
		GenerationID: "diff-dual", RoundID: "round-dual", Phase: PhaseDualWriteRepairing,
		Identity: testManifestIdentity(), SortBufferBytes: 1024, SortFanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := builder.Build(ctx, source.GenerationID, target.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Summary.Converged || result.Summary.BlockerCount != 0 || result.Summary.FindingCounts[FindingTargetOnly] != 1 {
		t.Fatalf("summary = %+v", result.Summary)
	}
	stage := result.Metadata.Stages[stageDiff]
	reader, err := store.OpenChunk(ctx, result.Metadata.GenerationID, stage.Chunks[0])
	if err != nil {
		t.Fatal(err)
	}
	record, ok, err := reader.Next()
	if err != nil || !ok || record.Diff == nil || record.Diff.Severity != SeverityWarning {
		t.Fatalf("record=%+v ok=%t err=%v", record, ok, err)
	}
}

func TestStreamDiffRejectsIncompleteInputGeneration(t *testing.T) {
	store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	builder := testStreamDiffBuilder(t, store, "diff-a")
	if _, err := builder.Build(context.Background(), "missing-source", "missing-target"); !errors.Is(err, ErrGenerationIncomplete) {
		t.Fatalf("error = %v, want ErrGenerationIncomplete", err)
	}
}

func TestStatePublishesGenerationSummaryWithoutFullMaps(t *testing.T) {
	state := NewState(PhaseSyncing)
	state.SetRecoveryComplete(true)
	state.SetInitialCopyComplete(true)
	state.BeginRound("round-a", RoundModeFull, time.Unix(1, 0))
	summary := generationRoundSummary{
		ID: "round-a", Mode: RoundModeFull, StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0),
		SourceGenerationID: "source-a", TargetGenerationID: "target-a", DiffGenerationID: "diff-a",
		SourceComplete: true, TargetComplete: true, DiffComplete: true,
		ScanComplete: true, Converged: true, SourceCount: 6000000,
	}
	if err := state.PublishGeneration(summary); err != nil {
		t.Fatal(err)
	}
	snapshot := state.Snapshot()
	if snapshot.LastGeneration == nil || snapshot.LastGeneration.SourceCount != 6000000 || !snapshot.Conditions.ReadyForRollout ||
		snapshot.LastComplete != nil || len(snapshot.Reconciled) != 0 {
		t.Fatalf("snapshot = %+v", snapshot)
	}

	state.BeginRound("round-b", RoundModeFull, time.Unix(3, 0))
	summary.ID, summary.ScanComplete = "round-b", false
	if err := state.PublishGeneration(summary); !errors.Is(err, ErrIncompleteRound) {
		t.Fatalf("error = %v, want ErrIncompleteRound", err)
	}
	if state.Snapshot().Conditions.ReadyForRollout {
		t.Fatal("incomplete generation retained ReadyForRollout")
	}

	state.BeginRound("round-incomplete-target", RoundModeFull, time.Unix(3, 0))
	summary.ID, summary.ScanComplete, summary.TargetComplete = "round-incomplete-target", true, false
	if err := state.PublishGeneration(summary); !errors.Is(err, ErrIncompleteRound) {
		t.Fatalf("error = %v, want ErrIncompleteRound", err)
	}
	if state.Snapshot().Conditions.ReadyForRollout {
		t.Fatal("incomplete Target generation retained ReadyForRollout")
	}

	state.BeginRound("round-c", RoundModeFull, time.Unix(4, 0))
	summary.ID, summary.ScanComplete, summary.TargetComplete, summary.Converged, summary.BlockerCount = "round-c", true, true, true, 1
	if err := state.PublishGeneration(summary); err != nil {
		t.Fatal(err)
	}
	if state.Snapshot().Conditions.ReadyForRollout {
		t.Fatal("blocker-bearing generation became ReadyForRollout")
	}
}

func TestGenerationSummaryAndRuntimeExceptionsGateReadyForRollout(t *testing.T) {
	base := generationRoundSummary{
		ID: "round-a", Mode: RoundModeFull, StartedAt: time.Unix(1, 0), CompletedAt: time.Unix(2, 0),
		SourceGenerationID: "source-a", TargetGenerationID: "target-a", DiffGenerationID: "diff-a",
		SourceComplete: true, TargetComplete: true, DiffComplete: true,
		ScanComplete: true, Converged: true, SourceCount: 1,
		FindingCounts: map[FindingKind]int64{}, WorkCounts: map[string]int64{"write": 1},
	}
	for _, tc := range []struct {
		name   string
		mutate func(*generationRoundSummary)
	}{
		{name: "pending", mutate: func(summary *generationRoundSummary) { summary.PendingCount = 1 }},
		{name: "active", mutate: func(summary *generationRoundSummary) { summary.ActiveCount = 1 }},
		{name: "unknown", mutate: func(summary *generationRoundSummary) { summary.UnknownCount = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := NewState(PhaseSyncing)
			state.SetRecoveryComplete(true)
			state.SetInitialCopyComplete(true)
			state.BeginRound(base.ID, base.Mode, base.StartedAt)
			summary := cloneGenerationSummary(base)
			tc.mutate(&summary)
			if err := state.PublishGeneration(summary); err != nil {
				t.Fatal(err)
			}
			if state.Snapshot().Current.Converged || state.Snapshot().Conditions.ReadyForRollout {
				t.Fatalf("exception summary became ready: %+v", state.Snapshot())
			}
		})
	}

	newReadyState := func() *State {
		state := NewState(PhaseSyncing)
		state.SetRecoveryComplete(true)
		state.SetInitialCopyComplete(true)
		state.BeginRound(base.ID, base.Mode, base.StartedAt)
		if err := state.PublishGeneration(base); err != nil {
			t.Fatal(err)
		}
		if !state.Snapshot().Conditions.ReadyForRollout {
			t.Fatalf("baseline is not ready: %+v", state.Snapshot())
		}
		return state
	}
	version := SourceVersion{Device: 1, Inode: 1, Kind: EntryRegular, Size: 1, MtimeNS: 1, CtimeNS: 1, Mode: 0o100644}
	for _, tc := range []struct {
		name   string
		mutate func(*State)
	}{
		{name: "pending repairs", mutate: func(state *State) { state.setPendingRepairs(1) }},
		{name: "active operation", mutate: func(state *State) { state.beginOperation() }},
		{name: "grace", mutate: func(state *State) { state.trackGrace("/a", version, time.Unix(3, 0)) }},
		{name: "retry", mutate: func(state *State) { state.queueRetry("/a", version, "operation", time.Unix(3, 0)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := newReadyState()
			tc.mutate(state)
			if state.Snapshot().Conditions.ReadyForRollout {
				t.Fatalf("runtime exception retained readiness: %+v", state.Snapshot())
			}
		})
	}
}

func testStreamDiffBuilder(t *testing.T, store *generationStore, generationID string) *streamDiffBuilder {
	t.Helper()
	builder, err := newStreamDiffBuilder(store, streamDiffConfig{
		GenerationID: generationID, RoundID: "round-" + generationID, Phase: PhaseSyncing,
		Identity: testManifestIdentity(), SortBufferBytes: 1024, SortFanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	builder.clock = func() time.Time { return testNow }
	return builder
}

func saveTestGeneration(t *testing.T, ctx context.Context, store *generationStore, generationID string, stage generationStage, kind generationRecordKind, records []generationRecord) generationMetadata {
	t.Helper()
	writer, err := newChunkWriter(kind)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	body, descriptor, err := writer.Close("chunk-000001")
	if err != nil {
		t.Fatal(err)
	}
	descriptor.Stage = stage
	if err := store.SaveChunk(ctx, generationID, stage, descriptor.ID, body, descriptor); err != nil {
		t.Fatal(err)
	}
	metadata := generationMetadata{
		FormatVersion: generationFormatVersion, GenerationID: generationID, RoundID: "round-" + generationID,
		Phase: PhaseSyncing, Identity: testManifestIdentity(), EntryCount: int64(len(records)), CreatedAt: testNow,
		Stages: map[generationStage]generationStageMetadata{stage: completedStage([]chunkDescriptor{descriptor})},
	}
	if _, err := store.SaveMetadata(ctx, metadata, 0); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishComplete(ctx, metadata); err != nil {
		t.Fatal(err)
	}
	return metadata
}

func sourceTestRecord(path string, kind EntryKind, checksum, hardlink string) generationRecord {
	size := int64(0)
	mode := uint32(0o644)
	if kind != EntryDirectory {
		size = 3
	} else {
		mode = 0o755
	}
	return generationRecord{Key: path, Source: &sourceGenerationRecord{
		Path: path, LocalPath: path, Kind: kind, Device: 1, Inode: 1, Size: size,
		MtimeNS: 1, CtimeNS: 1, VersionMode: 0o100644, Mode: mode,
		ChecksumSHA256: checksum, HardlinkKey: hardlink,
	}}
}

func targetTestRecord(path string, kind EntryKind, checksum, resource string, nlink uint32) generationRecord {
	mode, revision := uint32(0o644), int64(1)
	size := int64(3)
	var checksumPointer *string
	if kind == EntryDirectory {
		mode, size, nlink = 0o755, 0, 2
	} else {
		checksumPointer = &checksum
	}
	return generationRecord{Key: path, Target: &targetGenerationRecord{
		Path: path, Kind: kind, Size: size, Mode: &mode, MetadataComplete: true, IdentityKind: "inode",
		Revision: &revision, ResourceID: resource, Nlink: nlink, ChecksumSHA256: checksumPointer,
	}}
}

func readDiffFindings(t *testing.T, ctx context.Context, store *generationStore, metadata generationMetadata) []FindingKind {
	t.Helper()
	stage := metadata.Stages[stageDiff]
	var findings []FindingKind
	for _, descriptor := range stage.Chunks {
		reader, err := store.OpenChunk(ctx, metadata.GenerationID, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		for {
			record, ok, err := reader.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if record.Diff != nil && record.Diff.Finding != "" {
				findings = append(findings, record.Diff.Finding)
			}
		}
	}
	slices.Sort(findings)
	return findings
}
