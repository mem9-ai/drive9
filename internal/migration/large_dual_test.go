package migration

import (
	"context"
	"testing"
	"time"
)

func TestLargeDualRepairUsesBoundedPerPathGraceAndApply(t *testing.T) {
	ctx := context.Background()
	store, err := newGenerationStore(newMemoryGenerationObjects(), "job-a")
	if err != nil {
		t.Fatal(err)
	}
	source := saveTestGeneration(t, ctx, store, "dual-source", stageSource, recordSource, []generationRecord{
		sourceTestRecord("/file", EntryRegular, "aaa", ""),
	})
	target := saveTestGeneration(t, ctx, store, "dual-target", stageTarget, recordTarget, nil)
	builder, err := newStreamDiffBuilder(store, streamDiffConfig{
		GenerationID: "dual-diff", RoundID: "round-dual", Mode: RoundModeFast, Phase: PhaseDualWriteRepairing,
		Identity: testManifestIdentity(), SortBufferBytes: 1024, SortFanIn: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	difference, err := builder.Build(ctx, source.GenerationID, target.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	scanner, err := NewScanner(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	applies := 0
	worker := &Worker{
		scanner: scanner, generation: store, state: NewState(PhaseDualWriteRepairing),
		gracePeriod: time.Second, now: func() time.Time { return now },
		largeDualApply: func(_ context.Context, source, manifest map[string]SourceEntry, target map[string]TargetEntry) error {
			applies++
			if len(source) != 1 || len(manifest) != 1 || len(target) != 0 {
				t.Fatalf("source=%v manifest=%v target=%v", source, manifest, target)
			}
			return nil
		},
	}
	worker.state.BeginRound(difference.Summary.ID, RoundModeFast, now)
	if err := worker.largeDualRepair(ctx, difference.Metadata, difference.Summary, now); err != nil {
		t.Fatal(err)
	}
	if applies != 0 || len(worker.State().Grace) != 1 || worker.State().Conditions.CurrentConverged {
		t.Fatalf("first state=%+v applies=%d", worker.State(), applies)
	}

	now = now.Add(2 * time.Second)
	worker.state.BeginRound(difference.Summary.ID, RoundModeFast, now)
	if err := worker.largeDualRepair(ctx, difference.Metadata, difference.Summary, now); err != nil {
		t.Fatal(err)
	}
	snapshot := worker.State()
	if applies != 1 || len(snapshot.Grace) != 0 || len(snapshot.Reconciled) != 1 || snapshot.LastComplete != nil {
		t.Fatalf("second state=%+v applies=%d", snapshot, applies)
	}
}
