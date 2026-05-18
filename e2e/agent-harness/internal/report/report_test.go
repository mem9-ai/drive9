package report

import (
	"strings"
	"testing"
)

func TestGenerateKnownBugNonGating(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewRecorder(dir, "run1")
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.WriteManifest(Manifest{RunID: "run1", SelectedCases: []string{"case1"}}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Failure(Failure{CaseID: "case1", Class: "product", Severity: "P1", ExpectedOutcome: "bug_reproduced", Oracle: "cli_read_equals"}); err != nil {
		t.Fatal(err)
	}
	_, gating, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gating.GateStatus != "non_gating" {
		t.Fatalf("gate = %q", gating.GateStatus)
	}
	if gating.KnownBugReproduced != 1 || gating.Fail != 0 {
		t.Fatalf("gating = %+v", gating)
	}
}

func TestGenerateInconclusiveNonGating(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewRecorder(dir, "run1")
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.WriteManifest(Manifest{RunID: "run1", SelectedCases: []string{"case1"}, Cases: []CaseSummary{{ID: "case1", Suite: "fault", ExpectedOutcome: "baseline_pass"}}}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Failure(Failure{CaseID: "case1", Class: "inconclusive", Severity: "P2", ExpectedOutcome: "baseline_pass", Oracle: "recovery_classified"}); err != nil {
		t.Fatal(err)
	}
	summary, gating, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gating.GateStatus != "non_gating" || summary.Status != "inconclusive" {
		t.Fatalf("summary = %+v gating = %+v", summary, gating)
	}
	if gating.Fail != 0 || gating.NonGating != 1 {
		t.Fatalf("gating = %+v", gating)
	}
}

func TestGenerateUsesPersistedRunEnd(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewRecorder(dir, "run1")
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.WriteManifest(Manifest{
		RunID:         "run1",
		StartedAt:     "2026-01-01T00:00:00Z",
		SelectedCases: []string{"case1"},
		Cases:         []CaseSummary{{ID: "case1", Suite: "smoke", ExpectedOutcome: "baseline_pass"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Event(Event{Type: "run_end", TS: "2026-01-01T00:00:05Z"}); err != nil {
		t.Fatal(err)
	}
	summary, _, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if summary.EndedAt != "2026-01-01T00:00:05Z" || summary.DurationMS != 5000 {
		t.Fatalf("summary ended_at=%q duration=%d, want persisted run end", summary.EndedAt, summary.DurationMS)
	}
}

func TestGenerateReadsLargeFailureEntries(t *testing.T) {
	dir := t.TempDir()
	rec, err := NewRecorder(dir, "run1")
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.WriteManifest(Manifest{
		RunID:         "run1",
		SelectedCases: []string{"case1"},
		Cases:         []CaseSummary{{ID: "case1", Suite: "smoke", ExpectedOutcome: "baseline_pass"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Failure(Failure{
		CaseID:          "case1",
		Class:           "product",
		Severity:        "P1",
		ExpectedOutcome: "baseline_pass",
		Oracle:          "cli_read_equals",
		Message:         strings.Repeat("x", 128*1024),
	}); err != nil {
		t.Fatal(err)
	}
	_, gating, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gating.Fail != 1 {
		t.Fatalf("gating fail = %d, want 1", gating.Fail)
	}
}
