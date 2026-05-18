package report

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
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

func TestRecorderConcurrentWrites(t *testing.T) {
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

	const workers = 50
	var wg sync.WaitGroup
	errs := make(chan error, workers*3)
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(3)
		go func() {
			defer wg.Done()
			errs <- rec.Event(Event{CaseID: "case1", Type: "parallel_write"})
		}()
		go func() {
			defer wg.Done()
			errs <- rec.Failure(Failure{
				CaseID:          "case1",
				Class:           "product",
				Severity:        "P1",
				ExpectedOutcome: "baseline_pass",
				Oracle:          "cli_read_equals",
				Message:         "parallel failure",
			})
		}()
		go func() {
			defer wg.Done()
			errs <- rec.Metric(Metric{
				CaseID: "case1",
				Name:   "parallel_write",
				Value:  float64(i),
				Unit:   "count",
				Source: "test",
			})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	_, gating, err := Generate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if gating.Fail != 1 {
		t.Fatalf("gating fail = %d, want 1 failed case", gating.Fail)
	}
	if len(gating.BlockingFailures) != workers {
		t.Fatalf("blocking failures = %d, want %d", len(gating.BlockingFailures), workers)
	}
	if got := countJSONLLines(t, filepath.Join(dir, "events.jsonl")); got != workers {
		t.Fatalf("events lines = %d, want %d", got, workers)
	}
	if got := countJSONLLines(t, filepath.Join(dir, "failures.jsonl")); got != workers {
		t.Fatalf("failure lines = %d, want %d", got, workers)
	}
	if got := countJSONLLines(t, filepath.Join(dir, "metrics.jsonl")); got != workers {
		t.Fatalf("metric lines = %d, want %d", got, workers)
	}
}

func countJSONLLines(t *testing.T, path string) int {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Count(string(b), "\n")
}
