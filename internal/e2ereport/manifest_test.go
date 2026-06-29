package e2ereport

import "testing"

const sampleManifest = `{
  "suites": {
    "api-smoke":         {"title": "API smoke", "product_area": "api", "product_promise": "control surface works", "owner_hint": "api-team"},
    "git-feature-matrix":{"title": "Git feature matrix", "product_area": "git", "product_promise": "git compatibility", "owner_hint": "git-team", "failure_class": "correctness"},
    "fuse-write-perf-budget": {"title": "FUSE write perf budget", "product_area": "fuse", "product_promise": "performance", "owner_hint": "fs-team", "failure_class": "performance"}
  }
}`

func TestLoadManifest(t *testing.T) {
	m, err := LoadManifest([]byte(sampleManifest))
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(m.Suites) != 3 || m.Suites["git-feature-matrix"].OwnerHint != "git-team" {
		t.Fatalf("unexpected manifest: %+v", m.Suites)
	}
	if _, err := LoadManifest([]byte(`{"suites":{}}`)); err == nil {
		t.Fatal("expected error for empty suites")
	}
	if _, err := LoadManifest([]byte(`bad`)); err == nil {
		t.Fatal("expected error for bad json")
	}
}

func TestNormalizeStatus(t *testing.T) {
	cases := map[string]Status{
		"success": StatusSuccess, "Success": StatusSuccess,
		"failure": StatusFailure, "cancelled": StatusFailure, "weird": StatusFailure,
		"": StatusSkipped, "skipped": StatusSkipped,
	}
	for in, want := range cases {
		if got := NormalizeStatus(in); got != want {
			t.Errorf("NormalizeStatus(%q)=%q want %q", in, got, want)
		}
	}
}

func TestSynthesizeSummaries(t *testing.T) {
	m, _ := LoadManifest([]byte(sampleManifest))
	outcomes := map[string]string{
		"api-smoke":              "success",
		"git-feature-matrix":     "failure",
		"fuse-write-perf-budget": "skipped",
	}
	// One adopted structured summary overrides synthesis for its suite.
	adopted := []SuiteSummary{
		{Suite: "fuse-write-perf-budget", Status: StatusSuccess, ProductPromise: "performance",
			Metrics: []Metric{{Name: "p99_write_ms", Value: 50, Budget: f64(30)}}},
	}
	got := SynthesizeSummaries(m, TierPostMerge, outcomes, adopted)
	if len(got) != 3 {
		t.Fatalf("want 3 summaries, got %d", len(got))
	}

	bySuite := map[string]SuiteSummary{}
	for _, s := range got {
		bySuite[s.Suite] = s
	}
	// Synthesized failure picks up manifest metadata + failure class + default tier.
	gfm := bySuite["git-feature-matrix"]
	if gfm.Status != StatusFailure || gfm.FailureClass != FailureCorrectness || gfm.OwnerHint != "git-team" || gfm.Tier != TierPostMerge {
		t.Fatalf("git-feature-matrix synth wrong: %+v", gfm)
	}
	// Adopted summary wins (carries metrics) instead of being synthesized from "skipped".
	perf := bySuite["fuse-write-perf-budget"]
	if len(perf.Metrics) != 1 || perf.Status != StatusSuccess {
		t.Fatalf("adopted summary not used: %+v", perf)
	}

	// End-to-end: aggregate flags the synthesized failure and the adopted perf regression.
	r := Aggregate(RunContext{Trigger: TierPostMerge}, got)
	if r.OverallSuccess || len(r.Failed) != 1 || len(r.PerfRegressed) != 1 {
		t.Fatalf("aggregate wrong: failed=%d perf=%d ok=%v", len(r.Failed), len(r.PerfRegressed), r.OverallSuccess)
	}
}
