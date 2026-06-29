package e2ereport

import (
	"encoding/json"
	"strings"
	"testing"
)

func f64(v float64) *float64 { return &v }

func TestParseSuiteSummary(t *testing.T) {
	good := `{"suite":"api-smoke","status":"failure","tier":"post-merge","failure_class":"correctness","product_promise":"durable worksite"}`
	s, err := ParseSuiteSummary([]byte(good))
	if err != nil {
		t.Fatalf("parse good: %v", err)
	}
	if s.Suite != "api-smoke" || s.Status != StatusFailure || s.FailureClass != FailureCorrectness {
		t.Fatalf("unexpected parse: %+v", s)
	}
	if _, err := ParseSuiteSummary([]byte(`{"status":"failure"}`)); err == nil {
		t.Fatal("expected error for missing suite")
	}
	if _, err := ParseSuiteSummary([]byte(`not json`)); err == nil {
		t.Fatal("expected error for bad json")
	}
	// Enum fields must fail closed.
	if _, err := ParseSuiteSummary([]byte(`{"suite":"x"}`)); err == nil {
		t.Fatal("expected error for missing status")
	}
	if _, err := ParseSuiteSummary([]byte(`{"suite":"x","status":"passed"}`)); err == nil {
		t.Fatal("expected error for invalid status")
	}
	if _, err := ParseSuiteSummary([]byte(`{"suite":"x","status":"failure","failure_class":"bogus"}`)); err == nil {
		t.Fatal("expected error for invalid failure_class")
	}
	if _, err := ParseSuiteSummary([]byte(`{"suite":"x","status":"failure","tier":"weekly"}`)); err == nil {
		t.Fatal("expected error for invalid tier")
	}
}

func TestPerfOnlySignature(t *testing.T) {
	// A regression-only run (everything passed, one budget breach) still gets a
	// stable, non-empty signature so it can group/notify.
	r := Aggregate(RunContext{Trigger: TierNightly}, []SuiteSummary{
		{Suite: "fuse-write-perf-budget", Status: StatusSuccess,
			Metrics: []Metric{{Name: "p99_write_ms", Value: 40, Budget: f64(30)}}},
	})
	if !r.OverallSuccess {
		t.Fatal("perf-only run should still be OverallSuccess")
	}
	if r.FailureSignature() == "" {
		t.Fatal("perf-only run should have a non-empty signature")
	}
}

func sampleSuites() []SuiteSummary {
	return []SuiteSummary{
		{Suite: "git-feature-matrix", Status: StatusFailure, Tier: TierPostMerge, FailureClass: FailureCorrectness,
			ProductPromise: "git/fuse correctness", OwnerHint: "git-team", Detail: "11 cases failed"},
		{Suite: "api-smoke", Status: StatusSuccess, ProductPromise: "durable worksite"},
		{Suite: "fuse-write-perf-budget", Status: StatusSuccess, ProductPromise: "performance",
			Metrics: []Metric{{Name: "p99_write_ms", Value: 42, Unit: "ms", Budget: f64(30)}}},
	}
}

func TestAggregateAndSignature(t *testing.T) {
	ctx := RunContext{Trigger: TierPostMerge, Repo: "mem9-ai/drive9", RunURL: "https://x/run/1", SHA: "abc"}
	r := Aggregate(ctx, sampleSuites())
	if r.OverallSuccess {
		t.Fatal("should be failure (git-feature-matrix failed)")
	}
	if len(r.Failed) != 1 || r.Failed[0].Suite != "git-feature-matrix" {
		t.Fatalf("failed = %+v", r.Failed)
	}
	if len(r.PerfRegressed) != 1 || r.PerfRegressed[0].Suite != "fuse-write-perf-budget" {
		t.Fatalf("perf regressed = %+v", r.PerfRegressed)
	}
	// Suites sorted deterministically.
	if r.Suites[0].Suite != "api-smoke" {
		t.Fatalf("not sorted: %s", r.Suites[0].Suite)
	}
	sig := r.FailureSignature()
	if sig == "" {
		t.Fatal("expected non-empty signature")
	}
	// Signature is stable regardless of input order.
	rev := append([]SuiteSummary(nil), sampleSuites()...)
	rev[0], rev[2] = rev[2], rev[0]
	if Aggregate(ctx, rev).FailureSignature() != sig {
		t.Fatal("signature should be order-independent")
	}
}

func TestNotifyPolicy(t *testing.T) {
	cases := []struct {
		name    string
		trigger Tier
		suites  []SuiteSummary
		want    bool
	}{
		{"pr failure stays in github", TierPR, []SuiteSummary{{Suite: "a", Status: StatusFailure}}, false},
		{"post-merge failure notifies", TierPostMerge, []SuiteSummary{{Suite: "a", Status: StatusFailure}}, true},
		{"nightly failure notifies", TierNightly, []SuiteSummary{{Suite: "a", Status: StatusFailure}}, true},
		{"all success no notify", TierNightly, []SuiteSummary{{Suite: "a", Status: StatusSuccess}}, false},
		{"perf regression notifies even on PR success", TierPR, []SuiteSummary{
			{Suite: "p", Status: StatusSuccess, Metrics: []Metric{{Name: "x", Value: 10, Budget: f64(5)}}}}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := Aggregate(RunContext{Trigger: tc.trigger}, tc.suites).ShouldNotifyFeishu()
			if d.Notify != tc.want {
				t.Fatalf("notify=%v want %v (reason: %s)", d.Notify, tc.want, d.Reason)
			}
		})
	}
}

func TestPerfRegressionDetection(t *testing.T) {
	// Explicit Regression flag OR value over budget.
	s := SuiteSummary{Metrics: []Metric{
		{Name: "ok", Value: 5, Budget: f64(10)},
		{Name: "over_budget", Value: 20, Budget: f64(10)},
		{Name: "flagged", Value: 1, Regression: true},
	}}
	got := s.PerfRegressions()
	if len(got) != 2 {
		t.Fatalf("expected 2 regressions, got %d: %+v", len(got), got)
	}
}

func TestRenderContainsKeyFacts(t *testing.T) {
	r := Aggregate(RunContext{Trigger: TierNightly, RunURL: "https://x/run/9", SHA: "deadbeef"}, sampleSuites())
	md := r.Markdown()
	for _, want := range []string{"git-feature-matrix", "performance regression", "failure signature", "https://x/run/9"} {
		if !strings.Contains(strings.ToLower(md), strings.ToLower(want)) {
			t.Errorf("markdown missing %q", want)
		}
	}
	issue := r.IssueBody()
	if !strings.Contains(issue, r.FailureSignature()) || !strings.Contains(issue, "git-feature-matrix") {
		t.Errorf("issue body missing signature/suite:\n%s", issue)
	}
	// Feishu card is valid JSON-marshalable and carries the run URL + title.
	card := r.FeishuCard()
	raw, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("card marshal: %v", err)
	}
	cs := string(raw)
	if !strings.Contains(cs, "https://x/run/9") || !strings.Contains(cs, "header") {
		t.Errorf("card missing run url/header: %s", cs)
	}
	if !strings.Contains(cs, "git-feature-matrix") {
		t.Errorf("card missing failed suite: %s", cs)
	}
}

func TestPerfOnlyRenderIsConsistent(t *testing.T) {
	r := Aggregate(RunContext{Trigger: TierNightly, RunURL: "https://x/run/3"}, []SuiteSummary{
		{Suite: "fuse-write-perf-budget", Status: StatusSuccess, ProductPromise: "performance",
			Metrics: []Metric{{Name: "p99_write_ms", Value: 40, Unit: "ms", Budget: f64(30)}}},
	})
	md := strings.ToLower(r.Markdown())
	if !strings.Contains(md, "performance regression") || strings.Contains(md, "❌ failure") {
		t.Errorf("perf-only markdown should read as performance regression, not failure:\n%s", r.Markdown())
	}
	issue := r.IssueBody()
	if !strings.Contains(issue, "performance regression") {
		t.Errorf("perf-only issue body should say performance regression:\n%s", issue)
	}
	// No empty "Failed suites" section when nothing failed.
	if strings.Contains(issue, "Failed suites") {
		t.Errorf("perf-only issue body should not have a Failed suites section:\n%s", issue)
	}
}
