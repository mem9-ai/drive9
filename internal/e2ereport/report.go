// Package e2ereport defines the structured drive9 e2e suite-summary contract and
// the run-level aggregator that turns many suite outputs into one product-quality
// report: a workflow summary, a GitHub issue body with a stable failure
// signature, a Feishu/Lark notification decision, and the notification card.
//
// It is intentionally dependency-free (no AWS/MySQL/HTTP) so it is fully unit
// testable with fixtures and golden output, per PRD #641.
package e2ereport

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Status is a suite outcome, aligned with GitHub Actions step outcomes.
type Status string

const (
	StatusSuccess Status = "success"
	StatusFailure Status = "failure"
	StatusSkipped Status = "skipped"
)

// Tier is the automation tier that ran the suite.
type Tier string

const (
	TierPR        Tier = "pr"
	TierPostMerge Tier = "post-merge"
	TierNightly   Tier = "nightly"
	TierManual    Tier = "manual"
)

// FailureClass categorizes why a suite failed, so failures route to the right owner.
type FailureClass string

const (
	FailureNone           FailureClass = ""
	FailureCorrectness    FailureClass = "correctness"
	FailureInfrastructure FailureClass = "infrastructure"
	FailureDependency     FailureClass = "dependency"
	FailureFlaky          FailureClass = "flaky"
	FailurePerformance    FailureClass = "performance"
	FailureUnknown        FailureClass = "unknown"
)

// Metric is a measured value a suite reports, optionally budgeted/baselined so a
// performance regression can be detected explicitly (not from raw variance).
type Metric struct {
	Name       string   `json:"name"`
	Value      float64  `json:"value"`
	Unit       string   `json:"unit,omitempty"`
	Budget     *float64 `json:"budget,omitempty"`
	Baseline   *float64 `json:"baseline,omitempty"`
	Regression bool     `json:"regression,omitempty"`
}

// SuiteSummary is the structured per-suite reporting contract. Each participating
// e2e suite emits one of these (JSON); suites that have not adopted it yet are
// synthesized by the aggregator from the workflow step outcome + manifest.
type SuiteSummary struct {
	Suite          string       `json:"suite"`
	Status         Status       `json:"status"`
	DurationS      float64      `json:"duration_s,omitempty"`
	Tier           Tier         `json:"tier,omitempty"`
	ProductArea    string       `json:"product_area,omitempty"`
	ProductPromise string       `json:"product_promise,omitempty"`
	FailureClass   FailureClass `json:"failure_class,omitempty"`
	OwnerHint      string       `json:"owner_hint,omitempty"`
	Metrics        []Metric     `json:"metrics,omitempty"`
	ArtifactURL    string       `json:"artifact_url,omitempty"`
	ReportURL      string       `json:"report_url,omitempty"`
	Detail         string       `json:"detail,omitempty"`
}

func (s Status) valid() bool {
	switch s {
	case StatusSuccess, StatusFailure, StatusSkipped:
		return true
	default:
		return false
	}
}

func (c FailureClass) valid() bool {
	switch c {
	case FailureNone, FailureCorrectness, FailureInfrastructure, FailureDependency, FailureFlaky, FailurePerformance, FailureUnknown:
		return true
	default:
		return false
	}
}

func (t Tier) valid() bool {
	switch t {
	case "", TierPR, TierPostMerge, TierNightly, TierManual:
		return true
	default:
		return false
	}
}

// ParseSuiteSummary parses and validates one suite-summary JSON document. Enum
// fields are validated so a producer that omits or misspells status (or another
// enum) fails closed instead of flowing through as a zero-value "not failure".
func ParseSuiteSummary(data []byte) (SuiteSummary, error) {
	var s SuiteSummary
	if err := json.Unmarshal(data, &s); err != nil {
		return SuiteSummary{}, fmt.Errorf("parse suite summary: %w", err)
	}
	if strings.TrimSpace(s.Suite) == "" {
		return SuiteSummary{}, fmt.Errorf("suite summary missing required field: suite")
	}
	if !s.Status.valid() {
		return SuiteSummary{}, fmt.Errorf("suite %q has invalid status %q (want success|failure|skipped)", s.Suite, s.Status)
	}
	if !s.FailureClass.valid() {
		return SuiteSummary{}, fmt.Errorf("suite %q has invalid failure_class %q", s.Suite, s.FailureClass)
	}
	if !s.Tier.valid() {
		return SuiteSummary{}, fmt.Errorf("suite %q has invalid tier %q", s.Suite, s.Tier)
	}
	return s, nil
}

// PerfRegression reports a budget/baseline breach within a suite's metrics.
func (s SuiteSummary) PerfRegressions() []Metric {
	var out []Metric
	for _, m := range s.Metrics {
		if m.Regression || (m.Budget != nil && m.Value > *m.Budget) {
			out = append(out, m)
		}
	}
	return out
}

// RunContext describes the workflow run the summaries came from.
type RunContext struct {
	Trigger  Tier   `json:"trigger"`
	Repo     string `json:"repo"`
	SHA      string `json:"sha,omitempty"`
	RunURL   string `json:"run_url,omitempty"`
	Workflow string `json:"workflow,omitempty"`
	Branch   string `json:"branch,omitempty"`
}

// RunReport is the aggregated product-quality view of one e2e run.
type RunReport struct {
	Context        RunContext     `json:"context"`
	Suites         []SuiteSummary `json:"suites"`
	Failed         []SuiteSummary `json:"failed"`
	PerfRegressed  []SuiteSummary `json:"perf_regressed"`
	OverallSuccess bool           `json:"overall_success"`
}

// Aggregate combines suite summaries into a single run report. Summaries are
// sorted by suite for deterministic output.
func Aggregate(ctx RunContext, summaries []SuiteSummary) RunReport {
	sorted := append([]SuiteSummary(nil), summaries...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Suite < sorted[j].Suite })

	r := RunReport{Context: ctx, Suites: sorted, OverallSuccess: true}
	for _, s := range sorted {
		if s.Status == StatusFailure {
			r.Failed = append(r.Failed, s)
			r.OverallSuccess = false
		}
		if len(s.PerfRegressions()) > 0 {
			r.PerfRegressed = append(r.PerfRegressed, s)
		}
	}
	return r
}

// FailureSignature is a stable short id for what went wrong — the set of failed
// suites (with failure class) plus any performance-only regressions — so a
// recurring pattern groups into one issue thread instead of spawning new ones.
// Empty only when nothing failed and nothing regressed.
func (r RunReport) FailureSignature() string {
	parts := make([]string, 0, len(r.Failed)+len(r.PerfRegressed))
	for _, s := range r.Failed {
		fc := s.FailureClass
		if fc == FailureNone {
			fc = FailureUnknown
		}
		parts = append(parts, s.Suite+":"+string(fc))
	}
	for _, s := range r.PerfRegressed {
		// Failed suites are already represented above; add perf-only regressions
		// so a regression-only run still gets a stable, groupable signature.
		if s.Status != StatusFailure {
			parts = append(parts, s.Suite+":"+string(FailurePerformance))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return fmt.Sprintf("%x", sum[:6])
}

// NotifyDecision is whether to push this run to Feishu/Lark and why.
type NotifyDecision struct {
	Notify bool
	Reason string
}

// ShouldNotifyFeishu encodes the notification policy: PR failures stay in GitHub;
// post-merge/nightly/manual failures and explicit performance regressions are
// pushed. Successful runs are never pushed.
func (r RunReport) ShouldNotifyFeishu() NotifyDecision {
	if r.OverallSuccess && len(r.PerfRegressed) == 0 {
		return NotifyDecision{false, "run succeeded with no performance regression"}
	}
	if len(r.PerfRegressed) > 0 {
		return NotifyDecision{true, "explicit performance regression"}
	}
	// A failure exists.
	if r.Context.Trigger == TierPR {
		return NotifyDecision{false, "PR-tier failure stays in GitHub by policy"}
	}
	return NotifyDecision{true, fmt.Sprintf("%s-tier failure", triggerName(r.Context.Trigger))}
}

func triggerName(t Tier) string {
	if t == "" {
		return "unknown"
	}
	return string(t)
}
