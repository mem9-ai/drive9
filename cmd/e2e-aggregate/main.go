// Command e2e-aggregate turns one local-e2e run into a single product-quality
// report. It reads the suite manifest plus the per-suite step outcomes (and any
// structured suite summaries that have adopted the JSON contract), then emits:
//
//   - a markdown report appended to $GITHUB_STEP_SUMMARY,
//   - the aggregated RunReport JSON (--out) for the notifier/dashboard,
//   - a GitHub issue body (--issue-body) led by a stable failure signature,
//   - workflow outputs (notify, signature, overall_success, reason) to
//     $GITHUB_OUTPUT for downstream steps.
//
// It is intentionally tolerant: a missing manifest or summaries directory is a
// warning, not a failure, so adoption can be incremental.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mem9-ai/drive9/internal/e2ereport"
)

func main() {
	var (
		manifestPath  = flag.String("manifest", "e2e/suite-manifest.json", "path to the suite manifest JSON")
		outcomesPath  = flag.String("outcomes", "", "path to JSON object mapping suite id -> GitHub step outcome")
		summariesDir  = flag.String("summaries", "e2e/reports/summary", "directory of adopted per-suite summary JSON files")
		outPath       = flag.String("out", "", "write the aggregated RunReport JSON here")
		issueBodyPath = flag.String("issue-body", "", "write the GitHub issue body here")
		tierOverride  = flag.String("tier", "", "override automation tier (pr|post-merge|nightly|manual)")
		proofSuite    = flag.String("proof-fail-suite", "", "inject a synthetic failed suite to prove the notification path end to end (manual CI proof)")
		degraded      = flag.Bool("degraded", false, "the run is degraded (e.g. a required setup step hard-failed before suites ran): if no suite failure is otherwise detected, inject a synthetic pipeline failure so it is still reported and notified. Wire from GitHub failure().")
	)
	flag.Parse()

	if err := run(*manifestPath, *outcomesPath, *summariesDir, *outPath, *issueBodyPath, *tierOverride, *proofSuite, *degraded); err != nil {
		fmt.Fprintln(os.Stderr, "e2e-aggregate:", err)
		os.Exit(1)
	}
}

func run(manifestPath, outcomesPath, summariesDir, outPath, issueBodyPath, tierOverride, proofSuite string, degraded bool) error {
	tier := e2ereport.TierFromEvent(os.Getenv("GITHUB_EVENT_NAME"))
	if tierOverride != "" {
		parsed, err := parseTierOverride(tierOverride)
		if err != nil {
			return err
		}
		tier = parsed
	}

	manifest := loadManifest(manifestPath)
	outcomes, err := loadOutcomes(outcomesPath)
	if err != nil {
		return err
	}
	if proofSuite != "" {
		// Manual proof: a synthetic failed suite makes notify=true so the whole
		// path (aggregate -> card -> Feishu) is exercised on a green CI run. It
		// only affects this report; the real job gate reads actual step outcomes.
		outcomes[proofSuite] = string(e2ereport.StatusFailure)
		fmt.Fprintf(os.Stderr, "e2e-aggregate: proof mode — injected synthetic failed suite %q\n", proofSuite)
	}
	adopted := loadAdoptedSummaries(summariesDir)

	summaries := e2ereport.SynthesizeSummaries(manifest, tier, outcomes, adopted)
	report := e2ereport.Aggregate(runContextFromEnv(tier), summaries)

	if degraded && report.OverallSuccess && len(report.PerfRegressed) == 0 {
		// A required step hard-failed before suite outcomes were collected, so the
		// per-suite view is completely empty while the job will fail. Fail closed:
		// inject a pipeline failure so the run still gets a signature, issue, and
		// notification. Only when there is no other signal — a perf-only regression
		// already notifies and must keep its own signature/routing, not be masked
		// as an infrastructure failure.
		summaries = append(summaries, e2ereport.SuiteSummary{
			Suite:          "e2e-pipeline",
			Status:         e2ereport.StatusFailure,
			Tier:           tier,
			ProductArea:    "ci",
			ProductPromise: "the e2e pipeline runs to completion",
			FailureClass:   e2ereport.FailureInfrastructure,
			OwnerHint:      "ci",
			Detail:         "a required step failed before per-suite results were collected",
		})
		// Re-aggregate intentionally: the first Aggregate decided there was no
		// signal; rebuild the report so the injected suite flows into Failed,
		// the signature, and the notify decision.
		report = e2ereport.Aggregate(runContextFromEnv(tier), summaries)
		fmt.Fprintln(os.Stderr, "e2e-aggregate: degraded run — injected synthetic pipeline failure")
	}

	if err := appendStepSummary(report.Markdown()); err != nil {
		return err
	}
	if outPath != "" {
		raw, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(outPath, raw, 0o644); err != nil {
			return fmt.Errorf("write report: %w", err)
		}
	}
	if issueBodyPath != "" && len(report.Failed) > 0 {
		if err := os.WriteFile(issueBodyPath, []byte(report.IssueBody()), 0o644); err != nil {
			return fmt.Errorf("write issue body: %w", err)
		}
	}

	decision := report.ShouldNotifyFeishu()
	if err := writeOutputs(map[string]string{
		"notify":          boolStr(decision.Notify),
		"reason":          decision.Reason,
		"signature":       report.FailureSignature(),
		"overall_success": boolStr(report.OverallSuccess),
		"failed_count":    fmt.Sprintf("%d", len(report.Failed)),
		"perf_regressed":  fmt.Sprintf("%d", len(report.PerfRegressed)),
	}); err != nil {
		return err
	}

	fmt.Printf("e2e-aggregate: %d suites, %d failed, %d perf-regressed, notify=%v (%s)\n",
		len(report.Suites), len(report.Failed), len(report.PerfRegressed), decision.Notify, decision.Reason)
	return nil
}

func parseTierOverride(value string) (e2ereport.Tier, error) {
	switch tier := e2ereport.Tier(strings.TrimSpace(value)); tier {
	case e2ereport.TierPR, e2ereport.TierPostMerge, e2ereport.TierNightly, e2ereport.TierManual:
		return tier, nil
	default:
		return "", fmt.Errorf("invalid --tier %q (want pr|post-merge|nightly|manual)", value)
	}
}

func loadManifest(path string) e2ereport.SuiteManifest {
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-aggregate: no manifest at %s (%v); synthesizing without product metadata\n", path, err)
		return e2ereport.SuiteManifest{Suites: map[string]e2ereport.SuiteMeta{}}
	}
	m, err := e2ereport.LoadManifest(data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e-aggregate: bad manifest %s (%v); ignoring\n", path, err)
		return e2ereport.SuiteManifest{Suites: map[string]e2ereport.SuiteMeta{}}
	}
	return m
}

func loadOutcomes(path string) (map[string]string, error) {
	if path == "" {
		return map[string]string{}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read outcomes: %w", err)
	}
	var out map[string]string
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("parse outcomes: %w", err)
	}
	return out, nil
}

func loadAdoptedSummaries(dir string) []e2ereport.SuiteSummary {
	matches, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	sort.Strings(matches)
	var out []e2ereport.SuiteSummary
	for _, p := range matches {
		data, err := os.ReadFile(p)
		if err != nil {
			fmt.Fprintf(os.Stderr, "e2e-aggregate: skip %s (%v)\n", p, err)
			continue
		}
		s, err := e2ereport.ParseSuiteSummary(data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "e2e-aggregate: skip %s (%v)\n", p, err)
			continue
		}
		out = append(out, s)
	}
	return out
}

func runContextFromEnv(tier e2ereport.Tier) e2ereport.RunContext {
	get := os.Getenv
	ctx := e2ereport.RunContext{
		Trigger:  tier,
		Repo:     get("GITHUB_REPOSITORY"),
		SHA:      get("GITHUB_SHA"),
		Workflow: get("GITHUB_WORKFLOW"),
		Branch:   get("GITHUB_REF_NAME"),
	}
	if server, repo, runID := get("GITHUB_SERVER_URL"), get("GITHUB_REPOSITORY"), get("GITHUB_RUN_ID"); server != "" && repo != "" && runID != "" {
		ctx.RunURL = fmt.Sprintf("%s/%s/actions/runs/%s", server, repo, runID)
	}
	return ctx
}

func appendStepSummary(md string) error {
	path := os.Getenv("GITHUB_STEP_SUMMARY")
	if path == "" {
		fmt.Print(md)
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open step summary: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString("\n" + md + "\n"); err != nil {
		return fmt.Errorf("write step summary: %w", err)
	}
	return nil
}

func writeOutputs(kv map[string]string) error {
	path := os.Getenv("GITHUB_OUTPUT")
	keys := make([]string, 0, len(kv))
	for k := range kv {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		// Single-line values only; safe for the key=value GITHUB_OUTPUT form.
		fmt.Fprintf(&b, "%s=%s\n", k, strings.ReplaceAll(kv[k], "\n", " "))
	}
	if path == "" {
		fmt.Print(b.String())
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0o644)
	if err != nil {
		return fmt.Errorf("open github output: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.WriteString(b.String()); err != nil {
		return fmt.Errorf("write github output: %w", err)
	}
	return nil
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
