// Package report writes and regenerates drive9 agent harness artifacts.
package report

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const SchemaVersion = "agent-harness.v1"

type Manifest struct {
	SchemaVersion        string            `json:"schema_version"`
	RunID                string            `json:"run_id"`
	StartedAt            string            `json:"started_at"`
	Host                 string            `json:"host"`
	HarnessGitSHA        string            `json:"harness_git_sha"`
	Drive9Version        string            `json:"drive9_version"`
	Server               string            `json:"server"`
	Suites               []string          `json:"suites"`
	SelectedCases        []string          `json:"selected_cases"`
	Cases                []CaseSummary     `json:"cases"`
	ArtifactRoot         string            `json:"artifact_root"`
	MountRoot            string            `json:"mount_root"`
	RemoteRootBase       string            `json:"remote_root_base"`
	GeneratedMountpoints map[string]string `json:"generated_mountpoints"`
	GeneratedRemoteRoots map[string]string `json:"generated_remote_roots"`
	ProcessGroups        map[string]int    `json:"process_groups"`
	APIKeyRedacted       string            `json:"api_key_redacted"`
	ApprovalMode         string            `json:"approval_mode"`
}

type Event struct {
	SchemaVersion string   `json:"schema_version"`
	RunID         string   `json:"run_id"`
	CaseID        string   `json:"case_id"`
	TS            string   `json:"ts"`
	Type          string   `json:"type"`
	Message       string   `json:"message"`
	DurationMS    int64    `json:"duration_ms"`
	CommandID     string   `json:"command_id"`
	ExitCode      *int     `json:"exit_code"`
	Signal        string   `json:"signal"`
	ArtifactRefs  []string `json:"artifact_refs"`
}

type Failure struct {
	SchemaVersion   string   `json:"schema_version"`
	RunID           string   `json:"run_id"`
	CaseID          string   `json:"case_id"`
	TS              string   `json:"ts"`
	Severity        string   `json:"severity"`
	Class           string   `json:"class"`
	Oracle          string   `json:"oracle"`
	ExpectedOutcome string   `json:"expected_outcome"`
	Message         string   `json:"message"`
	Observed        any      `json:"observed"`
	Expected        any      `json:"expected"`
	ReproPath       string   `json:"repro_path"`
	ArtifactRefs    []string `json:"artifact_refs"`
}

type Metric struct {
	SchemaVersion string   `json:"schema_version"`
	RunID         string   `json:"run_id"`
	CaseID        string   `json:"case_id"`
	TS            string   `json:"ts"`
	Name          string   `json:"name"`
	Value         float64  `json:"value"`
	Unit          string   `json:"unit"`
	Source        string   `json:"source"`
	ArtifactRefs  []string `json:"artifact_refs"`
}

type Summary struct {
	SchemaVersion string            `json:"schema_version"`
	RunID         string            `json:"run_id"`
	Status        string            `json:"status"`
	StartedAt     string            `json:"started_at"`
	EndedAt       string            `json:"ended_at"`
	DurationMS    int64             `json:"duration_ms"`
	Cases         []CaseSummary     `json:"cases"`
	Counts        SummaryCounts     `json:"counts"`
	Artifacts     map[string]string `json:"artifacts"`
	Cleanup       map[string]string `json:"cleanup"`
}

type CaseSummary struct {
	ID              string `json:"id"`
	Suite           string `json:"suite"`
	ExpectedOutcome string `json:"expected_outcome"`
	Status          string `json:"status"`
}

type SummaryCounts struct {
	BySuite           map[string]int `json:"by_suite"`
	ByExpectedOutcome map[string]int `json:"by_expected_outcome"`
	BySeverity        map[string]int `json:"by_severity"`
	ByClass           map[string]int `json:"by_class"`
}

type Gating struct {
	SchemaVersion          string    `json:"schema_version"`
	RunID                  string    `json:"run_id"`
	GateStatus             string    `json:"gate_status"`
	Pass                   int       `json:"pass"`
	Fail                   int       `json:"fail"`
	KnownBugReproduced     int       `json:"known_bug_reproduced"`
	KnownBugFixedCandidate int       `json:"known_bug_fixed_candidate"`
	NonGating              int       `json:"non_gating"`
	BlockingFailures       []Failure `json:"blocking_failures"`
}

type Recorder struct {
	RunDir string
	RunID  string
}

func NewRecorder(runDir, runID string) (*Recorder, error) {
	for _, dir := range []string{runDir, filepath.Join(runDir, "mount"), filepath.Join(runDir, "debug"), filepath.Join(runDir, "metrics"), filepath.Join(runDir, "repro")} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return &Recorder{RunDir: runDir, RunID: runID}, nil
}

func (r *Recorder) WriteManifest(m Manifest) error {
	m.SchemaVersion = SchemaVersion
	return writeJSON(filepath.Join(r.RunDir, "manifest.json"), m)
}

func (r *Recorder) Event(e Event) error {
	e.SchemaVersion = SchemaVersion
	e.RunID = r.RunID
	if e.TS == "" {
		e.TS = now()
	}
	return appendJSONL(filepath.Join(r.RunDir, "events.jsonl"), e)
}

func (r *Recorder) Failure(f Failure) error {
	f.SchemaVersion = SchemaVersion
	f.RunID = r.RunID
	if f.TS == "" {
		f.TS = now()
	}
	return appendJSONL(filepath.Join(r.RunDir, "failures.jsonl"), f)
}

func (r *Recorder) Metric(m Metric) error {
	m.SchemaVersion = SchemaVersion
	m.RunID = r.RunID
	if m.TS == "" {
		m.TS = now()
	}
	return appendJSONL(filepath.Join(r.RunDir, "metrics.jsonl"), m)
}

func Generate(runDir string) (Summary, Gating, error) {
	manifest := Manifest{}
	if err := readJSON(filepath.Join(runDir, "manifest.json"), &manifest); err != nil {
		return Summary{}, Gating{}, err
	}
	failures, err := readFailures(filepath.Join(runDir, "failures.jsonl"))
	if err != nil {
		return Summary{}, Gating{}, err
	}
	ended := now()
	var durationMS int64
	if manifest.StartedAt != "" {
		if startedAt, err := time.Parse(time.RFC3339Nano, manifest.StartedAt); err == nil {
			if endedAt, err := time.Parse(time.RFC3339Nano, ended); err == nil {
				durationMS = endedAt.Sub(startedAt).Milliseconds()
			}
		}
	}
	summary := Summary{
		SchemaVersion: SchemaVersion,
		RunID:         manifest.RunID,
		Status:        "passed",
		StartedAt:     manifest.StartedAt,
		EndedAt:       ended,
		DurationMS:    durationMS,
		Counts: SummaryCounts{
			BySuite:           map[string]int{},
			ByExpectedOutcome: map[string]int{},
			BySeverity:        map[string]int{},
			ByClass:           map[string]int{},
		},
		Artifacts: map[string]string{
			"manifest": "manifest.json",
			"events":   "events.jsonl",
			"failures": "failures.jsonl",
			"metrics":  "metrics.jsonl",
			"summary":  "summary.json",
			"gating":   "gating.json",
		},
		Cleanup: map[string]string{},
	}
	gating := Gating{SchemaVersion: SchemaVersion, RunID: manifest.RunID, GateStatus: "pass"}
	selected := map[string]CaseSummary{}
	for _, cs := range manifest.Cases {
		cs.Status = "passed"
		selected[cs.ID] = cs
	}
	for _, id := range manifest.SelectedCases {
		if _, ok := selected[id]; !ok {
			selected[id] = CaseSummary{ID: id, Status: "passed"}
		}
	}
	knownBugCases := map[string]bool{}
	failedCases := map[string]bool{}
	inconclusiveCases := map[string]bool{}
	for _, f := range failures {
		summary.Counts.BySeverity[f.Severity]++
		summary.Counts.ByClass[f.Class]++
		cs := selected[f.CaseID]
		cs.ID = f.CaseID
		cs.ExpectedOutcome = f.ExpectedOutcome
		if f.Class == "inconclusive" {
			cs.Status = "inconclusive"
			if !inconclusiveCases[f.CaseID] {
				gating.NonGating++
				inconclusiveCases[f.CaseID] = true
			}
			selected[f.CaseID] = cs
			continue
		}
		if f.ExpectedOutcome == "bug_reproduced" && f.Class == "product" {
			cs.Status = "known_bug_reproduced"
			if !knownBugCases[f.CaseID] {
				gating.KnownBugReproduced++
				gating.NonGating++
				knownBugCases[f.CaseID] = true
			}
			selected[f.CaseID] = cs
			continue
		}
		cs.Status = "failed"
		if !failedCases[f.CaseID] {
			gating.Fail++
			failedCases[f.CaseID] = true
		}
		gating.BlockingFailures = append(gating.BlockingFailures, f)
		selected[f.CaseID] = cs
	}
	for _, cs := range selected {
		if cs.Status == "" {
			cs.Status = "passed"
		}
		if cs.Status == "passed" && cs.ExpectedOutcome == "bug_reproduced" {
			cs.Status = "known_bug_fixed_candidate"
			gating.KnownBugFixedCandidate++
		}
		if cs.Status == "passed" {
			gating.Pass++
		}
		summary.Cases = append(summary.Cases, cs)
		summary.Counts.BySuite[cs.Suite]++
		summary.Counts.ByExpectedOutcome[cs.ExpectedOutcome]++
	}
	sort.Slice(summary.Cases, func(i, j int) bool {
		return summary.Cases[i].ID < summary.Cases[j].ID
	})
	if gating.Fail > 0 {
		gating.GateStatus = "fail"
		summary.Status = "failed"
	} else if len(inconclusiveCases) > 0 {
		gating.GateStatus = "non_gating"
		summary.Status = "inconclusive"
	} else if gating.KnownBugReproduced > 0 {
		gating.GateStatus = "non_gating"
		summary.Status = "known_bugs_reproduced"
	}
	if err := writeJSON(filepath.Join(runDir, "summary.json"), summary); err != nil {
		return Summary{}, Gating{}, err
	}
	if err := writeJSON(filepath.Join(runDir, "gating.json"), gating); err != nil {
		return Summary{}, Gating{}, err
	}
	if err := writeMarkdown(filepath.Join(runDir, "summary.md"), summary, gating); err != nil {
		return Summary{}, Gating{}, err
	}
	return summary, gating, nil
}

func readFailures(path string) ([]Failure, error) {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []Failure
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var failure Failure
		if err := json.Unmarshal(scanner.Bytes(), &failure); err != nil {
			return nil, err
		}
		out = append(out, failure)
	}
	return out, scanner.Err()
}

func appendJSONL(path string, v any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(b, '\n')); err != nil {
		return err
	}
	return nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func readJSON(path string, v any) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func writeMarkdown(path string, s Summary, g Gating) error {
	body := fmt.Sprintf("---\ntitle: Drive9 Agent Harness Summary\n---\n\n## Run\n\n1. Run ID: `%s`\n2. Status: `%s`\n3. Gate: `%s`\n\n## Counts\n\n1. Pass: %d\n2. Fail: %d\n3. Known bug reproduced: %d\n4. Known bug fixed candidate: %d\n", s.RunID, s.Status, g.GateStatus, g.Pass, g.Fail, g.KnownBugReproduced, g.KnownBugFixedCandidate)
	return os.WriteFile(path, []byte(body), 0o644)
}

func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
