package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPerfScenarioLessOrdersCPPerfByWorkloadThenNumericSize(t *testing.T) {
	scenarios := []PerfScenario{
		{CaseID: "cp", Workload: "download_file", ScenarioID: "download_file-100mib-c001"},
		{CaseID: "cp", Workload: "download_file", ScenarioID: "download_file-1024mib-c001"},
		{CaseID: "cp", Workload: "upload_warm", ScenarioID: "upload_warm-100mib-c001"},
		{CaseID: "cp", Workload: "upload_warm", ScenarioID: "upload_warm-50mib-c001"},
		{CaseID: "cp", Workload: "download_file", ScenarioID: "download_file-50mib-c001"},
	}
	sort.Slice(scenarios, func(i, j int) bool {
		return perfScenarioLess(scenarios[i], scenarios[j])
	})
	got := []string{}
	for _, scenario := range scenarios {
		got = append(got, scenario.ScenarioID)
	}
	want := []string{
		"upload_warm-50mib-c001",
		"upload_warm-100mib-c001",
		"download_file-50mib-c001",
		"download_file-100mib-c001",
		"download_file-1024mib-c001",
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sorted scenarios = %v, want %v", got, want)
		}
	}
}

func TestPerfMarkdownOutputPathUsesTitleSlug(t *testing.T) {
	got := PerfMarkdownOutputPath("/tmp/run", "Drive9 fs cp Bottleneck 1 GiB Matrix Report", "")
	want := filepath.Join("/tmp/run", "perf", "drive9-fs-cp-bottleneck-1-gib-matrix-report.md")
	if got != want {
		t.Fatalf("output path = %q, want %q", got, want)
	}
	override := "/tmp/custom/report.md"
	if got := PerfMarkdownOutputPath("/tmp/run", "ignored", override); got != override {
		t.Fatalf("output override = %q, want %q", got, override)
	}
}

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

func TestGeneratePerfReportFromResults(t *testing.T) {
	dir := t.TempDir()
	rec := newPerfRun(t, dir, "run1", "upload-download")
	if err := rec.WritePerfEnvironment(PerfEnvironment{
		RunID:             "run1",
		Host:              "perf-host",
		OS:                "linux",
		Kernel:            "6.1.0",
		Architecture:      "amd64",
		ProductVersion:    "drive9 test",
		ServerEndpoint:    "https://drive9.example",
		CloudProvider:     "aws",
		InstanceType:      "c7i.2xlarge",
		VCPU:              "8",
		CPUModel:          "Intel Xeon",
		Memory:            "16 GiB",
		StorageType:       "gp3",
		StorageSize:       "100 GiB",
		StorageIOPS:       "3000",
		StorageThroughput: "125 MiB/s",
		StorageEncrypted:  "true",
	}); err != nil {
		t.Fatal(err)
	}
	addPerfResult(t, rec, "upload-download", "upload", "upload-1", "upload", "ok", "2026-01-01T00:00:00Z", 100, 50<<20, 10, "")
	addPerfResult(t, rec, "upload-download", "upload", "upload-2", "upload", "ok", "2026-01-01T00:00:01Z", 200, 50<<20, 10, "")
	addPerfResult(t, rec, "upload-download", "download", "download-1", "download", "ok", "2026-01-01T00:00:03Z", 150, 50<<20, 10, "")
	if _, _, err := Generate(dir); err != nil {
		t.Fatal(err)
	}
	title := "Drive9 Upload and Download Performance Test Report"
	report, err := GeneratePerfReport(dir, PerfOptions{Title: title})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Scenarios) != 2 {
		t.Fatalf("scenarios = %d, want 2", len(report.Scenarios))
	}
	if report.Infrastructure.StorageType != "gp3" || report.Infrastructure.StorageIOPS != "3000" {
		t.Fatalf("infrastructure = %+v", report.Infrastructure)
	}
	var upload PerfScenario
	for _, scenario := range report.Scenarios {
		if scenario.Workload == "upload" {
			upload = scenario
			break
		}
	}
	if upload.LatencyMin != 100 || upload.LatencyMax != 200 {
		t.Fatalf("upload latency min/max = %v/%v, want 100/200", upload.LatencyMin, upload.LatencyMax)
	}
	body, err := os.ReadFile(PerfMarkdownOutputPath(dir, title, ""))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{"| Case | Workload | Load | Success | QPS | Throughput | Min | Avg | p50 | p95 | p99 | Max | Gate |", "gp3", "125 MiB/s", "upload", "download"} {
		if !strings.Contains(text, want) {
			t.Fatalf("performance report missing %q:\n%s", want, text)
		}
	}
}

func TestGeneratePerfReportMissingEnvironmentUsesUnknown(t *testing.T) {
	dir := t.TempDir()
	rec := newPerfRun(t, dir, "run1", "case1")
	addPerfResult(t, rec, "case1", "case1", "op1", "metadata_op", "ok", "2026-01-01T00:00:00Z", 10, 0, 1, "")
	if _, _, err := Generate(dir); err != nil {
		t.Fatal(err)
	}
	report, err := GeneratePerfReport(dir, PerfOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Environment.StorageIOPS != "unknown" || report.Infrastructure.StorageThroughput != "unknown" {
		t.Fatalf("environment = %+v infrastructure = %+v", report.Environment, report.Infrastructure)
	}
	body, err := os.ReadFile(PerfMarkdownOutputPath(dir, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "| Storage IOPS | unknown |") {
		t.Fatalf("performance report missing unknown storage field:\n%s", string(body))
	}
}

func TestRootVolumeIDFromDescribeInstances(t *testing.T) {
	var resp awsDescribeInstancesResponse
	body := []byte(`{
	  "Reservations": [{
	    "Instances": [{
	      "RootDeviceName": "/dev/sda1",
	      "BlockDeviceMappings": [
	        {"DeviceName": "/dev/xvdb", "Ebs": {"VolumeId": "vol-data"}},
	        {"DeviceName": "/dev/sda1", "Ebs": {"VolumeId": "vol-root"}}
	      ]
	    }]
	  }]
	}`)
	if err := json.Unmarshal(body, &resp); err != nil {
		t.Fatal(err)
	}
	if got := rootVolumeIDFromDescribeInstances(resp); got != "vol-root" {
		t.Fatalf("root volume id = %q, want vol-root", got)
	}
}

func TestPerfEnvironmentAppliesEBSVolume(t *testing.T) {
	env := PerfEnvironment{
		StorageType:       unknown(),
		StorageSize:       unknown(),
		StorageIOPS:       unknown(),
		StorageThroughput: unknown(),
		StorageEncrypted:  unknown(),
	}
	env.applyEBSVolume(awsEBSVolume{
		VolumeType: "gp3",
		Size:       200,
		IOPS:       8000,
		Throughput: 2000,
		Encrypted:  false,
	})
	if env.StorageType != "gp3" ||
		env.StorageSize != "200 GiB" ||
		env.StorageIOPS != "8000" ||
		env.StorageThroughput != "2000 MiB/s" ||
		env.StorageEncrypted != "false" {
		t.Fatalf("environment storage fields = %+v", env)
	}
}

func TestGeneratePerfReportFailedScenarioRendersNA(t *testing.T) {
	dir := t.TempDir()
	rec := newPerfRun(t, dir, "run1", "failing")
	addPerfResult(t, rec, "failing", "failing", "upload-1", "upload", "failed", "2026-01-01T00:00:00Z", 100, 0, 1, "write failed")
	if err := rec.Failure(Failure{CaseID: "failing", Class: "product", Severity: "P1", ExpectedOutcome: "baseline_pass", Oracle: "throughput_min", Message: "write failed"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Generate(dir); err != nil {
		t.Fatal(err)
	}
	report, err := GeneratePerfReport(dir, PerfOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.OverallStatus != "FAIL" || report.Scenarios[0].GateStatus != "fail" {
		t.Fatalf("report status=%s scenario=%+v", report.OverallStatus, report.Scenarios[0])
	}
	body, err := os.ReadFile(PerfMarkdownOutputPath(dir, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{"| failing | upload | 1 operations | 0/1 | N/A | N/A | N/A | N/A | N/A | N/A | N/A | N/A | fail |", "failure_class=product"} {
		if !strings.Contains(text, want) {
			t.Fatalf("performance report missing %q:\n%s", want, text)
		}
	}
}

func TestGeneratePerfReportCPPerfObservations(t *testing.T) {
	dir := t.TempDir()
	rec := newPerfRun(t, dir, "run1", "cp-bottleneck")
	addPerfResult(t, rec, "cp-bottleneck", "upload_warm-50mib-c001", "op-1", "cp.upload_warm", "ok", "2026-01-01T00:00:00Z", 1000, 50<<20, 7, "")
	addPerfResult(t, rec, "cp-bottleneck", "upload_warm-50mib-c002", "op-2", "cp.upload_warm", "ok", "2026-01-01T00:00:02Z", 1000, 50<<20, 7, "")
	addPerfResult(t, rec, "cp-bottleneck", "upload_warm-50mib-c002", "op-3", "cp.upload_warm", "ok", "2026-01-01T00:00:02Z", 1000, 50<<20, 7, "")
	for i := 0; i < 4; i++ {
		addPerfResult(t, rec, "cp-bottleneck", "upload_warm-50mib-c004", "op-c4-"+string(rune('0'+i)), "cp.upload_warm", "ok", "2026-01-01T00:00:04Z", 2000, 50<<20, 7, "")
	}
	if _, _, err := Generate(dir); err != nil {
		t.Fatal(err)
	}
	report, err := GeneratePerfReport(dir, PerfOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Observations) != 1 || !strings.Contains(report.Observations[0], "classification=inconclusive") {
		t.Fatalf("observations = %+v", report.Observations)
	}
	body, err := os.ReadFile(PerfMarkdownOutputPath(dir, "", ""))
	if err != nil {
		t.Fatal(err)
	}
	text := string(body)
	for _, want := range []string{
		"This phase runs 50 MiB files, concurrency 1, 2, and 4, with `upload_warm`.",
		"| cp-bottleneck | upload_warm | 50 MiB, concurrency 4, 4 transfers |",
		"classification=inconclusive",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("performance report missing %q:\n%s", want, text)
		}
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

func newPerfRun(t *testing.T, dir, runID, caseID string) *Recorder {
	t.Helper()
	rec, err := NewRecorder(dir, runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.WriteManifest(Manifest{
		RunID:         runID,
		StartedAt:     "2026-01-01T00:00:00Z",
		Host:          "perf-host linux/amd64",
		Drive9Version: "drive9 test",
		Server:        "https://drive9.example",
		Suites:        []string{"stress"},
		SelectedCases: []string{caseID},
		Cases:         []CaseSummary{{ID: caseID, Suite: "stress", ExpectedOutcome: "baseline_pass"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Event(Event{Type: "run_end", TS: "2026-01-01T00:00:05Z"}); err != nil {
		t.Fatal(err)
	}
	return rec
}

func addPerfResult(t *testing.T, rec *Recorder, caseID, scenarioID, operationID, operation, status, started string, durationMS float64, bytes int64, requestUnits float64, errText string) {
	t.Helper()
	startedAt, err := time.Parse(time.RFC3339, started)
	if err != nil {
		t.Fatal(err)
	}
	endedAt := startedAt.Add(time.Duration(durationMS * float64(time.Millisecond)))
	errorClass := ""
	if status != "ok" && status != "skipped" {
		errorClass = "product"
	}
	if err := rec.PerfResult(PerfResult{
		CaseID:       caseID,
		ScenarioID:   scenarioID,
		OperationID:  operationID,
		Operation:    operation,
		Status:       status,
		StartedAt:    startedAt.Format(time.RFC3339Nano),
		EndedAt:      endedAt.Format(time.RFC3339Nano),
		DurationMS:   durationMS,
		Bytes:        bytes,
		RequestUnits: requestUnits,
		ErrorClass:   errorClass,
		Error:        errText,
	}); err != nil {
		t.Fatal(err)
	}
}
