package runner

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/casefile"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/mountproc"
	"github.com/mem9-ai/dat9/e2e/agent-harness/internal/report"
)

func TestGCRequiresConfirmDelete(t *testing.T) {
	err := GC(context.Background(), GCConfig{RunDir: t.TempDir()})
	if !errors.Is(err, ErrConfirmDeleteRequired) {
		t.Fatalf("err = %v, want ErrConfirmDeleteRequired", err)
	}
}

func TestRunIDIncludesRandomSuffix(t *testing.T) {
	got := runID()
	parts := strings.Split(got, "-")
	if len(parts) != 2 {
		t.Fatalf("runID = %q, want timestamp-random", got)
	}
	if len(parts[0]) != len("20060102T150405Z") {
		t.Fatalf("runID timestamp = %q, want compact UTC timestamp", parts[0])
	}
	if len(parts[1]) != 8 {
		t.Fatalf("runID suffix = %q, want 8 hex chars", parts[1])
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		t.Fatalf("runID suffix = %q, want hex: %v", parts[1], err)
	}
}

func TestPreflightSkipsStatusWhenProvisioning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX fake drive9 script")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "drive9")
	if err := os.WriteFile(bin, []byte("#!/usr/bin/env sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	err := Preflight(context.Background(), Config{
		ArtifactRoot:   dir,
		MountRoot:      dir,
		RemoteRootBase: "/agent-test",
		Drive9Bin:      bin,
		Server:         "http://127.0.0.1:1",
		APIKey:         "stale-key",
		Provision:      true,
	})
	if err != nil {
		t.Fatalf("Preflight returned %v, want nil", err)
	}
}

func TestFioWriteBytesPerSecond(t *testing.T) {
	got := fioWriteBytesPerSecond(`{"jobs":[{"write":{"bw_bytes":123.5}},{"write":{"bw_bytes":10}}]}`)
	if got != 133.5 {
		t.Fatalf("bw = %v, want 133.5", got)
	}
}

func TestCPPerfRequestUnitsAndScenarioID(t *testing.T) {
	if got := cpPerfRequestUnits(50 << 20); got != 7 {
		t.Fatalf("50 MiB request units = %v, want 7", got)
	}
	if got := cpPerfRequestUnits(100 << 20); got != 13 {
		t.Fatalf("100 MiB request units = %v, want 13", got)
	}
	if got := cpPerfRequestUnits(1024 << 20); got != 128 {
		t.Fatalf("1024 MiB request units = %v, want 128", got)
	}
	if got := cpPerfScenarioID("download_file", 50, 8); got != "download_file-50mib-c008" {
		t.Fatalf("scenario ID = %q", got)
	}
	if got := cpPerfScenarioID("upload_warm", 1024, 32); got != "upload_warm-1024mib-c032" {
		t.Fatalf("scenario ID = %q", got)
	}
}

func TestRunCPPerfWithFakeDrive9RecordsRows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX fake drive9 script")
	}
	dir := t.TempDir()
	bin := fakeCPPerfDrive9(t, dir, "")
	suiteDir := writeCPPerfSuite(t, dir, "upload_warm,download_file,download_null", 2)
	runDir, err := Run(context.Background(), Config{
		ArtifactRoot:   dir,
		MountRoot:      filepath.Join(dir, "mounts"),
		RemoteRootBase: "/agent-test-$RUN_ID",
		Drive9Bin:      bin,
		Server:         "http://127.0.0.1:1",
		APIKey:         "test-key",
		SuiteDir:       suiteDir,
		Suites:         []string{"stress"},
		CaseFilter:     "cp-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	results := readPerfResultsForTest(t, filepath.Join(runDir, "perf", "results.jsonl"))
	if len(results) != 6 {
		t.Fatalf("perf rows = %d, want 6", len(results))
	}
	for _, result := range results {
		if result.Status != "ok" {
			t.Fatalf("result = %+v, want ok", result)
		}
		if result.RequestUnits != 1 {
			t.Fatalf("request units = %v, want 1", result.RequestUnits)
		}
		if len(result.ArtifactRefs) == 0 {
			t.Fatalf("artifact refs empty for %+v", result)
		}
	}
	downloadDir := filepath.Join(runDir, "perf", "cp_perf", "cp-test", "downloads", "download_file-1mib-c002")
	if _, err := os.Stat(downloadDir); !os.IsNotExist(err) {
		t.Fatalf("download dir stat err = %v, want not exist", err)
	}
}

func TestRunCPPerfRecordsFailedTransfer(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX fake drive9 script")
	}
	dir := t.TempDir()
	bin := fakeCPPerfDrive9(t, dir, "transfer-0002.bin")
	suiteDir := writeCPPerfSuite(t, dir, "upload_warm", 2)
	runDir, err := Run(context.Background(), Config{
		ArtifactRoot:   dir,
		MountRoot:      filepath.Join(dir, "mounts"),
		RemoteRootBase: "/agent-test-$RUN_ID",
		Drive9Bin:      bin,
		Server:         "http://127.0.0.1:1",
		APIKey:         "test-key",
		SuiteDir:       suiteDir,
		Suites:         []string{"stress"},
		CaseFilter:     "cp-test",
	})
	if !errors.Is(err, ErrGateFailed) {
		t.Fatalf("err = %v, want ErrGateFailed", err)
	}
	results := readPerfResultsForTest(t, filepath.Join(runDir, "perf", "results.jsonl"))
	if len(results) != 2 {
		t.Fatalf("perf rows = %d, want 2", len(results))
	}
	failed := 0
	for _, result := range results {
		if result.Status == "failed" {
			failed++
			if !strings.Contains(result.Error, "forced cp failure") {
				t.Fatalf("error = %q, want forced cp failure", result.Error)
			}
		}
	}
	if failed != 1 {
		t.Fatalf("failed rows = %d, want 1", failed)
	}
}

func TestCPPerfMissingTelemetryToolsWriteNotes(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir)
	evidence, err := newCPPerfEvidence(dir, filepath.Join(dir, "perf", "evidence", "scenario"), true)
	if err != nil {
		t.Fatal(err)
	}
	telemetry, err := evidence.start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	telemetry.stop(context.Background())
	if err := evidence.writeNotes(); err != nil {
		t.Fatal(err)
	}
	notes, err := os.ReadFile(filepath.Join(dir, "perf", "evidence", "scenario", "notes.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(notes), "pidstat unavailable") {
		t.Fatalf("notes = %s, want missing pidstat note", string(notes))
	}
}

func fakeCPPerfDrive9(t *testing.T, dir, failToken string) string {
	t.Helper()
	bin := filepath.Join(dir, "drive9")
	script := `#!/usr/bin/env sh
if [ "$1" = "--version" ]; then
  echo fake-drive9
  exit 0
fi
if [ "$1" = "fs" ] && [ "$2" = "mkdir" ]; then
  exit 0
fi
if [ "$1" = "fs" ] && [ "$2" = "rm" ]; then
  exit 0
fi
if [ "$1" = "fs" ] && [ "$2" = "cp" ]; then
  fail_token="` + failToken + `"
  if [ -n "$fail_token" ]; then
    case "$3 $4" in
      *"$fail_token"*)
        echo forced cp failure >&2
        exit 7
        ;;
    esac
  fi
  if [ "$3" = "-" ]; then
    cat >/dev/null
  fi
  case "$4" in
    :*)
      ;;
    -)
      printf fake-download
      ;;
    /dev/null)
      ;;
    *)
      mkdir -p "$(dirname "$4")"
      printf fake-download > "$4"
      ;;
  esac
  exit 0
fi
echo "unexpected fake drive9 command: $*" >&2
exit 2
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func writeCPPerfSuite(t *testing.T, dir, modes string, transfers int) string {
	t.Helper()
	suiteDir := filepath.Join(dir, "cases")
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		t.Fatal(err)
	}
	var modeLines strings.Builder
	for _, mode := range strings.Split(modes, ",") {
		mode = strings.TrimSpace(mode)
		if mode == "" {
			continue
		}
		modeLines.WriteString("        - " + mode + "\n")
	}
	body := `defaults:
  timeout: 2m
  cleanup: always
cases:
  - id: cp-test
    suite: stress
    expected_outcome: baseline_pass
    remote_root_suffix: cp-test
    mountpoint_suffix: cp-test
    workload:
      type: cp_perf
      file_sizes_mib:
        - 1
      concurrency_levels:
        - 2
      modes:
` + modeLines.String() + `      transfers_per_scenario: ` + fmtInt(transfers) + `
      collect_host_telemetry: false
      min_bytes_per_second: 1
    oracles:
      - type: throughput_min
    severity:
      failure: P2
`
	if err := os.WriteFile(filepath.Join(suiteDir, "stress.yaml"), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return suiteDir
}

func readPerfResultsForTest(t *testing.T, path string) []report.PerfResult {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []report.PerfResult
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var result report.PerfResult
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Fatal(err)
		}
		out = append(out, result)
	}
	return out
}

func fmtInt(v int) string {
	return strconv.Itoa(v)
}

func TestRunReturnsGateFailed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX fake drive9 script")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "drive9")
	if err := os.WriteFile(bin, []byte("#!/usr/bin/env sh\nif [ \"$1\" = \"--version\" ]; then echo fake-drive9; exit 0; fi\nif [ \"$1\" = \"doctor\" ]; then exit 1; fi\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	suiteDir := filepath.Join(dir, "cases")
	if err := os.MkdirAll(suiteDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(suiteDir, "regression.yaml"), []byte(`
defaults:
  timeout: 2m
  cleanup: always
cases:
  - id: doctor-fails
    suite: regression
    expected_outcome: baseline_pass
    remote_root_suffix: doctor-fails
    mountpoint_suffix: doctor-fails
    workload:
      type: doctor_fuse
      expect_exit: 0
    oracles:
      - type: command_exit
    severity:
      failure: P1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	runDir, err := Run(context.Background(), Config{
		ArtifactRoot:   dir,
		MountRoot:      dir,
		RemoteRootBase: "/agent-test-$RUN_ID",
		Drive9Bin:      bin,
		Server:         "http://127.0.0.1:1",
		APIKey:         "test-key",
		SuiteDir:       suiteDir,
		Suites:         []string{"regression"},
	})
	if !errors.Is(err, ErrGateFailed) {
		t.Fatalf("err = %v, want ErrGateFailed", err)
	}
	if runDir == "" {
		t.Fatal("runDir is empty")
	}
	var gating struct {
		GateStatus string `json:"gate_status"`
		Fail       int    `json:"fail"`
	}
	if err := readJSON(filepath.Join(runDir, "gating.json"), &gating); err != nil {
		t.Fatal(err)
	}
	if gating.GateStatus != "fail" || gating.Fail != 1 {
		t.Fatalf("gating = %+v, want fail with 1 failure", gating)
	}
}

func TestGCRejectsUnsafeMountpoint(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run")
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(runDir, "manifest.json"), map[string]any{
		"mount_root":              filepath.Join(dir, "mounts"),
		"remote_root_base":        "/agent-test",
		"generated_mountpoints":   map[string]string{"case": filepath.Join(dir, "outside")},
		"generated_remote_roots":  map[string]string{},
		"generated_process_group": map[string]int{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := writeJSON(filepath.Join(runDir, "gating.json"), map[string]any{"gate_status": "pass"}); err != nil {
		t.Fatal(err)
	}
	err := GC(context.Background(), GCConfig{RunDir: runDir, ConfirmDelete: true, SuccessfulOnly: true})
	if !errors.Is(err, ErrUnsafeMountpoint) {
		t.Fatalf("err = %v, want ErrUnsafeMountpoint", err)
	}
}

func TestRunDoctorAllowsNoAllowOtherOnlyFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX fake drive9 script")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "drive9")
	script := "#!/usr/bin/env sh\nprintf '%s\\n' 'drive9 doctor fuse' 'FAIL /etc/fuse.conf user_allow_other: user_allow_other is not enabled'\nexit 1\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	rec, err := report.NewRecorder(dir, "run1")
	if err != nil {
		t.Fatal(err)
	}
	zero := 0
	c := casefile.Case{
		ID:              "doctor",
		ExpectedOutcome: "bug_reproduced",
		Severity:        casefile.SeveritySpec{Failure: "P2"},
		Workload: casefile.Workload{
			ExpectExit:                   &zero,
			AllowNonzeroWhenNoAllowOther: true,
		},
	}
	if err := runDoctor(context.Background(), rec, mountproc.Env{}, bin, dir, c); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "failures.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("failures.jsonl err = %v, want not exist", err)
	}
}

func TestPublishPerfUploadsBundleAndIndex(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX fake drive9 script")
	}
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run")
	writePublishRun(t, runDir)
	logPath := filepath.Join(dir, "drive9.log")
	bin := fakePublishDrive9(t, dir)
	t.Setenv("DRIVE9_FAKE_LOG", logPath)
	pub, err := PublishPerf(context.Background(), PublishPerfConfig{
		RunDir:        runDir,
		WorkspaceRoot: ":/performance-reports",
		Drive9Bin:     bin,
		Server:        "https://drive9.example",
		APIKey:        "test-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if pub.Status != "succeeded" {
		t.Fatalf("publish status = %q", pub.Status)
	}
	if pub.ArtifactPaths["perf/drive9-performance-test-report.md"] == "" || pub.ArtifactPaths["perf/publish-manifest.json"] == "" {
		t.Fatalf("artifact paths = %+v", pub.ArtifactPaths)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	logText := string(logBytes)
	for _, want := range []string{"fs cp", "perf/drive9-performance-test-report.md", "index.json"} {
		if !strings.Contains(logText, want) {
			t.Fatalf("drive9 log missing %q:\n%s", want, logText)
		}
	}
}

func TestPublishPerfPartialFailureWritesManifest(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses a POSIX fake drive9 script")
	}
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run")
	writePublishRun(t, runDir)
	logPath := filepath.Join(dir, "drive9.log")
	bin := fakePublishDrive9(t, dir)
	t.Setenv("DRIVE9_FAKE_LOG", logPath)
	t.Setenv("DRIVE9_FAIL_RESULTS", "1")
	pub, err := PublishPerf(context.Background(), PublishPerfConfig{
		RunDir:        runDir,
		WorkspaceRoot: ":/performance-reports",
		Drive9Bin:     bin,
		Server:        "https://drive9.example",
		APIKey:        "test-key",
	})
	if err == nil {
		t.Fatal("PublishPerf returned nil error, want partial failure")
	}
	if pub.Status != "partial_failed" {
		t.Fatalf("publish status = %q", pub.Status)
	}
	var persisted PublishManifest
	if err := readJSON(filepath.Join(runDir, "perf", "publish-manifest.json"), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Status != "partial_failed" || len(persisted.Errors) == 0 {
		t.Fatalf("persisted manifest = %+v", persisted)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(logBytes), "index.json") {
		t.Fatalf("partial publish should not update index:\n%s", string(logBytes))
	}
}

func writePublishRun(t *testing.T, runDir string) {
	t.Helper()
	rec, err := report.NewRecorder(runDir, "run1")
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.WriteManifest(report.Manifest{
		RunID:         "run1",
		StartedAt:     "2026-01-01T00:00:00Z",
		Host:          "perf-host linux/amd64",
		Drive9Version: "drive9 test",
		Server:        "https://drive9.example",
		Suites:        []string{"stress"},
		SelectedCases: []string{"case1"},
		Cases:         []report.CaseSummary{{ID: "case1", Suite: "stress", ExpectedOutcome: "baseline_pass"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.Event(report.Event{Type: "run_end", TS: "2026-01-01T00:00:05Z"}); err != nil {
		t.Fatal(err)
	}
	if err := rec.WritePerfEnvironment(report.PerfEnvironment{
		RunID:             "run1",
		Host:              "perf-host",
		OS:                "linux",
		Kernel:            "6.1.0",
		Architecture:      "amd64",
		ProductVersion:    "drive9 test",
		ServerEndpoint:    "https://drive9.example",
		CloudProvider:     "aws",
		InstanceType:      "c7i.large",
		VCPU:              "2",
		CPUModel:          "Intel Xeon",
		Memory:            "4 GiB",
		StorageType:       "gp3",
		StorageSize:       "64 GiB",
		StorageIOPS:       "3000",
		StorageThroughput: "125 MiB/s",
		StorageEncrypted:  "true",
	}); err != nil {
		t.Fatal(err)
	}
	if err := rec.PerfResult(report.PerfResult{
		CaseID:       "case1",
		ScenarioID:   "case1",
		OperationID:  "op1",
		Operation:    "upload",
		Status:       "ok",
		StartedAt:    "2026-01-01T00:00:00Z",
		EndedAt:      "2026-01-01T00:00:01Z",
		DurationMS:   1000,
		Bytes:        50 << 20,
		RequestUnits: 10,
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := report.Generate(runDir); err != nil {
		t.Fatal(err)
	}
	if _, err := report.GeneratePerfReport(runDir, report.PerfOptions{}); err != nil {
		t.Fatal(err)
	}
}

func fakePublishDrive9(t *testing.T, dir string) string {
	t.Helper()
	bin := filepath.Join(dir, "drive9")
	script := `#!/usr/bin/env sh
printf '%s\n' "$*" >> "$DRIVE9_FAKE_LOG"
if [ "$1" = "fs" ] && [ "$2" = "mkdir" ]; then
  exit 0
fi
if [ "$1" = "fs" ] && [ "$2" = "cat" ]; then
  if [ -n "$DRIVE9_FAKE_INDEX" ]; then
    cat "$DRIVE9_FAKE_INDEX"
    exit 0
  fi
  exit 1
fi
if [ "$1" = "fs" ] && [ "$2" = "cp" ]; then
  case "$3" in
    *results.jsonl)
      if [ "$DRIVE9_FAIL_RESULTS" = "1" ]; then
        echo "forced results upload failure" >&2
        exit 9
      fi
      ;;
  esac
  exit 0
fi
echo "unexpected fake drive9 command: $*" >&2
exit 2
`
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func TestPublishIndexJSONShape(t *testing.T) {
	entry := publishIndex{
		SchemaVersion: publishSchemaVersion,
		Entries: []publishIndexEntry{{
			Title:      "title",
			RunID:      "run1",
			ReportPath: ":/performance-reports/stress/2026-01-01/run1/perf/drive9-performance-test-report.md",
		}},
	}
	b, err := json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "run1") {
		t.Fatalf("index json = %s", string(b))
	}
}
