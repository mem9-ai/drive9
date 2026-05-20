package runner

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
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
	if pub.ArtifactPaths["perf/customer-report.md"] == "" || pub.ArtifactPaths["perf/publish-manifest.json"] == "" {
		t.Fatalf("artifact paths = %+v", pub.ArtifactPaths)
	}
	logBytes, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	logText := string(logBytes)
	for _, want := range []string{"fs cp", "perf/customer-report.md", "index.json"} {
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
			ReportPath: ":/performance-reports/stress/2026-01-01/run1/perf/customer-report.md",
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
