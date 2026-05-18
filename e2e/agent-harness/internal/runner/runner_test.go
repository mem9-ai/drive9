package runner

import (
	"context"
	"encoding/hex"
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
