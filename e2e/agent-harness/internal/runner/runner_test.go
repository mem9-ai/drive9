package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestGCRequiresConfirmDelete(t *testing.T) {
	err := GC(context.Background(), GCConfig{RunDir: t.TempDir()})
	if err == nil || err.Error() != "gc requires --confirm-delete" {
		t.Fatalf("err = %v, want confirm-delete error", err)
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
	if err == nil {
		t.Fatal("expected unsafe mountpoint rejection")
	}
}
