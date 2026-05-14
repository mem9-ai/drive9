package casefile

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadFileAppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smoke.yaml")
	if err := os.WriteFile(path, []byte(`
defaults:
  timeout: 2m
  cleanup: retain_on_failure
  mount_ready_timeout: 11s
  remote_visibility_timeout: 12s
  command_retry_count: 7
  command_retry_sleep: 2s
cases:
  - id: smoke-strict
    suite: smoke
    sync_mode: strict
    expected_outcome: baseline_pass
    remote_root_suffix: smoke-strict
    mountpoint_suffix: smoke-strict
    workload:
      type: mount_smoke
      files:
        - relative_path: a.txt
          content: hello
    oracles:
      - type: cli_read_equals
    severity:
      failure: P1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cases, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 1 {
		t.Fatalf("case count = %d, want 1", len(cases))
	}
	if got := cases[0].Timeout.Duration.String(); got != "2m0s" {
		t.Fatalf("timeout = %s", got)
	}
	if got := cases[0].Cleanup; got != "retain_on_failure" {
		t.Fatalf("cleanup = %q", got)
	}
	if got := cases[0].MountReadyTimeout(); got != 11*time.Second {
		t.Fatalf("mount ready timeout = %s", got)
	}
	if got := cases[0].RemoteVisibilityTimeout(); got != 12*time.Second {
		t.Fatalf("remote visibility timeout = %s", got)
	}
	if got := cases[0].CommandRetryCount(); got != 7 {
		t.Fatalf("retry count = %d", got)
	}
	if got := cases[0].CommandRetrySleep(); got != 2*time.Second {
		t.Fatalf("retry sleep = %s", got)
	}
}

func TestLoadFileRejectsGitExpectedLocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "regression.yaml")
	if err := os.WriteFile(path, []byte(`
defaults:
  timeout: 2m
  cleanup: retain_on_failure
cases:
  - id: git-lock
    suite: regression
    sync_mode: strict
    expected_outcome: bug_reproduced
    remote_root_suffix: git-lock
    mountpoint_suffix: git-lock
    workload:
      type: git_workflow
      clone_url: https://example.invalid/repo.git
      clone_dir: repo
      mutation:
        path: README.md
        mode: append
        content: hello
      commit_message: test
      expected_locks:
        - .git/index.lock
      expected_commit_delta: 1
    oracles:
      - type: no_git_locks
    severity:
      failure: P1
`), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadFile(path)
	if !errors.Is(err, ErrValidation) {
		t.Fatalf("err = %v, want ErrValidation", err)
	}
}

func TestValidateRejectsUnsafeRelativePaths(t *testing.T) {
	expectExit := 0
	tests := []struct {
		name string
		c    Case
	}{
		{
			name: "mount smoke file dotdot",
			c:    baseCase("mount_smoke", Workload{Type: "mount_smoke", Files: []FileSpec{{RelativePath: "../x", Content: "x"}}}, []OracleSpec{{Type: "cli_read_equals"}}),
		},
		{
			name: "path matrix absolute",
			c:    baseCase("path_matrix", Workload{Type: "path_matrix", Paths: []string{"/abs"}}, []OracleSpec{{Type: "cli_read_equals"}}),
		},
		{
			name: "git clone dir dotdot",
			c: baseCase("git_workflow", Workload{
				Type:          "git_workflow",
				CloneURL:      "fixture://basic",
				CloneDir:      "../repo",
				Mutation:      MutationSpec{Path: "README.md", Mode: "append", Content: "x"},
				CommitMessage: "test",
			}, []OracleSpec{{Type: "no_git_locks"}}),
		},
		{
			name: "git mutation backslash",
			c: baseCase("git_workflow", Workload{
				Type:          "git_workflow",
				CloneURL:      "fixture://basic",
				CloneDir:      "repo",
				Mutation:      MutationSpec{Path: `dir\README.md`, Mode: "append", Content: "x"},
				CommitMessage: "test",
			}, []OracleSpec{{Type: "no_git_locks"}}),
		},
		{
			name: "git remount verify dotdot",
			c: baseCase("git_workflow", Workload{
				Type:               "git_workflow",
				CloneURL:           "fixture://basic",
				CloneDir:           "repo",
				Mutation:           MutationSpec{Path: "README.md", Mode: "append", Content: "x"},
				CommitMessage:      "test",
				RemountVerifyPaths: []VerifySpec{{Path: "../README.md"}},
			}, []OracleSpec{{Type: "remount_hash_equal"}}),
		},
		{
			name: "oracle path dotdot",
			c:    baseCase("doctor_fuse", Workload{Type: "doctor_fuse", ExpectExit: &expectExit}, []OracleSpec{{Type: "command_exit", CommandID: "doctor", Path: "../x"}}),
		},
		{
			name: "oracle remote absolute",
			c:    baseCase("mount_smoke", Workload{Type: "mount_smoke", Files: []FileSpec{{RelativePath: "x", Content: "x"}}}, []OracleSpec{{Type: "cli_read_equals", RemotePath: "/x"}}),
		},
		{
			name: "oracle expected hash dotdot",
			c:    baseCase("mount_smoke", Workload{Type: "mount_smoke", Files: []FileSpec{{RelativePath: "x", Content: "x"}}}, []OracleSpec{{Type: "remount_hash_equal", ExpectedHashes: map[string]string{"../x": "abc"}}}),
		},
		{
			name: "oracle glob dotdot",
			c: baseCase("git_workflow", Workload{
				Type:          "git_workflow",
				CloneURL:      "fixture://basic",
				CloneDir:      "repo",
				Mutation:      MutationSpec{Path: "README.md", Mode: "append", Content: "x"},
				CommitMessage: "test",
			}, []OracleSpec{{Type: "no_git_locks", Globs: []string{"../*.lock"}}}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := Validate(tt.c); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func baseCase(workloadType string, workload Workload, oracles []OracleSpec) Case {
	return Case{
		ID:               "case-" + workloadType,
		Suite:            "regression",
		ExpectedOutcome:  "baseline_pass",
		RemoteRootSuffix: "case-" + workloadType,
		MountpointSuffix: "case-" + workloadType,
		Timeout:          Duration{Duration: 2 * time.Minute},
		Cleanup:          "always",
		Workload:         workload,
		Oracles:          oracles,
		Severity:         SeveritySpec{Failure: "P1"},
	}
}

func TestLoadRepositorySuites(t *testing.T) {
	cases, err := LoadSuites(filepath.Join("..", "..", "cases"), []string{"smoke", "regression", "stress", "fault"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) != 13 {
		t.Fatalf("case count = %d, want 13", len(cases))
	}
	for _, c := range cases {
		if c.Workload.Type == "git_workflow" && len(c.Workload.ExpectedLocks) != 0 {
			t.Fatalf("%s expected_locks must be empty", c.ID)
		}
	}
}
