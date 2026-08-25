package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/migration"
)

func testDependencies(t *testing.T) (dependencies, *[]string) {
	t.Helper()
	calls := []string{}
	return dependencies{
		getenv: func(name string) string {
			switch name {
			case migration.MigrationNodeNameEnv:
				return "node-a"
			case migration.MigrationPhaseEnv:
				return string(migration.PhaseSyncing)
			default:
				return ""
			}
		},
		credentialRoot: "/secret-root",
		load: func(configPath, nodeName, phase, secretRoot string, highest migration.Phase) (*migration.RuntimeStartup, error) {
			calls = append(calls, "load:"+configPath+":"+nodeName+":"+phase+":"+secretRoot+":"+string(highest))
			return &migration.RuntimeStartup{Source: migration.EBSSourceConfig{VolumeID: "vol-001"}, Phase: migration.PhaseSyncing}, nil
		},
		plan: func(context.Context, *migration.RuntimeStartup) (migration.PlanResult, error) {
			calls = append(calls, "plan")
			return migration.PlanResult{VolumeID: "vol-001"}, nil
		},
		start: func(context.Context, *migration.RuntimeStartup) error {
			calls = append(calls, "run")
			return nil
		},
		control: func(_ context.Context, request controlRequest) error {
			calls = append(calls, "control:"+request.Command+":"+request.JobID+":"+request.Output+":"+request.Type)
			return nil
		},
	}, &calls
}

func TestExecuteDispatchesSixCommands(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		wantCall string
	}{
		{name: "plan", args: []string{"plan", "-f", "/config.yaml"}, wantCall: "load:/config.yaml:node-a:SYNCING:/secret-root:"},
		{name: "run", args: []string{"run", "-f", "/config.yaml"}, wantCall: "run"},
		{name: "status", args: []string{"status", "--output", "json"}, wantCall: "control:status::json:"},
		{name: "diff", args: []string{"diff", "--job-id", "job-a", "--type", "content", "--limit", "10", "--output", "jsonl"}, wantCall: "control:diff:job-a:jsonl:content"},
		{name: "verify", args: []string{"verify-full", "--job-id", "job-a"}, wantCall: "control:verify-full:job-a::"},
		{name: "cutover", args: []string{"prepare-drive9-cutover", "--job-id", "job-a"}, wantCall: "control:prepare-drive9-cutover:job-a::"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps, calls := testDependencies(t)
			var stdout, stderr bytes.Buffer
			if code := execute(tc.args, &stdout, &stderr, deps); code != exitSuccess {
				t.Fatalf("exit=%d stderr=%q", code, stderr.String())
			}
			if !containsCall(*calls, tc.wantCall) {
				t.Fatalf("calls=%v, want %q", *calls, tc.wantCall)
			}
			if tc.name == "plan" && !containsCall(*calls, "plan") {
				t.Fatalf("calls=%v, plan command skipped planning", *calls)
			}
			if strings.Contains(stdout.String(), "secret") {
				t.Fatalf("stdout leaked secret: %q", stdout.String())
			}
		})
	}
}

func containsCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
}

func TestExecuteExitCodes(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		err  error
		want int
	}{
		{name: "argument", args: []string{"status"}, want: exitFailure},
		{name: "unknown command", args: []string{"unknown"}, want: exitFailure},
		{name: "invalid phase", args: []string{"plan", "-f", "/config.yaml"}, err: migration.ErrInvalidPhase, want: exitIllegalOperation},
		{name: "configuration", args: []string{"plan", "-f", "/config.yaml"}, err: errors.New("bad config"), want: exitFailure},
		{name: "illegal action", args: []string{"verify-full", "--job-id", "job-a"}, err: migration.ErrIllegalAction, want: exitIllegalOperation},
		{name: "socket unavailable", args: []string{"status", "--output", "json"}, err: migration.ErrControlUnavailable, want: exitControlUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps, _ := testDependencies(t)
			if len(tc.args) > 0 && (tc.args[0] == "plan" || tc.args[0] == "run") {
				deps.load = func(string, string, string, string, migration.Phase) (*migration.RuntimeStartup, error) {
					return nil, tc.err
				}
			} else if tc.err != nil {
				deps.control = func(context.Context, controlRequest) error { return tc.err }
			}
			if code := execute(tc.args, io.Discard, io.Discard, deps); code != tc.want {
				t.Fatalf("exit=%d, want %d", code, tc.want)
			}
		})
	}
}

func TestExecuteRejectsInvalidCommandOptions(t *testing.T) {
	deps, _ := testDependencies(t)
	for _, args := range [][]string{
		{"plan"},
		{"run", "-f", "config", "extra"},
		{"status", "--output", "yaml"},
		{"diff", "--job-id", "job-a", "--output", "json"},
		{"diff", "--job-id", "job-a", "--output", "jsonl", "--limit", "-1"},
		{"verify-full"},
		{"prepare-drive9-cutover"},
	} {
		if code := execute(args, io.Discard, io.Discard, deps); code != exitFailure {
			t.Fatalf("args=%v exit=%d", args, code)
		}
	}
}

func TestExecuteHelpAndPlanFailure(t *testing.T) {
	deps, _ := testDependencies(t)
	var output bytes.Buffer
	if code := execute([]string{"--help"}, &output, io.Discard, deps); code != exitSuccess || !strings.Contains(output.String(), "drive9-migration") {
		t.Fatalf("help exit=%d output=%q", code, output.String())
	}
	deps.plan = func(context.Context, *migration.RuntimeStartup) (migration.PlanResult, error) {
		return migration.PlanResult{VolumeID: "vol-001"}, migration.ErrPlanFailed
	}
	if code := execute([]string{"plan", "-f", "/config.yaml"}, io.Discard, io.Discard, deps); code != exitFailure {
		t.Fatalf("plan failure exit=%d", code)
	}
	deps.load = func(string, string, string, string, migration.Phase) (*migration.RuntimeStartup, error) {
		return nil, errors.Join(migration.ErrPreflight, migration.ErrInvalidPhase)
	}
	if code := execute([]string{"run", "-f", "/config.yaml"}, io.Discard, io.Discard, deps); code != exitIllegalOperation {
		t.Fatalf("illegal startup phase exit=%d", code)
	}
}

func TestExecuteUsesProvidedContextAndHelpDocumentsEveryCommand(t *testing.T) {
	deps, _ := testDependencies(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	deps.ctx = ctx
	deps.control = func(got context.Context, _ controlRequest) error {
		return got.Err()
	}
	if code := execute([]string{"status", "--output", "json"}, io.Discard, io.Discard, deps); code != exitFailure {
		t.Fatalf("canceled context exit=%d", code)
	}
	var output bytes.Buffer
	writeUsage(&output)
	for _, command := range []string{"plan", "run", "status", "diff", "verify-full", "prepare-drive9-cutover"} {
		if !strings.Contains(output.String(), command) {
			t.Fatalf("help omitted %q: %s", command, output.String())
		}
	}
}
