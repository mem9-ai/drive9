package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordedCall struct {
	name string
	args []string
}

type fakeKubectl struct {
	mu       sync.Mutex
	calls    []recordedCall
	podList  []byte
	statuses map[string][]byte
	errors   map[string]commandResult
}

func (f *fakeKubectl) run(_ context.Context, name string, args ...string) commandResult {
	f.mu.Lock()
	f.calls = append(f.calls, recordedCall{name: name, args: append([]string(nil), args...)})
	f.mu.Unlock()

	if len(args) > 0 && containsArg(args, "get") {
		return commandResult{stdout: f.podList}
	}
	pod := argAfter(args, "--namespace") + "/" + argBefore(args, "--container")
	if result, ok := f.errors[pod]; ok {
		return result
	}
	return commandResult{stdout: f.statuses[pod]}
}

func containsArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func argAfter(args []string, want string) string {
	for i := range args {
		if args[i] == want && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

func argBefore(args []string, want string) string {
	for i := range args {
		if args[i] == want && i > 0 {
			return args[i-1]
		}
	}
	return ""
}

func podListJSON(t *testing.T, pods ...map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "PodList",
		"items":      pods,
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func podJSON(namespace, name, batch, phase string) map[string]any {
	labels := map[string]string{
		"app.kubernetes.io/name":      "drive9-migration",
		"app.kubernetes.io/component": "worker",
	}
	if batch != "" {
		labels["app.kubernetes.io/instance"] = batch
	}
	return map[string]any{
		"metadata": map[string]any{
			"namespace": namespace,
			"name":      name,
			"labels":    labels,
		},
		"spec": map[string]any{
			"nodeName":   "node-a",
			"containers": []map[string]string{{"name": workerContainer}},
		},
		"status": map[string]string{"phase": phase},
	}
}

func statusJSON(t *testing.T, volumeID, phase string, conditions map[string]bool) []byte {
	t.Helper()
	job := map[string]any{
		"job_id":            volumeID,
		"volume_id":         volumeID,
		"node_name":         "node-a",
		"ebs_root":          "/ebs",
		"subpath":           "/data",
		"space_ref":         "space-a",
		"prefix":            "/data",
		"runtime_state":     "RUNNING",
		"phase":             phase,
		"startup_phase":     phase,
		"recovery_complete": true,
		"round": map[string]any{
			"id":              "round-1",
			"mode":            "full",
			"scan_complete":   true,
			"round_converged": true,
			"completed_at":    "2026-08-10T02:00:00Z",
		},
		"conditions":        conditions,
		"source_count":      42,
		"finding_counts":    map[string]int{},
		"backlog":           0,
		"full_verification": map[string]any{"status": "pending"},
		"fence_intent":      phase == "CUTOVER_READY",
		"fence_complete":    phase == "CUTOVER_READY",
	}
	body, err := json.Marshal(map[string]any{
		"volume_id": volumeID,
		"node_name": "node-a",
		"ebs_root":  "/ebs",
		"jobs":      []any{job},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func testDeps(fake *fakeKubectl) dependencies {
	return dependencies{
		ctx:         context.Background(),
		now:         func() time.Time { return time.Date(2026, 8, 10, 3, 0, 0, 0, time.UTC) },
		run:         fake.run,
		execTimeout: time.Second,
		concurrency: maxConcurrentExec,
	}
}

func TestExecuteBuildsKubectlArguments(t *testing.T) {
	fake := &fakeKubectl{
		podList: podListJSON(t, podJSON("migration", "worker-a", "batch-a", "Running")),
		statuses: map[string][]byte{
			"migration/worker-a": statusJSON(t, "vol-001", "SYNCING", map[string]bool{
				"ready_for_rollout": true, "current_converged": false, "attention": false,
			}),
		},
	}
	var stdout, stderr bytes.Buffer
	args := []string{
		"status", "--namespace", "migration", "--batch", "batch-a",
		"--context", "prod", "--kubeconfig", "/tmp/kubeconfig",
	}
	if code := execute(args, &stdout, &stderr, testDeps(fake)); code != exitSuccess {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}

	want := []recordedCall{
		{name: "kubectl", args: []string{
			"--kubeconfig", "/tmp/kubeconfig", "--context", "prod",
			"get", "pods", "--namespace", "migration",
			"--selector", fixedSelector + ",app.kubernetes.io/instance=batch-a",
			"--output", "json",
		}},
		{name: "kubectl", args: []string{
			"--kubeconfig", "/tmp/kubeconfig", "--context", "prod",
			"exec", "--namespace", "migration", "worker-a",
			"--container", workerContainer, "--",
			workerBinary, "status", "--output", "json",
		}},
	}
	if !reflect.DeepEqual(fake.calls, want) {
		t.Fatalf("calls=%#v\nwant=%#v", fake.calls, want)
	}
	if !strings.Contains(stdout.String(), "vol-001") || !strings.Contains(stdout.String(), "READY_FOR_ROLLOUT") {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestExecuteScopeArguments(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "current namespace",
			args: []string{"status"},
			want: []string{"get", "pods", "--selector", fixedSelector, "--output", "json"},
		},
		{
			name: "all namespaces",
			args: []string{"status", "--all-namespaces"},
			want: []string{"get", "pods", "--all-namespaces", "--selector", fixedSelector, "--output", "json"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeKubectl{podList: podListJSON(t), statuses: map[string][]byte{}}
			if code := execute(tc.args, io.Discard, io.Discard, testDeps(fake)); code != exitFailure {
				t.Fatalf("exit=%d, want %d for zero Pods", code, exitFailure)
			}
			if len(fake.calls) != 1 || !reflect.DeepEqual(fake.calls[0].args, tc.want) {
				t.Fatalf("calls=%#v want args=%#v", fake.calls, tc.want)
			}
		})
	}
}

func TestExecuteArgumentErrorsAndHelp(t *testing.T) {
	fake := &fakeKubectl{}
	deps := testDeps(fake)
	for _, args := range [][]string{
		{},
		{"unknown"},
		{"status", "--namespace", "a", "--all-namespaces"},
		{"status", "--output", "yaml"},
		{"status", "--batch", "bad,batch"},
		{"status", "extra"},
	} {
		if code := execute(args, io.Discard, io.Discard, deps); code != exitUsage {
			t.Fatalf("args=%v exit=%d, want %d", args, code, exitUsage)
		}
	}
	var help bytes.Buffer
	if code := execute([]string{"--help"}, &help, io.Discard, deps); code != exitSuccess {
		t.Fatalf("help exit=%d", code)
	}
	if !strings.Contains(help.String(), "kubectl drive9 migration status") {
		t.Fatalf("help=%q", help.String())
	}
	if len(fake.calls) != 0 {
		t.Fatalf("invalid commands invoked kubectl: %#v", fake.calls)
	}

	var parseError bytes.Buffer
	if code := execute([]string{"status", "--bogus"}, io.Discard, &parseError, deps); code != exitUsage {
		t.Fatalf("unknown flag exit=%d", code)
	}
	if got := strings.Count(parseError.String(), "flag provided but not defined"); got != 1 {
		t.Fatalf("unknown flag error count=%d, stderr=%q", got, parseError.String())
	}
}

func TestExecuteAttentionIsSuccessfulButPendingIsPartial(t *testing.T) {
	attention := &fakeKubectl{
		podList: podListJSON(t, podJSON("migration", "worker-a", "batch-a", "Running")),
		statuses: map[string][]byte{
			"migration/worker-a": statusJSON(t, "vol-001", "SYNCING", map[string]bool{
				"ready_for_rollout": false, "current_converged": false, "attention": true,
			}),
		},
	}
	var output bytes.Buffer
	if code := execute([]string{"status"}, &output, io.Discard, testDeps(attention)); code != exitSuccess {
		t.Fatalf("attention exit=%d output=%q", code, output.String())
	}
	if !strings.Contains(output.String(), "ATTENTION") {
		t.Fatalf("attention output=%q", output.String())
	}

	pending := &fakeKubectl{
		podList:  podListJSON(t, podJSON("migration", "worker-b", "batch-a", "Pending")),
		statuses: map[string][]byte{},
	}
	output.Reset()
	if code := execute([]string{"status"}, &output, io.Discard, testDeps(pending)); code != exitFailure {
		t.Fatalf("pending exit=%d output=%q", code, output.String())
	}
	if !strings.Contains(output.String(), "POD_PENDING") {
		t.Fatalf("pending output=%q", output.String())
	}
}

func TestExecutePreservesSuccessWhenOneExecFails(t *testing.T) {
	fake := &fakeKubectl{
		podList: podListJSON(t,
			podJSON("migration", "worker-a", "batch-a", "Running"),
			podJSON("migration", "worker-b", "batch-a", "Running"),
		),
		statuses: map[string][]byte{
			"migration/worker-a": statusJSON(t, "vol-001", "SYNCING", map[string]bool{
				"ready_for_rollout": true, "current_converged": false, "attention": false,
			}),
		},
		errors: map[string]commandResult{
			"migration/worker-b": {stderr: []byte("pods/exec is forbidden\n"), err: errors.New("exit 1")},
		},
	}
	var output bytes.Buffer
	if code := execute([]string{"status"}, &output, io.Discard, testDeps(fake)); code != exitFailure {
		t.Fatalf("exit=%d output=%q", code, output.String())
	}
	for _, want := range []string{"vol-001", "READY_FOR_ROLLOUT", "worker-b", "UNAVAILABLE"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("partial output omitted %q: %s", want, output.String())
		}
	}
}
