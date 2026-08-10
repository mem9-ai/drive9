package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func mutateStatusJSON(t *testing.T, mutate func(map[string]any)) []byte {
	t.Helper()
	var document map[string]any
	if err := json.Unmarshal(statusJSON(t, "vol-001", "SYNCING", map[string]bool{
		"ready_for_rollout": false, "current_converged": false, "attention": false,
	}), &document); err != nil {
		t.Fatal(err)
	}
	mutate(document)
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestDecodeWorkerStatusValidation(t *testing.T) {
	withFutureField := mutateStatusJSON(t, func(document map[string]any) {
		document["future_field"] = map[string]any{"kept": true}
	})
	status, raw, err := decodeWorkerStatus(withFutureField)
	if err != nil {
		t.Fatal(err)
	}
	if status.VolumeID != "vol-001" || !strings.Contains(string(raw), "future_field") {
		t.Fatalf("status=%+v raw=%s", status, raw)
	}

	for _, tc := range []struct {
		name string
		body []byte
		want string
	}{
		{name: "empty", body: nil, want: "EOF"},
		{name: "malformed", body: []byte("{"), want: "unexpected EOF"},
		{name: "trailing", body: append(statusJSON(t, "vol-001", "SYNCING", map[string]bool{
			"ready_for_rollout": false, "current_converged": false, "attention": false,
		}), []byte("\n{}")...), want: "trailing JSON"},
		{name: "missing identity", body: mutateStatusJSON(t, func(document map[string]any) {
			delete(document, "volume_id")
		}), want: "missing volume_id"},
		{name: "empty identity", body: mutateStatusJSON(t, func(document map[string]any) {
			document["volume_id"] = ""
		}), want: "empty volume_id"},
		{name: "invalid phase", body: mutateStatusJSON(t, func(document map[string]any) {
			document["phase"] = "UNKNOWN"
		}), want: "invalid phase"},
		{name: "missing condition", body: mutateStatusJSON(t, func(document map[string]any) {
			delete(document["conditions"].(map[string]any), "attention")
		}), want: "missing conditions.attention"},
		{name: "missing fence", body: mutateStatusJSON(t, func(document map[string]any) {
			delete(document, "fence_complete")
		}), want: "missing fence_complete"},
		{name: "null fence", body: mutateStatusJSON(t, func(document map[string]any) {
			document["fence_complete"] = nil
		}), want: "fence_complete must be a boolean"},
		{name: "null condition", body: mutateStatusJSON(t, func(document map[string]any) {
			document["conditions"].(map[string]any)["attention"] = nil
		}), want: "conditions.attention must be a boolean"},
		{name: "wrong fence type", body: mutateStatusJSON(t, func(document map[string]any) {
			document["fence_complete"] = "false"
		}), want: "cannot unmarshal string"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := decodeWorkerStatus(tc.body)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want substring %q", err, tc.want)
			}
		})
	}
}

func TestDeriveJobStatus(t *testing.T) {
	base := workerStatus{Phase: "SYNCING", StartupPhase: "SYNCING"}
	for _, tc := range []struct {
		name   string
		mutate func(*workerStatus)
		want   string
	}{
		{name: "syncing", want: "SYNCING"},
		{name: "ready for rollout", mutate: func(status *workerStatus) {
			status.Conditions.ReadyForRollout = true
		}, want: "READY_FOR_ROLLOUT"},
		{name: "repairing", mutate: func(status *workerStatus) {
			status.Phase, status.StartupPhase = "DUAL_WRITE_REPAIRING", "DUAL_WRITE_REPAIRING"
		}, want: "REPAIRING"},
		{name: "converged", mutate: func(status *workerStatus) {
			status.Phase, status.StartupPhase = "DUAL_WRITE_REPAIRING", "DUAL_WRITE_REPAIRING"
			status.Conditions.CurrentConverged = true
		}, want: "CONVERGED"},
		{name: "attention wins", mutate: func(status *workerStatus) {
			status.Conditions.ReadyForRollout = true
			status.Conditions.Attention = true
		}, want: "ATTENTION"},
		{name: "cutover", mutate: func(status *workerStatus) {
			status.Phase, status.StartupPhase = "CUTOVER_READY", "DUAL_WRITE_REPAIRING"
			status.FenceComplete = true
		}, want: "CUTOVER_READY"},
		{name: "incomplete fence", mutate: func(status *workerStatus) {
			status.Phase, status.StartupPhase = "CUTOVER_READY", "DUAL_WRITE_REPAIRING"
		}, want: "ATTENTION"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status := base
			if tc.mutate != nil {
				tc.mutate(&status)
			}
			if got := deriveJobStatus(status); got != tc.want {
				t.Fatalf("status=%s, want %s", got, tc.want)
			}
		})
	}
}

func batchJob(pod, phase, display string) jobResult {
	status := &workerStatus{VolumeID: "vol-" + pod, Phase: phase, StartupPhase: phase}
	return jobResult{
		Namespace: "migration", Batch: "batch-a", Pod: pod, JobID: status.VolumeID,
		Phase: phase, DisplayStatus: display, parsed: status,
	}
}

func TestDeriveBatchStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		jobs []jobResult
		want string
	}{
		{name: "needs attention", jobs: []jobResult{batchJob("a", "SYNCING", "ATTENTION")}, want: "NEEDS_ATTENTION"},
		{name: "mixed phase", jobs: []jobResult{
			batchJob("a", "SYNCING", "SYNCING"),
			batchJob("b", "DUAL_WRITE_REPAIRING", "REPAIRING"),
		}, want: "MIXED_PHASE"},
		{name: "syncing", jobs: []jobResult{
			batchJob("a", "SYNCING", "READY_FOR_ROLLOUT"),
			batchJob("b", "SYNCING", "SYNCING"),
		}, want: "SYNCING"},
		{name: "ready", jobs: []jobResult{
			batchJob("a", "SYNCING", "READY_FOR_ROLLOUT"),
			batchJob("b", "SYNCING", "READY_FOR_ROLLOUT"),
		}, want: "READY_FOR_ROLLOUT"},
		{name: "repairing", jobs: []jobResult{
			batchJob("a", "DUAL_WRITE_REPAIRING", "CONVERGED"),
			batchJob("b", "DUAL_WRITE_REPAIRING", "REPAIRING"),
		}, want: "REPAIRING"},
		{name: "converged", jobs: []jobResult{
			batchJob("a", "DUAL_WRITE_REPAIRING", "CONVERGED"),
			batchJob("b", "DUAL_WRITE_REPAIRING", "CONVERGED"),
		}, want: "CONVERGED"},
		{name: "cutover", jobs: []jobResult{
			batchJob("a", "CUTOVER_READY", "CUTOVER_READY"),
			batchJob("b", "CUTOVER_READY", "CUTOVER_READY"),
		}, want: "CUTOVER_READY"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := deriveBatchStatus(tc.jobs); got != tc.want {
				t.Fatalf("status=%s, want %s", got, tc.want)
			}
		})
	}
}

func TestCollectStatusPreservesFailuresAndDetectsDuplicates(t *testing.T) {
	fake := &fakeKubectl{
		podList: podListJSON(t,
			podJSON("migration", "worker-a", "batch-a", "Running"),
			podJSON("migration", "worker-b", "batch-a", "Running"),
			podJSON("migration", "worker-c", "batch-a", "Pending"),
			podJSON("migration", "worker-d", "", "Running"),
		),
		statuses: map[string][]byte{
			"migration/worker-a": statusJSON(t, "vol-001", "SYNCING", map[string]bool{
				"ready_for_rollout": true, "current_converged": false, "attention": false,
			}),
			"migration/worker-b": statusJSON(t, "vol-001", "SYNCING", map[string]bool{
				"ready_for_rollout": true, "current_converged": false, "attention": false,
			}),
			"migration/worker-d": statusJSON(t, "vol-004", "SYNCING", map[string]bool{
				"ready_for_rollout": false, "current_converged": false, "attention": false,
			}),
		},
	}
	deps := normalizeDependencies(testDeps(fake))
	jobs, batches, partial, err := collectStatus(context.Background(), options{}, deps)
	if err != nil {
		t.Fatal(err)
	}
	if !partial || len(jobs) != 4 || len(batches) != 2 {
		t.Fatalf("partial=%v jobs=%d batches=%+v", partial, len(jobs), batches)
	}
	byPod := make(map[string]jobResult)
	for _, job := range jobs {
		byPod[job.Pod] = job
	}
	for _, pod := range []string{"worker-a", "worker-b"} {
		if byPod[pod].DisplayStatus != "DUPLICATE" {
			t.Fatalf("%s status=%s", pod, byPod[pod].DisplayStatus)
		}
	}
	if byPod["worker-c"].DisplayStatus != "POD_PENDING" {
		t.Fatalf("pending=%+v", byPod["worker-c"])
	}
	if byPod["worker-d"].DisplayStatus != "UNAVAILABLE" || byPod["worker-d"].Batch != missingBatch {
		t.Fatalf("missing label=%+v", byPod["worker-d"])
	}
}

func TestCollectStatusPreservesNonRunningAndInvalidPods(t *testing.T) {
	failed := podJSON("migration", "worker-failed", "batch-a", "Failed")
	succeeded := podJSON("migration", "worker-succeeded", "batch-a", "Succeeded")
	terminating := podJSON("migration", "worker-terminating", "batch-a", "Running")
	terminating["metadata"].(map[string]any)["deletionTimestamp"] = "2026-08-10T03:00:00Z"
	wrongContainer := podJSON("migration", "worker-wrong-container", "batch-a", "Running")
	wrongContainer["spec"].(map[string]any)["containers"] = []map[string]string{{"name": "sidecar"}}
	fake := &fakeKubectl{
		podList:  podListJSON(t, failed, succeeded, terminating, wrongContainer),
		statuses: map[string][]byte{},
	}
	deps := normalizeDependencies(testDeps(fake))
	jobs, _, partial, err := collectStatus(context.Background(), options{}, deps)
	if err != nil {
		t.Fatal(err)
	}
	if !partial || len(fake.calls) != 1 {
		t.Fatalf("partial=%v calls=%#v", partial, fake.calls)
	}
	byPod := make(map[string]string)
	for _, job := range jobs {
		byPod[job.Pod] = job.DisplayStatus
	}
	want := map[string]string{
		"worker-failed":          "POD_FAILED",
		"worker-succeeded":       "POD_SUCCEEDED",
		"worker-terminating":     "TERMINATING",
		"worker-wrong-container": "UNAVAILABLE",
	}
	for pod, status := range want {
		if byPod[pod] != status {
			t.Fatalf("%s status=%q, want %q", pod, byPod[pod], status)
		}
	}
}

func TestCollectStatusCapsConcurrency(t *testing.T) {
	pods := make([]map[string]any, 0, 12)
	statuses := make(map[string][]byte)
	for i := 0; i < 12; i++ {
		pod := fmt.Sprintf("worker-%02d", i)
		pods = append(pods, podJSON("migration", pod, "batch-a", "Running"))
		statuses[pod] = statusJSON(t, fmt.Sprintf("vol-%03d", i), "SYNCING", map[string]bool{
			"ready_for_rollout": false, "current_converged": false, "attention": false,
		})
	}
	started := make(chan struct{}, len(pods))
	release := make(chan struct{})
	var active, maximum atomic.Int32
	run := func(ctx context.Context, _ string, args ...string) commandResult {
		if containsArg(args, "get") {
			return commandResult{stdout: podListJSON(t, pods...)}
		}
		current := active.Add(1)
		defer active.Add(-1)
		for {
			prior := maximum.Load()
			if current <= prior || maximum.CompareAndSwap(prior, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
			return commandResult{stdout: statuses[argBefore(args, "--container")]}
		case <-ctx.Done():
			return commandResult{err: ctx.Err()}
		}
	}
	deps := normalizeDependencies(dependencies{
		ctx: context.Background(), run: run, execTimeout: time.Second, concurrency: 99,
		now: time.Now,
	})
	type outcome struct {
		jobs    []jobResult
		partial bool
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		jobs, _, partial, err := collectStatus(context.Background(), options{}, deps)
		done <- outcome{jobs: jobs, partial: partial, err: err}
	}()
	for i := 0; i < maxConcurrentExec; i++ {
		<-started
	}
	select {
	case <-started:
		t.Fatal("more than eight exec calls started before a slot was released")
	default:
	}
	close(release)
	result := <-done
	if result.err != nil || result.partial || len(result.jobs) != len(pods) {
		t.Fatalf("result=%+v", result)
	}
	if maximum.Load() != maxConcurrentExec {
		t.Fatalf("maximum concurrency=%d, want %d", maximum.Load(), maxConcurrentExec)
	}
}

func TestCollectStatusTimesOutOneWorker(t *testing.T) {
	run := func(ctx context.Context, _ string, args ...string) commandResult {
		if containsArg(args, "get") {
			return commandResult{stdout: podListJSON(t, podJSON("migration", "worker-a", "batch-a", "Running"))}
		}
		<-ctx.Done()
		return commandResult{err: errors.New("child stopped")}
	}
	deps := normalizeDependencies(dependencies{
		ctx: context.Background(), run: run, execTimeout: 10 * time.Millisecond,
		concurrency: maxConcurrentExec, now: time.Now,
	})
	jobs, _, partial, err := collectStatus(context.Background(), options{}, deps)
	if err != nil {
		t.Fatal(err)
	}
	if !partial || len(jobs) != 1 || jobs[0].DisplayStatus != "UNAVAILABLE" {
		t.Fatalf("partial=%v jobs=%+v", partial, jobs)
	}
	if !strings.Contains(jobs[0].Error, "deadline exceeded") {
		t.Fatalf("error=%q", jobs[0].Error)
	}
}

func TestNormalizeMessageIsSingleLineAndBounded(t *testing.T) {
	message := normalizeMessage(" first\nsecond\t" + strings.Repeat("x", maxErrorLength+20))
	if strings.ContainsAny(message, "\n\t") || len([]rune(message)) != maxErrorLength {
		t.Fatalf("message length=%d value=%q", len([]rune(message)), message)
	}
}
