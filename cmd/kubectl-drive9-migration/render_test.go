package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func renderedJob(namespace, batch, pod, volumeID string) jobResult {
	status := &workerStatus{
		JobID: "job-" + volumeID, VolumeID: volumeID, NodeName: "node-a", SpaceRef: "space-a", Prefix: "/data",
		RuntimeState: "RUNNING",
		Phase:        "SYNCING", StartupPhase: "SYNCING", SourceCount: 42,
		Conditions: workerConditions{ReadyForRollout: true},
		CandidateCounts: workerCandidates{
			Mtime: 3, SourceTokenChanged: 5, NewPath: 7, Filtered: 11,
		},
		Round: workerRound{
			ID: "round-1", Mode: "full", CompletedAt: time.Date(2026, 8, 10, 2, 0, 0, 0, time.UTC),
			ScanComplete: true, Converged: true,
		},
		Verification: workerVerification{Status: "pending"},
		LargeScale:   true,
		Generation: &workerGeneration{
			Stage: "apply_files", ApplyPending: 2, ApplyUnknown: 1,
			InlineWorkers: 3, MultipartWorkers: 2, RebuildReason: "cache_miss",
		},
	}
	return jobResult{
		Namespace: namespace, Batch: batch, Pod: pod, Node: "node-a", PodPhase: "Running",
		JobID: status.JobID, VolumeID: volumeID, Phase: "SYNCING", DisplayStatus: "READY_FOR_ROLLOUT",
		Worker: json.RawMessage(`{"volume_id":"` + volumeID + `","future_field":true}`),
		parsed: status,
	}
}

func TestRenderCompactTableAndSummaries(t *testing.T) {
	jobs := []jobResult{
		renderedJob("migration", "batch-a", "worker-b", "vol-002"),
		renderedJob("migration", "batch-a", "worker-a", "vol-001"),
	}
	sortJobs(jobs)
	batches := summarizeBatches(jobs)
	var output bytes.Buffer
	if err := renderStatus(&output, options{output: "table"}, time.Now(), jobs, batches); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	if !strings.HasPrefix(text, "JOB") || strings.HasPrefix(text, "BATCH") {
		t.Fatalf("single-batch header=%q", strings.SplitN(text, "\n", 2)[0])
	}
	if strings.Index(text, "vol-001") > strings.Index(text, "vol-002") {
		t.Fatalf("rows are not sorted: %s", text)
	}
	for _, want := range []string{"READY_FOR_ROLLOUT", "full/ok", "apply_files", "42", "Batch migration/batch-a: READY_FOR_ROLLOUT", "observed 2 jobs"} {
		if !strings.Contains(text, want) {
			t.Fatalf("output omitted %q:\n%s", want, text)
		}
	}

	jobs = append(jobs, renderedJob("migration", "batch-b", "worker-c", "vol-003"))
	sortJobs(jobs)
	output.Reset()
	if err := renderStatus(&output, options{output: "table"}, time.Now(), jobs, summarizeBatches(jobs)); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(output.String(), "BATCH") {
		t.Fatalf("multi-batch output omitted BATCH: %s", output.String())
	}
}

func TestRenderWideIncludesOperationalDetails(t *testing.T) {
	job := renderedJob("migration", "batch-a", "worker-a", "vol-001")
	job.Error = "bounded detail"
	var output bytes.Buffer
	if err := renderStatus(&output, options{output: "wide"}, time.Now(), []jobResult{job}, summarizeBatches([]jobResult{job})); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"NAMESPACE", "NODE", "SPACE", "PREFIX", "CAND_MTIME", "CAND_SOURCE_TOKEN_CHANGED",
		"CAND_NEW_PATH", "CAND_FILTERED", "IN_FLIGHT", "GEN_STAGE", "GEN_PENDING", "GEN_UNKNOWN",
		"INLINE_W", "MULTIPART_W", "REBUILD", "migration", "node-a", "apply_files",
		"space-a", "/data", "3", "5", "7", "11", "2", "1", "cache_miss", "bounded detail",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("wide output omitted %q:\n%s", want, output.String())
		}
	}
}

func TestRenderJSONPreservesRawWorkerAndOmitsKubeconfig(t *testing.T) {
	generatedAt := time.Date(2026, 8, 10, 3, 0, 0, 0, time.UTC)
	job := renderedJob("migration", "batch-a", "worker-a", "vol-001")
	var output bytes.Buffer
	opts := options{
		output: "json", namespace: "migration", batch: "batch-a", contextName: "prod",
		kubeconfig: "/secret/kubeconfig",
	}
	if err := renderStatus(&output, opts, generatedAt, []jobResult{job}, summarizeBatches([]jobResult{job})); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "/secret/kubeconfig") {
		t.Fatalf("JSON leaked kubeconfig path: %s", output.String())
	}
	var aggregate aggregateOutput
	if err := json.Unmarshal(output.Bytes(), &aggregate); err != nil {
		t.Fatal(err)
	}
	if !aggregate.GeneratedAt.Equal(generatedAt) || aggregate.Scope.Context != "prod" || len(aggregate.Jobs) != 1 {
		t.Fatalf("aggregate=%+v", aggregate)
	}
	if !strings.Contains(string(aggregate.Jobs[0].Worker), "future_field") {
		t.Fatalf("raw Worker payload not preserved: %s", aggregate.Jobs[0].Worker)
	}
}
