package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

func collectStatus(ctx context.Context, opts options, deps dependencies) ([]jobResult, []batchSummary, bool, error) {
	listed := deps.run(ctx, "kubectl", getPodsArgs(opts)...)
	if listed.err != nil {
		return nil, nil, false, fmt.Errorf("list Drive9 Migration Worker Pods: %s", commandError(listed, ctx.Err()))
	}
	var pods podList
	if err := decodeOneJSON(listed.stdout, &pods); err != nil {
		return nil, nil, false, fmt.Errorf("decode kubectl Pod list: %w", err)
	}
	if len(pods.Items) == 0 {
		return nil, nil, false, errors.New("no Drive9 Migration Worker Pods matched the requested scope")
	}

	podJobs := make([][]jobResult, len(pods.Items))
	execIndexes := make([]int, 0, len(pods.Items))
	for i := range pods.Items {
		item := &pods.Items[i]
		batch := item.Metadata.Labels[instanceLabel]
		if batch == "" {
			batch = missingBatch
		}
		base := jobResult{
			Namespace: item.Metadata.Namespace,
			Batch:     batch,
			Pod:       item.Metadata.Name,
			Node:      item.Spec.NodeName,
			PodPhase:  item.Status.Phase,
		}
		switch {
		case item.Metadata.Name == "" || item.Metadata.Namespace == "":
			markCollectionFailure(&base, "UNAVAILABLE", "Pod identity is incomplete")
		case item.Metadata.DeletionTimestamp != nil:
			markCollectionFailure(&base, "TERMINATING", "Pod is terminating")
		case item.Status.Phase != "Running":
			phase := strings.ToUpper(item.Status.Phase)
			if phase == "" {
				phase = "UNKNOWN"
			}
			markCollectionFailure(&base, "POD_"+phase, "Pod phase is "+emptyAsUnknown(item.Status.Phase))
		case !hasContainer(*item, workerContainer):
			markCollectionFailure(&base, "UNAVAILABLE", "Pod has no drive9-migration container")
		default:
			execIndexes = append(execIndexes, i)
		}
		podJobs[i] = []jobResult{base}
	}

	semaphore := make(chan struct{}, deps.concurrency)
	var wait sync.WaitGroup
	for _, index := range execIndexes {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
				defer func() { <-semaphore }()
			case <-ctx.Done():
				markCollectionFailure(&podJobs[index][0], "UNAVAILABLE", normalizeMessage(ctx.Err().Error()))
				return
			}
			podJobs[index] = queryWorker(ctx, opts, deps, podJobs[index][0])
		}()
	}
	wait.Wait()

	jobs := make([]jobResult, 0, len(pods.Items))
	for _, results := range podJobs {
		jobs = append(jobs, results...)
	}
	for i := range jobs {
		if jobs[i].parsed == nil {
			continue
		}
		jobs[i].JobID = jobs[i].parsed.JobID
		jobs[i].VolumeID = jobs[i].parsed.VolumeID
		jobs[i].Phase = jobs[i].parsed.Phase
		if jobs[i].Batch == missingBatch {
			markCollectionFailure(&jobs[i], "UNAVAILABLE", "Pod is missing "+instanceLabel)
			continue
		}
		jobs[i].DisplayStatus = deriveJobStatus(*jobs[i].parsed)
	}
	markDuplicates(jobs)
	sortJobs(jobs)
	batches := summarizeBatches(jobs)
	partial := false
	for i := range jobs {
		partial = partial || jobs[i].collectionFailed
	}
	return jobs, batches, partial, nil
}

func getPodsArgs(opts options) []string {
	args := kubectlGlobalArgs(opts)
	args = append(args, "get", "pods")
	if opts.namespace != "" {
		args = append(args, "--namespace", opts.namespace)
	} else if opts.allNamespaces {
		args = append(args, "--all-namespaces")
	}
	selector := fixedSelector
	if opts.batch != "" {
		selector += "," + instanceLabel + "=" + opts.batch
	}
	return append(args, "--selector", selector, "--output", "json")
}

func execWorkerArgs(opts options, job jobResult) []string {
	args := kubectlGlobalArgs(opts)
	return append(args,
		"exec", "--namespace", job.Namespace, job.Pod,
		"--container", workerContainer, "--",
		workerBinary, "status", "--output", "json",
	)
}

func kubectlGlobalArgs(opts options) []string {
	args := make([]string, 0, 4)
	if opts.kubeconfig != "" {
		args = append(args, "--kubeconfig", opts.kubeconfig)
	}
	if opts.contextName != "" {
		args = append(args, "--context", opts.contextName)
	}
	return args
}

func queryWorker(parent context.Context, opts options, deps dependencies, base jobResult) []jobResult {
	ctx, cancel := context.WithTimeout(parent, deps.execTimeout)
	defer cancel()
	result := deps.run(ctx, "kubectl", execWorkerArgs(opts, base)...)
	if result.err != nil {
		markCollectionFailure(&base, "UNAVAILABLE", commandError(result, ctx.Err()))
		return []jobResult{base}
	}
	_, statuses, err := decodeWorkerStatus(result.stdout)
	if err != nil {
		markCollectionFailure(&base, "UNAVAILABLE", "invalid Worker status: "+err.Error())
		return []jobResult{base}
	}
	jobs := make([]jobResult, 0, len(statuses))
	for i := range statuses {
		job := base
		job.parsed = &statuses[i].status
		job.Worker = statuses[i].raw
		jobs = append(jobs, job)
	}
	return jobs
}

type decodedWorkerStatus struct {
	status workerStatus
	raw    json.RawMessage
}

func decodeWorkerStatus(body []byte) (workerStatusEnvelope, []decodedWorkerStatus, error) {
	trimmed := bytes.TrimSpace(body)
	var fields map[string]json.RawMessage
	if err := decodeOneJSON(trimmed, &fields); err != nil {
		return workerStatusEnvelope{}, nil, err
	}
	for _, name := range []string{"volume_id", "node_name", "ebs_root", "jobs"} {
		if _, ok := fields[name]; !ok {
			return workerStatusEnvelope{}, nil, fmt.Errorf("missing %s", name)
		}
	}
	var envelope workerStatusEnvelope
	if err := json.Unmarshal(trimmed, &envelope); err != nil {
		return workerStatusEnvelope{}, nil, err
	}
	if envelope.VolumeID == "" || envelope.NodeName == "" || envelope.EBSRoot == "" || len(envelope.Jobs) == 0 {
		return workerStatusEnvelope{}, nil, errors.New("invalid EBS status identity")
	}
	decoded := make([]decodedWorkerStatus, 0, len(envelope.Jobs))
	for _, raw := range envelope.Jobs {
		status, err := decodeJobStatus(raw)
		if err != nil {
			return workerStatusEnvelope{}, nil, err
		}
		if status.VolumeID != envelope.VolumeID || status.NodeName != envelope.NodeName {
			return workerStatusEnvelope{}, nil, fmt.Errorf("job %q EBS identity mismatch", status.JobID)
		}
		decoded = append(decoded, decodedWorkerStatus{status: status, raw: append(json.RawMessage(nil), raw...)})
	}
	return envelope, decoded, nil
}

func decodeJobStatus(raw json.RawMessage) (workerStatus, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return workerStatus{}, err
	}
	for _, name := range []string{"job_id", "volume_id", "node_name", "runtime_state", "startup_phase"} {
		if _, ok := fields[name]; !ok {
			return workerStatus{}, fmt.Errorf("missing Jobs[].%s", name)
		}
	}
	var status workerStatus
	if err := json.Unmarshal(raw, &status); err != nil {
		return workerStatus{}, err
	}
	if status.JobID == "" || status.VolumeID == "" {
		return workerStatus{}, errors.New("empty Job identity")
	}
	if status.RuntimeState != "INITIALIZING" && status.RuntimeState != "RETRYING" && status.RuntimeState != "RUNNING" && status.RuntimeState != "STOPPED" {
		return workerStatus{}, fmt.Errorf("invalid runtime_state %q", status.RuntimeState)
	}
	if !validPhase(status.StartupPhase) {
		return workerStatus{}, fmt.Errorf("invalid startup_phase %q", status.StartupPhase)
	}
	if status.RuntimeState != "RUNNING" {
		return status, nil
	}
	for _, name := range []string{"phase", "conditions", "fence_intent", "fence_complete"} {
		if _, ok := fields[name]; !ok {
			return workerStatus{}, fmt.Errorf("missing Jobs[].%s", name)
		}
	}
	for _, name := range []string{"fence_intent", "fence_complete"} {
		if err := validateBooleanField(fields, name, "Jobs[]."+name); err != nil {
			return workerStatus{}, err
		}
	}
	var conditionFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["conditions"], &conditionFields); err != nil {
		return workerStatus{}, fmt.Errorf("decode conditions: %w", err)
	}
	for _, name := range []string{"ready_for_rollout", "current_converged", "attention"} {
		if err := validateBooleanField(conditionFields, name, "Jobs[].conditions."+name); err != nil {
			return workerStatus{}, err
		}
	}
	if !validPhase(status.Phase) {
		return workerStatus{}, fmt.Errorf("invalid phase %q", status.Phase)
	}
	return status, nil
}

func validateBooleanField(fields map[string]json.RawMessage, name, path string) error {
	raw, ok := fields[name]
	if !ok {
		return fmt.Errorf("missing %s", path)
	}
	var value *bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	if value == nil {
		return fmt.Errorf("%s must be a boolean", path)
	}
	return nil
}

func decodeOneJSON(body []byte, destination any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return errors.New("trailing JSON data")
		}
		return err
	}
	return nil
}

func validPhase(phase string) bool {
	return phase == "SYNCING" || phase == "DUAL_WRITE_REPAIRING" || phase == "CUTOVER_READY"
}

func hasContainer(item pod, name string) bool {
	for _, container := range item.Spec.Containers {
		if container.Name == name {
			return true
		}
	}
	return false
}

func deriveJobStatus(status workerStatus) string {
	switch {
	case status.RuntimeState == "RETRYING" || status.RuntimeState == "STOPPED":
		return "ATTENTION"
	case status.RuntimeState == "INITIALIZING":
		return "INITIALIZING"
	case status.Conditions.Attention:
		return "ATTENTION"
	case status.Phase == "CUTOVER_READY" && status.FenceComplete:
		return "CUTOVER_READY"
	case status.Phase == "CUTOVER_READY":
		return "ATTENTION"
	case status.Conditions.ReadyForRollout:
		return "READY_FOR_ROLLOUT"
	case status.Conditions.CurrentConverged:
		return "CONVERGED"
	case status.Phase == "DUAL_WRITE_REPAIRING":
		return "REPAIRING"
	default:
		return "SYNCING"
	}
}

func markDuplicates(jobs []jobResult) {
	indexes := make(map[string][]int)
	for i := range jobs {
		if jobs[i].parsed == nil || jobs[i].Batch == missingBatch {
			continue
		}
		key := jobs[i].Namespace + "\x00" + jobs[i].Batch + "\x00" + jobs[i].JobID
		indexes[key] = append(indexes[key], i)
	}
	for _, duplicates := range indexes {
		if len(duplicates) < 2 {
			continue
		}
		for _, index := range duplicates {
			markCollectionFailure(&jobs[index], "DUPLICATE", "multiple Pods report this Job identity")
		}
	}
}

func summarizeBatches(jobs []jobResult) []batchSummary {
	grouped := make(map[string][]jobResult)
	for _, job := range jobs {
		key := job.Namespace + "\x00" + job.Batch
		grouped[key] = append(grouped[key], job)
	}
	summaries := make([]batchSummary, 0, len(grouped))
	for _, group := range grouped {
		summary := batchSummary{
			Namespace:    group[0].Namespace,
			Batch:        group[0].Batch,
			ObservedJobs: len(group),
			Status:       deriveBatchStatus(group),
		}
		for _, job := range group {
			if !job.collectionFailed {
				summary.Available++
			} else {
				summary.Unavailable++
			}
			if job.DisplayStatus == "ATTENTION" {
				summary.Attention++
			}
		}
		summaries = append(summaries, summary)
	}
	sort.Slice(summaries, func(i, j int) bool {
		if summaries[i].Namespace != summaries[j].Namespace {
			return summaries[i].Namespace < summaries[j].Namespace
		}
		return summaries[i].Batch < summaries[j].Batch
	})
	return summaries
}

func deriveBatchStatus(jobs []jobResult) string {
	phases := make(map[string]struct{})
	initializing := false
	for _, job := range jobs {
		switch job.DisplayStatus {
		case "UNAVAILABLE", "DUPLICATE", "ATTENTION", "TERMINATING":
			return "NEEDS_ATTENTION"
		}
		if strings.HasPrefix(job.DisplayStatus, "POD_") || job.Batch == missingBatch || job.parsed == nil {
			return "NEEDS_ATTENTION"
		}
		if job.DisplayStatus == "INITIALIZING" {
			initializing = true
			continue
		}
		phases[job.Phase] = struct{}{}
	}
	if len(phases) > 1 {
		return "MIXED_PHASE"
	}
	if initializing {
		return "SYNCING"
	}
	if len(phases) != 1 {
		return "MIXED_PHASE"
	}
	for phase := range phases {
		switch phase {
		case "CUTOVER_READY":
			return "CUTOVER_READY"
		case "DUAL_WRITE_REPAIRING":
			for _, job := range jobs {
				if job.DisplayStatus != "CONVERGED" {
					return "REPAIRING"
				}
			}
			return "CONVERGED"
		case "SYNCING":
			for _, job := range jobs {
				if job.DisplayStatus != "READY_FOR_ROLLOUT" {
					return "SYNCING"
				}
			}
			return "READY_FOR_ROLLOUT"
		default:
			return "NEEDS_ATTENTION"
		}
	}
	return "NEEDS_ATTENTION"
}

func sortJobs(jobs []jobResult) {
	sort.SliceStable(jobs, func(i, j int) bool {
		left, right := jobs[i], jobs[j]
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		if left.Batch != right.Batch {
			return left.Batch < right.Batch
		}
		if left.JobID != right.JobID {
			return left.JobID < right.JobID
		}
		return left.Pod < right.Pod
	})
}

func markCollectionFailure(job *jobResult, status, message string) {
	job.DisplayStatus = status
	job.collectionFailed = true
	message = normalizeMessage(message)
	if job.Error == "" {
		job.Error = message
	} else if message != "" && !strings.Contains(job.Error, message) {
		job.Error = normalizeMessage(job.Error + "; " + message)
	}
}

func commandError(result commandResult, contextErr error) string {
	if contextErr != nil {
		return normalizeMessage(contextErr.Error())
	}
	if message := normalizeMessage(string(result.stderr)); message != "" {
		return message
	}
	if result.err != nil {
		return normalizeMessage(result.err.Error())
	}
	return "kubectl command failed"
}

func normalizeMessage(message string) string {
	message = strings.Join(strings.Fields(message), " ")
	runes := []rune(message)
	if len(runes) > maxErrorLength {
		message = string(runes[:maxErrorLength])
	}
	return message
}

func emptyAsUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
