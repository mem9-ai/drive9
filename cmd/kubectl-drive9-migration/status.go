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

	jobs := make([]jobResult, len(pods.Items))
	execIndexes := make([]int, 0, len(pods.Items))
	for i := range pods.Items {
		item := &pods.Items[i]
		batch := item.Metadata.Labels[instanceLabel]
		if batch == "" {
			batch = missingBatch
		}
		jobs[i] = jobResult{
			Namespace: item.Metadata.Namespace,
			Batch:     batch,
			Pod:       item.Metadata.Name,
			Node:      item.Spec.NodeName,
			PodPhase:  item.Status.Phase,
		}
		switch {
		case item.Metadata.Name == "" || item.Metadata.Namespace == "":
			markCollectionFailure(&jobs[i], "UNAVAILABLE", "Pod identity is incomplete")
		case item.Metadata.DeletionTimestamp != nil:
			markCollectionFailure(&jobs[i], "TERMINATING", "Pod is terminating")
		case item.Status.Phase != "Running":
			phase := strings.ToUpper(item.Status.Phase)
			if phase == "" {
				phase = "UNKNOWN"
			}
			markCollectionFailure(&jobs[i], "POD_"+phase, "Pod phase is "+emptyAsUnknown(item.Status.Phase))
		case !hasContainer(*item, workerContainer):
			markCollectionFailure(&jobs[i], "UNAVAILABLE", "Pod has no drive9-migration container")
		default:
			execIndexes = append(execIndexes, i)
		}
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
				markCollectionFailure(&jobs[index], "UNAVAILABLE", normalizeMessage(ctx.Err().Error()))
				return
			}
			queryWorker(ctx, opts, deps, &jobs[index])
		}()
	}
	wait.Wait()

	for i := range jobs {
		if jobs[i].parsed == nil {
			continue
		}
		jobs[i].JobID = jobs[i].parsed.VolumeID
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

func queryWorker(parent context.Context, opts options, deps dependencies, job *jobResult) {
	ctx, cancel := context.WithTimeout(parent, deps.execTimeout)
	defer cancel()
	result := deps.run(ctx, "kubectl", execWorkerArgs(opts, *job)...)
	if result.err != nil {
		markCollectionFailure(job, "UNAVAILABLE", commandError(result, ctx.Err()))
		return
	}
	status, raw, err := decodeWorkerStatus(result.stdout)
	if err != nil {
		markCollectionFailure(job, "UNAVAILABLE", "invalid Worker status: "+err.Error())
		return
	}
	job.parsed = &status
	job.Worker = raw
}

func decodeWorkerStatus(body []byte) (workerStatus, json.RawMessage, error) {
	trimmed := bytes.TrimSpace(body)
	var fields map[string]json.RawMessage
	if err := decodeOneJSON(trimmed, &fields); err != nil {
		return workerStatus{}, nil, err
	}
	for _, name := range []string{"volume_id", "phase", "startup_phase", "conditions", "fence_intent", "fence_complete"} {
		if _, ok := fields[name]; !ok {
			return workerStatus{}, nil, fmt.Errorf("missing %s", name)
		}
	}
	for _, name := range []string{"fence_intent", "fence_complete"} {
		if err := validateBooleanField(fields, name, name); err != nil {
			return workerStatus{}, nil, err
		}
	}
	var conditionFields map[string]json.RawMessage
	if err := json.Unmarshal(fields["conditions"], &conditionFields); err != nil {
		return workerStatus{}, nil, fmt.Errorf("decode conditions: %w", err)
	}
	for _, name := range []string{"ready_for_rollout", "current_converged", "attention"} {
		if err := validateBooleanField(conditionFields, name, "conditions."+name); err != nil {
			return workerStatus{}, nil, err
		}
	}
	var status workerStatus
	if err := json.Unmarshal(trimmed, &status); err != nil {
		return workerStatus{}, nil, err
	}
	if status.VolumeID == "" {
		return workerStatus{}, nil, errors.New("empty volume_id")
	}
	if !validPhase(status.Phase) {
		return workerStatus{}, nil, fmt.Errorf("invalid phase %q", status.Phase)
	}
	if !validPhase(status.StartupPhase) {
		return workerStatus{}, nil, fmt.Errorf("invalid startup_phase %q", status.StartupPhase)
	}
	raw := append(json.RawMessage(nil), trimmed...)
	return status, raw, nil
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
	for _, job := range jobs {
		switch job.DisplayStatus {
		case "UNAVAILABLE", "DUPLICATE", "ATTENTION", "TERMINATING":
			return "NEEDS_ATTENTION"
		}
		if strings.HasPrefix(job.DisplayStatus, "POD_") || job.Batch == missingBatch || job.parsed == nil {
			return "NEEDS_ATTENTION"
		}
		phases[job.Phase] = struct{}{}
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
