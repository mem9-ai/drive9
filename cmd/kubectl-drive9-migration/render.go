package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
)

func renderStatus(output io.Writer, opts options, generatedAt time.Time, jobs []jobResult, batches []batchSummary) error {
	if opts.output == "json" {
		encoder := json.NewEncoder(output)
		encoder.SetIndent("", "  ")
		return encoder.Encode(aggregateOutput{
			GeneratedAt: generatedAt,
			Scope: queryScope{
				Namespace:     opts.namespace,
				AllNamespaces: opts.allNamespaces,
				Batch:         opts.batch,
				Context:       opts.contextName,
			},
			Batches: batches,
			Jobs:    jobs,
		})
	}
	return renderTable(output, opts.output == "wide", jobs, batches)
}

func renderTable(output io.Writer, wide bool, jobs []jobResult, batches []batchSummary) error {
	table := tabwriter.NewWriter(output, 0, 4, 2, ' ', 0)
	showBatch := shouldShowBatch(jobs)
	if wide {
		if _, err := fmt.Fprintln(table, "NAMESPACE\tBATCH\tJOB\tPHASE\tSTATUS\tROUND\tFILES\tDIFF\tRETRY\tVERIFY\tNODE\tPOD\tSPACE\tPREFIX\tCAND_MTIME\tCAND_SOURCE_TOKEN_CHANGED\tCAND_NEW_PATH\tCAND_FILTERED\tPENDING\tIN_FLIGHT\tERROR"); err != nil {
			return err
		}
		for _, job := range jobs {
			status := job.parsed
			candidateMtime, candidateSourceToken, candidateNewPath, candidateFiltered := candidateCounts(status)
			if _, err := fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				valueOrDash(job.Namespace), valueOrDash(job.Batch), valueOrDash(job.JobID), valueOrDash(job.Phase),
				job.DisplayStatus, roundDisplay(status), sourceCount(status), findingCount(status), retryCount(status),
				verificationDisplay(status), valueOrDash(job.Node), valueOrDash(job.Pod), spaceRef(status), prefix(status),
				candidateMtime, candidateSourceToken, candidateNewPath, candidateFiltered,
				pendingRepairs(status), inFlight(status), valueOrDash(job.Error)); err != nil {
				return err
			}
		}
	} else {
		headers := make([]string, 0, 10)
		if showBatch {
			headers = append(headers, "BATCH")
		}
		headers = append(headers, "JOB", "PHASE", "STATUS", "ROUND", "FILES", "DIFF", "RETRY", "VERIFY", "POD")
		if _, err := fmt.Fprintln(table, strings.Join(headers, "\t")); err != nil {
			return err
		}
		for _, job := range jobs {
			fields := make([]string, 0, 10)
			if showBatch {
				fields = append(fields, valueOrDash(job.Batch))
			}
			status := job.parsed
			fields = append(fields,
				valueOrDash(job.JobID), valueOrDash(job.Phase), job.DisplayStatus,
				roundDisplay(status), sourceCount(status), findingCount(status), retryCount(status),
				verificationDisplay(status), valueOrDash(job.Pod),
			)
			if _, err := fmt.Fprintln(table, strings.Join(fields, "\t")); err != nil {
				return err
			}
		}
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if len(batches) == 0 {
		return nil
	}
	if _, err := fmt.Fprintln(output); err != nil {
		return err
	}
	for _, summary := range batches {
		name := summary.Batch
		if summary.Namespace != "" {
			name = summary.Namespace + "/" + name
		}
		if _, err := fmt.Fprintf(output,
			"Batch %s: %s - observed %d jobs, %d available, %d attention, %d unavailable\n",
			name, summary.Status, summary.ObservedJobs, summary.Available, summary.Attention, summary.Unavailable,
		); err != nil {
			return err
		}
	}
	return nil
}

func shouldShowBatch(jobs []jobResult) bool {
	batches := make(map[string]struct{})
	for _, job := range jobs {
		batches[job.Namespace+"\x00"+job.Batch] = struct{}{}
	}
	return len(batches) != 1 || (len(jobs) > 0 && jobs[0].Batch == missingBatch)
}

func roundDisplay(status *workerStatus) string {
	if status == nil || status.Round.Mode == "" {
		return "-"
	}
	suffix := ""
	switch {
	case status.Round.FailureClass != "":
		suffix = "/failed"
	case !status.Round.CompletedAt.IsZero() && status.Round.Converged:
		suffix = "/ok"
	case !status.Round.CompletedAt.IsZero():
		suffix = "/complete"
	case !status.Round.StartedAt.IsZero() || status.Round.ID != "":
		suffix = "/running"
	}
	return string(status.Round.Mode) + suffix
}

func sourceCount(status *workerStatus) string {
	if status == nil {
		return "-"
	}
	return strconv.Itoa(status.SourceCount)
}

func findingCount(status *workerStatus) string {
	if status == nil {
		return "-"
	}
	total := 0
	for _, count := range status.FindingCounts {
		total += count
	}
	return strconv.Itoa(total)
}

func retryCount(status *workerStatus) string {
	if status == nil {
		return "-"
	}
	return strconv.Itoa(status.Backlog)
}

func verificationDisplay(status *workerStatus) string {
	if status == nil || status.Verification.Status == "" {
		return "-"
	}
	return strings.ToUpper(status.Verification.Status)
}

func spaceRef(status *workerStatus) string {
	if status == nil {
		return "-"
	}
	return valueOrDash(status.SpaceRef)
}

func prefix(status *workerStatus) string {
	if status == nil {
		return "-"
	}
	return valueOrDash(status.Prefix)
}

func candidateCounts(status *workerStatus) (string, string, string, string) {
	if status == nil {
		return "-", "-", "-", "-"
	}
	return strconv.Itoa(status.CandidateCounts.Mtime),
		strconv.Itoa(status.CandidateCounts.SourceTokenChanged),
		strconv.Itoa(status.CandidateCounts.NewPath),
		strconv.Itoa(status.CandidateCounts.Filtered)
}

func pendingRepairs(status *workerStatus) string {
	if status == nil {
		return "-"
	}
	return strconv.Itoa(status.PendingRepairs)
}

func inFlight(status *workerStatus) string {
	if status == nil {
		return "-"
	}
	return strconv.Itoa(status.InFlight)
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}
