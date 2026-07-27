package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const workloadErrorBackoff = 100 * time.Millisecond

type workloadRun struct {
	StartedAt  time.Time             `json:"started_at"`
	FinishedAt time.Time             `json:"finished_at"`
	Stats      workloadStatsSnapshot `json:"stats"`
}

func runWorkload(
	ctx context.Context,
	cfg benchConfig,
	spaces []spaceCredential,
	client httpDoer,
	progress io.Writer,
	errorOutput io.Writer,
) (workloadRun, error) {
	if len(spaces) == 0 {
		return workloadRun{}, fmt.Errorf("no spaces selected")
	}
	if progress == nil {
		progress = io.Discard
	}
	if errorOutput == nil {
		errorOutput = io.Discard
	}
	started := time.Now()
	startedAt := started.UTC()
	stats := newWorkloadStats()
	limiter := newPacedLimiter(cfg.IORPS)
	activationLimiter := newPacedLimiter(cfg.SpaceStartRPS)
	reportedErrors := workloadErrorReportState{}

	var (
		workerWG     sync.WaitGroup
		activeSpaces atomic.Int64
	)
	activationDone := make(chan struct{})
	go func() {
		defer close(activationDone)
		for spaceIndex, space := range spaces {
			if ctx.Err() != nil {
				return
			}
			if err := activationLimiter.Wait(ctx); err != nil {
				return
			}
			activeSpaces.Add(1)
			for workerIndex := range cfg.WorkersPerSpace {
				workerWG.Add(1)
				go func(spaceIndex, workerIndex int, space spaceCredential) {
					defer workerWG.Done()
					runSpaceWorker(
						ctx,
						cfg,
						client,
						limiter,
						stats,
						spaceIndex,
						workerIndex,
						space,
					)
				}(spaceIndex, workerIndex, space)
			}
		}
	}()
	done := make(chan struct{})
	go func() {
		<-activationDone
		workerWG.Wait()
		close(done)
	}()

	ticker := time.NewTicker(cfg.ReportInterval)
	defer ticker.Stop()
	finish := func() workloadRun {
		finishedAt := time.Now().UTC()
		snapshot := stats.Snapshot()
		printWorkloadErrorDeltas(
			errorOutput,
			finishedAt,
			time.Since(started),
			snapshot,
			&reportedErrors,
		)
		return workloadRun{
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Stats:      snapshot,
		}
	}
	for {
		select {
		case <-done:
			return finish(), nil
		case <-ticker.C:
			now := time.Now()
			snapshot := stats.Snapshot()
			printWorkloadProgress(
				progress,
				now.Sub(started),
				len(spaces),
				int(activeSpaces.Load()),
				cfg.WorkersPerSpace,
				snapshot,
			)
			printWorkloadErrorDeltas(
				errorOutput,
				now.UTC(),
				now.Sub(started),
				snapshot,
				&reportedErrors,
			)
		case <-ctx.Done():
			<-done
			return finish(), nil
		}
	}
}

func runSpaceWorker(
	ctx context.Context,
	cfg benchConfig,
	client httpDoer,
	limiter *pacedLimiter,
	stats *workloadStats,
	spaceIndex, workerIndex int,
	space spaceCredential,
) {
	var sequence uint64
	var completedCycles uint64
	for {
		if ctx.Err() != nil {
			return
		}
		slot := sequence % uint64(cfg.FilesPerWorker)
		remotePath := fmt.Sprintf(
			"/bench/drive9-space-bench/worker-%d/file-%d.bin",
			workerIndex,
			slot,
		)
		payload := makePayload(cfg.FileSize, space.TenantID, spaceIndex, workerIndex, sequence)
		sequence++

		if err := limiter.Wait(ctx); err != nil {
			return
		}
		started := time.Now()
		err := putWorkloadFile(ctx, cfg, client, space.APIKey, remotePath, payload)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			stats.setLastWriteError(newWorkloadErrorSample(
				cfg,
				space,
				workerIndex,
				remotePath,
				err,
			))
		}
		stats.recordWrite(time.Since(started), len(payload), err)
		if err != nil {
			if !waitWorkloadBackoff(ctx) {
				return
			}
			continue
		}

		if err := limiter.Wait(ctx); err != nil {
			return
		}
		started = time.Now()
		got, err := getWorkloadFile(ctx, cfg, client, space.APIKey, remotePath, len(payload))
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			stats.setLastReadError(newWorkloadErrorSample(
				cfg,
				space,
				workerIndex,
				remotePath,
				err,
			))
		}
		stats.recordRead(time.Since(started), len(got), err)
		if err != nil {
			if !waitWorkloadBackoff(ctx) {
				return
			}
			continue
		}
		if !bytes.Equal(got, payload) {
			stats.recordVerificationError()
			continue
		}
		completedCycles++
		if cfg.DeleteEvery == 0 ||
			completedCycles%uint64(cfg.DeleteEvery) != 0 {
			continue
		}

		if err := limiter.Wait(ctx); err != nil {
			return
		}
		started = time.Now()
		err = deleteWorkloadFile(ctx, cfg, client, space.APIKey, remotePath)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			stats.setLastDeleteError(newWorkloadErrorSample(
				cfg,
				space,
				workerIndex,
				remotePath,
				err,
			))
		}
		stats.recordDelete(time.Since(started), err)
		if err != nil && !waitWorkloadBackoff(ctx) {
			return
		}
	}
}

func newWorkloadErrorSample(
	cfg benchConfig,
	space spaceCredential,
	workerIndex int,
	remotePath string,
	err error,
) workloadErrorSample {
	sanitized := redactError(
		err,
		cfg.PublicKey,
		cfg.PrivateKey,
		space.APIKey,
	)
	return workloadErrorSample{
		TenantID:    space.TenantID,
		WorkerIndex: workerIndex,
		RemotePath:  remotePath,
		Message:     sanitized.Error(),
	}
}

func makePayload(
	size int,
	tenantID string,
	spaceIndex, workerIndex int,
	sequence uint64,
) []byte {
	seed := sha256.Sum256([]byte(
		tenantID + "/" +
			strconv.Itoa(spaceIndex) + "/" +
			strconv.Itoa(workerIndex) + "/" +
			strconv.FormatUint(sequence, 10),
	))
	payload := make([]byte, size)
	for index := range payload {
		payload[index] = seed[index%len(seed)] ^ byte(index) ^ byte(sequence)
	}
	return payload
}

func putWorkloadFile(
	parent context.Context,
	cfg benchConfig,
	client httpDoer,
	apiKey, remotePath string,
	payload []byte,
) error {
	ctx, cancel := context.WithTimeout(parent, cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		cfg.Server+"/v1/fs"+remotePath,
		bytes.NewReader(payload),
	)
	if err != nil {
		return fmt.Errorf("create write request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := client.Do(req)
	if err != nil {
		return redactError(fmt.Errorf("write request: %w", err), apiKey)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := readResponseBody(resp.Body)
	if err != nil {
		return fmt.Errorf("read write response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return responseError("write", resp.StatusCode, body, apiKey)
	}
	return nil
}

func getWorkloadFile(
	parent context.Context,
	cfg benchConfig,
	client httpDoer,
	apiKey, remotePath string,
	expectedSize int,
) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		cfg.Server+"/v1/fs"+remotePath,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("create read request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return nil, redactError(fmt.Errorf("read request: %w", err), apiKey)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, readErr := readResponseBody(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("read error response: %w", readErr)
		}
		return nil, responseError("read", resp.StatusCode, body, apiKey)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, int64(expectedSize)+1))
	if err != nil {
		return nil, fmt.Errorf("read file response: %w", err)
	}
	if len(body) != expectedSize {
		return body, fmt.Errorf(
			"read returned %d bytes; expected %d",
			len(body),
			expectedSize,
		)
	}
	return body, nil
}

func deleteWorkloadFile(
	parent context.Context,
	cfg benchConfig,
	client httpDoer,
	apiKey, remotePath string,
) error {
	ctx, cancel := context.WithTimeout(parent, cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		cfg.Server+"/v1/fs"+remotePath,
		nil,
	)
	if err != nil {
		return fmt.Errorf("create delete request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := client.Do(req)
	if err != nil {
		return redactError(fmt.Errorf("delete request: %w", err), apiKey)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := readResponseBody(resp.Body)
	if err != nil {
		return fmt.Errorf("read delete response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return responseError("delete", resp.StatusCode, body, apiKey)
	}
	return nil
}

func waitWorkloadBackoff(ctx context.Context) bool {
	timer := time.NewTimer(workloadErrorBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func printWorkloadProgress(
	w io.Writer,
	elapsed time.Duration,
	spaces, activeSpaces, workersPerSpace int,
	stats workloadStatsSnapshot,
) {
	requests := stats.WriteRequests + stats.ReadRequests + stats.DeleteRequests
	opsPerSecond := 0.0
	if elapsed > 0 {
		opsPerSecond = float64(requests) / elapsed.Seconds()
	}
	_, _ = fmt.Fprintf(
		w,
		"drive9 space bench: elapsed=%s spaces=%d active_spaces=%d/%d workers=%d "+
			"write=%d/%d read=%d/%d delete=%d/%d "+
			"verify_errors=%d ops_per_second=%.2f\n",
		elapsed.Round(time.Second),
		spaces,
		activeSpaces,
		spaces,
		spaces*workersPerSpace,
		stats.WriteSuccess,
		stats.WriteErrors,
		stats.ReadSuccess,
		stats.ReadErrors,
		stats.DeleteSuccess,
		stats.DeleteErrors,
		stats.VerificationErrors,
		opsPerSecond,
	)
}

type workloadErrorReportState struct {
	writeErrors  uint64
	readErrors   uint64
	deleteErrors uint64
}

func printWorkloadErrorDeltas(
	w io.Writer,
	now time.Time,
	elapsed time.Duration,
	stats workloadStatsSnapshot,
	reported *workloadErrorReportState,
) {
	if stats.WriteErrors > reported.writeErrors {
		printWorkloadErrorDelta(
			w,
			now,
			elapsed,
			"write",
			stats.WriteErrors-reported.writeErrors,
			stats.WriteErrors,
			stats.lastWriteError,
		)
	}
	if stats.ReadErrors > reported.readErrors {
		printWorkloadErrorDelta(
			w,
			now,
			elapsed,
			"read",
			stats.ReadErrors-reported.readErrors,
			stats.ReadErrors,
			stats.lastReadError,
		)
	}
	if stats.DeleteErrors > reported.deleteErrors {
		printWorkloadErrorDelta(
			w,
			now,
			elapsed,
			"delete",
			stats.DeleteErrors-reported.deleteErrors,
			stats.DeleteErrors,
			stats.lastDeleteError,
		)
	}
	reported.writeErrors = stats.WriteErrors
	reported.readErrors = stats.ReadErrors
	reported.deleteErrors = stats.DeleteErrors
}

func printWorkloadErrorDelta(
	w io.Writer,
	now time.Time,
	elapsed time.Duration,
	operation string,
	newErrors, totalErrors uint64,
	sample *workloadErrorSample,
) {
	_, _ = fmt.Fprintf(
		w,
		"drive9 space bench error: time=%s elapsed=%s operation=%s "+
			"new_errors=%d total_errors=%d",
		now.Format(time.RFC3339Nano),
		elapsed.Round(time.Second),
		operation,
		newErrors,
		totalErrors,
	)
	if sample != nil {
		_, _ = fmt.Fprintf(
			w,
			" tenant=%q worker=%d path=%q error=%q",
			sample.TenantID,
			sample.WorkerIndex,
			sample.RemotePath,
			sample.Message,
		)
	}
	_, _ = fmt.Fprintln(w)
}

func workloadFailed(stats workloadStatsSnapshot) bool {
	return stats.WriteErrors > 0 ||
		stats.ReadErrors > 0 ||
		stats.DeleteErrors > 0 ||
		stats.VerificationErrors > 0
}
