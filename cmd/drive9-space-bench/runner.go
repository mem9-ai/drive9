package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

func runBenchmark(
	ctx context.Context,
	cfg benchConfig,
	client httpDoer,
	progress io.Writer,
	errorOutput io.Writer,
) (benchmarkReport, error) {
	report := benchmarkReport{
		SchemaVersion: benchmarkReportSchema,
		StartedAt:     time.Now().UTC(),
		Config:        safeReportConfig(cfg),
		Spaces: spaceSummary{
			Requested: cfg.SpaceCount,
		},
	}
	finishWithError := func(err error) (benchmarkReport, error) {
		redacted := redactError(err, cfg.PublicKey, cfg.PrivateKey)
		report.FinishedAt = time.Now().UTC()
		report.Error = redacted.Error()
		return report, redacted
	}

	state, _, err := loadSpaceState(cfg.SpacesFile, cfg.Server)
	if err != nil {
		return finishWithError(err)
	}
	initialCount := len(state.Spaces)
	state, err = ensureSpaceCount(
		ctx,
		cfg,
		state,
		client,
		time.Now,
		progress,
	)
	report.Spaces.Configured = len(state.Spaces)
	report.Spaces.Reused = min(initialCount, cfg.SpaceCount)
	report.Spaces.Provisioned = max(0, len(state.Spaces)-initialCount)
	if err != nil {
		return finishWithError(err)
	}
	if len(state.Spaces) < cfg.SpaceCount {
		return finishWithError(fmt.Errorf(
			"space credential file contains %d spaces; require %d",
			len(state.Spaces),
			cfg.SpaceCount,
		))
	}
	selected := append([]spaceCredential(nil), state.Spaces[:cfg.SpaceCount]...)
	if err := waitForAllSpacesReady(ctx, cfg, selected, client, progress); err != nil {
		return finishWithError(err)
	}
	report.Spaces.Ready = len(selected)

	workloadCtx := ctx
	cancel := func() {}
	if cfg.Duration > 0 {
		workloadCtx, cancel = context.WithTimeout(ctx, cfg.Duration)
	}
	defer cancel()
	run, err := runWorkload(
		workloadCtx,
		cfg,
		selected,
		client,
		progress,
		errorOutput,
	)
	report.Workload = run
	report.FinishedAt = time.Now().UTC()
	switch {
	case cfg.Duration > 0 &&
		errors.Is(workloadCtx.Err(), context.DeadlineExceeded) &&
		ctx.Err() == nil:
		report.StopReason = "duration"
	case ctx.Err() != nil:
		report.StopReason = "signal"
	default:
		report.StopReason = "completed"
	}
	if err != nil {
		return finishWithError(err)
	}
	if workloadFailed(run.Stats) {
		return finishWithError(fmt.Errorf(
			"workload completed with write_errors=%d read_errors=%d verification_errors=%d",
			run.Stats.WriteErrors,
			run.Stats.ReadErrors,
			run.Stats.VerificationErrors,
		))
	}
	return report, nil
}
