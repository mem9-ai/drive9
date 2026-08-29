package migration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/pingcap/failpoint"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/logger"
)

const (
	largeSortBufferBytes      = 16 << 20
	largeSortFanIn            = 16
	largeManifestLimit        = 10000
	largeProgressLogEntries   = 100000
	largeProgressLogPages     = 100
	maxGenerationRecentErrors = 8
)

type largeObservation struct {
	source censusResult
	target manifestResult
	diff   streamDiffResult
}

func (w *Worker) largeScaleRound(ctx context.Context, mode RoundMode, id string, started time.Time) error {
	logger.Info(ctx, "migration large-scale round started", zap.String("phase", string(w.state.Snapshot().Phase)), zap.String("mode", string(mode)))
	if w.previousSource == nil {
		previous, err := w.generation.FindLatestCompleteSource(ctx, generationIdentityFromStartup(w.startup))
		if err != nil {
			w.state.FailRound(id, "recovery")
			return err
		}
		w.previousSource = previous
	}
	cacheStatus := "source_cache_miss"
	rebuildReason := "no_complete_source_generation"
	if w.previousSource != nil {
		cacheStatus = "source_cache_reused"
		rebuildReason = ""
	}
	w.setLargeProgress(GenerationStatus{Stage: "source", CacheStatus: cacheStatus, RebuildReason: rebuildReason})
	observation, err := w.buildLargeObservation(ctx, mode, id, "initial", w.previousSource)
	if err != nil {
		w.state.FailRound(id, "scan")
		return err
	}
	if err := injectMigrationLargeStageFault("prune_before"); err != nil {
		w.state.FailRound(id, "recovery")
		return err
	}
	w.pruneLargeObservation(ctx, observation)
	w.previousSource = &observation.source.Metadata
	w.state.setPendingRepairs(saturatingInt(observation.diff.Summary.BlockerCount))
	unsafe, err := w.largeDiffUnsafe(ctx, observation.diff.Metadata)
	if err != nil {
		w.state.FailRound(id, "inventory")
		return err
	}
	if unsafe {
		observation.diff.Summary.PendingCount = observation.diff.Summary.BlockerCount
		w.state.SetAttention(true)
		logger.Warn(ctx, "migration large-scale round blocked", zap.Int64("blockers", observation.diff.Summary.BlockerCount))
		return w.state.PublishGeneration(observation.diff.Summary)
	}
	if !observation.diff.Summary.Converged {
		if w.state.Snapshot().Phase == PhaseDualWriteRepairing {
			return w.largeDualRepair(ctx, observation.diff.Metadata, observation.diff.Summary, started)
		}
		if w.batchApply == nil || w.fileApply == nil {
			w.state.FailRound(id, "fenced")
			return ErrIllegalAction
		}
		if err := w.validateRoundBoundary(ctx); err != nil {
			w.state.FailRound(id, "identity")
			return err
		}
		w.state.beginOperation()
		err = w.applyLargeDiff(ctx, observation.diff.Metadata)
		w.state.endOperation()
		if err != nil {
			w.state.FailRound(id, "apply")
			return err
		}
		if err := injectMigrationLargeStageFault("post_apply_gate"); err != nil {
			w.state.FailRound(id, "reread")
			return err
		}
		observation, err = w.buildLargeObservation(ctx, mode, id, "post", &observation.source.Metadata)
		if err != nil {
			w.state.FailRound(id, "reread")
			return err
		}
		if err := injectMigrationLargeStageFault("prune_before"); err != nil {
			w.state.FailRound(id, "recovery")
			return err
		}
		w.pruneLargeObservation(ctx, observation)
		w.previousSource = &observation.source.Metadata
		w.state.setPendingRepairs(saturatingInt(observation.diff.Summary.BlockerCount))
		unsafe, err = w.largeDiffUnsafe(ctx, observation.diff.Metadata)
		if err != nil {
			w.state.FailRound(id, "reread")
			return err
		}
	}
	observation.diff.Summary.StartedAt = started
	if err := w.state.PublishGeneration(observation.diff.Summary); err != nil {
		return err
	}
	if observation.diff.Summary.Converged {
		w.state.SetInitialCopyComplete(true)
	}
	w.state.SetAttention(unsafe)
	logger.Info(ctx, "migration large-scale round completed",
		zap.Bool("converged", observation.diff.Summary.Converged),
		zap.Int64("source_entries", observation.diff.Summary.SourceCount),
		zap.Int64("target_entries", observation.diff.Summary.TargetCount),
		zap.Int64("blockers", observation.diff.Summary.BlockerCount))
	return nil
}

func (w *Worker) pruneLargeObservation(ctx context.Context, observation largeObservation) {
	if err := w.generation.PruneReplaced(ctx, observation.source.Metadata, observation.target.Metadata, observation.diff.Metadata); err != nil {
		w.updateLargeProgress(func(status *GenerationStatus) {
			if status.CacheStatus == "" {
				status.CacheStatus = "prune_failed"
			} else {
				status.CacheStatus += ",prune_failed"
			}
		})
		logger.Warn(ctx, "migration generation prune failed", zap.Error(err))
	}
}

func (w *Worker) buildLargeObservation(ctx context.Context, mode RoundMode, roundID, suffix string, previous *generationMetadata) (largeObservation, error) {
	if w.generation == nil || w.manifestAPI == nil || w.startup == nil {
		return largeObservation{}, fmt.Errorf("large-scale pipeline is not initialized")
	}
	identity := generationIdentityFromStartup(w.startup)
	performance := w.startup.Config.JobDefaults.Performance
	sourceID := w.largeGenerationID(roundID, suffix+"-source")
	targetID := w.largeGenerationID(roundID, suffix+"-target")
	diffID := w.largeGenerationID(roundID, suffix+"-diff")
	census, err := newCensusBuilder(w.scanner, w.generation, censusConfig{
		GenerationID: sourceID, RoundID: roundID, Phase: w.state.Snapshot().Phase, Identity: identity,
		HashWorkers: performance.SmallFileWorkers, SortBufferBytes: largeSortBufferBytes, SortFanIn: largeSortFanIn, Budget: w.memoryBudget,
	})
	if err != nil {
		return largeObservation{}, err
	}
	census.clock = w.clock
	census.onProgress = func(entries int64) {
		w.markLargeProgress(func(status *GenerationStatus) {
			status.Stage = "source"
			status.SourceCount = entries
		})
		if entries > 0 && entries%largeProgressLogEntries == 0 {
			logger.Info(ctx, "migration large-scale source progress", zap.Int64("entries", entries))
		}
	}
	source, err := census.Build(ctx, previous)
	if err != nil {
		return largeObservation{}, err
	}
	w.markLargeProgress(func(status *GenerationStatus) {
		status.Stage = "target"
		status.SourceGenerationID = sourceID
		status.SourceComplete = true
		status.SourceCount = source.Metadata.EntryCount
		status.HashReuseCount = source.Metadata.HashReuseCount
		status.HashNewCount = source.Metadata.HashNewCount
		copySourceProgress(status, source.Metadata)
		status.ArtifactBytes += generationArtifactBytes(source.Metadata)
		status.Stages = []string{"source"}
	})
	var targetResume *generationMetadata
	if suffix == "initial" && w.state.Snapshot().Phase == PhaseSyncing {
		targetResume, err = w.generation.FindResumableTarget(ctx, identity)
		if err != nil {
			return largeObservation{}, err
		}
		if targetResume != nil {
			targetID = targetResume.GenerationID
			w.updateLargeProgress(func(status *GenerationStatus) { status.CacheStatus += ",target_resumed" })
		}
	}
	manifest, err := newManifestBuilder(w.manifestAPI, w.generation, manifestConfig{
		GenerationID: targetID, RoundID: roundID, Phase: w.state.Snapshot().Phase, Identity: identity,
		TargetPrefix: w.startup.Job.Target.Prefix, PageLimit: largeManifestLimit,
		SortBufferBytes: largeSortBufferBytes, SortFanIn: largeSortFanIn, Budget: w.memoryBudget,
	})
	if err != nil {
		return largeObservation{}, err
	}
	manifest.clock = w.clock
	manifest.onProgress = func(metadata generationMetadata) {
		w.markLargeProgress(func(status *GenerationStatus) {
			status.Stage = "target"
			status.ManifestPages = metadata.ManifestPages
			status.ManifestCursor = metadata.ManifestCursor
			status.TargetCount = metadata.EntryCount
			copyManifestProgress(status, metadata)
		})
		if metadata.ManifestPages > 0 && metadata.ManifestPages%largeProgressLogPages == 0 {
			logger.Info(ctx, "migration large-scale manifest progress",
				zap.Int64("pages", metadata.ManifestPages), zap.Int64("entries", metadata.EntryCount))
		}
	}
	target, err := manifest.Build(ctx, targetResume)
	if err != nil {
		return largeObservation{}, err
	}
	w.markLargeProgress(func(status *GenerationStatus) {
		status.Stage = "diff"
		status.TargetGenerationID = targetID
		status.TargetComplete = true
		status.TargetCount = target.Metadata.EntryCount
		status.ManifestPages = target.Metadata.ManifestPages
		status.ManifestCursor = target.Metadata.ManifestCursor
		copyManifestProgress(status, target.Metadata)
		status.ArtifactBytes += generationArtifactBytes(target.Metadata)
		status.Stages = []string{"source", "target"}
	})
	diff, err := newStreamDiffBuilder(w.generation, streamDiffConfig{
		GenerationID: diffID, RoundID: roundID, Mode: mode, Phase: w.state.Snapshot().Phase, Identity: identity,
		SortBufferBytes: largeSortBufferBytes, SortFanIn: largeSortFanIn, Budget: w.memoryBudget,
	})
	if err != nil {
		return largeObservation{}, err
	}
	diff.clock = w.clock
	difference, err := diff.Build(ctx, source.Metadata.GenerationID, target.Metadata.GenerationID)
	if err != nil {
		return largeObservation{}, err
	}
	w.markLargeProgress(func(status *GenerationStatus) {
		status.Stage = "complete"
		status.DiffGenerationID = diffID
		status.DiffComplete = true
		status.BlockerCount = difference.Summary.BlockerCount
		status.PendingCount = difference.Summary.PendingCount
		status.ActiveCount = difference.Summary.ActiveCount
		status.UnknownCount = difference.Summary.UnknownCount
		status.FindingCounts = cloneMap(difference.Summary.FindingCounts)
		status.WorkCounts = cloneMap(difference.Summary.WorkCounts)
		status.ArtifactBytes += generationArtifactBytes(difference.Metadata)
		status.Stages = []string{"source", "target", "diff"}
	})
	return largeObservation{source: source, target: target, diff: difference}, nil
}

func copySourceProgress(status *GenerationStatus, metadata generationMetadata) {
	status.SourceDirectories = metadata.DirectoryCount
	status.SourceFiles = metadata.FileCount
	status.SourceLogicalBytes = metadata.LogicalBytes
	status.SourceWarnings = metadata.WarningCount
	status.SourceBlockers = metadata.BlockerCount
	status.SourceScanDurationMS = metadata.SourceScanDurationMS
	status.SourceHashDurationMS = metadata.SourceHashDurationMS
	status.SourceScanRate = perSecond(metadata.EntryCount, metadata.SourceScanDurationMS)
	status.SourceHashRate = perSecond(metadata.HashNewCount, metadata.SourceHashDurationMS)
	status.SourceQueueCapacity = metadata.SourceQueueCapacity
}

func perSecond(count, milliseconds int64) float64 {
	if count <= 0 || milliseconds <= 0 {
		return 0
	}
	return float64(count) * 1000 / float64(milliseconds)
}

func copyManifestProgress(status *GenerationStatus, metadata generationMetadata) {
	status.ManifestRawEntries = metadata.ManifestRawEntries
	status.ManifestResponseBytes = metadata.ManifestResponseBytes
	status.ManifestEmptyPages = metadata.ManifestEmptyPages
	status.ManifestCursorAdvances = metadata.ManifestCursorAdvances
	status.ManifestSortRuns = metadata.ManifestSortRuns
	status.ManifestLastPageAt = metadata.ManifestLastPageAt
}

func (w *Worker) largeGenerationID(roundID, suffix string) string {
	nonce := w.generationNonce
	if nonce == "" {
		nonce = "local"
	}
	return nonce + "-" + roundID + "-" + suffix
}

func (w *Worker) setLargeProgress(status GenerationStatus) {
	now := w.clock().UTC()
	if status.LastProgressAt.IsZero() {
		status.LastProgressAt = now
	}
	status.LastStatusAt = now
	if w.fileApply != nil {
		status.InlineWorkers = w.fileApply.inlineLimit.Current()
		status.MultipartWorkers = w.fileApply.multipartLimit.Current()
	}
	if w.memoryBudget != nil {
		status.MemoryUsedBytes, status.MemoryPeakBytes, status.MemoryLimitBytes = w.memoryBudget.Snapshot()
	}
	copy := cloneGenerationStatus(status)
	w.largeProgress.Store(&copy)
}

func (w *Worker) updateLargeProgress(update func(*GenerationStatus)) {
	var status GenerationStatus
	if current := w.largeProgress.Load(); current != nil {
		status = cloneGenerationStatus(*current)
	}
	update(&status)
	w.setLargeProgress(status)
}

func (w *Worker) markLargeProgress(update func(*GenerationStatus)) {
	w.updateLargeProgress(func(status *GenerationStatus) {
		update(status)
		status.LastProgressAt = w.clock().UTC()
	})
}

func cloneGenerationStatus(status GenerationStatus) GenerationStatus {
	status.Stages = append([]string(nil), status.Stages...)
	status.FindingCounts = cloneMap(status.FindingCounts)
	status.WorkCounts = cloneMap(status.WorkCounts)
	status.RecentErrors = append([]GenerationRecentError(nil), status.RecentErrors...)
	return status
}

func (w *Worker) recordLargeError(err error) {
	if err == nil {
		return
	}
	w.updateLargeProgress(func(status *GenerationStatus) {
		status.RecentErrors = append(status.RecentErrors, GenerationRecentError{
			Stage: status.Stage, Class: classifyLargeError(err), At: w.clock().UTC(),
		})
		if len(status.RecentErrors) > maxGenerationRecentErrors {
			status.RecentErrors = append([]GenerationRecentError(nil), status.RecentErrors[len(status.RecentErrors)-maxGenerationRecentErrors:]...)
		}
	})
}

func injectMigrationLargeStageFault(boundary string) error {
	var injected error
	failpoint.Inject("migrationLargeStageFault", func(value failpoint.Value) {
		if selected, ok := value.(string); ok && selected == boundary {
			injected = fmt.Errorf("injected migration large-scale %s crash", boundary)
		}
	})
	return injected
}

func classifyLargeError(err error) string {
	switch {
	case errors.Is(err, ErrApplyRescan):
		return "apply_rescan"
	case errors.Is(err, ErrSourceChanged):
		return "source_changed"
	case errors.Is(err, ErrGenerationIncomplete):
		return "generation_incomplete"
	case errors.Is(err, ErrGenerationInvalid), errors.Is(err, ErrGenerationMismatch):
		return "generation_invalid"
	case errors.Is(err, ErrMemoryBudgetExceeded):
		return "memory_budget"
	case isAuthError(err):
		return "auth"
	case retryableWorkerError(err):
		return "retryable"
	default:
		return "fatal"
	}
}

func generationArtifactBytes(metadata generationMetadata) int64 {
	var bytes int64
	for _, stage := range metadata.Stages {
		for _, chunk := range stage.Chunks {
			bytes += chunk.PayloadBytes
		}
	}
	return bytes
}

func (w *Worker) applyLargeDiff(ctx context.Context, metadata generationMetadata) error {
	read := func() generationRecordReader {
		return &sortRunReader{
			ctx: ctx, store: w.generation, generationID: metadata.GenerationID,
			chunks: metadata.Stages[stageDiff].Chunks,
		}
	}
	for _, stage := range []struct {
		name  string
		apply func(context.Context, generationRecordReader) (applyStageResult, error)
	}{
		{name: "mkdir", apply: w.batchApply.ApplyDirectories},
		{name: "files", apply: w.fileApply.ApplyFiles},
		{name: "links", apply: w.fileApply.ApplyLinks},
		{name: "modes", apply: w.batchApply.ApplyModes},
		{name: "delete", apply: w.batchApply.ApplyDeletes},
	} {
		logger.Info(ctx, "migration large-scale apply stage started", zap.String("stage", stage.name))
		w.markLargeProgress(func(status *GenerationStatus) {
			status.Stage = "apply_" + stage.name
			status.ApplyInFlight = 1
		})
		result, err := stage.apply(ctx, read())
		if err == nil {
			err = injectMigrationLargeStageFault("apply_" + stage.name + "_after")
		}
		w.updateLargeProgress(func(status *GenerationStatus) {
			status.ApplyInFlight = 0
			status.ApplyTotal += result.Total
			status.ApplyVerified += result.Verified
			status.ApplyPending += result.Pending
			status.ApplyUnknown += result.Unknown
			status.ApplyRetry += result.Pending + result.Unknown
			if err != nil {
				status.ApplyFailed++
			}
		})
		logger.Info(ctx, "migration large-scale apply stage completed",
			zap.String("stage", stage.name), zap.Int64("verified", result.Verified),
			zap.Int64("pending", result.Pending), zap.Int64("unknown", result.Unknown))
		if err != nil {
			return err
		}
		if result.Pending > 0 || result.Unknown > 0 {
			return ErrApplyRescan
		}
	}
	return nil
}

func (w *Worker) largeDiffUnsafe(ctx context.Context, metadata generationMetadata) (bool, error) {
	reader := &sortRunReader{
		ctx: ctx, store: w.generation, generationID: metadata.GenerationID,
		chunks: metadata.Stages[stageDiff].Chunks,
	}
	for {
		record, ok, err := reader.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
		if record.Diff == nil || record.Diff.Severity != SeverityBlocker {
			continue
		}
		switch record.Diff.Finding {
		case FindingSourceOnly, FindingTargetOnly, FindingContent, FindingMetadata,
			FindingSparseFile, FindingSymlinkTarget, FindingModeBits:
		default:
			return true, nil
		}
	}
}

func generationIdentityFromStartup(startup *Startup) generationIdentity {
	return generationIdentity{
		JobID: startup.Job.JobID, ConfigHash: startup.ConfigHash, VolumeID: startup.Job.VolumeID,
		EBSRoot: startup.Job.EBSRoot, SourceSubpath: startup.Job.Subpath, SourceRoot: startup.Job.Source.Root,
		Endpoint: startup.Config.Drive9.Endpoint, SpaceRef: startup.Job.Target.SpaceRef, Prefix: startup.Job.Target.Prefix,
	}
}

func saturatingInt(value int64) int {
	if value > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(value)
}
