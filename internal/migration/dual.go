package migration

import (
	"context"
	"errors"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

func (w *Worker) dualRound(ctx context.Context, mode RoundMode, id string, started time.Time) error {
	source, target, candidates, err := w.dualObservation(ctx, mode)
	if err != nil {
		w.state.FailRound(id, "scan")
		return err
	}
	w.state.clearAbsentWork(source.Entries)
	round, err := BuildRound(id, mode, started, source, target)
	if err != nil {
		w.state.FailRound(id, "inventory")
		return err
	}
	normalizeDualFindings(&round)
	w.state.setPendingRepairs(repairFindingCount(round))
	observedAt := w.clock()
	expired, unsafe := make(map[string]SourceEntry), false
	for path := range candidates {
		entry, exists := source.Entries[path]
		if !exists {
			continue
		}
		mismatch, safe := dualMismatch(round, entry)
		if !mismatch {
			w.state.MarkReconciled(path, entry.Version)
			continue
		}
		if !safe {
			unsafe = true
			continue
		}
		candidate := w.state.trackGrace(path, entry.Version, observedAt)
		if !observedAt.Before(candidate.FirstSeen.Add(w.graceWindow())) {
			expired[path] = entry
		}
	}
	if len(expired) > 0 {
		if err := w.validateRoundBoundary(ctx); err != nil {
			w.state.FailRound(id, "identity")
			return err
		}
		if err := w.apply.ApplyWithManifest(ctx, expired, source.Entries, target.Entries); err != nil {
			if errors.Is(err, ErrUnsafeApply) || (!retryableWorkerError(err) && !isAuthError(err)) {
				w.state.SetAttention(true)
				w.state.FailRound(id, "apply")
				return err
			} else {
				for path, entry := range expired {
					w.state.queueRetry(path, entry.Version, classifyRetry(err), w.clock())
				}
				_ = w.state.PublishRound(round)
				return err
			}
		} else {
			for path, entry := range expired {
				if err := w.refreshDualTarget(ctx, path, entry, target.Entries); err != nil {
					w.state.queueRetry(path, entry.Version, "target_reread", w.clock())
					_ = w.state.PublishRound(round)
					return err
				}
				w.state.MarkReconciled(path, entry.Version)
			}
			round, err = BuildRound(id, mode, started, source, target)
			if err != nil {
				return err
			}
			normalizeDualFindings(&round)
			w.state.setPendingRepairs(repairFindingCount(round))
		}
	}
	for _, finding := range round.Findings {
		if finding.Severity == SeverityBlocker {
			if _, selected := candidates[finding.Path]; !selected {
				unsafe = true
			}
		}
	}
	w.state.SetAttention(unsafe)
	return w.state.PublishRound(round)
}

func (w *Worker) dualObservation(ctx context.Context, mode RoundMode) (ScanResult, TargetScan, map[string]struct{}, error) {
	if mode == RoundModeDeep || mode == RoundModeVerification {
		source, target, err := w.scanPair(ctx)
		return source, target, allSourcePaths(source), err
	}
	source, err := w.scanSource(ctx)
	if err != nil {
		return source, TargetScan{}, nil, err
	}
	snapshot := w.state.Snapshot()
	if snapshot.LastComplete != nil {
		for path, entry := range source.Entries {
			prior, exists := snapshot.LastComplete.Source[path]
			if exists && prior.Version == entry.Version {
				entry.ChecksumSHA256 = prior.ChecksumSHA256
				source.Entries[path] = entry
			}
		}
	}
	candidates, counts := dualCandidates(source, snapshot)
	w.state.setCandidateCounts(counts)
	for path, entry := range source.Entries {
		if _, selected := candidates[path]; selected && entry.Kind == EntryRegular {
			deep, readErr := w.scanner.ReadStableEntry(ctx, entry)
			if readErr != nil {
				return source, TargetScan{}, nil, readErr
			}
			entry.ChecksumSHA256 = deep.ChecksumSHA256
			source.Entries[path] = entry
		}
	}
	target := TargetScan{Complete: true, Entries: make(map[string]TargetEntry)}
	if snapshot.LastComplete != nil {
		target.Entries = cloneMap(snapshot.LastComplete.Target)
	}
	for path := range candidates {
		entry, exists := source.Entries[path]
		if !exists {
			entry = SourceEntry{Kind: target.Entries[path].Kind}
		}
		if err := w.refreshDualTarget(ctx, path, entry, target.Entries); err != nil {
			return source, TargetScan{}, nil, err
		}
	}
	return source, target, candidates, nil
}

func dualCandidates(source ScanResult, snapshot StateSnapshot) (map[string]struct{}, CandidateCounts) {
	result := make(map[string]struct{})
	var counts CandidateCounts
	for path, entry := range source.Entries {
		selected := false
		if snapshot.RepairMtimeFloor == nil || entry.Version.MtimeNS >= snapshot.RepairMtimeFloor.UnixNano() {
			counts.Mtime++
			selected = true
		}
		prior, reconciled := snapshot.Reconciled[path]
		if !reconciled {
			counts.NewPath++
			selected = true
		} else if prior != entry.Version {
			counts.SourceTokenChanged++
			selected = true
		}
		if selected {
			result[path] = struct{}{}
		}
	}
	for _, candidate := range snapshot.Grace {
		result[candidate.Path] = struct{}{}
	}
	for _, retry := range snapshot.Retry {
		result[retry.Path] = struct{}{}
	}
	for path := range source.Entries {
		if _, selected := result[path]; !selected {
			counts.Filtered++
		}
	}
	return result, counts
}

func allSourcePaths(source ScanResult) map[string]struct{} {
	result := make(map[string]struct{}, len(source.Entries))
	for path := range source.Entries {
		result[path] = struct{}{}
	}
	return result
}

func (w *Worker) refreshDualTarget(ctx context.Context, path string, source SourceEntry, entries map[string]TargetEntry) error {
	directory := source.Kind == EntryDirectory
	if observed, exists := entries[path]; exists {
		directory = observed.Kind == EntryDirectory
	}
	stat, err := w.api.StatCtx(ctx, targetRemotePath(w.inventory.prefix, path, directory))
	if client.IsNotFound(err) {
		delete(entries, path)
		return nil
	}
	if err != nil {
		return err
	}
	kind := targetKind(client.BatchStatResult{IsDir: stat.IsDir, Mode: stat.Mode, HasMode: stat.HasMode})
	entry := TargetEntry{Path: path, Kind: kind, Size: stat.Size, Mode: stat.Mode & 0o777, HasMode: stat.HasMode, Revision: stat.Revision, ResourceID: stat.ResourceID, Nlink: stat.Nlink, ChecksumSHA256: stat.ChecksumSHA256}
	for existingPath, existing := range entries {
		if existingPath != path && existing.Kind == EntryRegular && existing.ResourceID == entry.ResourceID {
			existing.Nlink = entry.Nlink
			entries[existingPath] = existing
		}
	}
	entries[path] = entry
	return nil
}

func normalizeDualFindings(round *Round) {
	round.Converged = true
	for i := range round.Findings {
		if round.Findings[i].Kind == FindingTargetOnly {
			round.Findings[i].Severity = SeverityWarning
		}
		if round.Findings[i].Severity == SeverityBlocker {
			round.Converged = false
		}
	}
}

func dualMismatch(round Round, source SourceEntry) (mismatch, safe bool) {
	safe = true
	for _, finding := range round.Findings {
		if finding.Path != source.Path || finding.Severity != SeverityBlocker {
			continue
		}
		mismatch = true
		switch finding.Kind {
		case FindingSourceOnly:
			safe = safe && (source.Kind != EntryRegular || source.Mode&0o777 == 0o644)
		case FindingContent:
			safe = safe && source.Kind == EntryRegular && source.ChecksumSHA256 != ""
		default:
			safe = false
		}
	}
	return mismatch, safe
}

func (w *Worker) graceWindow() time.Duration {
	if w.gracePeriod > 0 {
		return w.gracePeriod
	}
	return DefaultGracePeriod
}

func classifyRetry(err error) string {
	if errors.Is(err, ErrApplyRescan) {
		return "cas_conflict"
	}
	return "operation"
}
