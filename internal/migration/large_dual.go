package migration

import (
	"context"
	"errors"
	"time"
)

func (w *Worker) largeDualRepair(ctx context.Context, metadata generationMetadata, summary generationRoundSummary, started time.Time) error {
	stage, exists := metadata.Stages[stageDiff]
	if !exists || !stage.Complete {
		return ErrGenerationIncomplete
	}
	sorter, err := newExternalSorter(w.generation, externalSortConfig{
		GenerationID: metadata.GenerationID, Stage: stageDiff, Kind: recordDiff, IDPrefix: "dual-path",
		MaxBufferBytes: largeSortBufferBytes, FanIn: largeSortFanIn, Budget: w.memoryBudget,
	})
	if err != nil {
		return err
	}
	pathSorted, err := sorter.Sort(ctx, &pathKeyedDiffReader{input: &sortRunReader{
		ctx: ctx, store: w.generation, generationID: metadata.GenerationID, chunks: stage.Chunks,
	}})
	if err != nil {
		return err
	}
	reader := &sortRunReader{ctx: ctx, store: w.generation, generationID: metadata.GenerationID, chunks: pathSorted.Chunks}
	var group []generationRecord
	currentPath := ""
	var mismatches int64
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
		mismatch, err := w.repairLargeDualPath(ctx, group)
		if mismatch {
			mismatches++
		}
		group = nil
		return err
	}
	for {
		record, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		path := record.Diff.Path
		if currentPath != "" && path != currentPath {
			if err := flush(); err != nil {
				return err
			}
		}
		currentPath = path
		group = append(group, record)
	}
	if err := flush(); err != nil {
		return err
	}
	summary.StartedAt = started
	summary.PendingCount = mismatches
	summary.Converged = false
	w.state.setPendingRepairs(saturatingInt(mismatches))
	w.state.SetAttention(false)
	return w.state.PublishGeneration(summary)
}

type pathKeyedDiffReader struct {
	input generationRecordReader
}

func (r *pathKeyedDiffReader) Next() (generationRecord, bool, error) {
	for {
		record, ok, err := r.input.Next()
		if err != nil || !ok {
			return record, ok, err
		}
		if record.Diff == nil || record.Diff.Path == "" {
			continue
		}
		record.Key = record.Diff.Path + "\x00" + record.Key
		return record, true, nil
	}
}

func (w *Worker) repairLargeDualPath(ctx context.Context, records []generationRecord) (bool, error) {
	var source *sourceGenerationRecord
	var target *targetGenerationRecord
	var linkPlan *diffGenerationRecord
	mismatch, safe := false, true
	for _, record := range records {
		diff := record.Diff
		if diff == nil {
			continue
		}
		if diff.Source != nil {
			source = diff.Source
		}
		if diff.Target != nil {
			target = diff.Target
		}
		if diff.Operation == "link-0-primary" || diff.Operation == "link-1-alias" {
			linkPlan = diff
		}
		if diff.Severity != SeverityBlocker || diff.Finding == "" {
			continue
		}
		mismatch = true
		switch diff.Finding {
		case FindingSourceOnly:
			safe = safe && (diff.Source == nil || diff.Source.Kind != EntryRegular || diff.Source.Mode&0o777 == 0o644)
		case FindingContent:
			safe = safe && diff.Source != nil && diff.Source.Kind == EntryRegular && diff.Source.ChecksumSHA256 != ""
		default:
			safe = false
		}
	}
	if !mismatch {
		if source == nil {
			for _, record := range records {
				if record.Diff != nil && record.Diff.Finding == FindingTargetOnly {
					w.state.clearPath(record.Diff.Path)
				}
			}
		}
		return false, nil
	}
	if !safe || source == nil {
		w.state.SetAttention(true)
		return true, ErrUnsafeApply
	}
	entry := source.sourceEntry()
	observedAt := w.clock()
	candidate := w.state.trackGrace(entry.Path, entry.Version, observedAt)
	if observedAt.Before(candidate.FirstSeen.Add(w.graceWindow())) {
		return true, nil
	}
	if linkPlan != nil && linkPlan.Operation == "link-1-alias" && linkPlan.PrimaryTarget == nil {
		return true, nil
	}
	if err := w.validateRoundBoundary(ctx); err != nil {
		return true, err
	}
	sourceWork := map[string]SourceEntry{entry.Path: entry}
	manifest := map[string]SourceEntry{entry.Path: entry}
	targets := make(map[string]TargetEntry)
	if target != nil {
		targets[target.Path] = target.targetEntry()
	}
	if linkPlan != nil && linkPlan.PrimarySource != nil {
		primary := linkPlan.PrimarySource.sourceEntry()
		manifest[primary.Path] = primary
		if linkPlan.PrimaryTarget != nil {
			targets[linkPlan.PrimaryTarget.Path] = linkPlan.PrimaryTarget.targetEntry()
		}
	}
	apply := w.largeDualApply
	if apply == nil {
		if w.apply == nil {
			return true, ErrIllegalAction
		}
		apply = w.apply.ApplyWithManifest
	}
	if err := apply(ctx, sourceWork, manifest, targets); err != nil {
		if errors.Is(err, ErrUnsafeApply) || (!retryableWorkerError(err) && !isAuthError(err)) {
			w.state.SetAttention(true)
			return true, err
		}
		w.state.queueRetry(entry.Path, entry.Version, classifyRetry(err), w.clock())
		return true, err
	}
	w.state.MarkReconciled(entry.Path, entry.Version)
	return true, nil
}

func (r targetGenerationRecord) targetEntry() TargetEntry {
	entry := TargetEntry{
		Path: r.Path, Kind: r.Kind, Size: r.Size, ResourceID: r.ResourceID,
		Nlink: r.Nlink,
	}
	if r.Mode != nil {
		entry.Mode, entry.HasMode = *r.Mode, true
	}
	if r.Revision != nil {
		entry.Revision = *r.Revision
	}
	if r.ChecksumSHA256 != nil {
		entry.ChecksumSHA256 = *r.ChecksumSHA256
	}
	return entry
}
