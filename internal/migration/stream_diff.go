package migration

import (
	"context"
	"fmt"
	"strings"
	"time"
)

type streamDiffConfig struct {
	GenerationID    string
	RoundID         string
	Mode            RoundMode
	Phase           Phase
	Identity        generationIdentity
	SortBufferBytes int64
	SortFanIn       int
	Budget          *memoryBudget
}

type streamDiffResult struct {
	Metadata generationMetadata
	Summary  generationRoundSummary
}

type streamDiffBuilder struct {
	store  *generationStore
	config streamDiffConfig
	clock  func() time.Time
}

func newStreamDiffBuilder(store *generationStore, config streamDiffConfig) (*streamDiffBuilder, error) {
	if store == nil {
		return nil, fmt.Errorf("stream diff requires generation store")
	}
	if validateGenerationIdentifier(config.GenerationID) != nil || validateGenerationIdentifier(config.RoundID) != nil {
		return nil, fmt.Errorf("stream diff generation or round ID is invalid")
	}
	if config.Mode == "" {
		config.Mode = RoundModeFull
	}
	if config.Mode != RoundModeDeep && config.Mode != RoundModeFull && config.Mode != RoundModeFast && config.Mode != RoundModeVerification {
		return nil, fmt.Errorf("stream diff mode is invalid")
	}
	if config.Phase != PhaseSyncing && config.Phase != PhaseDualWriteRepairing && config.Phase != PhaseCutoverReady {
		return nil, fmt.Errorf("stream diff phase is invalid")
	}
	if validateGenerationIdentity(config.Identity) != nil || config.Identity.JobID != store.jobID {
		return nil, fmt.Errorf("stream diff identity is invalid")
	}
	if config.SortBufferBytes <= 0 || config.SortFanIn < 2 {
		return nil, fmt.Errorf("stream diff sort limits are invalid")
	}
	return &streamDiffBuilder{store: store, config: config, clock: time.Now}, nil
}

func (b *streamDiffBuilder) Build(ctx context.Context, sourceGenerationID, targetGenerationID string) (streamDiffResult, error) {
	sourceMetadata, err := b.store.LoadComplete(ctx, sourceGenerationID, b.config.Identity)
	if err != nil {
		return streamDiffResult{}, err
	}
	targetMetadata, err := b.store.LoadComplete(ctx, targetGenerationID, b.config.Identity)
	if err != nil {
		return streamDiffResult{}, err
	}
	sourceStage, sourceOK := sourceMetadata.Stages[stageSource]
	targetStage, targetOK := targetMetadata.Stages[stageTarget]
	if !sourceOK || !targetOK || !sourceStage.Complete || !targetStage.Complete {
		return streamDiffResult{}, ErrGenerationIncomplete
	}

	diffCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	queueSize := max(2, b.config.SortFanIn*2)
	basicInput := make(chan generationRecord, queueSize)
	resourceInput := make(chan generationRecord, queueSize)
	sourceLinkInput := make(chan generationRecord, queueSize)
	basicOutcome := b.startSort(diffCtx, cancel, basicInput, "diff-basic")
	resourceOutcome := b.startSort(diffCtx, cancel, resourceInput, "links-resource")
	sourceLinkOutcome := b.startSort(diffCtx, cancel, sourceLinkInput, "links-source")
	sourceReader := &sortRunReader{ctx: diffCtx, store: b.store, generationID: sourceGenerationID, chunks: sourceStage.Chunks}
	targetReader := &sortRunReader{ctx: diffCtx, store: b.store, generationID: targetGenerationID, chunks: targetStage.Chunks}
	mergeErr := b.mergePaths(diffCtx, sourceReader, targetReader, basicInput, resourceInput, sourceLinkInput)
	if mergeErr == nil {
		if findings, exists := sourceMetadata.Stages[stageDiff]; exists && findings.Complete {
			reader := &sortRunReader{ctx: diffCtx, store: b.store, generationID: sourceGenerationID, chunks: findings.Chunks}
			for {
				record, ok, err := reader.Next()
				if err != nil {
					mergeErr = err
					break
				}
				if !ok {
					break
				}
				record.Key = "source\x00" + record.Key
				if err := sendGenerationRecord(diffCtx, basicInput, record); err != nil {
					mergeErr = err
					break
				}
			}
		}
	}
	close(basicInput)
	close(resourceInput)
	close(sourceLinkInput)
	basicSorted := <-basicOutcome
	resourceSorted := <-resourceOutcome
	sourceLinksSorted := <-sourceLinkOutcome
	if mergeErr != nil {
		return streamDiffResult{}, mergeErr
	}
	if err := firstSortError(basicSorted.err, resourceSorted.err, sourceLinksSorted.err); err != nil {
		return streamDiffResult{}, err
	}

	ownerInput := make(chan generationRecord, queueSize)
	linkFindingInput := make(chan generationRecord, queueSize)
	linkPlanInput := make(chan generationRecord, queueSize)
	ownerOutcome := b.startSort(diffCtx, cancel, ownerInput, "links-owner")
	linkFindingOutcome := b.startSort(diffCtx, cancel, linkFindingInput, "links-findings")
	linkPlanOutcome := b.startSort(diffCtx, cancel, linkPlanInput, "links-plan")
	if err := b.processResourceGroups(diffCtx, resourceSorted.result.Chunks, ownerInput, linkFindingInput); err != nil {
		close(ownerInput)
		close(linkFindingInput)
		close(linkPlanInput)
		<-ownerOutcome
		<-linkFindingOutcome
		<-linkPlanOutcome
		return streamDiffResult{}, err
	}
	close(ownerInput)
	ownerSorted := <-ownerOutcome
	if ownerSorted.err != nil {
		close(linkFindingInput)
		close(linkPlanInput)
		<-linkFindingOutcome
		<-linkPlanOutcome
		return streamDiffResult{}, ownerSorted.err
	}
	if err := b.processOwnerGroups(diffCtx, ownerSorted.result.Chunks, linkFindingInput); err != nil {
		close(linkFindingInput)
		close(linkPlanInput)
		<-linkFindingOutcome
		<-linkPlanOutcome
		return streamDiffResult{}, err
	}
	if err := b.processSourceLinkGroups(diffCtx, sourceLinksSorted.result.Chunks, linkPlanInput); err != nil {
		close(linkFindingInput)
		close(linkPlanInput)
		<-linkFindingOutcome
		<-linkPlanOutcome
		return streamDiffResult{}, err
	}
	close(linkPlanInput)
	linkPlan := <-linkPlanOutcome
	if linkPlan.err != nil {
		close(linkFindingInput)
		<-linkFindingOutcome
		return streamDiffResult{}, linkPlan.err
	}
	close(linkFindingInput)
	linkFindings := <-linkFindingOutcome
	if linkFindings.err != nil {
		return streamDiffResult{}, linkFindings.err
	}

	allFindings := append([]chunkDescriptor(nil), basicSorted.result.Chunks...)
	allFindings = append(allFindings, linkFindings.result.Chunks...)
	allFindings = append(allFindings, linkPlan.result.Chunks...)
	finalSorter, err := newExternalSorter(b.store, externalSortConfig{
		GenerationID: b.config.GenerationID, Stage: stageDiff, Kind: recordDiff, IDPrefix: "diff-final",
		MaxBufferBytes: b.config.SortBufferBytes, FanIn: b.config.SortFanIn, Budget: b.config.Budget,
	})
	if err != nil {
		return streamDiffResult{}, err
	}
	final, err := finalSorter.Sort(ctx, &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: allFindings})
	if err != nil {
		return streamDiffResult{}, err
	}
	if err := injectMigrationLargeStageFault("diff_sort"); err != nil {
		return streamDiffResult{}, err
	}
	findingCounts, workCounts, blockers, err := b.countFindings(ctx, final.Chunks)
	if err != nil {
		return streamDiffResult{}, err
	}
	completedAt := b.clock().UTC()
	metadata := generationMetadata{
		FormatVersion: generationFormatVersion, GenerationID: b.config.GenerationID, RoundID: b.config.RoundID,
		Phase: b.config.Phase, Identity: b.config.Identity,
		SourceGeneration: sourceGenerationID, TargetGeneration: targetGenerationID,
		EntryCount: sourceMetadata.EntryCount, TargetEntryCount: targetMetadata.EntryCount,
		FindingCounts: findingCounts, WorkCounts: workCounts, CreatedAt: completedAt,
		Stages: map[generationStage]generationStageMetadata{stageDiff: completedStage(final.Chunks)},
	}
	if _, err := b.store.SaveMetadata(ctx, metadata, 0); err != nil {
		return streamDiffResult{}, err
	}
	if err := injectMigrationLargeStageFault("diff_publish"); err != nil {
		return streamDiffResult{}, err
	}
	if err := b.store.PublishComplete(ctx, metadata); err != nil {
		return streamDiffResult{}, err
	}
	summary := generationRoundSummary{
		ID: b.config.RoundID, Mode: b.config.Mode, StartedAt: sourceMetadata.CreatedAt, CompletedAt: completedAt,
		SourceGenerationID: sourceGenerationID, TargetGenerationID: targetGenerationID, DiffGenerationID: b.config.GenerationID,
		SourceComplete: true, TargetComplete: true, DiffComplete: true,
		ScanComplete: true, Converged: blockers == 0, SourceCount: sourceMetadata.EntryCount,
		TargetCount: targetMetadata.EntryCount, FindingCounts: findingCounts, WorkCounts: workCounts, BlockerCount: blockers,
	}
	return streamDiffResult{Metadata: metadata, Summary: summary}, nil
}

func (b *streamDiffBuilder) startSort(ctx context.Context, cancel context.CancelFunc, input <-chan generationRecord, prefix string) <-chan sortOutcome {
	outcome := make(chan sortOutcome, 1)
	go func() {
		sorter, err := newExternalSorter(b.store, externalSortConfig{
			GenerationID: b.config.GenerationID, Stage: stageDiff, Kind: recordDiff, IDPrefix: prefix,
			MaxBufferBytes: b.config.SortBufferBytes, FanIn: b.config.SortFanIn, Budget: b.config.Budget,
		})
		var result externalSortResult
		if err == nil {
			result, err = sorter.Sort(ctx, channelGenerationReader{ctx: ctx, input: input})
		}
		outcome <- sortOutcome{result: result, err: err}
		if err != nil {
			cancel()
		}
	}()
	return outcome
}

func (b *streamDiffBuilder) mergePaths(ctx context.Context, source, target generationRecordReader, findings, links, sourceLinks chan<- generationRecord) error {
	sourceRecord, sourceOK, err := source.Next()
	if err != nil {
		return err
	}
	targetRecord, targetOK, err := target.Next()
	if err != nil {
		return err
	}
	for sourceOK || targetOK {
		switch {
		case sourceOK && (!targetOK || sourceRecord.Key < targetRecord.Key):
			if err := b.emitMissingSource(ctx, findings, sourceRecord.Source); err != nil {
				return err
			}
			if err := b.emitSourceLinkObservation(ctx, sourceLinks, sourceRecord.Source, nil); err != nil {
				return err
			}
			sourceRecord, sourceOK, err = source.Next()
		case targetOK && (!sourceOK || targetRecord.Key < sourceRecord.Key):
			if err := b.emitFinding(ctx, findings, FindingTargetOnly, "basic", targetRecord.Target.Path, nil, targetRecord.Target); err != nil {
				return err
			}
			if err := b.emitLinkObservation(ctx, links, nil, targetRecord.Target); err != nil {
				return err
			}
			targetRecord, targetOK, err = target.Next()
		default:
			if err := b.comparePath(ctx, findings, sourceRecord.Source, targetRecord.Target); err != nil {
				return err
			}
			if err := b.emitLinkObservation(ctx, links, sourceRecord.Source, targetRecord.Target); err != nil {
				return err
			}
			if err := b.emitSourceLinkObservation(ctx, sourceLinks, sourceRecord.Source, targetRecord.Target); err != nil {
				return err
			}
			sourceRecord, sourceOK, err = source.Next()
			if err == nil {
				targetRecord, targetOK, err = target.Next()
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (b *streamDiffBuilder) emitSourceLinkObservation(ctx context.Context, output chan<- generationRecord, source *sourceGenerationRecord, target *targetGenerationRecord) error {
	if source == nil || source.Kind != EntryRegular || source.HardlinkKey == "" {
		return nil
	}
	owner := sourceLinkOwner(source)
	return sendGenerationRecord(ctx, output, generationRecord{
		Key: owner + "\x00" + source.Path,
		Diff: &diffGenerationRecord{
			Path: source.Path, Operation: "source_link_observation", DependencyKey: owner,
			Source: source, Target: target,
		},
	})
}

func (b *streamDiffBuilder) emitMissingSource(ctx context.Context, output chan<- generationRecord, source *sourceGenerationRecord) error {
	if err := b.emitFinding(ctx, output, FindingSourceOnly, "basic", source.Path, source, nil); err != nil {
		return err
	}
	needsMode := source.Kind == EntryDirectory && source.Mode&0o777 != 0o755 ||
		source.Kind == EntryRegular && source.Mode&0o777 != 0o644 || source.Kind == EntrySymlink
	if needsMode {
		return b.emitFinding(ctx, output, FindingMetadata, "final-mode", source.Path, source, nil)
	}
	return nil
}

func (b *streamDiffBuilder) comparePath(ctx context.Context, output chan<- generationRecord, source *sourceGenerationRecord, target *targetGenerationRecord) error {
	if source == nil || target == nil {
		return fmt.Errorf("stream diff comparison lacks source or target")
	}
	if source.Kind != target.Kind {
		return b.emitFinding(ctx, output, FindingType, "basic", source.Path, source, target)
	}
	if !target.MetadataComplete || target.ResourceID == "" {
		if err := b.emitFinding(ctx, output, FindingIdentity, "metadata-complete", source.Path, source, target); err != nil {
			return err
		}
	}
	if target.Mode == nil || source.Mode&0o777 != *target.Mode&0o777 {
		if err := b.emitFinding(ctx, output, FindingMetadata, "mode", source.Path, source, target); err != nil {
			return err
		}
	}
	if source.Kind != EntryDirectory && (target.Revision == nil || *target.Revision <= 0) {
		if err := b.emitFinding(ctx, output, FindingRevision, "revision", source.Path, source, target); err != nil {
			return err
		}
	}
	if source.Kind != EntryDirectory && (source.Size != target.Size || target.ChecksumSHA256 == nil || source.ChecksumSHA256 != *target.ChecksumSHA256) {
		if err := b.emitFinding(ctx, output, FindingContent, "content", source.Path, source, target); err != nil {
			return err
		}
	}
	return nil
}

func (b *streamDiffBuilder) emitFinding(ctx context.Context, output chan<- generationRecord, kind FindingKind, reason, path string, source *sourceGenerationRecord, target *targetGenerationRecord) error {
	operation := diffOperation(kind, source)
	dependency := diffDependency(operation, reason, path, source, target)
	key := strings.Join([]string{operation, dependency, path, string(kind)}, "\x00")
	severity := SeverityBlocker
	if kind == FindingTargetOnly && b.config.Phase != PhaseSyncing {
		severity = SeverityWarning
	}
	return sendGenerationRecord(ctx, output, generationRecord{Key: key, Diff: &diffGenerationRecord{
		Path: path, Operation: operation, DependencyKey: dependency, Finding: kind, Severity: severity,
		Source: source, Target: target,
	}})
}

func diffDependency(operation, reason, path string, source *sourceGenerationRecord, target *targetGenerationRecord) string {
	switch operation {
	case "mkdir":
		return fmt.Sprintf("depth:%08d", pathDepth(path))
	case "chmod":
		if source != nil && source.Kind == EntryDirectory {
			return fmt.Sprintf("1-dir:%08d", 99999999-pathDepth(path))
		}
		return "0-file:" + path
	case "delete":
		if target != nil && target.Kind == EntryDirectory {
			return fmt.Sprintf("depth:%08d", 99999999-pathDepth(path))
		}
	}
	return reason
}

func (b *streamDiffBuilder) emitLinkObservation(ctx context.Context, output chan<- generationRecord, source *sourceGenerationRecord, target *targetGenerationRecord) error {
	if target == nil || target.Kind != EntryRegular || target.ResourceID == "" {
		return nil
	}
	if source != nil && source.Kind != EntryRegular {
		source = nil
	}
	key := target.ResourceID + "\x00" + target.Path
	return sendGenerationRecord(ctx, output, generationRecord{Key: key, Diff: &diffGenerationRecord{
		Path: target.Path, Operation: "link_observation", DependencyKey: target.ResourceID,
		Source: source, Target: target,
	}})
}

type linkResourceGroup struct {
	resourceID  string
	firstPath   string
	firstSource *sourceGenerationRecord
	firstTarget *targetGenerationRecord
	owner       string
	paths       uint32
	nlink       uint32
	safe        bool
}

func (b *streamDiffBuilder) processResourceGroups(ctx context.Context, chunks []chunkDescriptor, owners, findings chan<- generationRecord) error {
	reader := &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: chunks}
	var group linkResourceGroup
	flush := func() error {
		if group.resourceID == "" || group.safe && group.paths == group.nlink {
			group = linkResourceGroup{}
			return nil
		}
		err := b.emitFinding(ctx, findings, FindingIdentity, "resource:"+group.resourceID, group.firstPath, group.firstSource, group.firstTarget)
		group = linkResourceGroup{}
		return err
	}
	for {
		record, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			return flush()
		}
		observation := record.Diff
		if observation == nil || observation.Target == nil {
			return fmt.Errorf("resource observation is invalid")
		}
		resourceID := observation.Target.ResourceID
		if group.resourceID != "" && group.resourceID != resourceID {
			if err := flush(); err != nil {
				return err
			}
		}
		if group.resourceID == "" {
			group = linkResourceGroup{resourceID: resourceID, firstPath: observation.Path, firstSource: observation.Source, firstTarget: observation.Target, safe: true}
		}
		group.paths++
		if observation.Target.Nlink == 0 || group.nlink != 0 && group.nlink != observation.Target.Nlink {
			group.safe = false
		} else {
			group.nlink = observation.Target.Nlink
		}
		owner := sourceLinkOwner(observation.Source)
		if owner != "" {
			if group.owner != "" && group.owner != owner {
				group.safe = false
			} else {
				group.owner = owner
			}
			ownerRecord := *observation
			ownerRecord.DependencyKey = owner
			if err := sendGenerationRecord(ctx, owners, generationRecord{Key: owner + "\x00" + observation.Path, Diff: &ownerRecord}); err != nil {
				return err
			}
		}
	}
}

func (b *streamDiffBuilder) processOwnerGroups(ctx context.Context, chunks []chunkDescriptor, findings chan<- generationRecord) error {
	reader := &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: chunks}
	var owner, resource, firstPath string
	var firstSource *sourceGenerationRecord
	var firstTarget *targetGenerationRecord
	unsafe := false
	flush := func() error {
		if owner != "" && unsafe {
			return b.emitFinding(ctx, findings, FindingIdentity, "owner:"+owner, firstPath, firstSource, firstTarget)
		}
		return nil
	}
	for {
		record, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			return flush()
		}
		observation := record.Diff
		if observation == nil || observation.Source == nil || observation.Target == nil {
			return fmt.Errorf("owner observation is invalid")
		}
		currentOwner := sourceLinkOwner(observation.Source)
		if owner != "" && owner != currentOwner {
			if err := flush(); err != nil {
				return err
			}
			owner, resource, firstPath, firstSource, firstTarget, unsafe = "", "", "", nil, nil, false
		}
		if owner == "" {
			owner, resource, firstPath = currentOwner, observation.Target.ResourceID, observation.Path
			firstSource, firstTarget = observation.Source, observation.Target
		} else if resource != observation.Target.ResourceID {
			unsafe = true
		}
	}
}

func (b *streamDiffBuilder) processSourceLinkGroups(ctx context.Context, chunks []chunkDescriptor, plans chan<- generationRecord) error {
	reader := &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: chunks}
	owner, primary := "", ""
	var primarySource *sourceGenerationRecord
	var primaryTarget *targetGenerationRecord
	for {
		record, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		observation := record.Diff
		if observation == nil || observation.Source == nil {
			return fmt.Errorf("source link observation is invalid")
		}
		currentOwner := sourceLinkOwner(observation.Source)
		operation := "link-1-alias"
		if owner != currentOwner {
			owner, primary, operation = currentOwner, observation.Path, "link-0-primary"
			primarySource, primaryTarget = observation.Source, observation.Target
		}
		plan := *observation
		plan.Operation = operation
		plan.PrimaryPath = primary
		plan.PrimarySource = primarySource
		plan.PrimaryTarget = primaryTarget
		if err := sendGenerationRecord(ctx, plans, generationRecord{
			Key:  operation + "\x00" + owner + "\x00" + observation.Path,
			Diff: &plan,
		}); err != nil {
			return err
		}
	}
}

func sourceLinkOwner(source *sourceGenerationRecord) string {
	if source == nil {
		return ""
	}
	if source.HardlinkKey != "" {
		return "hardlink:" + source.HardlinkKey
	}
	return "path:" + source.Path
}

func diffOperation(kind FindingKind, source *sourceGenerationRecord) string {
	switch kind {
	case FindingSourceOnly:
		if source != nil && source.Kind == EntryDirectory {
			return "mkdir"
		}
		return "write"
	case FindingTargetOnly:
		return "delete"
	case FindingContent:
		return "write"
	case FindingMetadata:
		return "chmod"
	default:
		return "block"
	}
}

func (b *streamDiffBuilder) countFindings(ctx context.Context, chunks []chunkDescriptor) (map[FindingKind]int64, map[string]int64, int64, error) {
	counts := make(map[FindingKind]int64)
	workCounts := make(map[string]int64)
	var blockers int64
	reader := &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: chunks}
	for {
		record, ok, err := reader.Next()
		if err != nil {
			return nil, nil, 0, err
		}
		if !ok {
			return counts, workCounts, blockers, nil
		}
		if record.Diff == nil {
			continue
		}
		workCounts[record.Diff.Operation]++
		if record.Diff.Finding != "" {
			counts[record.Diff.Finding]++
			if record.Diff.Severity == SeverityBlocker {
				blockers++
			}
		}
	}
}
