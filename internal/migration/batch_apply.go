package migration

import (
	"context"
	"fmt"
	"sync"

	"github.com/mem9-ai/drive9/pkg/client"
)

type batchApplyClient interface {
	BatchMkdirCtx(context.Context, []client.BatchMkdirItem) ([]client.BatchMkdirResult, error)
	BatchChmodCtx(context.Context, []client.BatchChmodItem) ([]client.BatchChmodResult, error)
	BatchStatWithOptionsCtx(context.Context, []string, client.BatchStatOptions) ([]client.BatchStatResult, error)
	DeleteFileCtx(context.Context, string) error
	DeleteDirCtx(context.Context, string) error
}

func (e *batchApplyEngine) ApplyDeletes(ctx context.Context, input generationRecordReader) (applyStageResult, error) {
	if e.config.Phase != PhaseSyncing {
		return applyStageResult{}, ErrUnsafeApply
	}
	if input == nil {
		return applyStageResult{}, fmt.Errorf("delete apply requires Diff input")
	}
	if err := ctx.Err(); err != nil {
		return applyStageResult{}, err
	}
	var result applyStageResult
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		record, ok, err := input.Next()
		if err != nil {
			return result, err
		}
		if !ok {
			return result, nil
		}
		if record.Diff == nil || record.Diff.Operation != "delete" || record.Diff.Finding != FindingTargetOnly || record.Diff.Target == nil {
			continue
		}
		result.Total++
		target := record.Diff.Target
		remote := targetRemotePath(e.config.Prefix, record.Diff.Path, target.Kind == EntryDirectory)
		before, err := e.api.BatchStatWithOptionsCtx(ctx, []string{remote}, client.BatchStatOptions{})
		if err != nil || len(before) != 1 || !safeDeleteStat(before[0], remote, target) {
			result.Pending++
			return result, ErrApplyRescan
		}
		if target.Kind == EntryDirectory {
			err = e.api.DeleteDirCtx(ctx, remote)
		} else {
			err = e.api.DeleteFileCtx(ctx, remote)
		}
		after, statErr := e.api.BatchStatWithOptionsCtx(ctx, []string{remote}, client.BatchStatOptions{})
		if statErr == nil && len(after) == 1 && after[0].Status == 404 {
			result.Verified++
			continue
		}
		if err != nil {
			result.Unknown++
		} else {
			result.Pending++
		}
		return result, ErrApplyRescan
	}
}

type batchApplyConfig struct {
	Prefix  string
	Phase   Phase
	Workers int
	Budget  *memoryBudget
}

type applyStageResult struct {
	Total    int64
	Verified int64
	Pending  int64
	Unknown  int64
}

func (r *applyStageResult) add(other applyStageResult) {
	r.Total += other.Total
	r.Verified += other.Verified
	r.Pending += other.Pending
	r.Unknown += other.Unknown
}

type batchApplyEngine struct {
	api     batchApplyClient
	scanner *Scanner
	config  batchApplyConfig
}

func newBatchApplyEngine(api batchApplyClient, scanner *Scanner, config batchApplyConfig) (*batchApplyEngine, error) {
	if api == nil || scanner == nil {
		return nil, fmt.Errorf("batch apply requires client and scanner")
	}
	prefix, err := validateTargetPrefix(config.Prefix)
	if err != nil {
		return nil, err
	}
	if config.Phase != PhaseSyncing && config.Phase != PhaseDualWriteRepairing {
		return nil, fmt.Errorf("%w: batch apply phase %q", ErrInvalidPhase, config.Phase)
	}
	if config.Workers <= 0 {
		return nil, fmt.Errorf("batch apply workers must be positive")
	}
	config.Prefix = prefix
	return &batchApplyEngine{api: api, scanner: scanner, config: config}, nil
}

func (e *batchApplyEngine) ApplyDirectories(ctx context.Context, input generationRecordReader) (applyStageResult, error) {
	if e.config.Phase != PhaseSyncing {
		return applyStageResult{}, ErrUnsafeApply
	}
	if input == nil {
		return applyStageResult{}, fmt.Errorf("directory apply requires Diff input")
	}
	if err := ctx.Err(); err != nil {
		return applyStageResult{}, err
	}
	var result applyStageResult
	var window []generationRecord
	currentDepth := -1
	flush := func() error {
		if len(window) == 0 {
			return nil
		}
		partial, err := e.processDirectoryWindow(ctx, window)
		result.add(partial)
		window = nil
		if err != nil {
			return err
		}
		if partial.Pending > 0 || partial.Unknown > 0 {
			return ErrApplyRescan
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		record, ok, err := input.Next()
		if err != nil {
			return result, err
		}
		if !ok {
			break
		}
		if !isDirectoryCreateRecord(record) {
			continue
		}
		depth := pathDepth(record.Diff.Path)
		if currentDepth >= 0 && depth < currentDepth {
			return result, fmt.Errorf("directory work is not ascending by depth")
		}
		if currentDepth >= 0 && depth != currentDepth {
			if err := flush(); err != nil {
				return result, err
			}
		}
		currentDepth = depth
		window = append(window, record)
		if len(window) == e.config.Workers*client.MaxBatchMkdirItems {
			if err := flush(); err != nil {
				return result, err
			}
		}
	}
	if err := flush(); err != nil {
		return result, err
	}
	return result, nil
}

func (e *batchApplyEngine) processDirectoryWindow(ctx context.Context, records []generationRecord) (applyStageResult, error) {
	if err := ctx.Err(); err != nil {
		return applyStageResult{}, err
	}
	if e.config.Budget != nil {
		release, err := e.config.Budget.Acquire(ctx, int64(len(records))*4096)
		if err != nil {
			return applyStageResult{}, err
		}
		defer release()
	}
	type outcome struct {
		result applyStageResult
		err    error
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	outcomes := make(chan outcome, e.config.Workers)
	var wait sync.WaitGroup
	for first := 0; first < len(records); first += client.MaxBatchMkdirItems {
		last := min(first+client.MaxBatchMkdirItems, len(records))
		batch := append([]generationRecord(nil), records[first:last]...)
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := e.applyDirectoryBatch(ctx, batch)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	wait.Wait()
	close(outcomes)
	var result applyStageResult
	var firstErr error
	for outcome := range outcomes {
		result.add(outcome.result)
		if outcome.err != nil && firstErr == nil {
			firstErr = outcome.err
			cancel()
		}
	}
	return result, firstErr
}

func (e *batchApplyEngine) applyDirectoryBatch(ctx context.Context, records []generationRecord) (applyStageResult, error) {
	result := applyStageResult{Total: int64(len(records))}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	items := make([]client.BatchMkdirItem, len(records))
	for index, record := range records {
		if err := e.validateSourceRecord(record.Diff.Source); err != nil {
			return result, err
		}
		items[index] = client.BatchMkdirItem{Path: targetRemotePath(e.config.Prefix, record.Diff.Path, true), Mode: 0o755}
	}
	responses, mutationErr := e.api.BatchMkdirCtx(ctx, items)
	candidates := make([]int, 0, len(items))
	if mutationErr != nil {
		for index := range items {
			candidates = append(candidates, index)
		}
	} else {
		if len(responses) != len(items) {
			result.Unknown = int64(len(items))
			return result, ErrApplyRescan
		}
		for index, response := range responses {
			if response.Path != items[index].Path {
				result.Unknown++
				continue
			}
			if response.OK() {
				candidates = append(candidates, index)
			} else {
				result.Pending++
			}
		}
	}
	verified, err := e.verifyDirectoryCandidates(ctx, records, items, candidates)
	result.Verified += int64(verified)
	missing := int64(len(candidates) - verified)
	if mutationErr != nil {
		result.Unknown += missing
	} else {
		result.Pending += missing
	}
	if err != nil {
		return result, err
	}
	if mutationErr != nil && missing > 0 {
		return result, fmt.Errorf("%w: batch mkdir outcome unknown: %v", ErrApplyRescan, mutationErr)
	}
	return result, nil
}

func (e *batchApplyEngine) verifyDirectoryCandidates(ctx context.Context, records []generationRecord, items []client.BatchMkdirItem, candidates []int) (int, error) {
	if len(candidates) == 0 {
		return 0, nil
	}
	paths := make([]string, len(candidates))
	for index, candidate := range candidates {
		paths[index] = items[candidate].Path
	}
	stats, err := e.api.BatchStatWithOptionsCtx(ctx, paths, client.BatchStatOptions{})
	if err != nil {
		return 0, err
	}
	if len(stats) != len(paths) {
		return 0, ErrApplyRescan
	}
	verified := 0
	for index, stat := range stats {
		candidate := candidates[index]
		if stat.Path != paths[index] || !stat.OK() || !stat.IsDir || !stat.HasMode || stat.ResourceID == "" || stat.Nlink == 0 {
			continue
		}
		if err := e.validateSourceRecord(records[candidate].Diff.Source); err != nil {
			return verified, err
		}
		verified++
	}
	return verified, nil
}

func (e *batchApplyEngine) ApplyModes(ctx context.Context, input generationRecordReader) (applyStageResult, error) {
	if e.config.Phase != PhaseSyncing {
		return applyStageResult{}, ErrUnsafeApply
	}
	if input == nil {
		return applyStageResult{}, fmt.Errorf("mode apply requires Diff input")
	}
	if err := ctx.Err(); err != nil {
		return applyStageResult{}, err
	}
	var result applyStageResult
	var window []generationRecord
	barrier := ""
	flush := func() error {
		if len(window) == 0 {
			return nil
		}
		partial, err := e.processModeWindow(ctx, window)
		result.add(partial)
		window = nil
		if err != nil {
			return err
		}
		if partial.Pending > 0 || partial.Unknown > 0 {
			return ErrApplyRescan
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		record, ok, err := input.Next()
		if err != nil {
			return result, err
		}
		if !ok {
			break
		}
		if !isModeRecord(record) {
			continue
		}
		current := modeBarrier(record)
		if barrier != "" && current != barrier {
			if err := flush(); err != nil {
				return result, err
			}
		}
		barrier = current
		window = append(window, record)
		if len(window) == e.config.Workers*client.MaxBatchChmodItems {
			if err := flush(); err != nil {
				return result, err
			}
		}
	}
	if err := flush(); err != nil {
		return result, err
	}
	return result, nil
}

func (e *batchApplyEngine) processModeWindow(ctx context.Context, records []generationRecord) (applyStageResult, error) {
	if err := ctx.Err(); err != nil {
		return applyStageResult{}, err
	}
	if e.config.Budget != nil {
		release, err := e.config.Budget.Acquire(ctx, int64(len(records))*4096)
		if err != nil {
			return applyStageResult{}, err
		}
		defer release()
	}
	type outcome struct {
		result applyStageResult
		err    error
	}
	outcomes := make(chan outcome, e.config.Workers)
	var wait sync.WaitGroup
	for first := 0; first < len(records); first += client.MaxBatchChmodItems {
		last := min(first+client.MaxBatchChmodItems, len(records))
		batch := append([]generationRecord(nil), records[first:last]...)
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := e.applyModeBatch(ctx, batch)
			outcomes <- outcome{result: result, err: err}
		}()
	}
	wait.Wait()
	close(outcomes)
	var result applyStageResult
	var firstErr error
	for outcome := range outcomes {
		result.add(outcome.result)
		if outcome.err != nil && firstErr == nil {
			firstErr = outcome.err
		}
	}
	return result, firstErr
}

func (e *batchApplyEngine) applyModeBatch(ctx context.Context, records []generationRecord) (applyStageResult, error) {
	result := applyStageResult{Total: int64(len(records))}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	paths := make([]string, len(records))
	for index, record := range records {
		if err := e.validateSourceRecord(record.Diff.Source); err != nil {
			return result, err
		}
		paths[index] = targetRemotePath(e.config.Prefix, record.Diff.Path, record.Diff.Source.Kind == EntryDirectory)
	}
	before, err := e.api.BatchStatWithOptionsCtx(ctx, paths, client.BatchStatOptions{})
	if err != nil || len(before) != len(paths) {
		result.Unknown = int64(len(paths))
		return result, ErrApplyRescan
	}
	var items []client.BatchChmodItem
	var indexes []int
	for index, stat := range before {
		source := records[index].Diff.Source
		if !safeModeStat(stat, paths[index], source) {
			result.Pending++
			continue
		}
		if stat.Mode&0o777 == source.Mode&0o777 {
			if err := e.validateSourceRecord(source); err != nil {
				return result, err
			}
			result.Verified++
			continue
		}
		item := client.BatchChmodItem{Path: paths[index], Mode: source.Mode & 0o777, ExpectedResourceID: stat.ResourceID}
		if source.Kind != EntryDirectory {
			revision := stat.Revision
			item.ExpectedRevision = &revision
		}
		items = append(items, item)
		indexes = append(indexes, index)
	}
	if len(items) == 0 {
		return result, nil
	}
	responses, mutationErr := e.api.BatchChmodCtx(ctx, items)
	var candidates []int
	if mutationErr != nil {
		for index := range items {
			candidates = append(candidates, index)
		}
	} else if len(responses) != len(items) {
		result.Unknown += int64(len(items))
		return result, ErrApplyRescan
	} else {
		for index, response := range responses {
			if response.Path == items[index].Path && response.OK() {
				candidates = append(candidates, index)
			} else {
				result.Pending++
			}
		}
	}
	postPaths := make([]string, len(candidates))
	for index, candidate := range candidates {
		postPaths[index] = items[candidate].Path
	}
	after, statErr := e.api.BatchStatWithOptionsCtx(ctx, postPaths, client.BatchStatOptions{})
	if statErr != nil || len(after) != len(postPaths) {
		result.Unknown += int64(len(candidates))
		return result, ErrApplyRescan
	}
	verified := 0
	for index, stat := range after {
		candidate := candidates[index]
		recordIndex := indexes[candidate]
		source := records[recordIndex].Diff.Source
		prior := before[recordIndex]
		if !safeModeStat(stat, postPaths[index], source) || stat.Mode&0o777 != source.Mode&0o777 ||
			stat.ResourceID != prior.ResourceID || source.Kind != EntryDirectory && (stat.Revision != prior.Revision || stat.Nlink != prior.Nlink) {
			continue
		}
		if err := e.validateSourceRecord(source); err != nil {
			return result, err
		}
		verified++
	}
	result.Verified += int64(verified)
	missing := int64(len(candidates) - verified)
	if mutationErr != nil {
		result.Unknown += missing
	} else {
		result.Pending += missing
	}
	if mutationErr != nil && missing > 0 {
		return result, fmt.Errorf("%w: batch chmod outcome unknown: %v", ErrApplyRescan, mutationErr)
	}
	return result, nil
}

func (e *batchApplyEngine) validateSourceRecord(source *sourceGenerationRecord) error {
	if source == nil {
		return ErrUnsafeApply
	}
	entry := source.sourceEntry()
	return e.scanner.validateSourcePath(sourceLocalPath(entry), entry.Version)
}

func isDirectoryCreateRecord(record generationRecord) bool {
	return record.Diff != nil && record.Diff.Operation == "mkdir" && record.Diff.Finding == FindingSourceOnly &&
		record.Diff.Source != nil && record.Diff.Source.Kind == EntryDirectory
}

func isModeRecord(record generationRecord) bool {
	return record.Diff != nil && record.Diff.Operation == "chmod" && record.Diff.Source != nil
}

func modeBarrier(record generationRecord) string {
	if record.Diff.Source.Kind != EntryDirectory {
		return "file"
	}
	return fmt.Sprintf("directory:%08d", pathDepth(record.Diff.Path))
}

func safeModeStat(stat client.BatchStatResult, path string, source *sourceGenerationRecord) bool {
	if source == nil || stat.Path != path || !stat.OK() || !stat.HasMode || stat.ResourceID == "" || stat.Nlink == 0 {
		return false
	}
	if source.Kind == EntryDirectory {
		return stat.IsDir
	}
	if stat.IsDir || stat.Revision <= 0 {
		return false
	}
	kind := targetKind(stat)
	return kind == source.Kind
}

func safeDeleteStat(stat client.BatchStatResult, path string, target *targetGenerationRecord) bool {
	if target == nil || stat.Path != path || !stat.OK() || stat.ResourceID == "" || stat.ResourceID != target.ResourceID || stat.Nlink != target.Nlink {
		return false
	}
	if target.Kind == EntryDirectory {
		return stat.IsDir
	}
	return !stat.IsDir && target.Revision != nil && stat.Revision == *target.Revision
}
