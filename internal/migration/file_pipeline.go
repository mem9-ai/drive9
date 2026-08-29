package migration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type filePipelineClient interface {
	BatchWriteCtx(context.Context, []client.BatchWriteItem) ([]client.BatchWriteResult, error)
	BatchStatWithOptionsCtx(context.Context, []string, client.BatchStatOptions) ([]client.BatchStatResult, error)
	WriteStreamConditionalWithChecksumAndPreCompleteCheck(context.Context, string, io.Reader, int64, client.ProgressFunc, int64, string, func() error) (*client.StatResult, error)
	StatCtx(context.Context, string) (*client.StatResult, error)
	DeleteFileCtx(context.Context, string) error
	HardlinkCtx(context.Context, string, string) error
	SymlinkCtx(context.Context, string, string) error
}

type filePipelineConfig struct {
	Prefix            string
	Phase             Phase
	InlineThreshold   int64
	InlineWorkers     int
	MultipartWorkers  int
	MaxBytesPerSecond int64
	Budget            *memoryBudget
}

type filePipeline struct {
	api            filePipelineClient
	scanner        *Scanner
	config         filePipelineConfig
	limiter        *byteTokenBucket
	inlineLimit    *adaptiveLimit
	multipartLimit *adaptiveLimit
	clock          func() time.Time
	onProgress     func(filePipelineProgress)
}

type filePipelineProgress struct {
	BatchCount      int64
	PayloadBytes    int64
	InlineFiles     int64
	InlineBytes     int64
	MultipartFiles  int64
	MultipartBytes  int64
	RetryableErrors int64
	LastLatencyMS   int64
}

type fileTask struct {
	diff *diffGenerationRecord
}

func newFilePipeline(api filePipelineClient, scanner *Scanner, config filePipelineConfig) (*filePipeline, error) {
	if api == nil || scanner == nil {
		return nil, fmt.Errorf("file pipeline requires client and scanner")
	}
	prefix, err := validateTargetPrefix(config.Prefix)
	if err != nil {
		return nil, err
	}
	if config.Phase != PhaseSyncing && config.Phase != PhaseDualWriteRepairing {
		return nil, fmt.Errorf("%w: file pipeline phase %q", ErrInvalidPhase, config.Phase)
	}
	if config.InlineWorkers <= 0 || config.MultipartWorkers <= 0 || config.MaxBytesPerSecond <= 0 {
		return nil, fmt.Errorf("file pipeline limits must be positive")
	}
	now := time.Now()
	inlineLimit, err := newAdaptiveLimit(min(8, config.InlineWorkers), now)
	if err != nil {
		return nil, err
	}
	multipartLimit, err := newAdaptiveLimit(config.MultipartWorkers, now)
	if err != nil {
		return nil, err
	}
	config.Prefix = prefix
	return &filePipeline{
		api: api, scanner: scanner, config: config, limiter: newByteTokenBucket(config.MaxBytesPerSecond),
		inlineLimit: inlineLimit, multipartLimit: multipartLimit, clock: time.Now,
	}, nil
}

func (e *filePipeline) ApplyFiles(ctx context.Context, input generationRecordReader) (applyStageResult, error) {
	if e.config.Phase != PhaseSyncing {
		return applyStageResult{}, ErrUnsafeApply
	}
	if input == nil {
		return applyStageResult{}, fmt.Errorf("file pipeline requires Diff input")
	}
	if err := ctx.Err(); err != nil {
		return applyStageResult{}, err
	}
	var result applyStageResult
	var inlineBatches [][]fileTask
	var inlineBatch []fileTask
	var inlineBytes int64
	var multipart []fileTask
	flush := func() error {
		if len(inlineBatch) > 0 {
			inlineBatches = append(inlineBatches, inlineBatch)
			inlineBatch, inlineBytes = nil, 0
		}
		if len(inlineBatches) == 0 && len(multipart) == 0 {
			return nil
		}
		partial, err := e.executeFileWave(ctx, inlineBatches, multipart)
		result.add(partial)
		inlineBatches, multipart = nil, nil
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
		if !isFileContentRecord(record) {
			continue
		}
		task := fileTask{diff: record.Diff}
		if e.inline(task.diff.Source.Size) {
			if len(inlineBatch) > 0 && (len(inlineBatch) == client.MaxBatchWriteItems || inlineBytes+task.diff.Source.Size > client.MaxBatchWriteBytes) {
				inlineBatches = append(inlineBatches, inlineBatch)
				inlineBatch, inlineBytes = nil, 0
			}
			if len(inlineBatches) >= e.inlineLimit.Current() {
				if err := flush(); err != nil {
					return result, err
				}
			}
			inlineBatch = append(inlineBatch, task)
			inlineBytes += task.diff.Source.Size
		} else {
			if len(multipart) >= e.multipartLimit.Current() {
				if err := flush(); err != nil {
					return result, err
				}
			}
			multipart = append(multipart, task)
		}
	}
	if err := flush(); err != nil {
		return result, err
	}
	return result, nil
}

func (e *filePipeline) inline(size int64) bool {
	return size == 0 || e.config.InlineThreshold > 0 && size < e.config.InlineThreshold && size <= client.MaxBatchWriteBytes
}

func isFileContentRecord(record generationRecord) bool {
	if record.Diff == nil || record.Diff.Source == nil || record.Diff.Source.Kind != EntryRegular {
		return false
	}
	if record.Diff.Operation == "link-0-primary" {
		return true
	}
	return record.Diff.Operation == "write" && record.Diff.Source.HardlinkKey == ""
}

func (e *filePipeline) executeFileWave(ctx context.Context, inline [][]fileTask, multipart []fileTask) (applyStageResult, error) {
	started := time.Now()
	if err := ctx.Err(); err != nil {
		return applyStageResult{}, err
	}
	if e.config.Budget != nil {
		reserved := int64(len(multipart)) * (256 << 20)
		for _, batch := range inline {
			reserved += 1 << 20
			for _, task := range batch {
				reserved += task.diff.Source.Size * 2
			}
		}
		release, err := e.config.Budget.Acquire(ctx, reserved)
		if err != nil {
			return applyStageResult{}, err
		}
		defer release()
	}
	type outcome struct {
		result applyStageResult
		err    error
		kind   string
	}
	outcomes := make(chan outcome, len(inline)+len(multipart))
	var wait sync.WaitGroup
	for _, batch := range inline {
		batch := append([]fileTask(nil), batch...)
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := e.applyInlineBatch(ctx, batch)
			outcomes <- outcome{result: result, err: err, kind: "inline"}
		}()
	}
	for _, task := range multipart {
		task := task
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := e.applyMultipart(ctx, task)
			outcomes <- outcome{result: result, err: err, kind: "multipart"}
		}()
	}
	wait.Wait()
	close(outcomes)
	var result applyStageResult
	var firstErr error
	progress := filePipelineProgress{BatchCount: int64(len(inline)), MultipartFiles: int64(len(multipart))}
	for _, batch := range inline {
		progress.InlineFiles += int64(len(batch))
		for _, task := range batch {
			progress.InlineBytes += task.diff.Source.Size
		}
	}
	for _, task := range multipart {
		progress.MultipartBytes += task.diff.Source.Size
	}
	progress.PayloadBytes = progress.InlineBytes + progress.MultipartBytes
	for outcome := range outcomes {
		result.add(outcome.result)
		limit := e.inlineLimit
		if outcome.kind == "multipart" {
			limit = e.multipartLimit
		}
		if outcome.err != nil {
			if isBackpressureError(outcome.err) {
				progress.RetryableErrors++
			}
			limit.OnFailure(outcome.err, e.clock())
			if firstErr == nil {
				firstErr = outcome.err
			}
		} else {
			limit.OnSuccess(e.clock())
		}
	}
	progress.LastLatencyMS = time.Since(started).Milliseconds()
	if e.onProgress != nil {
		e.onProgress(progress)
	}
	return result, firstErr
}

type inlineMutation struct {
	task     fileTask
	path     string
	before   client.BatchStatResult
	data     []byte
	checksum string
	item     client.BatchWriteItem
}

func (e *filePipeline) applyInlineBatch(ctx context.Context, tasks []fileTask) (applyStageResult, error) {
	result := applyStageResult{Total: int64(len(tasks))}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	paths := make([]string, len(tasks))
	for index, task := range tasks {
		paths[index] = targetRemotePath(e.config.Prefix, task.diff.Path, false)
	}
	stats, err := e.api.BatchStatWithOptionsCtx(ctx, paths, client.BatchStatOptions{IncludeChecksum: true})
	if err != nil || len(stats) != len(tasks) {
		result.Unknown = int64(len(tasks))
		return result, ErrApplyRescan
	}
	var mutations []inlineMutation
	for index, task := range tasks {
		if !observedBatchTarget(stats[index], paths[index], task.diff.Target) {
			result.Pending++
			continue
		}
		if batchStatMatchesSource(stats[index], task.diff.Source) {
			if err := e.validateSource(task.diff.Source); err != nil {
				return result, err
			}
			result.Verified++
			continue
		}
		data, checksum, err := e.readInline(ctx, task.diff.Source)
		if err != nil {
			return result, err
		}
		expected := int64(0)
		if task.diff.Target != nil && task.diff.Target.Revision != nil {
			expected = *task.diff.Target.Revision
		}
		mutations = append(mutations, inlineMutation{
			task: task, path: paths[index], before: stats[index], data: data, checksum: checksum,
			item: client.BatchWriteItem{Path: paths[index], ExpectedRevision: expected, Data: data, Mode: task.diff.Source.Mode & 0o777, HasMode: true},
		})
	}
	if len(mutations) == 0 {
		return result, nil
	}
	items := make([]client.BatchWriteItem, len(mutations))
	for index := range mutations {
		items[index] = mutations[index].item
	}
	responses, mutationErr := e.api.BatchWriteCtx(ctx, items)
	var candidates []int
	committedRevision := make(map[int]int64)
	if mutationErr != nil {
		for index := range mutations {
			candidates = append(candidates, index)
		}
	} else if len(responses) != len(mutations) {
		result.Unknown += int64(len(mutations))
		return result, ErrApplyRescan
	} else {
		for index, response := range responses {
			if response.Path == mutations[index].path && response.OK() {
				candidates = append(candidates, index)
				committedRevision[index] = response.Revision
			} else {
				result.Pending++
			}
		}
	}
	postPaths := make([]string, len(candidates))
	for index, candidate := range candidates {
		postPaths[index] = mutations[candidate].path
	}
	after, statErr := e.api.BatchStatWithOptionsCtx(ctx, postPaths, client.BatchStatOptions{IncludeChecksum: true})
	if statErr != nil || len(after) != len(postPaths) {
		result.Unknown += int64(len(candidates))
		return result, ErrApplyRescan
	}
	verified := 0
	for index, stat := range after {
		candidate := candidates[index]
		mutation := mutations[candidate]
		if !verifiedInlineStat(stat, mutation, committedRevision[candidate], mutationErr != nil) {
			continue
		}
		if err := e.validateSource(mutation.task.diff.Source); err != nil {
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
	if missing > 0 {
		return result, ErrApplyRescan
	}
	return result, nil
}

func (e *filePipeline) readInline(ctx context.Context, source *sourceGenerationRecord) ([]byte, string, error) {
	entry := source.sourceEntry()
	rooted, err := e.scanner.openStableSource(sourceLocalPath(entry), entry.Version)
	if err != nil {
		return nil, "", err
	}
	body, readErr := io.ReadAll(io.LimitReader(rooted.file, source.Size+1))
	if e.scanner.afterRead != nil {
		e.scanner.afterRead(sourceLocalPath(entry))
	}
	validateErr := rooted.validate()
	closeErr := rooted.close()
	if readErr != nil {
		return nil, "", readErr
	}
	if validateErr != nil || int64(len(body)) != source.Size {
		return nil, "", ErrSourceChanged
	}
	if closeErr != nil {
		return nil, "", closeErr
	}
	sum := sha256.Sum256(body)
	checksum := hex.EncodeToString(sum[:])
	if checksum != source.ChecksumSHA256 {
		return nil, "", ErrSourceChanged
	}
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	return body, checksum, nil
}

func (e *filePipeline) applyMultipart(ctx context.Context, task fileTask) (applyStageResult, error) {
	result := applyStageResult{Total: 1}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	remote := targetRemotePath(e.config.Prefix, task.diff.Path, false)
	before, expected, err := e.currentTarget(ctx, remote, task.diff.Target)
	if err != nil {
		result.Pending = 1
		return result, err
	}
	if statMatchesSource(before, task.diff.Source, false) {
		if err := e.validateSource(task.diff.Source); err != nil {
			return result, err
		}
		result.Verified = 1
		return result, nil
	}
	entry := task.diff.Source.sourceEntry()
	rooted, err := e.scanner.openStableSource(sourceLocalPath(entry), entry.Version)
	if err != nil {
		return result, err
	}
	limited := &limitedSource{File: rooted.file, limiter: e.limiter, ctx: ctx}
	committed, uploadErr := e.api.WriteStreamConditionalWithChecksumAndPreCompleteCheck(
		ctx, remote, limited, entry.Version.Size, nil, expected, task.diff.Source.ChecksumSHA256,
		func() error {
			if err := rooted.validate(); err != nil {
				return err
			}
			latest, _, err := e.currentTarget(ctx, remote, task.diff.Target)
			if err != nil {
				return err
			}
			if !sameStatIdentity(before, latest) {
				return ErrApplyRescan
			}
			return nil
		},
	)
	closeErr := rooted.close()
	if uploadErr != nil {
		observed, statErr := e.api.StatCtx(ctx, remote)
		if statErr == nil && statMatchesSource(observed, task.diff.Source, false) && (before == nil || sameStatResource(before, observed)) {
			if err := e.validateSource(task.diff.Source); err == nil {
				result.Verified = 1
				return result, nil
			}
		}
		result.Unknown = 1
		if errors.Is(uploadErr, client.ErrConflict) {
			return result, ErrApplyRescan
		}
		return result, uploadErr
	}
	if closeErr != nil {
		return result, closeErr
	}
	if !statMatchesSource(committed, task.diff.Source, false) || before != nil && !sameStatResource(before, committed) {
		result.Pending = 1
		return result, ErrApplyVerification
	}
	if err := e.validateSource(task.diff.Source); err != nil {
		return result, err
	}
	result.Verified = 1
	return result, nil
}

func (e *filePipeline) currentTarget(ctx context.Context, path string, observed *targetGenerationRecord) (*client.StatResult, int64, error) {
	stat, err := e.api.StatCtx(ctx, path)
	if observed == nil {
		if client.IsNotFound(err) {
			return nil, 0, nil
		}
		if err != nil {
			return nil, 0, err
		}
		return nil, 0, ErrApplyRescan
	}
	if err != nil || !statMatchesTarget(stat, observed) {
		return nil, 0, ErrApplyRescan
	}
	return stat, *observed.Revision, nil
}

func (e *filePipeline) ApplyLinks(ctx context.Context, input generationRecordReader) (applyStageResult, error) {
	if e.config.Phase != PhaseSyncing {
		return applyStageResult{}, ErrUnsafeApply
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
		if record.Diff == nil || record.Diff.Source == nil {
			continue
		}
		var partial applyStageResult
		switch {
		case record.Diff.Operation == "link-1-alias":
			partial, err = e.applyHardlink(ctx, record.Diff)
		case record.Diff.Operation == "write" && record.Diff.Source.Kind == EntrySymlink:
			partial, err = e.applySymlink(ctx, record.Diff)
		default:
			continue
		}
		result.add(partial)
		if err != nil {
			return result, err
		}
	}
}

func (e *filePipeline) applyHardlink(ctx context.Context, diff *diffGenerationRecord) (applyStageResult, error) {
	result := applyStageResult{Total: 1}
	primaryPath := targetRemotePath(e.config.Prefix, diff.PrimaryPath, false)
	aliasPath := targetRemotePath(e.config.Prefix, diff.Path, false)
	primary, err := e.api.StatCtx(ctx, primaryPath)
	if err != nil || !statMatchesSource(primary, diff.Source, false) {
		result.Pending = 1
		return result, ErrApplyRescan
	}
	alias, err := e.api.StatCtx(ctx, aliasPath)
	if err == nil && alias.ResourceID == primary.ResourceID {
		if err := e.validateSource(diff.Source); err != nil {
			return result, err
		}
		result.Verified = 1
		return result, nil
	}
	if err != nil && !client.IsNotFound(err) {
		result.Unknown = 1
		return result, err
	}
	if err == nil {
		if err := e.api.DeleteFileCtx(ctx, aliasPath); err != nil {
			result.Unknown = 1
			return result, err
		}
	}
	if err := e.validateSource(diff.Source); err != nil {
		return result, err
	}
	if err := e.api.HardlinkCtx(ctx, primaryPath, aliasPath); err != nil {
		result.Unknown = 1
		return result, err
	}
	committed, err := e.api.StatCtx(ctx, aliasPath)
	if err != nil || !statMatchesSource(committed, diff.Source, false) || committed.ResourceID != primary.ResourceID || committed.Nlink < primary.Nlink {
		result.Pending = 1
		return result, ErrApplyVerification
	}
	if err := e.validateSource(diff.Source); err != nil {
		return result, err
	}
	result.Verified = 1
	return result, nil
}

func (e *filePipeline) applySymlink(ctx context.Context, diff *diffGenerationRecord) (applyStageResult, error) {
	result := applyStageResult{Total: 1}
	remote := targetRemotePath(e.config.Prefix, diff.Path, false)
	current, err := e.api.StatCtx(ctx, remote)
	if err == nil && statMatchesSource(current, diff.Source, false) {
		if err := e.validateSource(diff.Source); err != nil {
			return result, err
		}
		result.Verified = 1
		return result, nil
	}
	if err != nil && !client.IsNotFound(err) {
		result.Unknown = 1
		return result, err
	}
	if err == nil {
		if err := e.api.DeleteFileCtx(ctx, remote); err != nil {
			result.Unknown = 1
			return result, err
		}
	}
	if err := e.validateSource(diff.Source); err != nil {
		return result, err
	}
	if err := e.api.SymlinkCtx(ctx, diff.Source.LinkTarget, remote); err != nil {
		result.Unknown = 1
		return result, err
	}
	committed, err := e.api.StatCtx(ctx, remote)
	if err != nil || !statMatchesSource(committed, diff.Source, false) {
		result.Pending = 1
		return result, ErrApplyVerification
	}
	if err := e.validateSource(diff.Source); err != nil {
		return result, err
	}
	result.Verified = 1
	return result, nil
}

func (e *filePipeline) validateSource(source *sourceGenerationRecord) error {
	return e.scanner.validateSourcePath(sourceLocalPath(source.sourceEntry()), source.sourceEntry().Version)
}

func observedBatchTarget(stat client.BatchStatResult, path string, target *targetGenerationRecord) bool {
	if target == nil {
		return stat.Path == path && stat.Status == 404
	}
	if stat.Path != path || !stat.OK() || stat.IsDir || stat.ResourceID != target.ResourceID || target.Revision == nil || stat.Revision != *target.Revision || stat.Nlink != target.Nlink {
		return false
	}
	return target.Mode == nil || stat.HasMode && stat.Mode&0o777 == *target.Mode&0o777
}

func batchStatMatchesSource(stat client.BatchStatResult, source *sourceGenerationRecord) bool {
	return stat.OK() && !stat.IsDir && stat.HasMode && targetKind(stat) == source.Kind && stat.Size == source.Size &&
		stat.ChecksumSHA256 == source.ChecksumSHA256 && stat.Mode&0o777 == source.Mode&0o777 && stat.Revision > 0 && stat.ResourceID != "" && stat.Nlink > 0
}

func verifiedInlineStat(stat client.BatchStatResult, mutation inlineMutation, committedRevision int64, unknown bool) bool {
	if stat.Path != mutation.path || !batchStatMatchesSource(stat, mutation.task.diff.Source) {
		return false
	}
	if !unknown && (committedRevision <= 0 || stat.Revision != committedRevision) {
		return false
	}
	if mutation.task.diff.Target == nil {
		return stat.Nlink == 1
	}
	return stat.ResourceID == mutation.before.ResourceID && stat.Nlink == mutation.before.Nlink
}

func statMatchesTarget(stat *client.StatResult, target *targetGenerationRecord) bool {
	if stat == nil || target == nil || stat.IsDir || target.Revision == nil || stat.Revision != *target.Revision || stat.ResourceID != target.ResourceID || stat.Nlink != target.Nlink {
		return false
	}
	return target.Mode == nil || stat.HasMode && stat.Mode&0o777 == *target.Mode&0o777
}

func statMatchesSource(stat *client.StatResult, source *sourceGenerationRecord, requireMode bool) bool {
	if stat == nil || source == nil || stat.IsDir || !stat.HasMode || stat.Size != source.Size || stat.ChecksumSHA256 != source.ChecksumSHA256 || stat.Revision <= 0 || stat.ResourceID == "" || stat.Nlink == 0 {
		return false
	}
	modeType := stat.Mode & 0o170000
	wantType := uint32(0o100000)
	if source.Kind == EntrySymlink {
		wantType = 0o120000
	}
	if modeType != 0 && modeType != wantType {
		return false
	}
	return !requireMode || stat.Mode&0o777 == source.Mode&0o777
}

func sameStatIdentity(left, right *client.StatResult) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Revision == right.Revision && left.ResourceID == right.ResourceID && left.Nlink == right.Nlink && left.Mode == right.Mode
}

func sameStatResource(left, right *client.StatResult) bool {
	return left != nil && right != nil && left.ResourceID == right.ResourceID && left.Nlink == right.Nlink
}
