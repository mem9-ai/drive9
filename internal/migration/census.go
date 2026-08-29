package migration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type censusConfig struct {
	GenerationID    string
	RoundID         string
	Phase           Phase
	Identity        generationIdentity
	HashWorkers     int
	SortBufferBytes int64
	SortFanIn       int
	Budget          *memoryBudget
}

type censusResult struct {
	Metadata        generationMetadata
	MaxHashInFlight int
}

type censusBuilder struct {
	scanner                   *Scanner
	store                     *generationStore
	config                    censusConfig
	clock                     func() time.Time
	beforeDirectoryValidation func()
	onProgress                func(int64)
}

func newCensusBuilder(scanner *Scanner, store *generationStore, config censusConfig) (*censusBuilder, error) {
	if scanner == nil || store == nil {
		return nil, fmt.Errorf("census requires scanner and generation store")
	}
	if err := validateGenerationIdentifier(config.GenerationID); err != nil {
		return nil, fmt.Errorf("census generation ID: %w", err)
	}
	if err := validateGenerationIdentifier(config.RoundID); err != nil {
		return nil, fmt.Errorf("census round ID: %w", err)
	}
	if config.Phase != PhaseSyncing && config.Phase != PhaseDualWriteRepairing && config.Phase != PhaseCutoverReady {
		return nil, fmt.Errorf("census phase is invalid")
	}
	if err := validateGenerationIdentity(config.Identity); err != nil || config.Identity.JobID != store.jobID {
		return nil, fmt.Errorf("census identity is invalid")
	}
	if config.HashWorkers <= 0 || config.SortBufferBytes <= 0 || config.SortFanIn < 2 {
		return nil, fmt.Errorf("census worker and sort limits must be positive")
	}
	return &censusBuilder{scanner: scanner, store: store, config: config, clock: time.Now}, nil
}

func (b *censusBuilder) Build(ctx context.Context, previous *generationMetadata) (censusResult, error) {
	scanStarted := time.Now()
	if b.config.Budget != nil {
		release, err := b.config.Budget.Acquire(ctx, int64(b.config.HashWorkers*MaxSourceReadBufferBytes))
		if err != nil {
			return censusResult{}, err
		}
		defer release()
	}
	sortCtx, cancelSort := context.WithCancel(ctx)
	defer cancelSort()
	queueSize := max(2, b.config.HashWorkers*2)
	sourceInput := make(chan generationRecord, queueSize)
	directoryInput := make(chan generationRecord, queueSize)
	findingInput := make(chan generationRecord, queueSize)
	sourceOutcome := b.startSort(sortCtx, cancelSort, sourceInput, stageSource, recordSource, "source-raw")
	directoryOutcome := b.startSort(sortCtx, cancelSort, directoryInput, stageDirectoryIdentity, recordDirectoryIdentity, "directory")
	findingOutcome := b.startSort(sortCtx, cancelSort, findingInput, stageDiff, recordDiff, "source-findings")

	findingCounts := make(map[FindingKind]int64)
	var warningCount, blockerCount int64
	var findingSequence int64
	var excludedEntries, excludedDirectories, excludedFiles, excludedBytes int64
	var observedEntries int64
	emitFinding := func(finding Finding) error {
		findingCounts[finding.Kind]++
		switch finding.Severity {
		case SeverityWarning:
			warningCount++
		case SeverityBlocker:
			blockerCount++
		}
		findingSequence++
		return sendGenerationRecord(sortCtx, findingInput, generationRecord{
			Key: fmt.Sprintf("finding-%020d", findingSequence),
			Diff: &diffGenerationRecord{
				Path: finding.Path, Operation: "source_finding", DependencyKey: finding.Path,
				Finding: finding.Kind, Severity: finding.Severity,
			},
		})
	}
	walk, walkErr := b.scanner.walkSource(sortCtx, func(entry SourceEntry) error {
		observedEntries++
		if b.onProgress != nil && observedEntries%10000 == 0 {
			b.onProgress(observedEntries - excludedEntries)
		}
		if b.config.Identity.Prefix == "/" && (entry.Path == ControlPrefix || strings.HasPrefix(entry.Path, ControlPrefix+"/")) {
			excludedEntries++
			if entry.Kind == EntryDirectory {
				excludedDirectories++
			}
			if entry.Kind == EntryRegular {
				excludedFiles++
				excludedBytes += entry.Version.Size
			}
			return emitFinding(Finding{Path: entry.Path, Kind: FindingControlPrefix, Severity: SeverityBlocker})
		}
		return sendGenerationRecord(sortCtx, sourceInput, generationRecord{Key: entry.Path, Source: sourceGenerationRecordFromEntry(entry)})
	}, func(directory scannedDirectory) error {
		record, err := directoryGenerationRecord(directory)
		if err != nil {
			return err
		}
		return sendGenerationRecord(sortCtx, directoryInput, record)
	}, emitFinding, nil)
	if b.onProgress != nil {
		b.onProgress(observedEntries - excludedEntries)
	}
	close(sourceInput)
	close(directoryInput)
	close(findingInput)
	sourceSorted := <-sourceOutcome
	directorySorted := <-directoryOutcome
	findingSorted := <-findingOutcome
	sortErr := firstSortError(sourceSorted.err, directorySorted.err, findingSorted.err)
	if walkErr != nil && !errors.Is(walkErr, context.Canceled) {
		return censusResult{}, walkErr
	}
	if sortErr != nil {
		return censusResult{}, sortErr
	}
	if walkErr != nil {
		return censusResult{}, walkErr
	}
	if err := injectMigrationLargeStageFault("source_scan"); err != nil {
		return censusResult{}, err
	}
	scanDuration := time.Since(scanStarted)
	if b.beforeDirectoryValidation != nil {
		b.beforeDirectoryValidation()
	}
	if err := b.validateDirectories(ctx, directorySorted.result.Chunks); err != nil {
		return censusResult{}, err
	}

	current := &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: sourceSorted.result.Chunks}
	var prior generationRecordReader
	if previous != nil {
		loaded, err := b.store.LoadComplete(ctx, previous.GenerationID, b.config.Identity)
		if err != nil {
			return censusResult{}, err
		}
		stage, exists := loaded.Stages[stageSource]
		if !exists || !stage.Complete {
			return censusResult{}, ErrGenerationIncomplete
		}
		prior = &sortRunReader{ctx: ctx, store: b.store, generationID: loaded.GenerationID, chunks: stage.Chunks}
	}
	hashStarted := time.Now()
	hashed, err := newSourceHashReader(ctx, b.scanner, current, prior, b.config.HashWorkers)
	if err != nil {
		return censusResult{}, err
	}
	defer func() { _ = hashed.Close() }()
	finalSorter, err := newExternalSorter(b.store, externalSortConfig{
		GenerationID: b.config.GenerationID, Stage: stageSource, Kind: recordSource, IDPrefix: "source-final",
		MaxBufferBytes: b.config.SortBufferBytes, FanIn: b.config.SortFanIn, Budget: b.config.Budget,
	})
	if err != nil {
		return censusResult{}, err
	}
	finalSource, err := finalSorter.Sort(ctx, hashed)
	if err != nil {
		return censusResult{}, err
	}
	if err := hashed.Close(); err != nil {
		return censusResult{}, err
	}
	if err := injectMigrationLargeStageFault("source_hash_sort"); err != nil {
		return censusResult{}, err
	}
	hashDuration := time.Since(hashStarted)
	metadata := generationMetadata{
		FormatVersion:        generationFormatVersion,
		GenerationID:         b.config.GenerationID,
		RoundID:              b.config.RoundID,
		Phase:                b.config.Phase,
		Identity:             b.config.Identity,
		EntryCount:           int64(walk.EntryCount) - excludedEntries,
		DirectoryCount:       int64(walk.DirectoryCount) - excludedDirectories,
		FileCount:            int64(walk.FileCount) - excludedFiles,
		LogicalBytes:         walk.LogicalBytes - excludedBytes,
		WarningCount:         warningCount,
		BlockerCount:         blockerCount,
		SourceScanDurationMS: boundedDurationMillis(scanDuration),
		SourceHashDurationMS: boundedDurationMillis(hashDuration),
		SourceQueueCapacity:  int64(queueSize),
		HashReuseCount:       hashed.stats.reused,
		HashNewCount:         hashed.stats.hashed,
		FindingCounts:        findingCounts,
		CreatedAt:            b.clock().UTC(),
		Stages: map[generationStage]generationStageMetadata{
			stageSource:            completedStage(finalSource.Chunks),
			stageDirectoryIdentity: completedStage(directorySorted.result.Chunks),
			stageDiff:              completedStage(findingSorted.result.Chunks),
		},
	}
	if _, err := b.store.SaveMetadata(ctx, metadata, 0); err != nil {
		return censusResult{}, err
	}
	if err := injectMigrationLargeStageFault("source_publish"); err != nil {
		return censusResult{}, err
	}
	if err := b.store.PublishComplete(ctx, metadata); err != nil {
		return censusResult{}, err
	}
	return censusResult{Metadata: metadata, MaxHashInFlight: hashed.stats.maxInFlight}, nil
}

func boundedDurationMillis(duration time.Duration) int64 {
	if duration <= 0 {
		return 0
	}
	return max(int64(1), duration.Milliseconds())
}

type sortOutcome struct {
	result externalSortResult
	err    error
}

func (b *censusBuilder) startSort(ctx context.Context, cancel context.CancelFunc, input <-chan generationRecord, stage generationStage, kind generationRecordKind, prefix string) <-chan sortOutcome {
	outcome := make(chan sortOutcome, 1)
	go func() {
		sorter, err := newExternalSorter(b.store, externalSortConfig{
			GenerationID: b.config.GenerationID, Stage: stage, Kind: kind, IDPrefix: prefix,
			MaxBufferBytes: b.config.SortBufferBytes, FanIn: b.config.SortFanIn, Budget: b.config.Budget,
		})
		if err == nil {
			var result externalSortResult
			result, err = sorter.Sort(ctx, channelGenerationReader{ctx: ctx, input: input})
			outcome <- sortOutcome{result: result, err: err}
		} else {
			outcome <- sortOutcome{err: err}
		}
		if err != nil {
			cancel()
		}
	}()
	return outcome
}

type channelGenerationReader struct {
	ctx   context.Context
	input <-chan generationRecord
}

func (r channelGenerationReader) Next() (generationRecord, bool, error) {
	select {
	case <-r.ctx.Done():
		return generationRecord{}, false, r.ctx.Err()
	case record, ok := <-r.input:
		return record, ok, nil
	}
}

func sendGenerationRecord(ctx context.Context, output chan<- generationRecord, record generationRecord) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case output <- record:
		return nil
	}
}

func firstSortError(errs ...error) error {
	for _, err := range errs {
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func completedStage(chunks []chunkDescriptor) generationStageMetadata {
	var count int64
	for _, chunk := range chunks {
		count += chunk.RecordCount
	}
	return generationStageMetadata{Complete: true, RecordCount: count, Chunks: chunks}
}

func sourceGenerationRecordFromEntry(entry SourceEntry) *sourceGenerationRecord {
	return &sourceGenerationRecord{
		Path: entry.Path, LocalPath: entry.LocalPath, Kind: entry.Kind,
		Device: entry.Version.Device, Inode: entry.Version.Inode, Size: entry.Version.Size,
		MtimeNS: entry.Version.MtimeNS, CtimeNS: entry.Version.CtimeNS, VersionMode: entry.Version.Mode,
		Mode: entry.Mode, ChecksumSHA256: entry.ChecksumSHA256, LinkTarget: entry.LinkTarget,
		HardlinkKey: entry.HardlinkKey, HardlinkPrimary: entry.HardlinkPrimary,
	}
}

func (r sourceGenerationRecord) sourceEntry() SourceEntry {
	return SourceEntry{
		Path: r.Path, LocalPath: r.LocalPath, Kind: r.Kind,
		Version: SourceVersion{
			Device: r.Device, Inode: r.Inode, Kind: r.Kind, Size: r.Size,
			MtimeNS: r.MtimeNS, CtimeNS: r.CtimeNS, Mode: r.VersionMode,
		},
		Mode: r.Mode, ChecksumSHA256: r.ChecksumSHA256, LinkTarget: r.LinkTarget,
		HardlinkKey: r.HardlinkKey, HardlinkPrimary: r.HardlinkPrimary,
	}
}

func canReuseSourceChecksum(current, previous sourceGenerationRecord) bool {
	return previous.ChecksumSHA256 != "" && current.Device == previous.Device && current.Inode == previous.Inode &&
		current.Kind == previous.Kind && current.Size == previous.Size && current.MtimeNS == previous.MtimeNS &&
		current.CtimeNS == previous.CtimeNS && current.VersionMode == previous.VersionMode
}

type previousSourceLookup struct {
	reader  generationRecordReader
	current generationRecord
	has     bool
	done    bool
}

func (l *previousSourceLookup) reuse(record *sourceGenerationRecord) (bool, error) {
	if l.reader == nil {
		return false, nil
	}
	for !l.done && (!l.has || l.current.Key < record.Path) {
		current, ok, err := l.reader.Next()
		if err != nil {
			return false, err
		}
		if !ok {
			l.done, l.has = true, false
			break
		}
		l.current, l.has = current, true
	}
	if !l.has || l.current.Key != record.Path || l.current.Source == nil || !canReuseSourceChecksum(*record, *l.current.Source) {
		return false, nil
	}
	record.ChecksumSHA256 = l.current.Source.ChecksumSHA256
	return true, nil
}

type sourceHashStats struct {
	reused      int64
	hashed      int64
	maxInFlight int
}

type sourceHashJob struct {
	index  int
	record generationRecord
	reused bool
}

type sourceHashResult struct {
	index  int
	record generationRecord
	reused bool
	hashed bool
	err    error
}

type sourceHashReader struct {
	ctx      context.Context
	cancel   context.CancelFunc
	scanner  *Scanner
	current  generationRecordReader
	previous previousSourceLookup
	workers  int
	jobs     chan sourceHashJob
	results  chan sourceHashResult
	wait     sync.WaitGroup
	pending  []sourceHashResult
	index    int
	stats    sourceHashStats
	closed   bool
}

func newSourceHashReader(ctx context.Context, scanner *Scanner, current, previous generationRecordReader, workers int) (*sourceHashReader, error) {
	if scanner == nil || current == nil || workers <= 0 {
		return nil, fmt.Errorf("source hash reader requires scanner, input, and workers")
	}
	workerCtx, cancel := context.WithCancel(ctx)
	r := &sourceHashReader{
		ctx: workerCtx, cancel: cancel, scanner: scanner, current: current,
		previous: previousSourceLookup{reader: previous}, workers: workers,
		jobs: make(chan sourceHashJob, workers), results: make(chan sourceHashResult, workers),
	}
	for range workers {
		r.wait.Add(1)
		go r.runWorker()
	}
	return r, nil
}

func (r *sourceHashReader) Next() (generationRecord, bool, error) {
	if r.index < len(r.pending) {
		result := r.pending[r.index]
		r.index++
		return result.record, true, nil
	}
	if r.closed {
		return generationRecord{}, false, nil
	}
	var batch []sourceHashJob
	for index := 0; index < r.workers; index++ {
		record, ok, err := r.current.Next()
		if err != nil {
			return generationRecord{}, false, err
		}
		if !ok {
			break
		}
		reused, err := r.previous.reuse(record.Source)
		if err != nil {
			return generationRecord{}, false, err
		}
		batch = append(batch, sourceHashJob{index: index, record: record, reused: reused})
	}
	if len(batch) == 0 {
		if err := r.Close(); err != nil {
			return generationRecord{}, false, err
		}
		return generationRecord{}, false, nil
	}
	r.stats.maxInFlight = max(r.stats.maxInFlight, len(batch))
	for _, job := range batch {
		select {
		case <-r.ctx.Done():
			return generationRecord{}, false, r.ctx.Err()
		case r.jobs <- job:
		}
	}
	results := make([]sourceHashResult, 0, len(batch))
	var firstErr error
	for range batch {
		select {
		case <-r.ctx.Done():
			return generationRecord{}, false, r.ctx.Err()
		case result := <-r.results:
			results = append(results, result)
			if result.err != nil && firstErr == nil {
				firstErr = result.err
			}
		}
	}
	if firstErr != nil {
		return generationRecord{}, false, firstErr
	}
	sort.Slice(results, func(i, j int) bool { return results[i].index < results[j].index })
	for _, result := range results {
		if result.reused {
			r.stats.reused++
		}
		if result.hashed {
			r.stats.hashed++
		}
	}
	r.pending, r.index = results, 1
	return results[0].record, true, nil
}

func (r *sourceHashReader) runWorker() {
	defer r.wait.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case job, ok := <-r.jobs:
			if !ok {
				return
			}
			result := sourceHashResult{index: job.index, record: job.record, reused: job.reused}
			if job.record.Source == nil {
				result.err = fmt.Errorf("source hash job lacks Source record")
			} else if job.record.Source.Kind == EntryRegular && !job.reused {
				deep, err := r.scanner.ReadStableEntry(r.ctx, job.record.Source.sourceEntry())
				if err != nil {
					result.err = err
				} else {
					result.record.Source.ChecksumSHA256 = deep.ChecksumSHA256
					result.hashed = true
				}
			}
			select {
			case <-r.ctx.Done():
				return
			case r.results <- result:
			}
		}
	}
}

func (r *sourceHashReader) Close() error {
	if r == nil || r.closed {
		return nil
	}
	r.closed = true
	close(r.jobs)
	r.wait.Wait()
	r.cancel()
	return nil
}

func directoryGenerationRecord(directory scannedDirectory) (generationRecord, error) {
	path, localPath := "/", "/"
	if directory.name != "." {
		var ok bool
		path, ok = canonicalSourcePath(directory.name)
		if !ok {
			return generationRecord{}, fmt.Errorf("directory path is not canonical")
		}
		localPath = "/" + filepath.ToSlash(directory.name)
	}
	record := &directoryIdentityRecord{
		Path: path, LocalPath: localPath,
		Device: directory.version.Device, Inode: directory.version.Inode, Size: directory.version.Size,
		MtimeNS: directory.version.MtimeNS, CtimeNS: directory.version.CtimeNS, VersionMode: directory.version.Mode,
	}
	return generationRecord{Key: path, DirectoryIdentity: record}, nil
}

func (b *censusBuilder) validateDirectories(ctx context.Context, chunks []chunkDescriptor) error {
	root, rootInfo, err := b.scanner.openRoot()
	if err != nil {
		return ErrSourceChanged
	}
	defer func() { _ = root.Close() }()
	reader := &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: chunks}
	rootSeen := false
	for {
		record, ok, err := reader.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		directory := record.DirectoryIdentity
		if directory == nil {
			return ErrSourceChanged
		}
		expected := SourceVersion{
			Device: directory.Device, Inode: directory.Inode, Kind: EntryDirectory, Size: directory.Size,
			MtimeNS: directory.MtimeNS, CtimeNS: directory.CtimeNS, Mode: directory.VersionMode,
		}
		name := b.scanner.root
		var info os.FileInfo
		if directory.LocalPath == "/" {
			if rootSeen {
				return ErrSourceChanged
			}
			rootSeen = true
			info = rootInfo
		} else {
			relative := filepath.FromSlash(directory.LocalPath[1:])
			if err := b.scanner.validateAncestors(root, relative); err != nil {
				return ErrSourceChanged
			}
			info, err = root.Lstat(relative)
			if err != nil {
				return ErrSourceChanged
			}
			name = filepath.Join(b.scanner.root, relative)
		}
		identity, err := b.scanner.identity(name, info)
		if err != nil || identity.version != expected {
			return ErrSourceChanged
		}
	}
	rootAfter, err := os.Lstat(b.scanner.root)
	if !rootSeen || err != nil || !rootAfter.IsDir() || !os.SameFile(rootInfo, rootAfter) {
		return ErrSourceChanged
	}
	return nil
}
