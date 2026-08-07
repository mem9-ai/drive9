package migration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

var (
	ErrApplyRescan       = errors.New("migration apply requires rescan")
	ErrUnsafeApply       = errors.New("unsafe migration apply")
	ErrApplyVerification = errors.New("migration apply verification failed")
)

type ApplyConfig struct {
	Prefix             string
	Phase              Phase
	SmallFileThreshold int64
	SmallWorkers       int
	LargeWorkers       int
	MaxBytesPerSecond  int64
}

// ApplyEngine applies one complete in-memory diff through pkg/client.
type ApplyEngine struct {
	api              *client.Client
	scanner          *Scanner
	config           ApplyConfig
	limiter          *byteTokenBucket
	onCAS            func(SourceEntry, *client.StatResult, int64, time.Time, error)
	onOperationStart func()
	onOperationDone  func()
}

func NewApplyEngine(api *client.Client, scanner *Scanner, config ApplyConfig) (*ApplyEngine, error) {
	if api == nil || scanner == nil {
		return nil, errors.New("apply engine requires Client and Scanner")
	}
	prefix, err := validateTargetPrefix(config.Prefix)
	if err != nil {
		return nil, err
	}
	if config.Phase != PhaseSyncing && config.Phase != PhaseDualWriteRepairing {
		return nil, fmt.Errorf("%w: apply phase %q", ErrInvalidPhase, config.Phase)
	}
	if config.SmallFileThreshold <= 0 || config.SmallWorkers <= 0 || config.LargeWorkers <= 0 || config.MaxBytesPerSecond <= 0 {
		return nil, errors.New("apply limits and worker counts must be positive")
	}
	config.Prefix = prefix
	return &ApplyEngine{api: api, scanner: scanner, config: config, limiter: newByteTokenBucket(config.MaxBytesPerSecond)}, nil
}

func (e *ApplyEngine) Apply(ctx context.Context, source map[string]SourceEntry, target map[string]TargetEntry) error {
	return e.ApplyWithManifest(ctx, source, source, target)
}

// ApplyWithManifest limits mutations to source while using manifest to resolve namespace dependencies.
func (e *ApplyEngine) ApplyWithManifest(ctx context.Context, source, manifest map[string]SourceEntry, target map[string]TargetEntry) error {
	if target == nil {
		target = make(map[string]TargetEntry)
	}
	directories, files, links := make([]SourceEntry, 0), make([]SourceEntry, 0), make([]SourceEntry, 0)
	primaries := matchingHardlinkPrimaries(manifest, target)
	uploadPrimaries := make(map[string]struct{})
	paths := sortedSourcePaths(source)
	for _, sourcePath := range paths {
		entry := source[sourcePath]
		observed, exists := target[sourcePath]
		if err := validateApplyEntry(e.config.Phase, entry, observed, exists); err != nil {
			return err
		}
		switch entry.Kind {
		case EntryDirectory:
			directories = append(directories, entry)
		case EntryRegular:
			if entry.HardlinkKey == "" {
				files = append(files, entry)
				break
			}
			if _, anchored := primaries[entry.HardlinkKey]; anchored {
				links = append(links, entry)
			} else if _, selected := uploadPrimaries[entry.HardlinkKey]; !selected {
				files = append(files, entry)
				primaries[entry.HardlinkKey] = entry.Path
				uploadPrimaries[entry.HardlinkKey] = struct{}{}
			} else {
				links = append(links, entry)
			}
		case EntrySymlink:
			links = append(links, entry)
		default:
			return fmt.Errorf("%w: unsupported source type at %s", ErrUnsafeApply, entry.Path)
		}
	}
	sort.SliceStable(directories, func(i, j int) bool { return pathDepth(directories[i].Path) < pathDepth(directories[j].Path) })
	for _, entry := range directories {
		if _, exists := target[entry.Path]; !exists {
			if err := e.validateSourceEntry(entry); err != nil {
				return err
			}
			mode := entry.Mode
			if e.config.Phase == PhaseSyncing {
				mode = 0o755
			}
			operationDone := e.beginOperation()
			err := e.api.MkdirCtx(ctx, targetRemotePath(e.config.Prefix, entry.Path, true), mode)
			operationDone()
			if err != nil {
				if errors.Is(err, client.ErrConflict) {
					return fmt.Errorf("%w: mkdir target %s: %w", ErrApplyRescan, entry.Path, err)
				}
				return fmt.Errorf("mkdir target %s: %w", entry.Path, err)
			}
			if err := e.validateSourceEntry(entry); err != nil {
				return err
			}
		}
	}
	if err := e.applyFilePools(ctx, files, target); err != nil {
		return err
	}
	for _, entry := range files {
		if entry.HardlinkKey == "" {
			continue
		}
		stat, err := e.api.StatCtx(ctx, targetRemotePath(e.config.Prefix, entry.Path, false))
		if err != nil {
			return fmt.Errorf("reread hardlink upload primary %s: %w", entry.Path, err)
		}
		if stat.IsDir || stat.Revision <= 0 || stat.ResourceID == "" || !stat.HasMode || stat.ChecksumSHA256 == "" {
			return fmt.Errorf("%w: hardlink upload primary %s", ErrApplyVerification, entry.Path)
		}
		target[entry.Path] = TargetEntry{Path: entry.Path, Kind: EntryRegular, Size: stat.Size, Mode: stat.Mode & 0o777, HasMode: true, Revision: stat.Revision, ResourceID: stat.ResourceID, Nlink: stat.Nlink, ChecksumSHA256: stat.ChecksumSHA256}
	}
	for _, entry := range links {
		if err := e.applyLink(ctx, entry, manifest, target, primaries); err != nil {
			return err
		}
	}
	if err := e.applyModes(ctx, append(files, directories...), target); err != nil {
		return err
	}
	return e.applyDeletes(ctx, manifest, target)
}

func matchingHardlinkPrimaries(source map[string]SourceEntry, target map[string]TargetEntry) map[string]string {
	primaries := make(map[string]string)
	for _, sourcePath := range sortedSourcePaths(source) {
		entry := source[sourcePath]
		if entry.HardlinkKey == "" {
			continue
		}
		observed, exists := target[sourcePath]
		if !exists || observed.Kind != EntryRegular || observed.ResourceID == "" || observed.Revision <= 0 || !observed.HasMode || observed.Mode&0o777 != entry.Mode&0o777 || observed.Size != entry.Version.Size || entry.ChecksumSHA256 == "" || observed.ChecksumSHA256 != entry.ChecksumSHA256 {
			continue
		}
		if _, exists := primaries[entry.HardlinkKey]; !exists {
			primaries[entry.HardlinkKey] = entry.Path
		}
	}
	return primaries
}

func validateApplyEntry(phase Phase, source SourceEntry, target TargetEntry, exists bool) error {
	if source.Path == "" || source.Kind == EntrySpecial || source.Version.Kind != "" && source.Version.Kind != source.Kind {
		return fmt.Errorf("%w: invalid source identity at %s", ErrUnsafeApply, source.Path)
	}
	if exists {
		if target.Kind != source.Kind || target.ResourceID == "" || !target.HasMode || target.Kind != EntryDirectory && target.Revision <= 0 {
			return fmt.Errorf("%w: unsafe target identity at %s", ErrUnsafeApply, source.Path)
		}
		if phase == PhaseDualWriteRepairing && target.Mode&0o777 != source.Mode&0o777 {
			return fmt.Errorf("%w: post-T0 metadata update at %s", ErrUnsafeApply, source.Path)
		}
	} else if phase == PhaseDualWriteRepairing && source.Kind == EntryRegular && source.Mode&0o777 != 0o644 {
		return fmt.Errorf("%w: post-T0 create cannot preserve mode at %s", ErrUnsafeApply, source.Path)
	}
	return nil
}

func (e *ApplyEngine) applyFilePools(ctx context.Context, files []SourceEntry, target map[string]TargetEntry) error {
	small, large := make([]SourceEntry, 0), make([]SourceEntry, 0)
	for _, entry := range files {
		if entry.Version.Size < e.config.SmallFileThreshold {
			small = append(small, entry)
		} else {
			large = append(large, entry)
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsByPool := make(chan error, 2)
	go func() { errorsByPool <- e.runFileWorkers(ctx, cancel, small, target, e.config.SmallWorkers) }()
	go func() { errorsByPool <- e.runFileWorkers(ctx, cancel, large, target, e.config.LargeWorkers) }()
	var first error
	for range 2 {
		if err := <-errorsByPool; err != nil && first == nil {
			first = err
			cancel()
		}
	}
	return first
}

func (e *ApplyEngine) runFileWorkers(ctx context.Context, cancel context.CancelFunc, files []SourceEntry, target map[string]TargetEntry, workers int) error {
	tasks := make(chan SourceEntry, len(files))
	for _, entry := range files {
		tasks <- entry
	}
	close(tasks)
	var wait sync.WaitGroup
	var once sync.Once
	var first error
	for range min(workers, max(1, len(files))) {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for entry := range tasks {
				if ctx.Err() != nil {
					return
				}
				observed, exists := target[entry.Path]
				if err := e.applyRegular(ctx, entry, observed, exists); err != nil {
					once.Do(func() { first = err; cancel() })
					return
				}
			}
		}()
	}
	wait.Wait()
	return first
}

func (e *ApplyEngine) applyRegular(ctx context.Context, source SourceEntry, observed TargetEntry, exists bool) error {
	deep, err := e.scanner.ReadStableEntry(ctx, source)
	if err != nil {
		return fmt.Errorf("hash source %s: %w", source.Path, err)
	}
	remote := targetRemotePath(e.config.Prefix, source.Path, false)
	expected, current, err := e.currentTarget(ctx, remote, observed, exists)
	if err != nil {
		return err
	}
	if current != nil && current.Size == deep.Size && current.ChecksumSHA256 == deep.ChecksumSHA256 {
		return e.validateSourceEntry(source)
	}
	file, err := e.openSource(source)
	if err != nil {
		return err
	}
	limited := &limitedSource{File: file, limiter: e.limiterForSize(source.Version.Size), ctx: ctx}
	started := time.Now()
	operationDone := e.beginOperation()
	committed, uploadErr := e.api.WriteStreamConditionalWithChecksumAndPreCompleteCheck(
		ctx, remote, limited, deep.Size, nil, expected, deep.ChecksumSHA256,
		func() error { return e.validateOpenSource(file, source) },
	)
	operationDone()
	if e.onCAS != nil {
		e.onCAS(source, current, expected, started, uploadErr)
	}
	closeErr := file.Close()
	if uploadErr != nil {
		if errors.Is(uploadErr, client.ErrConflict) {
			return fmt.Errorf("%w: conditional upload %s: %w", ErrApplyRescan, source.Path, uploadErr)
		}
		return fmt.Errorf("upload source %s: %w", source.Path, uploadErr)
	}
	if closeErr != nil {
		return closeErr
	}
	modeType := committed.Mode & 0o170000
	if committed.Revision <= 0 || committed.ResourceID == "" || committed.IsDir || !committed.HasMode ||
		(modeType != 0 && modeType != 0o100000) || committed.Size != deep.Size || committed.ChecksumSHA256 != deep.ChecksumSHA256 {
		return fmt.Errorf("%w: post-upload target %s", ErrApplyVerification, source.Path)
	}
	if e.config.Phase == PhaseDualWriteRepairing && committed.Mode&0o777 != source.Mode&0o777 {
		return fmt.Errorf("%w: post-T0 mode mismatch at %s", ErrUnsafeApply, source.Path)
	}
	return e.validateSourceEntry(source)
}

func (e *ApplyEngine) currentTarget(ctx context.Context, remote string, observed TargetEntry, exists bool) (int64, *client.StatResult, error) {
	stat, err := e.api.StatCtx(ctx, remote)
	if !exists {
		if client.IsNotFound(err) {
			return 0, nil, nil
		}
		if err != nil {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("%w: target appeared at %s", ErrApplyRescan, observed.Path)
	}
	if err != nil {
		if client.IsNotFound(err) {
			return 0, nil, fmt.Errorf("%w: target disappeared at %s", ErrApplyRescan, observed.Path)
		}
		return 0, nil, fmt.Errorf("reread target %s: %w", observed.Path, err)
	}
	modeType := stat.Mode & 0o170000
	if stat.IsDir || !stat.HasMode || (modeType != 0 && modeType != 0o100000) || stat.Mode&0o777 != observed.Mode&0o777 ||
		stat.Revision != observed.Revision || stat.ResourceID != observed.ResourceID {
		return 0, nil, fmt.Errorf("%w: target changed at %s", ErrApplyRescan, observed.Path)
	}
	return observed.Revision, stat, nil
}

func (e *ApplyEngine) openSource(source SourceEntry) (*os.File, error) {
	name, err := e.scanner.resolvePath(sourceLocalPath(source))
	if err != nil {
		return nil, err
	}
	file, err := os.Open(name)
	if err != nil {
		return nil, sourceChangeError(err)
	}
	info, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, ErrSourceChanged
	}
	identity, identityErr := e.scanner.identity(name, info)
	if identityErr != nil || identity.version != source.Version {
		_ = file.Close()
		return nil, ErrSourceChanged
	}
	return file, nil
}

func sourceLocalPath(source SourceEntry) string {
	if source.LocalPath != "" {
		return source.LocalPath
	}
	return source.Path
}

func (e *ApplyEngine) validateSourceEntry(source SourceEntry) error {
	return e.validateSource(sourceLocalPath(source), source.Version)
}

func (e *ApplyEngine) validateOpenSource(file *os.File, source SourceEntry) error {
	name, err := e.scanner.resolvePath(sourceLocalPath(source))
	if err != nil {
		return err
	}
	opened, openErr := file.Stat()
	current, pathErr := os.Lstat(name)
	if openErr != nil || pathErr != nil {
		return ErrSourceChanged
	}
	openedIdentity, openIdentityErr := e.scanner.identity(name, opened)
	pathIdentity, pathIdentityErr := e.scanner.identity(name, current)
	if openIdentityErr != nil || pathIdentityErr != nil || openedIdentity.version != source.Version || pathIdentity.version != source.Version {
		return ErrSourceChanged
	}
	return nil
}

func (e *ApplyEngine) validateSource(sourcePath string, expected SourceVersion) error {
	name, err := e.scanner.resolvePath(sourcePath)
	if err != nil {
		return err
	}
	info, err := os.Lstat(name)
	if err != nil {
		return ErrSourceChanged
	}
	identity, err := e.scanner.identity(name, info)
	if err != nil || identity.version != expected {
		return ErrSourceChanged
	}
	return nil
}

func (e *ApplyEngine) applyLink(ctx context.Context, entry SourceEntry, source map[string]SourceEntry, target map[string]TargetEntry, primaries map[string]string) error {
	if err := e.validateSourceEntry(entry); err != nil {
		return err
	}
	observed, exists := target[entry.Path]
	remote := targetRemotePath(e.config.Prefix, entry.Path, false)
	matched := false
	if entry.Kind == EntrySymlink {
		matched = exists && observed.ChecksumSHA256 != "" && observed.ChecksumSHA256 == entry.ChecksumSHA256
	} else {
		primaryPath := primaries[entry.HardlinkKey]
		primary, ok := source[primaryPath]
		if !ok {
			return fmt.Errorf("%w: missing hardlink primary for %s", ErrUnsafeApply, entry.Path)
		}
		deep, err := e.scanner.ReadStableEntry(ctx, primary)
		if err != nil {
			return err
		}
		primaryObserved, primaryExists := target[primary.Path]
		if !primaryExists {
			return fmt.Errorf("%w: hardlink primary disappeared at %s", ErrApplyRescan, primary.Path)
		}
		_, primaryStat, err := e.currentTarget(ctx, targetRemotePath(e.config.Prefix, primary.Path, false), primaryObserved, true)
		if err != nil {
			return fmt.Errorf("hardlink primary %s: %w", primary.Path, err)
		}
		if primaryStat == nil || primaryStat.ResourceID == "" || primaryStat.Size != deep.Size || primaryStat.ChecksumSHA256 != deep.ChecksumSHA256 {
			return fmt.Errorf("%w: hardlink primary identity at %s", ErrApplyVerification, primary.Path)
		}
		matched = exists && observed.ResourceID == primaryStat.ResourceID
		if !matched && (!exists || e.config.Phase == PhaseSyncing) {
			if exists {
				operationDone := e.beginOperation()
				err := e.api.DeleteFileCtx(ctx, remote)
				operationDone()
				if err != nil {
					return err
				}
			}
			if err := e.validateSourceEntry(entry); err != nil {
				return err
			}
			operationDone := e.beginOperation()
			err = e.api.HardlinkCtx(ctx, targetRemotePath(e.config.Prefix, primary.Path, false), remote)
			operationDone()
			if err != nil {
				if errors.Is(err, client.ErrConflict) {
					return fmt.Errorf("%w: hardlink target %s: %w", ErrApplyRescan, entry.Path, err)
				}
				return err
			}
			return e.validateSourceEntry(entry)
		}
	}
	if matched {
		return nil
	}
	if exists {
		if e.config.Phase != PhaseSyncing {
			return fmt.Errorf("%w: post-T0 link replacement at %s", ErrUnsafeApply, entry.Path)
		}
		operationDone := e.beginOperation()
		err := e.api.DeleteFileCtx(ctx, remote)
		operationDone()
		if err != nil {
			return err
		}
	}
	if err := e.validateSourceEntry(entry); err != nil {
		return err
	}
	operationDone := e.beginOperation()
	err := e.api.SymlinkCtx(ctx, entry.LinkTarget, remote)
	operationDone()
	if err != nil {
		if errors.Is(err, client.ErrConflict) {
			return fmt.Errorf("%w: symlink target %s: %w", ErrApplyRescan, entry.Path, err)
		}
		return err
	}
	return e.validateSourceEntry(entry)
}

func (e *ApplyEngine) applyModes(ctx context.Context, entries []SourceEntry, target map[string]TargetEntry) error {
	if e.config.Phase != PhaseSyncing {
		return nil
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].Kind == entries[j].Kind {
			return pathDepth(entries[i].Path) > pathDepth(entries[j].Path)
		}
		return entries[i].Kind == EntryRegular
	})
	for _, entry := range entries {
		observed, exists := target[entry.Path]
		if exists && observed.Mode&0o777 == entry.Mode&0o777 {
			continue
		}
		if err := e.validateSourceEntry(entry); err != nil {
			return err
		}
		operationDone := e.beginOperation()
		err := e.api.ChmodCtx(ctx, targetRemotePath(e.config.Prefix, entry.Path, entry.Kind == EntryDirectory), entry.Mode&0o777)
		operationDone()
		if err != nil {
			return fmt.Errorf("chmod target %s: %w", entry.Path, err)
		}
		if err := e.validateSourceEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

func (e *ApplyEngine) applyDeletes(ctx context.Context, source map[string]SourceEntry, target map[string]TargetEntry) error {
	if e.config.Phase != PhaseSyncing {
		return nil
	}
	paths := make([]string, 0)
	for path := range target {
		if _, exists := source[path]; !exists {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	sort.SliceStable(paths, func(i, j int) bool { return pathDepth(paths[i]) > pathDepth(paths[j]) })
	for _, path := range paths {
		entry := target[path]
		if entry.ResourceID == "" || entry.Kind != EntryDirectory && entry.Revision <= 0 || entry.Kind == EntrySpecial {
			return fmt.Errorf("%w: unsafe delete at %s", ErrUnsafeApply, path)
		}
		remote := targetRemotePath(e.config.Prefix, path, entry.Kind == EntryDirectory)
		var err error
		operationDone := e.beginOperation()
		if entry.Kind == EntryDirectory {
			err = e.api.DeleteDirCtx(ctx, remote)
		} else {
			err = e.api.DeleteFileCtx(ctx, remote)
		}
		operationDone()
		if err != nil {
			return fmt.Errorf("delete target %s: %w", path, err)
		}
	}
	return nil
}

func sortedSourcePaths(source map[string]SourceEntry) []string {
	paths := make([]string, 0, len(source))
	for path := range source {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func pathDepth(value string) int { return strings.Count(strings.Trim(value, "/"), "/") }

func (e *ApplyEngine) beginOperation() func() {
	if e.onOperationStart != nil {
		e.onOperationStart()
	}
	return func() {
		if e.onOperationDone != nil {
			e.onOperationDone()
		}
	}
}

func (e *ApplyEngine) limiterForSize(int64) *byteTokenBucket { return e.limiter }

type limitedSource struct {
	*os.File
	limiter *byteTokenBucket
	ctx     context.Context
}

func (r *limitedSource) Read(buffer []byte) (int, error) {
	count, err := r.File.Read(buffer)
	if count > 0 {
		if waitErr := r.limiter.WaitN(r.ctx, count); waitErr != nil {
			return 0, waitErr
		}
	}
	return count, err
}

func (r *limitedSource) ReadAt(buffer []byte, offset int64) (int, error) {
	count, err := r.File.ReadAt(buffer, offset)
	if count > 0 {
		if waitErr := r.limiter.WaitN(r.ctx, count); waitErr != nil {
			return 0, waitErr
		}
	}
	return count, err
}

type byteTokenBucket struct {
	mu           sync.Mutex
	rate, tokens float64
	last         time.Time
}

func newByteTokenBucket(bytesPerSecond int64) *byteTokenBucket {
	rate := float64(bytesPerSecond)
	return &byteTokenBucket{rate: rate, tokens: rate, last: time.Now()}
}

func (b *byteTokenBucket) WaitN(ctx context.Context, bytes int) error {
	for bytes > 0 {
		chunk := min(bytes, int(b.rate))
		delay := b.reserveAt(time.Now(), chunk)
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		bytes -= chunk
	}
	return nil
}

func (b *byteTokenBucket) reserveAt(now time.Time, bytes int) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	base := time.Duration(0)
	if now.Before(b.last) {
		base = b.last.Sub(now)
		now = b.last
	} else {
		b.tokens = min(b.rate, b.tokens+now.Sub(b.last).Seconds()*b.rate)
		b.last = now
	}
	need := float64(bytes)
	if b.tokens >= need {
		b.tokens -= need
		return base
	}
	delay := time.Duration((need - b.tokens) / b.rate * float64(time.Second))
	b.tokens = 0
	b.last = now.Add(delay)
	return base + delay
}

var _ io.ReaderAt = (*limitedSource)(nil)
