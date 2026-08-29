package migration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

type manifestPageClient interface {
	ManifestPageCtx(context.Context, string, string, int) (client.ManifestPage, error)
}

type manifestConfig struct {
	GenerationID    string
	RoundID         string
	Phase           Phase
	Identity        generationIdentity
	TargetPrefix    string
	PageLimit       int
	SortBufferBytes int64
	SortFanIn       int
	Budget          *memoryBudget
}

type manifestResult struct {
	Metadata               generationMetadata
	FilteredControlEntries int64
}

type manifestBuilder struct {
	api        manifestPageClient
	store      *generationStore
	config     manifestConfig
	clock      func() time.Time
	onProgress func(generationMetadata)
}

func newManifestBuilder(api manifestPageClient, store *generationStore, config manifestConfig) (*manifestBuilder, error) {
	if api == nil || store == nil {
		return nil, fmt.Errorf("manifest builder requires client and generation store")
	}
	if err := validateGenerationIdentifier(config.GenerationID); err != nil {
		return nil, fmt.Errorf("manifest generation ID: %w", err)
	}
	if err := validateGenerationIdentifier(config.RoundID); err != nil {
		return nil, fmt.Errorf("manifest round ID: %w", err)
	}
	if config.Phase != PhaseSyncing && config.Phase != PhaseDualWriteRepairing && config.Phase != PhaseCutoverReady {
		return nil, fmt.Errorf("manifest phase is invalid")
	}
	if err := validateGenerationIdentity(config.Identity); err != nil || config.Identity.JobID != store.jobID {
		return nil, fmt.Errorf("manifest identity is invalid")
	}
	prefix, err := validateTargetPrefix(config.TargetPrefix)
	if err != nil || prefix != config.TargetPrefix || config.Identity.Prefix != prefix {
		return nil, fmt.Errorf("manifest target prefix is invalid")
	}
	if config.PageLimit <= 0 || config.PageLimit > client.MaxManifestPageEntries || config.SortBufferBytes <= 0 || config.SortFanIn < 2 {
		return nil, fmt.Errorf("manifest page and sort limits are invalid")
	}
	return &manifestBuilder{api: api, store: store, config: config, clock: time.Now}, nil
}

func (b *manifestBuilder) Build(ctx context.Context, resume *generationMetadata) (manifestResult, error) {
	if b.config.Budget != nil {
		release, err := b.config.Budget.Acquire(ctx, 64<<20)
		if err != nil {
			return manifestResult{}, err
		}
		defer release()
	}
	metadata := generationMetadata{
		FormatVersion: generationFormatVersion, GenerationID: b.config.GenerationID, RoundID: b.config.RoundID,
		Phase: b.config.Phase, Identity: b.config.Identity, CreatedAt: b.clock().UTC(),
		Stages: map[generationStage]generationStageMetadata{stageTargetRaw: {Complete: false}},
	}
	var metadataRevision int64
	if b.config.Phase == PhaseSyncing && resume != nil {
		if resume.GenerationID != b.config.GenerationID {
			return manifestResult{}, ErrGenerationMismatch
		}
		loaded, revision, err := b.store.LoadMetadata(ctx, resume.GenerationID, b.config.Identity)
		if err != nil {
			return manifestResult{}, err
		}
		if loaded.Phase != PhaseSyncing {
			return manifestResult{}, ErrGenerationMismatch
		}
		metadata, metadataRevision = loaded, revision
	}
	rawStage := metadata.Stages[stageTargetRaw]
	filteredControl := int64(0)
	for !rawStage.Complete {
		page, err := b.api.ManifestPageCtx(ctx, manifestRequestPrefix(b.config.TargetPrefix), metadata.ManifestCursor, b.config.PageLimit)
		if err != nil {
			return manifestResult{}, err
		}
		pageRecords := make([]generationRecord, 0, len(page.Entries))
		for _, entry := range page.Entries {
			record, filtered, err := b.targetRecord(entry)
			if err != nil {
				return manifestResult{}, err
			}
			if filtered {
				filteredControl++
				continue
			}
			pageRecords = append(pageRecords, record)
		}
		pageNumber := metadata.ManifestPages + 1
		metadata.ManifestRawEntries += int64(len(page.Entries))
		metadata.ManifestResponseBytes += page.ResponseBytes
		if len(page.Entries) == 0 {
			metadata.ManifestEmptyPages++
		}
		if !page.Done {
			metadata.ManifestCursorAdvances++
		}
		metadata.ManifestLastPageAt = b.clock().UTC()
		if len(pageRecords) > 0 {
			sorter, err := newExternalSorter(b.store, externalSortConfig{
				GenerationID: b.config.GenerationID, Stage: stageTargetRaw, Kind: recordTarget,
				IDPrefix: fmt.Sprintf("target-raw-%06d", pageNumber), MaxBufferBytes: b.config.SortBufferBytes, FanIn: b.config.SortFanIn, Budget: b.config.Budget,
			})
			if err != nil {
				return manifestResult{}, err
			}
			sorted, err := sorter.Sort(ctx, &recordSliceReader{records: pageRecords})
			if err != nil {
				return manifestResult{}, err
			}
			rawStage.Chunks = append(rawStage.Chunks, sorted.Chunks...)
			rawStage.RecordCount += int64(len(pageRecords))
			metadata.ManifestSortRuns += int64(sorted.Stats.InitialRuns)
		}
		metadata.ManifestPages = pageNumber
		metadata.ManifestCursor = page.NextCursor
		metadata.EntryCount += int64(len(pageRecords))
		rawStage.Complete = page.Done
		metadata.Stages[stageTargetRaw] = rawStage
		revision, err := b.store.SaveMetadata(ctx, metadata, metadataRevision)
		if err != nil {
			return manifestResult{}, err
		}
		metadataRevision = revision
		if b.onProgress != nil {
			b.onProgress(metadata)
		}
		if err := injectMigrationLargeStageFault("target_raw_page"); err != nil {
			return manifestResult{}, err
		}
	}

	input := &sortRunReader{ctx: ctx, store: b.store, generationID: b.config.GenerationID, chunks: rawStage.Chunks}
	sorter, err := newExternalSorter(b.store, externalSortConfig{
		GenerationID: b.config.GenerationID, Stage: stageTarget, Kind: recordTarget, IDPrefix: "target-final",
		MaxBufferBytes: b.config.SortBufferBytes, FanIn: b.config.SortFanIn, Budget: b.config.Budget,
	})
	if err != nil {
		return manifestResult{}, err
	}
	final, err := sorter.Sort(ctx, input)
	if err != nil {
		return manifestResult{}, err
	}
	if err := injectMigrationLargeStageFault("target_sort"); err != nil {
		return manifestResult{}, err
	}
	metadata.Stages[stageTarget] = completedStage(final.Chunks)
	metadata.ManifestSortRuns += int64(final.Stats.InitialRuns)
	if _, err := b.store.SaveMetadata(ctx, metadata, metadataRevision); err != nil {
		return manifestResult{}, err
	}
	if err := injectMigrationLargeStageFault("target_publish"); err != nil {
		return manifestResult{}, err
	}
	if err := b.store.PublishComplete(ctx, metadata); err != nil {
		return manifestResult{}, err
	}
	return manifestResult{Metadata: metadata, FilteredControlEntries: filteredControl}, nil
}

type recordSliceReader struct {
	records []generationRecord
	index   int
}

func (r *recordSliceReader) Next() (generationRecord, bool, error) {
	if r.index >= len(r.records) {
		return generationRecord{}, false, nil
	}
	record := r.records[r.index]
	r.index++
	return record, true, nil
}

func (b *manifestBuilder) targetRecord(entry client.ManifestEntry) (generationRecord, bool, error) {
	logical, err := manifestLogicalPath(b.config.TargetPrefix, entry.Path)
	if err != nil {
		return generationRecord{}, false, err
	}
	controlPath := strings.TrimSuffix(logical, "/")
	if controlPath == ControlPrefix || strings.HasPrefix(controlPath, ControlPrefix+"/") {
		return generationRecord{}, true, nil
	}
	var kind EntryKind
	switch entry.Type {
	case client.ManifestEntryRegular:
		kind = EntryRegular
	case client.ManifestEntryDirectory:
		kind = EntryDirectory
		logical = strings.TrimSuffix(logical, "/")
	case client.ManifestEntrySymlink:
		kind = EntrySymlink
	default:
		return generationRecord{}, false, fmt.Errorf("unsupported Manifest type %q", entry.Type)
	}
	record := &targetGenerationRecord{
		Path: logical, Kind: kind, Size: entry.Size, Mode: clonePointer(entry.Mode),
		MetadataComplete: entry.MetadataComplete, IdentityKind: string(entry.IdentityKind),
		Revision: clonePointer(entry.Revision), ResourceID: entry.ResourceID, Nlink: entry.Nlink,
		ChecksumSHA256: clonePointer(entry.ChecksumSHA256),
	}
	return generationRecord{Key: logical, Target: record}, false, nil
}

func manifestRequestPrefix(prefix string) string {
	if prefix == "/" {
		return prefix
	}
	return prefix + "/"
}

func manifestLogicalPath(prefix, remote string) (string, error) {
	if prefix == "/" {
		return remote, nil
	}
	base := prefix + "/"
	if !strings.HasPrefix(remote, base) || remote == base {
		return "", fmt.Errorf("manifest path %q escapes prefix %q", remote, prefix)
	}
	return "/" + strings.TrimPrefix(remote, base), nil
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
