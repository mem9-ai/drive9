package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
)

const maxGenerationMetadataBytes int64 = 4 << 20

var (
	ErrGenerationIncomplete = errors.New("migration generation is incomplete")
	ErrGenerationInvalid    = errors.New("migration generation is invalid")
	ErrGenerationMismatch   = errors.New("migration generation identity mismatch")
)

type generationStage string

const (
	stageSource            generationStage = "source"
	stageDirectoryIdentity generationStage = "directory-identities"
	stageTargetRaw         generationStage = "target-raw"
	stageTarget            generationStage = "target"
	stageDiff              generationStage = "diff"
)

type generationIdentity struct {
	JobID         string `json:"job_id"`
	ConfigHash    string `json:"config_hash"`
	VolumeID      string `json:"volume_id"`
	EBSRoot       string `json:"ebs_root"`
	SourceSubpath string `json:"source_subpath"`
	SourceRoot    string `json:"source_root"`
	Endpoint      string `json:"endpoint"`
	SpaceRef      string `json:"space_ref"`
	Prefix        string `json:"prefix"`
}

type generationStageMetadata struct {
	Complete    bool              `json:"complete"`
	RecordCount int64             `json:"record_count"`
	Chunks      []chunkDescriptor `json:"chunks"`
}

type generationMetadata struct {
	FormatVersion          string                                      `json:"format_version"`
	GenerationID           string                                      `json:"generation_id"`
	RoundID                string                                      `json:"round_id"`
	Phase                  Phase                                       `json:"phase"`
	Identity               generationIdentity                          `json:"identity"`
	SourceGeneration       string                                      `json:"source_generation_id,omitempty"`
	TargetGeneration       string                                      `json:"target_generation_id,omitempty"`
	ManifestCursor         string                                      `json:"manifest_cursor,omitempty"`
	ManifestPages          int64                                       `json:"manifest_pages,omitempty"`
	ManifestRawEntries     int64                                       `json:"manifest_raw_entries,omitempty"`
	ManifestResponseBytes  int64                                       `json:"manifest_response_bytes,omitempty"`
	ManifestEmptyPages     int64                                       `json:"manifest_empty_pages,omitempty"`
	ManifestCursorAdvances int64                                       `json:"manifest_cursor_advances,omitempty"`
	ManifestSortRuns       int64                                       `json:"manifest_sort_runs,omitempty"`
	ManifestLastPageAt     time.Time                                   `json:"manifest_last_page_at,omitempty"`
	EntryCount             int64                                       `json:"entry_count"`
	TargetEntryCount       int64                                       `json:"target_entry_count,omitempty"`
	DirectoryCount         int64                                       `json:"directory_count"`
	FileCount              int64                                       `json:"file_count"`
	LogicalBytes           int64                                       `json:"logical_bytes"`
	WarningCount           int64                                       `json:"warning_count,omitempty"`
	BlockerCount           int64                                       `json:"blocker_count,omitempty"`
	SourceScanDurationMS   int64                                       `json:"source_scan_duration_ms,omitempty"`
	SourceHashDurationMS   int64                                       `json:"source_hash_duration_ms,omitempty"`
	SourceQueueCapacity    int64                                       `json:"source_queue_capacity,omitempty"`
	HashReuseCount         int64                                       `json:"hash_reuse_count"`
	HashNewCount           int64                                       `json:"hash_new_count"`
	FindingCounts          map[FindingKind]int64                       `json:"finding_counts,omitempty"`
	WorkCounts             map[string]int64                            `json:"work_counts,omitempty"`
	CreatedAt              time.Time                                   `json:"created_at"`
	Stages                 map[generationStage]generationStageMetadata `json:"stages"`
}

type generationCompleteMarker struct {
	FormatVersion  string            `json:"format_version"`
	GenerationID   string            `json:"generation_id"`
	MetadataSHA256 string            `json:"metadata_sha256"`
	Chunks         []chunkDescriptor `json:"chunks"`
	PublishedAt    time.Time         `json:"published_at"`
}

type generationObjectStore interface {
	EnsureDirectory(context.Context, string) error
	Put(context.Context, string, []byte, int64) (int64, error)
	Get(context.Context, string, int64) ([]byte, int64, error)
	List(context.Context, string) ([]generationObjectInfo, error)
	DeleteFile(context.Context, string) error
	DeleteDirectory(context.Context, string) error
}

type generationObjectInfo struct {
	Name      string
	Directory bool
}

type clientGenerationObjects struct {
	api *client.Client
}

func (s clientGenerationObjects) EnsureDirectory(ctx context.Context, path string) error {
	return s.api.MkdirCtx(ctx, path, 0o700)
}

func (s clientGenerationObjects) Put(ctx context.Context, path string, body []byte, expectedRevision int64) (int64, error) {
	stat, err := s.api.WriteStreamConditionalWithChecksum(ctx, path, bytes.NewReader(body), int64(len(body)), nil, expectedRevision, checksumHex(body))
	if err != nil {
		return 0, err
	}
	if stat == nil || stat.Revision <= 0 || stat.Size != int64(len(body)) || stat.ChecksumSHA256 != checksumHex(body) {
		return 0, fmt.Errorf("generation object post-write verification failed")
	}
	return stat.Revision, nil
}

func (s clientGenerationObjects) Get(ctx context.Context, path string, maxBytes int64) ([]byte, int64, error) {
	before, err := s.api.StatCtx(ctx, path)
	if err != nil {
		return nil, 0, err
	}
	if before.IsDir || before.Revision <= 0 || before.Size < 0 || before.Size > maxBytes {
		return nil, 0, fmt.Errorf("generation object metadata is invalid")
	}
	stream, err := s.api.ReadStreamRange(ctx, path, 0, maxBytes+1)
	if err != nil {
		return nil, 0, err
	}
	body, readErr := io.ReadAll(io.LimitReader(stream, maxBytes+1))
	closeErr := stream.Close()
	if readErr != nil {
		return nil, 0, readErr
	}
	if closeErr != nil {
		return nil, 0, closeErr
	}
	if int64(len(body)) > maxBytes || int64(len(body)) != before.Size {
		return nil, 0, fmt.Errorf("generation object size changed during read")
	}
	after, err := s.api.StatCtx(ctx, path)
	if err != nil || after.IsDir || after.Revision != before.Revision || after.Size != before.Size || after.ResourceID != before.ResourceID {
		return nil, 0, fmt.Errorf("generation object changed during read")
	}
	return body, before.Revision, nil
}

func (s clientGenerationObjects) List(ctx context.Context, path string) ([]generationObjectInfo, error) {
	entries, err := s.api.ListCtx(ctx, path)
	if err != nil {
		return nil, err
	}
	result := make([]generationObjectInfo, len(entries))
	for index, entry := range entries {
		result[index] = generationObjectInfo{Name: entry.Name, Directory: entry.IsDir}
	}
	return result, nil
}

func (s clientGenerationObjects) DeleteFile(ctx context.Context, path string) error {
	return s.api.DeleteFileCtx(ctx, path)
}

func (s clientGenerationObjects) DeleteDirectory(ctx context.Context, path string) error {
	return s.api.DeleteDirCtx(ctx, path)
}

type generationStore struct {
	objects generationObjectStore
	jobID   string
}

func newGenerationStore(objects generationObjectStore, jobID string) (*generationStore, error) {
	if objects == nil {
		return nil, fmt.Errorf("generation store requires object store")
	}
	if err := validateJobID(jobID); err != nil {
		return nil, err
	}
	return &generationStore{objects: objects, jobID: jobID}, nil
}

func NewGenerationStore(api *client.Client, jobID string) (*generationStore, error) {
	if api == nil {
		return nil, fmt.Errorf("generation store requires client")
	}
	return newGenerationStore(clientGenerationObjects{api: api}, jobID)
}

func (s *generationStore) SaveChunk(ctx context.Context, generationID string, stage generationStage, chunkID string, body []byte, descriptor chunkDescriptor) error {
	if err := s.validateLocation(generationID, stage, chunkID); err != nil {
		return err
	}
	descriptor.Stage = stage
	if descriptor.ID != chunkID || descriptor.Kind != recordKindForStage(stage) {
		return fmt.Errorf("%w: chunk descriptor location mismatch", ErrGenerationInvalid)
	}
	reader, err := newChunkReader(body, descriptor)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrGenerationInvalid, err)
	}
	for {
		_, ok, err := reader.Next()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrGenerationInvalid, err)
		}
		if !ok {
			break
		}
	}
	if err := s.ensureDirectories(ctx, generationID, stage); err != nil {
		return err
	}
	return s.putExact(ctx, s.chunkPath(generationID, stage, chunkID), body, 0)
}

func (s *generationStore) SaveMetadata(ctx context.Context, metadata generationMetadata, expectedRevision int64) (int64, error) {
	if err := s.validateMetadata(metadata, false); err != nil {
		return 0, err
	}
	body, err := json.Marshal(metadata)
	if err != nil {
		return 0, fmt.Errorf("encode generation metadata: %w", err)
	}
	if int64(len(body)) > maxGenerationMetadataBytes {
		return 0, fmt.Errorf("generation metadata exceeds %d bytes", maxGenerationMetadataBytes)
	}
	if err := s.ensureDirectories(ctx, metadata.GenerationID, ""); err != nil {
		return 0, err
	}
	path := s.metadataPath(metadata.GenerationID)
	revision, err := s.objects.Put(ctx, path, body, expectedRevision)
	if err == nil {
		return revision, nil
	}
	observed, observedRevision, readErr := s.objects.Get(ctx, path, maxGenerationMetadataBytes)
	if readErr == nil && bytes.Equal(observed, body) {
		return observedRevision, nil
	}
	return 0, fmt.Errorf("save generation metadata: %w", err)
}

func (s *generationStore) PublishComplete(ctx context.Context, metadata generationMetadata) error {
	if err := s.validateMetadata(metadata, true); err != nil {
		return err
	}
	metadataBody, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("encode generation metadata: %w", err)
	}
	storedMetadata, _, err := s.objects.Get(ctx, s.metadataPath(metadata.GenerationID), maxGenerationMetadataBytes)
	if err != nil || !bytes.Equal(storedMetadata, metadataBody) {
		return fmt.Errorf("%w: stored metadata does not match complete generation", ErrGenerationInvalid)
	}
	marker := generationCompleteMarker{
		FormatVersion:  generationFormatVersion,
		GenerationID:   metadata.GenerationID,
		MetadataSHA256: checksumHex(metadataBody),
		Chunks:         flattenGenerationChunks(metadata),
		PublishedAt:    time.Now().UTC(),
	}
	body, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode generation complete marker: %w", err)
	}
	if err := s.putExact(ctx, s.completePath(metadata.GenerationID), body, 0); err != nil {
		return fmt.Errorf("publish generation complete marker: %w", err)
	}
	return nil
}

func (s *generationStore) LoadComplete(ctx context.Context, generationID string, expected generationIdentity) (generationMetadata, error) {
	if err := validateGenerationIdentifier(generationID); err != nil {
		return generationMetadata{}, fmt.Errorf("%w: generation ID", ErrGenerationInvalid)
	}
	markerBody, _, err := s.objects.Get(ctx, s.completePath(generationID), maxGenerationMetadataBytes)
	if err != nil {
		return generationMetadata{}, fmt.Errorf("%w: complete marker: %v", ErrGenerationIncomplete, err)
	}
	var marker generationCompleteMarker
	if err := json.Unmarshal(markerBody, &marker); err != nil || marker.FormatVersion != generationFormatVersion || marker.GenerationID != generationID {
		return generationMetadata{}, fmt.Errorf("%w: malformed complete marker", ErrGenerationInvalid)
	}
	metadataBody, _, err := s.objects.Get(ctx, s.metadataPath(generationID), maxGenerationMetadataBytes)
	if err != nil || checksumHex(metadataBody) != marker.MetadataSHA256 {
		return generationMetadata{}, fmt.Errorf("%w: metadata missing or checksum mismatch", ErrGenerationInvalid)
	}
	var metadata generationMetadata
	if err := json.Unmarshal(metadataBody, &metadata); err != nil {
		return generationMetadata{}, fmt.Errorf("%w: decode metadata: %v", ErrGenerationInvalid, err)
	}
	if err := s.validateMetadata(metadata, true); err != nil {
		return generationMetadata{}, err
	}
	if metadata.Identity != expected {
		return generationMetadata{}, ErrGenerationMismatch
	}
	if !equalChunkDescriptors(marker.Chunks, flattenGenerationChunks(metadata)) {
		return generationMetadata{}, fmt.Errorf("%w: marker chunk list mismatch", ErrGenerationInvalid)
	}
	if err := s.validateChunkPayloads(ctx, generationID, marker.Chunks); err != nil {
		return generationMetadata{}, err
	}
	return metadata, nil
}

func (s *generationStore) LoadMetadata(ctx context.Context, generationID string, expected generationIdentity) (generationMetadata, int64, error) {
	if err := validateGenerationIdentifier(generationID); err != nil {
		return generationMetadata{}, 0, fmt.Errorf("%w: generation ID", ErrGenerationInvalid)
	}
	body, revision, err := s.objects.Get(ctx, s.metadataPath(generationID), maxGenerationMetadataBytes)
	if err != nil {
		return generationMetadata{}, 0, fmt.Errorf("%w: metadata: %v", ErrGenerationIncomplete, err)
	}
	var metadata generationMetadata
	if err := json.Unmarshal(body, &metadata); err != nil {
		return generationMetadata{}, 0, fmt.Errorf("%w: decode metadata: %v", ErrGenerationInvalid, err)
	}
	if err := s.validateMetadata(metadata, false); err != nil {
		return generationMetadata{}, 0, err
	}
	if metadata.GenerationID != generationID || metadata.Identity != expected {
		return generationMetadata{}, 0, ErrGenerationMismatch
	}
	if err := s.validateChunkPayloads(ctx, generationID, flattenGenerationChunks(metadata)); err != nil {
		return generationMetadata{}, 0, err
	}
	return metadata, revision, nil
}

func (s *generationStore) validateChunkPayloads(ctx context.Context, generationID string, descriptors []chunkDescriptor) error {
	for _, descriptor := range descriptors {
		reader, err := s.OpenChunk(ctx, generationID, descriptor)
		if err != nil {
			return err
		}
		for {
			_, ok, err := reader.Next()
			if err != nil {
				return fmt.Errorf("%w: chunk %s: %v", ErrGenerationInvalid, descriptor.ID, err)
			}
			if !ok {
				break
			}
		}
	}
	return nil
}

func (s *generationStore) OpenChunk(ctx context.Context, generationID string, descriptor chunkDescriptor) (*chunkReader, error) {
	if err := s.validateLocation(generationID, descriptor.Stage, descriptor.ID); err != nil {
		return nil, err
	}
	if descriptor.Kind != recordKindForStage(descriptor.Stage) {
		return nil, fmt.Errorf("%w: chunk kind does not match stage", ErrGenerationInvalid)
	}
	body, _, err := s.objects.Get(ctx, s.chunkPath(generationID, descriptor.Stage, descriptor.ID), maxChunkPayloadBytes)
	if err != nil {
		return nil, fmt.Errorf("%w: read chunk %s: %v", ErrGenerationInvalid, descriptor.ID, err)
	}
	reader, err := newChunkReader(body, descriptor)
	if err != nil {
		return nil, fmt.Errorf("%w: chunk %s: %v", ErrGenerationInvalid, descriptor.ID, err)
	}
	return reader, nil
}

// PruneReplaced removes older generations only after every retained replacement
// has a durable complete marker matching its metadata and chunk manifest.
func (s *generationStore) PruneReplaced(ctx context.Context, replacements ...generationMetadata) error {
	if len(replacements) == 0 {
		return fmt.Errorf("generation prune requires at least one replacement")
	}
	keep := make(map[string]struct{}, len(replacements))
	for _, metadata := range replacements {
		if err := s.validateMetadata(metadata, true); err != nil {
			return err
		}
		metadataBody, err := json.Marshal(metadata)
		if err != nil {
			return fmt.Errorf("encode replacement generation metadata: %w", err)
		}
		markerBody, _, err := s.objects.Get(ctx, s.completePath(metadata.GenerationID), maxGenerationMetadataBytes)
		if err != nil {
			return fmt.Errorf("%w: replacement complete marker: %v", ErrGenerationIncomplete, err)
		}
		var marker generationCompleteMarker
		if err := json.Unmarshal(markerBody, &marker); err != nil || marker.FormatVersion != generationFormatVersion ||
			marker.GenerationID != metadata.GenerationID || marker.MetadataSHA256 != checksumHex(metadataBody) ||
			!equalChunkDescriptors(marker.Chunks, flattenGenerationChunks(metadata)) {
			return fmt.Errorf("%w: replacement complete marker mismatch", ErrGenerationInvalid)
		}
		keep[metadata.GenerationID] = struct{}{}
	}
	generationIDs, err := s.listGenerationIDs(ctx)
	if err != nil {
		return err
	}
	for _, generationID := range generationIDs {
		if _, retained := keep[generationID]; retained {
			continue
		}
		if err := s.cleanupDirectory(ctx, s.generationBase(generationID)); err != nil {
			return fmt.Errorf("prune generation %s: %w", generationID, err)
		}
	}
	return nil
}

func (s *generationStore) CleanupVerification(ctx context.Context) error {
	base := s.verificationBase()
	return s.cleanupDirectory(ctx, base)
}

func (s *generationStore) cleanupDirectory(ctx context.Context, directory string) error {
	base := s.verificationBase()
	if directory != base && !strings.HasPrefix(directory, base) {
		return fmt.Errorf("generation cleanup path escapes Job verification root")
	}
	entries, err := s.objects.List(ctx, directory)
	if err != nil {
		if client.IsNotFound(err) {
			return nil
		}
		return err
	}
	for _, entry := range entries {
		if entry.Name == "" || strings.ContainsAny(entry.Name, "/\\\x00") || entry.Name == "." || entry.Name == ".." {
			return fmt.Errorf("generation cleanup received unsafe child name")
		}
		path := directory + entry.Name
		if entry.Directory {
			if err := s.cleanupDirectory(ctx, path+"/"); err != nil {
				return err
			}
		} else if err := s.objects.DeleteFile(ctx, path); err != nil && !client.IsNotFound(err) {
			return err
		}
	}
	if err := s.objects.DeleteDirectory(ctx, directory); err != nil && !client.IsNotFound(err) {
		return err
	}
	return nil
}

func (s *generationStore) FindLatestCompleteSource(ctx context.Context, expected generationIdentity) (*generationMetadata, error) {
	generations, err := s.listGenerationIDs(ctx)
	if err != nil {
		return nil, err
	}
	var latest *generationMetadata
	for _, generationID := range generations {
		metadata, err := s.LoadComplete(ctx, generationID, expected)
		if err != nil {
			continue
		}
		stage, exists := metadata.Stages[stageSource]
		if !exists || !stage.Complete {
			continue
		}
		if latest == nil || metadata.CreatedAt.After(latest.CreatedAt) {
			copy := metadata
			latest = &copy
		}
	}
	return latest, nil
}

func (s *generationStore) FindResumableTarget(ctx context.Context, expected generationIdentity) (*generationMetadata, error) {
	generations, err := s.listGenerationIDs(ctx)
	if err != nil {
		return nil, err
	}
	var latest *generationMetadata
	for _, generationID := range generations {
		metadata, _, err := s.LoadMetadata(ctx, generationID, expected)
		if err != nil || metadata.Phase != PhaseSyncing || metadata.ManifestPages == 0 {
			continue
		}
		raw, exists := metadata.Stages[stageTargetRaw]
		if !exists {
			continue
		}
		if target, complete := metadata.Stages[stageTarget]; complete && target.Complete {
			continue
		}
		if latest == nil || metadata.CreatedAt.After(latest.CreatedAt) {
			copy := metadata
			copy.Stages[stageTargetRaw] = raw
			latest = &copy
		}
	}
	return latest, nil
}

func (s *generationStore) listGenerationIDs(ctx context.Context) ([]string, error) {
	entries, err := s.objects.List(ctx, s.verificationBase())
	if err != nil {
		if client.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	var result []string
	for _, entry := range entries {
		if !entry.Directory || validateGenerationIdentifier(entry.Name) != nil {
			continue
		}
		result = append(result, entry.Name)
	}
	return result, nil
}

func (s *generationStore) verificationBase() string {
	return ControlPrefix + "/jobs/" + s.jobID + "/verification/"
}

func (s *generationStore) validateMetadata(metadata generationMetadata, requireComplete bool) error {
	if metadata.FormatVersion != generationFormatVersion || metadata.GenerationID == "" || metadata.RoundID == "" || metadata.CreatedAt.IsZero() {
		return fmt.Errorf("%w: incomplete metadata header", ErrGenerationInvalid)
	}
	if err := validateGenerationIdentifier(metadata.GenerationID); err != nil {
		return fmt.Errorf("%w: invalid generation ID", ErrGenerationInvalid)
	}
	if err := validateGenerationIdentifier(metadata.RoundID); err != nil {
		return fmt.Errorf("%w: invalid round ID", ErrGenerationInvalid)
	}
	if metadata.Phase != PhaseSyncing && metadata.Phase != PhaseDualWriteRepairing && metadata.Phase != PhaseCutoverReady {
		return fmt.Errorf("%w: invalid phase", ErrGenerationInvalid)
	}
	if err := validateGenerationIdentity(metadata.Identity); err != nil || metadata.Identity.JobID != s.jobID {
		return fmt.Errorf("%w: invalid generation identity", ErrGenerationInvalid)
	}
	if len(metadata.Stages) == 0 {
		return fmt.Errorf("%w: no generation stages", ErrGenerationInvalid)
	}
	for stage, stageMetadata := range metadata.Stages {
		if !validGenerationStage(stage) || stageMetadata.RecordCount < 0 || requireComplete && !stageMetadata.Complete {
			return fmt.Errorf("%w: invalid or incomplete stage %q", ErrGenerationInvalid, stage)
		}
		var count int64
		seen := make(map[string]struct{}, len(stageMetadata.Chunks))
		for _, descriptor := range stageMetadata.Chunks {
			if descriptor.Stage != stage || descriptor.Kind != recordKindForStage(stage) || descriptor.FormatVersion != generationFormatVersion {
				return fmt.Errorf("%w: stage %q descriptor mismatch", ErrGenerationInvalid, stage)
			}
			if err := validateGenerationIdentifier(descriptor.ID); err != nil {
				return fmt.Errorf("%w: invalid chunk ID", ErrGenerationInvalid)
			}
			if _, exists := seen[descriptor.ID]; exists {
				return fmt.Errorf("%w: duplicate chunk ID %q", ErrGenerationInvalid, descriptor.ID)
			}
			seen[descriptor.ID] = struct{}{}
			count += descriptor.RecordCount
		}
		if count != stageMetadata.RecordCount {
			return fmt.Errorf("%w: stage %q record count mismatch", ErrGenerationInvalid, stage)
		}
	}
	return nil
}

func validateGenerationIdentity(identity generationIdentity) error {
	if validateJobID(identity.JobID) != nil || identity.ConfigHash == "" || !volumeIDPattern.MatchString(identity.VolumeID) ||
		identity.EBSRoot == "" || identity.SourceSubpath == "" || identity.SourceRoot == "" || identity.Endpoint == "" ||
		identity.SpaceRef == "" || identity.Prefix == "" {
		return fmt.Errorf("incomplete generation identity")
	}
	return nil
}

func (s *generationStore) validateLocation(generationID string, stage generationStage, chunkID string) error {
	if err := validateGenerationIdentifier(generationID); err != nil {
		return fmt.Errorf("invalid generation ID")
	}
	if !validGenerationStage(stage) {
		return fmt.Errorf("invalid generation stage %q", stage)
	}
	if err := validateGenerationIdentifier(chunkID); err != nil {
		return fmt.Errorf("invalid chunk ID")
	}
	return nil
}

func validateGenerationIdentifier(value string) error {
	return validateJobID(value)
}

func validGenerationStage(stage generationStage) bool {
	return stage == stageSource || stage == stageDirectoryIdentity || stage == stageTargetRaw || stage == stageTarget || stage == stageDiff
}

func recordKindForStage(stage generationStage) generationRecordKind {
	switch stage {
	case stageSource:
		return recordSource
	case stageDirectoryIdentity:
		return recordDirectoryIdentity
	case stageTargetRaw, stageTarget:
		return recordTarget
	case stageDiff:
		return recordDiff
	default:
		return ""
	}
}

func (s *generationStore) ensureDirectories(ctx context.Context, generationID string, stage generationStage) error {
	directories := []string{
		ControlPrefix + "/",
		ControlPrefix + "/jobs/",
		ControlPrefix + "/jobs/" + s.jobID + "/",
		ControlPrefix + "/jobs/" + s.jobID + "/verification/",
		s.generationBase(generationID),
	}
	if stage != "" {
		directories = append(directories, s.stageBase(generationID, stage))
	}
	for _, directory := range directories {
		if err := s.objects.EnsureDirectory(ctx, directory); err != nil {
			return fmt.Errorf("ensure generation directory %s: %w", directory, err)
		}
	}
	return nil
}

func (s *generationStore) putExact(ctx context.Context, path string, body []byte, expectedRevision int64) error {
	if _, err := s.objects.Put(ctx, path, body, expectedRevision); err == nil {
		return nil
	} else {
		observed, _, readErr := s.objects.Get(ctx, path, int64(len(body)))
		if readErr == nil && bytes.Equal(observed, body) {
			return nil
		}
		return err
	}
}

func flattenGenerationChunks(metadata generationMetadata) []chunkDescriptor {
	stages := []generationStage{stageSource, stageDirectoryIdentity, stageTargetRaw, stageTarget, stageDiff}
	var result []chunkDescriptor
	for _, stage := range stages {
		result = append(result, metadata.Stages[stage].Chunks...)
	}
	return result
}

func equalChunkDescriptors(left, right []chunkDescriptor) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (s *generationStore) generationBase(generationID string) string {
	return ControlPrefix + "/jobs/" + s.jobID + "/verification/" + generationID + "/"
}

func (s *generationStore) stageBase(generationID string, stage generationStage) string {
	return s.generationBase(generationID) + string(stage) + "/"
}

func (s *generationStore) chunkPath(generationID string, stage generationStage, chunkID string) string {
	return s.stageBase(generationID, stage) + chunkID + ".chunk"
}

func (s *generationStore) metadataPath(generationID string) string {
	return s.generationBase(generationID) + "meta.json"
}

func (s *generationStore) completePath(generationID string) string {
	return s.generationBase(generationID) + "complete.json"
}
