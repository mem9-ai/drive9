package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/pkg/xattr"
	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

const (
	promotionStateDirName       = ".drive9-promotions"
	promotionSourceIdentityName = "drive9.source-incarnation"
	promotionRecordVersion      = 2
	promotionMaxEntries         = 4096
	promotionMaxInlineBytes     = 64 << 20
	promotionMaxMetadataBytes   = 8 << 20
	promotionMaxPathBytes       = 4096
	promotionMaxSymlinkBytes    = 4096
	promotionMaxDepth           = 128
	promotionMaxRetainedOps     = 8
	promotionMaxRetainedBytes   = 256 << 20
)

var promotionSourceIdentityXAttr = "user." + promotionSourceIdentityName

type localPromotionPhase string

const (
	promotionPhaseAllocating           localPromotionPhase = "ALLOCATING"
	promotionPhasePrepared             localPromotionPhase = "PREPARED"
	promotionPhaseCreateRequested      localPromotionPhase = "CREATE_REQUESTED"
	promotionPhaseStaging              localPromotionPhase = "STAGING"
	promotionPhaseStagedVerified       localPromotionPhase = "STAGED_VERIFIED"
	promotionPhaseCommitRequested      localPromotionPhase = "COMMIT_REQUESTED"
	promotionPhaseRemoteCommitted      localPromotionPhase = "REMOTE_COMMITTED"
	promotionPhaseSourceQuarantined    localPromotionPhase = "SOURCE_QUARANTINED"
	promotionPhasePublishedQuarantined localPromotionPhase = "PUBLISHED_QUARANTINED"
	promotionPhaseLocalAborted         localPromotionPhase = "LOCAL_ABORTED"
)

type localPromotionContent struct {
	RelativePath   string `json:"relative_path"`
	EntryHash      string `json:"entry_hash"`
	SizeBytes      uint64 `json:"size_bytes"`
	Checksum       string `json:"checksum_sha256"`
	IdempotencyKey string `json:"idempotency_key"`
	ContentID      string `json:"content_id,omitempty"`
}

type localPromotionRecord struct {
	Version                     int                     `json:"version"`
	OperationID                 string                  `json:"operation_id"`
	MigrationID                 string                  `json:"migration_id,omitempty"`
	AllocationEpoch             uint64                  `json:"allocation_epoch,omitempty"`
	AllocationSequence          uint64                  `json:"allocation_sequence,omitempty"`
	AllocationIdempotencyKey    string                  `json:"allocation_idempotency_key"`
	AllocationProof             string                  `json:"allocation_proof,omitempty"`
	CreateBefore                time.Time               `json:"create_before,omitempty"`
	SourcePath                  string                  `json:"source_path"`
	SourceParentDev             uint64                  `json:"source_parent_dev"`
	SourceParentIno             uint64                  `json:"source_parent_ino"`
	TargetPath                  string                  `json:"target_path"`
	RemoteTarget                string                  `json:"remote_target"`
	LocalRootUUID               string                  `json:"local_root_uuid"`
	SourceIncarnationUUID       string                  `json:"source_incarnation_uuid"`
	ManifestHash                string                  `json:"manifest_hash"`
	Plan                        promotion.ImportPlan    `json:"plan"`
	OwnerEpoch                  uint64                  `json:"owner_epoch,omitempty"`
	OwnerToken                  string                  `json:"owner_token"`
	PendingOwnerToken           string                  `json:"pending_owner_token,omitempty"`
	RecoveryToken               string                  `json:"recovery_token"`
	ActivityDeadline            time.Time               `json:"activity_deadline,omitempty"`
	LeaseExpiresAt              time.Time               `json:"lease_expires_at,omitempty"`
	AcceptedRestoreGeneration   uint64                  `json:"accepted_restore_generation,omitempty"`
	AcceptedDatabaseIncarnation string                  `json:"accepted_database_incarnation,omitempty"`
	AcceptedWriterGeneration    uint64                  `json:"accepted_writer_generation,omitempty"`
	RestoreGeneration           uint64                  `json:"restore_generation,omitempty"`
	DatabaseIncarnation         string                  `json:"database_incarnation,omitempty"`
	WriterGeneration            uint64                  `json:"writer_generation,omitempty"`
	ServerState                 string                  `json:"server_state,omitempty"`
	TerminalResultDigest        string                  `json:"terminal_result_digest,omitempty"`
	ResultAcknowledged          bool                    `json:"result_acknowledged,omitempty"`
	FenceState                  string                  `json:"fence_state"`
	Phase                       localPromotionPhase     `json:"phase"`
	Contents                    []localPromotionContent `json:"contents,omitempty"`
	CreatedAt                   time.Time               `json:"created_at"`
	UpdatedAt                   time.Time               `json:"updated_at"`
	Checksum                    string                  `json:"checksum"`
}

type promotionSourceSnapshot struct {
	Manifest *promotion.CanonicalManifest
	Files    map[string]string
	Paths    map[string]string
	Facts    map[string]promotionSourceFact
	RootPath string
	RootFact promotionSourceFact
}

type promotionSourceFact struct {
	Mode    fs.FileMode
	Size    int64
	MtimeNS int64
	Dev     uint64
	Ino     uint64
}

func (fsys *Dat9FS) renameLocalToRemoteSynchronously(ctx context.Context, input *gofuse.RenameIn, sourcePath, targetPath string) gofuse.Status {
	fsys.promotionBarrier.Lock()
	defer fsys.promotionBarrier.Unlock()
	if status := fsys.promotionGuard(); status != gofuse.OK {
		return status
	}

	if fsys.localOverlay == nil || fsys.client == nil || fsys.opts == nil || !fsys.opts.EnableSynchronousPromotion {
		return gofuse.Status(syscall.EXDEV)
	}
	oldInfo, newInfo, status := fsys.renamePreflightForPromotion(ctx, input, sourcePath, targetPath)
	if status != gofuse.OK {
		return status
	}
	// P1 is absent-target and directory-tree only. File roots, replacement,
	// opened handles and Git ownership remain EXDEV until stable handle
	// migration is implemented.
	if !oldInfo.isDir || newInfo.exists || fsys.opts.EnableGitWorkspaces ||
		fsys.promotionHasOpenHandle(sourcePath) || fsys.promotionHasOpenHandle(targetPath) {
		return gofuse.Status(syscall.EXDEV)
	}
	if fsys.promotionHasPendingState(sourcePath) || fsys.promotionHasPendingState(targetPath) {
		return gofuse.Status(syscall.EXDEV)
	}

	sourceAbs, err := fsys.localOverlay.abs(sourcePath)
	if err != nil {
		return localErrToFuseStatus(err)
	}
	// Finish every unsupported-shape check before creating the reserved source
	// identity or private journal. An EXDEV result must be observationally
	// identical to the historical gate-off behavior.
	if err := fsys.rejectPromotionPathXAttrs(sourcePath, sourceAbs, true); err != nil {
		return promotionFuseStatus(err)
	}
	snapshot, err := fsys.snapshotPromotionSource(ctx, sourcePath, targetPath, sourceAbs)
	if err != nil {
		return promotionFuseStatus(err)
	}
	if err := fsys.checkPromotionRetentionBudget(snapshot.Manifest.ByteTotal); err != nil {
		return promotionFuseStatus(err)
	}
	remoteTarget := fsys.remotePath(targetPath)
	plan, err := fsys.client.PlanPromotionImport(ctx, promotion.PlanImportRequest{
		Target: remoteTarget, ExpectedTargetAbsent: true, Manifest: snapshot.Manifest.Entries,
	})
	if err != nil {
		return promotionFuseStatus(err)
	}
	if err := validatePromotionPlan(remoteTarget, snapshot.Manifest, plan); err != nil {
		return gofuse.EIO
	}
	stateRoot, localRootUUID, err := fsys.ensurePromotionStateRoot()
	if err != nil {
		return gofuse.Status(syscall.EIO)
	}
	sourceUUID, err := ensurePromotionSourceIdentity(sourceAbs)
	if err != nil {
		return gofuse.Status(syscall.EXDEV)
	}
	if err := verifyUniquePromotionSourceIdentity(fsys.localOverlay.root, sourceAbs, sourceUUID); err != nil {
		return gofuse.EIO
	}
	sourceParentDev, sourceParentIno, err := promotionDirectoryIdentity(filepath.Dir(sourceAbs))
	if err != nil {
		return gofuse.Status(syscall.EXDEV)
	}
	if err := revalidatePromotionSnapshot(snapshot); err != nil {
		return promotionFuseStatus(err)
	}

	now := time.Now().UTC()
	record := &localPromotionRecord{
		Version:                  promotionRecordVersion,
		OperationID:              randomPromotionSecret(16),
		AllocationIdempotencyKey: randomPromotionSecret(32),
		SourcePath:               sourcePath,
		SourceParentDev:          sourceParentDev,
		SourceParentIno:          sourceParentIno,
		TargetPath:               targetPath,
		RemoteTarget:             remoteTarget,
		LocalRootUUID:            localRootUUID,
		SourceIncarnationUUID:    sourceUUID,
		ManifestHash:             snapshot.Manifest.ManifestHash,
		Plan:                     *plan,
		OwnerToken:               randomPromotionSecret(32),
		RecoveryToken:            randomPromotionSecret(32),
		FenceState:               "INSTALLED",
		Phase:                    promotionPhaseAllocating,
		CreatedAt:                now,
		UpdatedAt:                now,
	}
	opDir := filepath.Join(stateRoot, "operations", record.OperationID)
	if err := persistPromotionManifest(opDir, snapshot.Manifest.Entries); err != nil {
		return gofuse.Status(syscall.EIO)
	}
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}

	allocation, err := fsys.client.AllocatePromotionImport(ctx, promotion.AllocateImportRequest{
		Target: remoteTarget, ExpectedTargetAbsent: true, IdempotencyKey: record.AllocationIdempotencyKey,
	})
	if err != nil {
		if promotionOutcomeIsKnown(err) {
			record.Phase = promotionPhaseLocalAborted
			record.FenceState = "RELEASED"
			if persistErr := persistPromotionRecord(opDir, record); persistErr != nil {
				return fsys.failPromotionClosed(record, persistErr)
			}
			return promotionFuseStatus(err)
		}
		return fsys.failPromotionClosed(record, err)
	}
	if err := validatePromotionAllocation(allocation); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	record.MigrationID = allocation.MigrationID
	record.AllocationEpoch = allocation.AllocationEpoch
	record.AllocationSequence = allocation.AllocationSequence
	record.AllocationProof = allocation.AllocationProof
	record.CreateBefore = allocation.CreateBefore
	record.Phase = promotionPhasePrepared
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}

	record.Phase = promotionPhaseCreateRequested
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	created, err := fsys.client.CreatePromotionImport(ctx, promotion.CreateImportRequest{
		MigrationID:          record.MigrationID,
		AllocationProof:      record.AllocationProof,
		Target:               remoteTarget,
		ExpectedTargetAbsent: true,
		Manifest:             snapshot.Manifest.Entries,
		Plan:                 record.Plan,
		OwnerToken:           record.OwnerToken,
		RecoveryToken:        record.RecoveryToken,
	})
	if err != nil {
		return fsys.failPromotionBeforeCommit(record, opDir, err)
	}
	if err := observeLocalPromotionStatus(record, created); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	record.Phase = promotionPhaseStaging
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}

	for _, entry := range snapshot.Manifest.Entries {
		if entry.Type != promotion.EntryTypeFile {
			continue
		}
		content := localPromotionContent{
			RelativePath: entry.RelativePath, EntryHash: entry.EntryHash,
			SizeBytes: entry.ExpectedSizeBytes, Checksum: entry.ExpectedChecksumSHA256,
			IdempotencyKey: randomPromotionSecret(32),
		}
		record.Contents = append(record.Contents, content)
		if err := persistPromotionRecord(opDir, record); err != nil {
			return fsys.failPromotionClosed(record, err)
		}
		if err := fsys.renewPromotionOwner(ctx, record); err != nil {
			return fsys.abortPromotion(record, opDir, err)
		}
		file, err := os.Open(snapshot.Files[entry.RelativePath])
		if err != nil {
			return fsys.abortPromotion(record, opDir, err)
		}
		stored, putErr := fsys.client.PutPromotionInlineContent(ctx, record.MigrationID, promotion.InlineContentMetadata{
			OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken,
			RelativePath: entry.RelativePath, EntryHash: entry.EntryHash,
			SizeBytes: entry.ExpectedSizeBytes, ChecksumSHA256: entry.ExpectedChecksumSHA256,
			IdempotencyKey: content.IdempotencyKey,
		}, file)
		_ = file.Close()
		if putErr != nil {
			return fsys.abortPromotion(record, opDir, putErr)
		}
		if err := validatePromotionInlineContent(content, stored); err != nil {
			return fsys.failPromotionClosed(record, err)
		}
		record.Contents[len(record.Contents)-1].ContentID = stored.ContentID
		if err := persistPromotionRecord(opDir, record); err != nil {
			return fsys.failPromotionClosed(record, err)
		}
	}

	if err := revalidatePromotionSnapshot(snapshot); err != nil {
		return fsys.abortPromotion(record, opDir, err)
	}
	if err := fsys.renewPromotionOwner(ctx, record); err != nil {
		return fsys.abortPromotion(record, opDir, err)
	}
	verified, err := fsys.client.VerifyPromotionImport(ctx, record.MigrationID, promotion.VerifyImportRequest{
		OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken, ManifestHash: record.ManifestHash,
	})
	if err != nil {
		return fsys.abortPromotion(record, opDir, err)
	}
	if err := observeLocalPromotionStatus(record, verified); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	record.Phase = promotionPhaseStagedVerified
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	if err := revalidatePromotionSnapshot(snapshot); err != nil {
		return fsys.abortPromotion(record, opDir, err)
	}
	if err := fsys.renewPromotionOwner(ctx, record); err != nil {
		return fsys.abortPromotion(record, opDir, err)
	}
	record.Phase = promotionPhaseCommitRequested
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	committed, err := fsys.client.CommitPromotionImport(ctx, record.MigrationID, promotion.OwnerRequest{
		OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken,
	})
	if err != nil {
		// A sent commit is outcome-unknown. Only the authenticated status of the
		// same migration may choose the next action; never allocate a replacement.
		resolveCtx, resolveCancel := detachedPromotionContext(ctx)
		defer resolveCancel()
		committed, err = fsys.client.GetPromotionImport(resolveCtx, record.MigrationID, promotion.GetImportRequest{RecoveryToken: record.RecoveryToken})
		if err != nil {
			return fsys.failPromotionClosed(record, err)
		}
		ctx = resolveCtx
	}
	if committed.State == "VERIFIED" || committed.State == "COMMITTING" {
		retryCtx, retryCancel := detachedPromotionContext(ctx)
		defer retryCancel()
		committed, err = fsys.client.CommitPromotionImport(retryCtx, record.MigrationID, promotion.OwnerRequest{
			OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken,
		})
		if err != nil {
			return fsys.failPromotionClosed(record, err)
		}
	}
	if committed.State == "ABORTED" {
		if err := observeLocalPromotionStatus(record, committed); err != nil {
			return fsys.failPromotionClosed(record, err)
		}
		record.Phase = promotionPhaseLocalAborted
		record.FenceState = "RELEASED"
		if persistErr := persistPromotionRecord(opDir, record); persistErr != nil {
			return fsys.failPromotionClosed(record, persistErr)
		}
		return promotionFuseStatus(errors.New("promotion aborted"))
	}
	if committed.State != "COMMITTED" || committed.TerminalResultDigest == "" {
		return fsys.failPromotionClosed(record, fmt.Errorf("commit returned state %s", committed.State))
	}
	if err := observeLocalPromotionStatus(record, committed); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	record.Phase = promotionPhaseRemoteCommitted
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	if err := verifyPromotionSourceIdentity(sourceAbs, record.SourceIncarnationUUID); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	if err := verifyPromotionSourceParent(sourceAbs, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}

	quarantine := filepath.Join(opDir, "source")
	if err := os.Rename(sourceAbs, quarantine); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	if err := syncPromotionDirectories(filepath.Dir(sourceAbs), opDir); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	record.Phase = promotionPhaseSourceQuarantined
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}

	// Publish the committed remote identity into this mount only after the
	// source is durably hidden. There are no open handles in P1.
	fsys.finishLocalRename(input, sourcePath, targetPath)
	record.Phase = promotionPhasePublishedQuarantined
	record.FenceState = "RELEASED"
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	fsys.acknowledgePromotionResult(opDir, record)
	return gofuse.OK
}

func (fsys *Dat9FS) promotionHasOpenHandle(source string) bool {
	if fsys.openHandles != nil && fsys.openHandles.HasPathPrefix(source) {
		return true
	}
	prefix := strings.TrimSuffix(source, "/") + "/"
	for _, handle := range fsys.dirHandles.Snapshot() {
		if handle != nil && (handle.Path == source || strings.HasPrefix(handle.Path, prefix)) {
			return true
		}
	}
	return false
}

func (fsys *Dat9FS) promotionHasPendingState(source string) bool {
	prefix := strings.TrimSuffix(source, "/") + "/"
	if fsys.pendingIndex != nil && (fsys.pendingIndex.HasPending(source) || len(fsys.pendingIndex.ListByPrefix(prefix)) != 0) {
		return true
	}
	if fsys.writeBack != nil {
		if _, ok := fsys.writeBack.GetMeta(source); ok || len(fsys.writeBack.ListByPrefix(prefix)) != 0 {
			return true
		}
	}
	if fsys.commitQueue != nil && (fsys.commitQueue.HasPath(source) || fsys.commitQueue.HasPrefix(prefix)) {
		return true
	}
	return false
}

func (fsys *Dat9FS) snapshotPromotionSource(ctx context.Context, sourcePath, targetPath, sourceAbs string) (*promotionSourceSnapshot, error) {
	threshold := fsys.negotiatedInlineThreshold()
	if threshold <= 0 {
		return nil, promotion.ErrStorageBackendUnsupported
	}
	limits := promotion.ManifestLimits{
		MaxEntries: promotionMaxEntries, MaxMetadataBytes: promotionMaxMetadataBytes,
		MaxTotalInlineBytes: promotionMaxInlineBytes, MaxPathBytes: promotionMaxPathBytes,
		MaxSymlinkBytes: promotionMaxSymlinkBytes, MaxDepth: promotionMaxDepth,
		InlineThreshold: uint64(threshold),
	}
	files := make(map[string]string)
	paths := make(map[string]string)
	facts := make(map[string]promotionSourceFact)
	rootInfo, err := os.Lstat(sourceAbs)
	if err != nil {
		return nil, err
	}
	entries := []promotion.ManifestEntry{{
		RelativePath: ".", Type: promotion.EntryTypeDirectory,
		Mode: localOverlayPOSIXMode(rootInfo.Mode()), MtimeNS: rootInfo.ModTime().UnixNano(),
	}}
	rootFact, err := promotionFact(rootInfo)
	if err != nil {
		return nil, err
	}
	err = filepath.WalkDir(sourceAbs, func(current string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == sourceAbs {
			return nil
		}
		rel, err := filepath.Rel(sourceAbs, current)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		localSource := path.Join(sourcePath, rel)
		localTarget := path.Join(targetPath, rel)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.IsDir() {
			if fsys.observeDirPathPolicyWithContext(ctx, localSource) != PathLayerLocalOnly ||
				fsys.observeDirPathPolicyWithContext(ctx, localTarget) != PathLayerRemotePersistent {
				return promotion.ErrStorageBackendUnsupported
			}
		} else if fsys.observePathPolicyWithContext(ctx, localSource) != PathLayerLocalOnly ||
			fsys.observePathPolicyWithContext(ctx, localTarget) != PathLayerRemotePersistent {
			return promotion.ErrStorageBackendUnsupported
		}
		if err := fsys.rejectPromotionPathXAttrs(localSource, current, false); err != nil {
			return err
		}
		fact, err := promotionFact(info)
		if err != nil {
			return err
		}
		facts[rel] = fact
		paths[rel] = current
		entry := promotion.ManifestEntry{RelativePath: rel, Mode: localOverlayPOSIXMode(info.Mode()), MtimeNS: info.ModTime().UnixNano()}
		switch {
		case info.IsDir():
			entry.Type = promotion.EntryTypeDirectory
		case info.Mode()&fs.ModeSymlink != 0:
			entry.Type = promotion.EntryTypeSymlink
			target, err := os.Readlink(current)
			if err != nil {
				return err
			}
			entry.SymlinkTarget = target
		case info.Mode().IsRegular():
			if fact.Size >= threshold {
				return promotion.ErrStorageBackendUnsupported
			}
			checksum, err := checksumPromotionFile(current, fact)
			if err != nil {
				return err
			}
			entry.Type = promotion.EntryTypeFile
			entry.ExpectedSizeBytes = uint64(fact.Size)
			entry.ExpectedChecksumSHA256 = checksum
			files[rel] = current
		default:
			return promotion.ErrStorageBackendUnsupported
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	manifest, err := promotion.CanonicalizeManifest(entries, limits)
	if err != nil {
		return nil, err
	}
	return &promotionSourceSnapshot{Manifest: manifest, Files: files, Paths: paths, Facts: facts, RootPath: sourceAbs, RootFact: rootFact}, nil
}

func promotionFact(info fs.FileInfo) (promotionSourceFact, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return promotionSourceFact{}, promotion.ErrStorageBackendUnsupported
	}
	if info.Mode().IsRegular() && uint64(stat.Nlink) != 1 {
		return promotionSourceFact{}, promotion.ErrStorageBackendUnsupported
	}
	return promotionSourceFact{Mode: info.Mode(), Size: info.Size(), MtimeNS: info.ModTime().UnixNano(), Dev: uint64(stat.Dev), Ino: uint64(stat.Ino)}, nil
}

func checksumPromotionFile(name string, expected promotionSourceFact) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()
	before, err := file.Stat()
	if err != nil {
		return "", err
	}
	beforeFact, err := promotionFact(before)
	if err != nil || beforeFact != expected {
		return "", fmt.Errorf("promotion source changed before read")
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	after, err := file.Stat()
	if err != nil {
		return "", err
	}
	afterFact, err := promotionFact(after)
	if err != nil || afterFact != expected {
		return "", fmt.Errorf("promotion source changed during read")
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func revalidatePromotionSnapshot(snapshot *promotionSourceSnapshot) error {
	rootInfo, err := os.Lstat(snapshot.RootPath)
	if err != nil {
		return err
	}
	rootFact, err := promotionFact(rootInfo)
	if err != nil || rootFact != snapshot.RootFact {
		return fmt.Errorf("promotion source root changed")
	}
	paths := make([]string, 0, len(snapshot.Facts))
	for rel := range snapshot.Facts {
		paths = append(paths, rel)
	}
	sort.Strings(paths)
	for _, rel := range paths {
		name := snapshot.Paths[rel]
		info, err := os.Lstat(name)
		if err != nil {
			return err
		}
		fact, err := promotionFact(info)
		if err != nil || fact != snapshot.Facts[rel] {
			return fmt.Errorf("promotion source changed at %q", rel)
		}
	}
	seen := make(map[string]struct{}, len(snapshot.Facts))
	if err := filepath.WalkDir(snapshot.RootPath, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if current == snapshot.RootPath {
			return nil
		}
		rel, err := filepath.Rel(snapshot.RootPath, current)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if _, ok := snapshot.Facts[rel]; !ok {
			return fmt.Errorf("promotion source gained unexpected path %q", rel)
		}
		seen[rel] = struct{}{}
		return nil
	}); err != nil {
		return err
	}
	if len(seen) != len(snapshot.Facts) {
		return fmt.Errorf("promotion source path set changed")
	}
	for _, entry := range snapshot.Manifest.Entries {
		if entry.Type != promotion.EntryTypeFile {
			continue
		}
		fact, ok := snapshot.Facts[entry.RelativePath]
		if !ok {
			return fmt.Errorf("promotion source fact missing at %q", entry.RelativePath)
		}
		checksum, err := checksumPromotionFile(snapshot.Paths[entry.RelativePath], fact)
		if err != nil || checksum != entry.ExpectedChecksumSHA256 {
			return fmt.Errorf("promotion source content changed at %q", entry.RelativePath)
		}
	}
	return nil
}

func (fsys *Dat9FS) ensurePromotionStateRoot() (string, string, error) {
	root := filepath.Join(fsys.localOverlay.root, promotionStateDirName)
	if err := os.MkdirAll(filepath.Join(root, "operations"), 0o700); err != nil {
		return "", "", err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return "", "", err
	}
	uuidPath := filepath.Join(root, "local-root-uuid")
	raw, err := os.ReadFile(uuidPath)
	if err == nil && strings.TrimSpace(string(raw)) != "" {
		return root, strings.TrimSpace(string(raw)), nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	value := randomPromotionSecret(16)
	if err := writePromotionFileAtomically(root, uuidPath, []byte(value+"\n"), 0o600); err != nil {
		return "", "", err
	}
	return root, value, nil
}

// checkPromotionRetentionBudget bounds private source quarantines without
// deleting them before a server-issued source-release proof exists. Budget
// exhaustion rejects a new promotion before any source/journal mutation.
func (fsys *Dat9FS) checkPromotionRetentionBudget(newBytes uint64) error {
	opRoot := filepath.Join(fsys.localOverlay.root, promotionStateDirName, "operations")
	operations, err := os.ReadDir(opRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var retainedOps int
	var retainedBytes uint64
	for _, operation := range operations {
		if !operation.IsDir() {
			return fmt.Errorf("unexpected promotion state entry %q", operation.Name())
		}
		opDir := filepath.Join(opRoot, operation.Name())
		record, _, err := loadPromotionRecord(opDir)
		if err != nil {
			return err
		}
		if record.OperationID != operation.Name() {
			return fmt.Errorf("promotion operation directory identity mismatch")
		}
		if record.Phase != promotionPhasePublishedQuarantined {
			continue
		}
		if _, err := os.Lstat(filepath.Join(opDir, "source")); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return err
		}
		retainedOps++
		if ^uint64(0)-retainedBytes < record.Plan.ByteTotal {
			return promotion.ErrManifestLimitExceeded
		}
		retainedBytes += record.Plan.ByteTotal
	}
	if retainedOps >= promotionMaxRetainedOps || newBytes > promotionMaxRetainedBytes ||
		retainedBytes > promotionMaxRetainedBytes-newBytes {
		return promotion.ErrManifestLimitExceeded
	}
	return nil
}

func ensurePromotionSourceIdentity(sourceAbs string) (string, error) {
	info, err := os.Lstat(sourceAbs)
	if err != nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
		return "", promotion.ErrStorageBackendUnsupported
	}
	value, err := xattr.LGet(sourceAbs, promotionSourceIdentityXAttr)
	if err == nil {
		if len(value) != 32 {
			return "", fmt.Errorf("invalid promotion source identity")
		}
		return string(value), nil
	}
	if !errors.Is(err, xattr.ENOATTR) {
		return "", err
	}
	identity := randomPromotionSecret(16)
	if err := xattr.LSetWithFlags(sourceAbs, promotionSourceIdentityXAttr, []byte(identity), xattr.XATTR_CREATE); err != nil {
		if value, getErr := xattr.LGet(sourceAbs, promotionSourceIdentityXAttr); getErr == nil && len(value) == 32 {
			identity = string(value)
		} else {
			return "", err
		}
	}
	if err := syncPromotionFileAndParent(sourceAbs); err != nil {
		return "", err
	}
	return identity, nil
}

// verifyUniquePromotionSourceIdentity prevents a copied local tree from
// reusing the recovery identity of another live source. Recovery decisions
// are destructive (move the matching source into or out of quarantine), so a
// duplicated reserved xattr must fail closed rather than select one path by
// traversal order.
func verifyUniquePromotionSourceIdentity(overlayRoot, sourceAbs, want string) error {
	overlayRoot = filepath.Clean(overlayRoot)
	sourceAbs = filepath.Clean(sourceAbs)
	privateRoot := filepath.Join(overlayRoot, promotionStateDirName)
	matches := 0
	err := filepath.WalkDir(overlayRoot, func(current string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		current = filepath.Clean(current)
		if current == privateRoot {
			return filepath.SkipDir
		}
		value, err := xattr.LGet(current, promotionSourceIdentityXAttr)
		if errors.Is(err, xattr.ENOATTR) {
			return nil
		}
		if err != nil {
			return err
		}
		if len(value) != 32 {
			return fmt.Errorf("invalid promotion source identity at %q", current)
		}
		if string(value) != want {
			return nil
		}
		matches++
		if current != sourceAbs || matches > 1 {
			return fmt.Errorf("duplicate promotion source identity at %q", current)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if matches != 1 {
		return fmt.Errorf("promotion source identity is not uniquely bound")
	}
	return nil
}

func verifyPromotionSourceIdentity(sourceAbs, want string) error {
	got, err := xattr.LGet(sourceAbs, promotionSourceIdentityXAttr)
	if err != nil {
		return err
	}
	if string(got) != want {
		return fmt.Errorf("promotion source identity changed")
	}
	return nil
}

func promotionDirectoryIdentity(name string) (uint64, uint64, error) {
	info, err := os.Lstat(name)
	if err != nil || !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
		return 0, 0, promotion.ErrStorageBackendUnsupported
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, promotion.ErrStorageBackendUnsupported
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func verifyPromotionSourceParent(sourceAbs string, record *localPromotionRecord) error {
	dev, ino, err := promotionDirectoryIdentity(filepath.Dir(sourceAbs))
	if err != nil {
		return err
	}
	if dev != record.SourceParentDev || ino != record.SourceParentIno {
		return fmt.Errorf("promotion source parent identity changed")
	}
	return nil
}

func rejectPromotionXAttrs(name string, root bool) error {
	names, err := xattr.LList(name)
	if err != nil {
		return err
	}
	for _, attr := range names {
		// macOS attaches com.apple.provenance to ordinary files created under
		// protected temporary/local roots. It is kernel/platform provenance,
		// not user metadata, is never copied to the manifest, and cannot affect
		// content identity. Treating it as a user xattr would make the preview
		// impossible to enable on Darwin.
		if runtime.GOOS == "darwin" && attr == "com.apple.provenance" {
			continue
		}
		if root && attr == promotionSourceIdentityXAttr {
			continue
		}
		return fmt.Errorf("%w: unsupported xattr %q", promotion.ErrStorageBackendUnsupported, attr)
	}
	return nil
}

func (fsys *Dat9FS) rejectPromotionPathXAttrs(localPath, name string, root bool) error {
	if fsys.xattrs != nil {
		if names := fsys.xattrs.List(localPath); len(names) != 0 {
			sort.Strings(names)
			return fmt.Errorf("%w: unsupported mount xattr %q", promotion.ErrStorageBackendUnsupported, names[0])
		}
	}
	return rejectPromotionXAttrs(name, root)
}

func promotionOutcomeIsKnown(err error) bool {
	var status *client.StatusError
	return errors.As(err, &status) && status.StatusCode < 500
}

func detachedPromotionContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(parent), 30*time.Second)
}

func (fsys *Dat9FS) failPromotionClosed(record *localPromotionRecord, cause error) gofuse.Status {
	fsys.promotionRecoveryNeeded.Store(true)
	migrationID := "<unallocated>"
	phase := localPromotionPhase("")
	if record != nil {
		migrationID = record.MigrationID
		phase = record.Phase
	}
	logger.Error(context.Background(), "synchronous_promotion_requires_restart_recovery",
		zap.String("migration_id", migrationID),
		zap.String("phase", string(phase)),
		zap.Error(cause))
	return gofuse.EIO
}

func (fsys *Dat9FS) failPromotionBeforeCommit(record *localPromotionRecord, opDir string, cause error) gofuse.Status {
	if record.MigrationID == "" {
		return promotionFuseStatus(cause)
	}
	if !promotionOutcomeIsKnown(cause) {
		return fsys.failPromotionClosed(record, cause)
	}
	if record.OwnerEpoch == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		result, err := fsys.client.RetirePromotionImport(ctx, record.MigrationID, promotion.RetireImportRequest{AllocationProof: record.AllocationProof})
		if err != nil {
			return fsys.failPromotionClosed(record, err)
		}
		if result.Import != nil {
			if err := observeLocalPromotionStatus(record, result.Import); err != nil {
				return fsys.failPromotionClosed(record, err)
			}
			return fsys.abortPromotion(record, opDir, cause)
		}
		if !result.Retired {
			return fsys.failPromotionClosed(record, errors.New("retire returned no durable winner"))
		}
		record.Phase = promotionPhaseLocalAborted
		record.FenceState = "RELEASED"
		if err := persistPromotionRecord(opDir, record); err != nil {
			return fsys.failPromotionClosed(record, err)
		}
		return promotionFuseStatus(cause)
	}
	return fsys.abortPromotion(record, opDir, cause)
}

func (fsys *Dat9FS) abortPromotion(record *localPromotionRecord, opDir string, cause error) gofuse.Status {
	logger.Warn(context.Background(), "synchronous_promotion_aborting_before_commit",
		zap.String("migration_id", record.MigrationID),
		zap.Error(cause))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	status, err := fsys.client.AbortPromotionImport(ctx, record.MigrationID, promotion.OwnerRequest{OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken})
	if err != nil || status.State != "ABORTED" || status.TerminalResultDigest == "" {
		if err == nil {
			err = fmt.Errorf("abort returned state %s", status.State)
		}
		return fsys.failPromotionClosed(record, err)
	}
	if err := observeLocalPromotionStatus(record, status); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	record.Phase = promotionPhaseLocalAborted
	record.FenceState = "RELEASED"
	if err := persistPromotionRecord(opDir, record); err != nil {
		return fsys.failPromotionClosed(record, err)
	}
	fsys.acknowledgePromotionResult(opDir, record)
	return promotionFuseStatus(cause)
}

func (fsys *Dat9FS) acknowledgePromotionResult(opDir string, record *localPromotionRecord) {
	if record == nil || record.ResultAcknowledged || record.MigrationID == "" ||
		(record.ServerState != "COMMITTED" && record.ServerState != "ABORTED") || record.TerminalResultDigest == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wantState := record.ServerState
	wantDigest := record.TerminalResultDigest
	status, err := fsys.client.AcknowledgePromotionImportResult(ctx, record.MigrationID, promotion.AcknowledgeImportResultRequest{
		RecoveryToken:        record.RecoveryToken,
		TerminalState:        wantState,
		TerminalResultDigest: wantDigest,
	})
	if err != nil {
		logger.Warn(context.Background(), "synchronous_promotion_result_ack_deferred",
			zap.String("migration_id", record.MigrationID),
			zap.Error(err))
		return
	}
	if err := observeLocalPromotionStatus(record, status); err != nil || status.State != wantState ||
		status.TerminalResultDigest != wantDigest {
		logger.Error(context.Background(), "synchronous_promotion_result_ack_invalid",
			zap.String("migration_id", record.MigrationID),
			zap.Error(err))
		return
	}
	record.ResultAcknowledged = true
	if err := persistPromotionRecord(opDir, record); err != nil {
		logger.Warn(context.Background(), "synchronous_promotion_result_ack_journal_deferred",
			zap.String("migration_id", record.MigrationID),
			zap.Error(err))
	}
}

func validatePromotionPlan(target string, manifest *promotion.CanonicalManifest, plan *promotion.ImportPlan) error {
	if plan == nil || manifest == nil {
		return fmt.Errorf("promotion plan response is empty")
	}
	if plan.Target != target || !plan.ExpectedTargetAbsent || plan.ManifestHash != manifest.ManifestHash ||
		plan.EntryTotal != manifest.EntryTotal || plan.ByteTotal != manifest.ByteTotal ||
		plan.MaxContentSize != manifest.MaxContentSize || plan.StorageMode != promotion.StorageModeDB9Inline ||
		plan.StoragePlanDigest == "" || plan.BackendCapabilityGeneration == 0 || plan.ConfigGeneration == 0 ||
		!plan.PlanExpiresAt.After(time.Now()) {
		return fmt.Errorf("promotion plan response does not match the requested manifest")
	}
	return nil
}

func validatePromotionAllocation(allocation *promotion.Allocation) error {
	if allocation == nil || allocation.MigrationID == "" || allocation.AllocationEpoch == 0 ||
		allocation.AllocationSequence == 0 || allocation.AllocationProof == "" ||
		!allocation.CreateBefore.After(time.Now()) {
		return fmt.Errorf("promotion allocation response is incomplete")
	}
	identity, err := promotion.DecodeMigrationID(allocation.MigrationID)
	if err != nil || identity.AllocationEpoch != allocation.AllocationEpoch ||
		identity.AllocationSequence != allocation.AllocationSequence {
		return fmt.Errorf("promotion allocation identity mismatch")
	}
	return nil
}

func validatePromotionInlineContent(expected localPromotionContent, content *promotion.InlineContent) error {
	if content == nil || content.ContentID == "" || content.State != "STAGED" ||
		content.RelativePath != expected.RelativePath || content.SizeBytes != expected.SizeBytes ||
		content.ChecksumSHA256 != expected.Checksum {
		return fmt.Errorf("promotion inline-content response mismatch")
	}
	return nil
}

func observeLocalPromotionStatus(record *localPromotionRecord, status *promotion.ImportStatus) error {
	expectedOwnerEpoch := record.OwnerEpoch
	if expectedOwnerEpoch == 0 && status != nil {
		expectedOwnerEpoch = status.OwnerEpoch
	}
	return observeLocalPromotionStatusForOwner(record, status, expectedOwnerEpoch)
}

func observeLocalPromotionStatusForOwner(record *localPromotionRecord, status *promotion.ImportStatus, expectedOwnerEpoch uint64) error {
	if record == nil || status == nil {
		return fmt.Errorf("promotion status response is empty")
	}
	if status.MigrationID != record.MigrationID || status.AllocationEpoch != record.AllocationEpoch ||
		status.AllocationSequence != record.AllocationSequence || status.Target != record.RemoteTarget ||
		status.ManifestHash != record.ManifestHash {
		return fmt.Errorf("promotion status identity mismatch")
	}
	if status.OwnerEpoch == 0 || status.OwnerEpoch != expectedOwnerEpoch {
		return fmt.Errorf("promotion status owner epoch mismatch: got %d want %d", status.OwnerEpoch, expectedOwnerEpoch)
	}
	if status.State == "" || status.ActivityDeadline.IsZero() || status.LeaseExpiresAt.IsZero() ||
		status.RestoreGeneration == 0 || status.DatabaseIncarnation == "" || status.WriterGeneration == 0 {
		return fmt.Errorf("promotion status incarnation tuple is incomplete")
	}
	terminal := status.State == "COMMITTED" || status.State == "ABORTED"
	if terminal {
		if status.TerminalResultBlob == "" || len(status.TerminalResultDigest) != sha256.Size*2 {
			return fmt.Errorf("promotion terminal result is incomplete")
		}
		sum := sha256.Sum256([]byte(status.TerminalResultBlob))
		if hex.EncodeToString(sum[:]) != status.TerminalResultDigest {
			return fmt.Errorf("promotion terminal result digest mismatch")
		}
	} else if status.TerminalResultBlob != "" || status.TerminalResultDigest != "" {
		return fmt.Errorf("promotion nonterminal status carries a terminal result")
	}

	record.OwnerEpoch = status.OwnerEpoch
	record.ActivityDeadline = status.ActivityDeadline
	record.LeaseExpiresAt = status.LeaseExpiresAt
	if record.AcceptedRestoreGeneration == 0 && record.AcceptedDatabaseIncarnation == "" && record.AcceptedWriterGeneration == 0 {
		record.AcceptedRestoreGeneration = status.RestoreGeneration
		record.AcceptedDatabaseIncarnation = status.DatabaseIncarnation
		record.AcceptedWriterGeneration = status.WriterGeneration
	} else if record.AcceptedRestoreGeneration == 0 || record.AcceptedDatabaseIncarnation == "" || record.AcceptedWriterGeneration == 0 {
		return fmt.Errorf("promotion accepted incarnation tuple is incomplete")
	}
	record.RestoreGeneration = status.RestoreGeneration
	record.DatabaseIncarnation = status.DatabaseIncarnation
	record.WriterGeneration = status.WriterGeneration
	record.ServerState = status.State
	record.TerminalResultDigest = status.TerminalResultDigest
	return nil
}

func (fsys *Dat9FS) renewPromotionOwner(ctx context.Context, record *localPromotionRecord) error {
	status, err := fsys.client.RenewPromotionImport(ctx, record.MigrationID, promotion.OwnerRequest{
		OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken,
	})
	if err != nil {
		return err
	}
	if status.OwnerEpoch != record.OwnerEpoch || status.State == "COMMITTED" || status.State == "ABORTED" {
		return fmt.Errorf("promotion renewal returned state=%s owner_epoch=%d", status.State, status.OwnerEpoch)
	}
	return observeLocalPromotionStatus(record, status)
}

func persistPromotionManifest(opDir string, manifest []promotion.ManifestEntry) error {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	return writePromotionFileAtomically(opDir, filepath.Join(opDir, "manifest.json"), raw, 0o600)
}

func persistPromotionRecord(opDir string, record *localPromotionRecord) error {
	record.UpdatedAt = time.Now().UTC()
	record.Checksum = ""
	raw, err := json.Marshal(record)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(raw)
	record.Checksum = hex.EncodeToString(sum[:])
	raw, err = json.Marshal(record)
	if err != nil {
		return err
	}
	return writePromotionFileAtomically(opDir, filepath.Join(opDir, "record.json"), raw, 0o600)
}

func loadPromotionRecord(opDir string) (*localPromotionRecord, []promotion.ManifestEntry, error) {
	raw, err := os.ReadFile(filepath.Join(opDir, "record.json"))
	if err != nil {
		return nil, nil, err
	}
	var record localPromotionRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, nil, err
	}
	if record.Version != promotionRecordVersion || record.Checksum == "" {
		return nil, nil, fmt.Errorf("invalid promotion record version/checksum")
	}
	want := record.Checksum
	record.Checksum = ""
	canonical, err := json.Marshal(&record)
	if err != nil {
		return nil, nil, err
	}
	sum := sha256.Sum256(canonical)
	if hex.EncodeToString(sum[:]) != want {
		return nil, nil, fmt.Errorf("promotion record checksum mismatch")
	}
	record.Checksum = want
	manifestRaw, err := os.ReadFile(filepath.Join(opDir, "manifest.json"))
	if err != nil {
		return nil, nil, err
	}
	var entries []promotion.ManifestEntry
	if err := json.Unmarshal(manifestRaw, &entries); err != nil {
		return nil, nil, err
	}
	manifest, err := promotion.ValidateCanonicalManifest(entries, record.Plan.Limits)
	if err != nil {
		return nil, nil, err
	}
	if manifest.ManifestHash != record.ManifestHash || manifest.ManifestHash != record.Plan.ManifestHash {
		return nil, nil, fmt.Errorf("promotion manifest hash mismatch")
	}
	if err := validateLocalPromotionRecordShape(&record); err != nil {
		return nil, nil, err
	}
	return &record, entries, nil
}

func validateLocalPromotionRecordShape(record *localPromotionRecord) error {
	if record.OperationID == "" || record.AllocationIdempotencyKey == "" || record.SourcePath == "" ||
		record.TargetPath == "" || record.RemoteTarget == "" || record.LocalRootUUID == "" ||
		record.SourceIncarnationUUID == "" || record.SourceParentDev == 0 || record.SourceParentIno == 0 ||
		record.ManifestHash == "" || record.Plan.Target != record.RemoteTarget ||
		record.Plan.ManifestHash != record.ManifestHash || record.OwnerToken == "" || record.RecoveryToken == "" {
		return fmt.Errorf("promotion record identity is incomplete")
	}
	if !strings.HasPrefix(record.SourcePath, "/") || path.Clean(record.SourcePath) != record.SourcePath ||
		!strings.HasPrefix(record.TargetPath, "/") || path.Clean(record.TargetPath) != record.TargetPath ||
		!strings.HasPrefix(record.RemoteTarget, "/") || path.Clean(record.RemoteTarget) != record.RemoteTarget ||
		record.SourcePath == record.TargetPath {
		return fmt.Errorf("promotion record path identity is invalid")
	}
	legal := false
	switch record.FenceState {
	case "INSTALLED":
		switch record.Phase {
		case promotionPhaseAllocating, promotionPhasePrepared, promotionPhaseCreateRequested,
			promotionPhaseStaging, promotionPhaseStagedVerified, promotionPhaseCommitRequested,
			promotionPhaseRemoteCommitted, promotionPhaseSourceQuarantined:
			legal = true
		}
	case "RECONCILING":
		legal = record.Phase == promotionPhaseRemoteCommitted || record.Phase == promotionPhaseSourceQuarantined ||
			record.Phase == promotionPhasePublishedQuarantined
	case "RELEASED":
		legal = record.Phase == promotionPhasePublishedQuarantined || record.Phase == promotionPhaseLocalAborted
	}
	if !legal {
		return fmt.Errorf("illegal promotion phase/fence pair %s/%s", record.Phase, record.FenceState)
	}
	unallocatedAbort := record.Phase == promotionPhaseLocalAborted && record.MigrationID == ""
	if record.Phase != promotionPhaseAllocating && !unallocatedAbort {
		identity, err := promotion.DecodeMigrationID(record.MigrationID)
		if err != nil || identity.AllocationEpoch != record.AllocationEpoch || identity.AllocationSequence != record.AllocationSequence ||
			record.AllocationProof == "" || record.CreateBefore.IsZero() {
			return fmt.Errorf("promotion record allocation identity is incomplete")
		}
	}
	if unallocatedAbort && (record.AllocationEpoch != 0 || record.AllocationSequence != 0 || record.AllocationProof != "" ||
		!record.CreateBefore.IsZero() || record.OwnerEpoch != 0 || record.ServerState != "" || record.TerminalResultDigest != "") {
		return fmt.Errorf("unallocated promotion abort carries server identity")
	}
	ownerRequired := record.Phase == promotionPhaseStaging || record.Phase == promotionPhaseStagedVerified ||
		record.Phase == promotionPhaseCommitRequested || record.Phase == promotionPhaseRemoteCommitted ||
		record.Phase == promotionPhaseSourceQuarantined || record.Phase == promotionPhasePublishedQuarantined ||
		(record.Phase == promotionPhaseLocalAborted && record.OwnerEpoch != 0)
	if ownerRequired && (record.OwnerEpoch == 0 || record.ActivityDeadline.IsZero() || record.RestoreGeneration == 0 ||
		record.DatabaseIncarnation == "" || record.WriterGeneration == 0 || record.AcceptedRestoreGeneration == 0 ||
		record.AcceptedDatabaseIncarnation == "" || record.AcceptedWriterGeneration == 0) {
		return fmt.Errorf("promotion record owner/incarnation is incomplete")
	}
	acceptedTupleFields := 0
	if record.AcceptedRestoreGeneration != 0 {
		acceptedTupleFields++
	}
	if record.AcceptedDatabaseIncarnation != "" {
		acceptedTupleFields++
	}
	if record.AcceptedWriterGeneration != 0 {
		acceptedTupleFields++
	}
	if acceptedTupleFields != 0 && acceptedTupleFields != 3 {
		return fmt.Errorf("promotion record accepted incarnation tuple is partial")
	}
	switch record.Phase {
	case promotionPhaseRemoteCommitted, promotionPhaseSourceQuarantined, promotionPhasePublishedQuarantined:
		if record.ServerState != "COMMITTED" || !validPromotionResultDigest(record.TerminalResultDigest) {
			return fmt.Errorf("committed promotion record terminal result is incomplete")
		}
	case promotionPhaseLocalAborted:
		if record.OwnerEpoch == 0 {
			if record.ServerState != "" || record.TerminalResultDigest != "" {
				return fmt.Errorf("retired promotion record carries a server result")
			}
		} else if record.ServerState != "ABORTED" || !validPromotionResultDigest(record.TerminalResultDigest) {
			return fmt.Errorf("aborted promotion record terminal result is incomplete")
		}
	default:
		if record.ServerState == "COMMITTED" || record.ServerState == "ABORTED" || record.TerminalResultDigest != "" {
			return fmt.Errorf("nonterminal promotion record carries a terminal result")
		}
	}
	if record.ResultAcknowledged && (record.ServerState != "COMMITTED" && record.ServerState != "ABORTED") {
		return fmt.Errorf("promotion record acknowledges a nonterminal result")
	}
	return nil
}

func validPromotionResultDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// recoverSynchronousPromotions runs before the FUSE server is mounted. A
// crashed syscall no longer has a caller waiting for success, so every
// pre-commit record is safely driven to ABORTED/retired. Accepted or unknown
// commits are resolved by the original migration ID; committed work only
// moves forward through quarantine and durable RELEASED.
func (fsys *Dat9FS) recoverSynchronousPromotions(ctx context.Context) error {
	if fsys == nil || fsys.opts == nil || fsys.localOverlay == nil {
		return nil
	}
	if !fsys.opts.EnableSynchronousPromotion {
		return fsys.rejectUnresolvedPromotionJournal()
	}
	stateRoot, localRootUUID, err := fsys.ensurePromotionStateRoot()
	if err != nil {
		return err
	}
	opRoot := filepath.Join(stateRoot, "operations")
	operations, err := os.ReadDir(opRoot)
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if !operation.IsDir() {
			return fmt.Errorf("unexpected promotion state entry %q", operation.Name())
		}
		opDir := filepath.Join(opRoot, operation.Name())
		record, manifest, err := loadPromotionRecord(opDir)
		if err != nil {
			return fmt.Errorf("recover promotion %s: %w", operation.Name(), err)
		}
		if record.LocalRootUUID != localRootUUID {
			return fmt.Errorf("recover promotion %s: local root identity mismatch", operation.Name())
		}
		if record.OperationID != operation.Name() {
			return fmt.Errorf("recover promotion %s: operation directory identity mismatch", operation.Name())
		}
		if record.FenceState == "RELEASED" {
			if record.Phase != promotionPhasePublishedQuarantined && record.Phase != promotionPhaseLocalAborted {
				return fmt.Errorf("recover promotion %s: illegal released phase %s", operation.Name(), record.Phase)
			}
			if record.Phase == promotionPhaseLocalAborted {
				fsys.acknowledgePromotionResult(opDir, record)
				continue
			}
			if err := fsys.recoverSynchronousPromotion(ctx, opDir, record, manifest); err != nil {
				return fmt.Errorf("recover promotion %s: %w", operation.Name(), err)
			}
			continue
		}
		if record.FenceState != "INSTALLED" && record.FenceState != "RECONCILING" {
			return fmt.Errorf("recover promotion %s: unsupported fence state %s", operation.Name(), record.FenceState)
		}
		if err := fsys.recoverSynchronousPromotion(ctx, opDir, record, manifest); err != nil {
			return fmt.Errorf("recover promotion %s: %w", operation.Name(), err)
		}
	}
	return nil
}

// rejectUnresolvedPromotionJournal prevents an operator from bypassing a
// durable promotion fence by restarting the mount without the preview flag.
// A fresh gate-off mount remains unchanged; completed RELEASED records are
// harmless, but any pending or unreadable record requires explicit recovery.
func (fsys *Dat9FS) rejectUnresolvedPromotionJournal() error {
	opRoot := filepath.Join(fsys.localOverlay.root, promotionStateDirName, "operations")
	operations, err := os.ReadDir(opRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, operation := range operations {
		if !operation.IsDir() {
			return fmt.Errorf("unexpected promotion state entry %q", operation.Name())
		}
		opDir := filepath.Join(opRoot, operation.Name())
		record, _, err := loadPromotionRecord(opDir)
		if err != nil {
			return fmt.Errorf("inspect promotion %s: %w", operation.Name(), err)
		}
		if record.OperationID != operation.Name() {
			return fmt.Errorf("inspect promotion %s: operation directory identity mismatch", operation.Name())
		}
		if record.FenceState != "RELEASED" {
			return fmt.Errorf("promotion %s requires recovery at %s/%s; re-enable synchronous promotion",
				record.MigrationID, record.Phase, record.FenceState)
		}
	}
	return nil
}

func (fsys *Dat9FS) recoverSynchronousPromotion(ctx context.Context, opDir string, record *localPromotionRecord, manifest []promotion.ManifestEntry) error {
	switch record.Phase {
	case promotionPhaseAllocating:
		allocation, err := fsys.client.AllocatePromotionImport(ctx, promotion.AllocateImportRequest{
			Target: record.RemoteTarget, ExpectedTargetAbsent: true, IdempotencyKey: record.AllocationIdempotencyKey,
		})
		if err != nil {
			if promotionOutcomeIsKnown(err) {
				record.Phase = promotionPhaseLocalAborted
				record.FenceState = "RELEASED"
				return persistPromotionRecord(opDir, record)
			}
			return err
		}
		if err := validatePromotionAllocation(allocation); err != nil {
			return err
		}
		record.MigrationID = allocation.MigrationID
		record.AllocationEpoch = allocation.AllocationEpoch
		record.AllocationSequence = allocation.AllocationSequence
		record.AllocationProof = allocation.AllocationProof
		record.CreateBefore = allocation.CreateBefore
		record.Phase = promotionPhasePrepared
		if err := persistPromotionRecord(opDir, record); err != nil {
			return err
		}
		fallthrough
	case promotionPhasePrepared:
		return fsys.recoverRetirePromotion(ctx, opDir, record)
	case promotionPhaseCreateRequested:
		status, err := fsys.client.GetPromotionImport(ctx, record.MigrationID, promotion.GetImportRequest{AllocationProof: record.AllocationProof})
		if client.IsNotFound(err) {
			// Re-dispatch the exact durably recorded request. Create and Retire
			// serialize on the same claim, so this cannot invent a second winner.
			status, err = fsys.client.CreatePromotionImport(ctx, promotion.CreateImportRequest{
				MigrationID: record.MigrationID, AllocationProof: record.AllocationProof,
				Target: record.RemoteTarget, ExpectedTargetAbsent: true, Manifest: manifest,
				Plan: record.Plan, OwnerToken: record.OwnerToken, RecoveryToken: record.RecoveryToken,
			})
		}
		if err != nil {
			if promotionOutcomeIsKnown(err) {
				return fsys.recoverRetirePromotion(ctx, opDir, record)
			}
			return err
		}
		if err := observeLocalPromotionStatus(record, status); err != nil {
			return err
		}
		record.Phase = promotionPhaseStaging
		if err := persistPromotionRecord(opDir, record); err != nil {
			return err
		}
		return fsys.recoverAbortPromotion(ctx, opDir, record)
	case promotionPhaseStaging, promotionPhaseStagedVerified:
		return fsys.recoverAbortPromotion(ctx, opDir, record)
	case promotionPhaseCommitRequested:
		status, err := fsys.client.GetPromotionImport(ctx, record.MigrationID, promotion.GetImportRequest{RecoveryToken: record.RecoveryToken})
		if err != nil {
			return err
		}
		if record.PendingOwnerToken != "" || (status.State == "VERIFIED" && !time.Now().Before(status.LeaseExpiresAt)) {
			status, err = fsys.recoverPromotionOwner(ctx, opDir, record)
			if err != nil {
				return err
			}
		}
		switch status.State {
		case "COMMITTED":
			if status.TerminalResultDigest == "" {
				return fmt.Errorf("committed result missing digest")
			}
			if err := observeLocalPromotionStatus(record, status); err != nil {
				return err
			}
			record.Phase = promotionPhaseRemoteCommitted
			if err := persistPromotionRecord(opDir, record); err != nil {
				return err
			}
			return fsys.recoverCommittedPromotion(opDir, record)
		case "ABORTED":
			return fsys.persistRecoveredLocalAbort(opDir, record, status)
		case "ABORTING":
			if err := observeLocalPromotionStatus(record, status); err != nil {
				return err
			}
			return fsys.recoverAbortPromotion(ctx, opDir, record)
		case "VERIFIED", "COMMITTING":
			status, err = fsys.client.CommitPromotionImport(ctx, record.MigrationID, promotion.OwnerRequest{OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken})
			if isPromotionConflict(err) {
				status, err = fsys.recoverPromotionOwner(ctx, opDir, record)
				if err == nil && (status.State == "VERIFIED" || status.State == "COMMITTING") {
					status, err = fsys.client.CommitPromotionImport(ctx, record.MigrationID, promotion.OwnerRequest{OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken})
				}
			}
			if err != nil {
				return err
			}
			if status.State == "ABORTED" {
				return fsys.persistRecoveredLocalAbort(opDir, record, status)
			}
			if status.State == "ABORTING" {
				if err := observeLocalPromotionStatus(record, status); err != nil {
					return err
				}
				return fsys.recoverAbortPromotion(ctx, opDir, record)
			}
			if status.State != "COMMITTED" || status.TerminalResultDigest == "" {
				return fmt.Errorf("commit recovery returned state %s", status.State)
			}
			if err := observeLocalPromotionStatus(record, status); err != nil {
				return err
			}
			record.Phase = promotionPhaseRemoteCommitted
			if err := persistPromotionRecord(opDir, record); err != nil {
				return err
			}
			return fsys.recoverCommittedPromotion(opDir, record)
		default:
			return fmt.Errorf("unexpected commit recovery state %s", status.State)
		}
	case promotionPhaseRemoteCommitted, promotionPhaseSourceQuarantined, promotionPhasePublishedQuarantined:
		if record.FenceState == "RECONCILING" {
			return fsys.recoverRolledBackPromotion(opDir, record)
		}
		status, err := fsys.client.GetPromotionImport(ctx, record.MigrationID, promotion.GetImportRequest{RecoveryToken: record.RecoveryToken})
		if err != nil {
			return err
		}
		if status.Target != record.RemoteTarget || status.ManifestHash != record.ManifestHash {
			return fmt.Errorf("committed recovery identity mismatch")
		}
		switch status.State {
		case "COMMITTED":
			if status.TerminalResultDigest == "" || status.TerminalResultDigest != record.TerminalResultDigest {
				return fmt.Errorf("committed recovery result mismatch")
			}
			if err := observeLocalPromotionStatus(record, status); err != nil {
				return err
			}
			if record.Phase == promotionPhaseRemoteCommitted {
				if err := persistPromotionRecord(opDir, record); err != nil {
					return err
				}
				return fsys.recoverCommittedPromotion(opDir, record)
			}
			if record.Phase == promotionPhaseSourceQuarantined {
				if err := persistPromotionRecord(opDir, record); err != nil {
					return err
				}
				return fsys.releaseRecoveredPromotion(opDir, record)
			}
			return persistPromotionRecord(opDir, record)
		case "ABORTED":
			if status.RestoreGeneration <= record.RestoreGeneration || status.DatabaseIncarnation == record.DatabaseIncarnation || status.TerminalResultDigest == "" {
				return fmt.Errorf("committed result changed without a newer restore incarnation")
			}
			if err := observeLocalPromotionStatus(record, status); err != nil {
				return err
			}
			record.FenceState = "RECONCILING"
			if err := persistPromotionRecord(opDir, record); err != nil {
				return err
			}
			return fsys.recoverRolledBackPromotion(opDir, record)
		default:
			return fmt.Errorf("unexpected committed recovery state %s", status.State)
		}
	default:
		return fmt.Errorf("unsupported recovery phase %s", record.Phase)
	}
}

func (fsys *Dat9FS) recoverRetirePromotion(ctx context.Context, opDir string, record *localPromotionRecord) error {
	result, err := fsys.client.RetirePromotionImport(ctx, record.MigrationID, promotion.RetireImportRequest{AllocationProof: record.AllocationProof})
	if err != nil {
		return err
	}
	if result.Import != nil {
		if err := observeLocalPromotionStatus(record, result.Import); err != nil {
			return err
		}
		return fsys.recoverAbortPromotion(ctx, opDir, record)
	}
	if !result.Retired {
		return fmt.Errorf("allocation retirement had no winner")
	}
	record.Phase = promotionPhaseLocalAborted
	record.FenceState = "RELEASED"
	if err := persistPromotionRecord(opDir, record); err != nil {
		return err
	}
	fsys.acknowledgePromotionResult(opDir, record)
	return nil
}

func (fsys *Dat9FS) recoverAbortPromotion(ctx context.Context, opDir string, record *localPromotionRecord) error {
	status, err := fsys.client.AbortPromotionImport(ctx, record.MigrationID, promotion.OwnerRequest{OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken})
	if isPromotionConflict(err) {
		status, err = fsys.recoverPromotionOwner(ctx, opDir, record)
		if err != nil {
			return err
		}
		switch status.State {
		case "ABORTED":
			return fsys.persistRecoveredLocalAbort(opDir, record, status)
		case "ABORTING":
			// The original cleanup attempt is durable and remains owned by
			// this exact owner tuple; retrying Abort only resumes it.
		case "CREATED", "STAGING", "VERIFIED":
		default:
			return fmt.Errorf("abort ownership recovery returned state %s", status.State)
		}
		status, err = fsys.client.AbortPromotionImport(ctx, record.MigrationID, promotion.OwnerRequest{OwnerEpoch: record.OwnerEpoch, OwnerToken: record.OwnerToken})
	}
	if err != nil {
		return err
	}
	if status.State != "ABORTED" || status.TerminalResultDigest == "" {
		return fmt.Errorf("abort recovery returned state %s", status.State)
	}
	return fsys.persistRecoveredLocalAbort(opDir, record, status)
}

func isPromotionConflict(err error) bool {
	var status *client.StatusError
	return errors.As(err, &status) && status.Code == "promotion_conflict"
}

// recoverPromotionOwner adopts only the original migration after its durable
// owner lease expires. The replacement token is journaled before dispatch so
// a lost TakeOver response can be recovered without inventing another token.
func (fsys *Dat9FS) recoverPromotionOwner(ctx context.Context, opDir string, record *localPromotionRecord) (*promotion.ImportStatus, error) {
	status, err := fsys.client.GetPromotionImport(ctx, record.MigrationID, promotion.GetImportRequest{RecoveryToken: record.RecoveryToken})
	if err != nil {
		return nil, err
	}
	if record.PendingOwnerToken != "" && status.OwnerEpoch == record.OwnerEpoch+1 {
		if err := observeLocalPromotionStatusForOwner(record, status, record.OwnerEpoch+1); err != nil {
			return nil, err
		}
		record.OwnerToken = record.PendingOwnerToken
		record.PendingOwnerToken = ""
		if err := persistPromotionRecord(opDir, record); err != nil {
			return nil, err
		}
		return status, nil
	}
	if err := observeLocalPromotionStatus(record, status); err != nil {
		return nil, err
	}
	if status.State == "COMMITTED" || status.State == "ABORTED" || status.State == "ABORTING" || status.State == "COMMITTING" {
		return status, nil
	}
	if status.State != "CREATED" && status.State != "STAGING" && status.State != "VERIFIED" {
		return nil, fmt.Errorf("promotion ownership recovery returned state %s", status.State)
	}
	if record.PendingOwnerToken == "" {
		record.PendingOwnerToken = randomPromotionSecret(32)
		if err := persistPromotionRecord(opDir, record); err != nil {
			return nil, err
		}
	}
	if wait := time.Until(status.LeaseExpiresAt) + 25*time.Millisecond; wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	taken, err := fsys.client.TakeOverPromotionImport(ctx, record.MigrationID, promotion.TakeOverImportRequest{
		ExpectedOwnerEpoch: record.OwnerEpoch,
		RecoveryToken:      record.RecoveryToken,
		NewOwnerToken:      record.PendingOwnerToken,
	})
	if err != nil {
		// A lost response or a deadline winner is resolved only by another
		// authenticated read of this same migration.
		resolveCtx, resolveCancel := detachedPromotionContext(ctx)
		defer resolveCancel()
		taken, err = fsys.client.GetPromotionImport(resolveCtx, record.MigrationID, promotion.GetImportRequest{RecoveryToken: record.RecoveryToken})
		if err != nil {
			return nil, err
		}
	}
	if taken.OwnerEpoch == record.OwnerEpoch+1 {
		if err := observeLocalPromotionStatusForOwner(record, taken, record.OwnerEpoch+1); err != nil {
			return nil, err
		}
		record.OwnerToken = record.PendingOwnerToken
		record.PendingOwnerToken = ""
		if err := persistPromotionRecord(opDir, record); err != nil {
			return nil, err
		}
		return taken, nil
	}
	if err := observeLocalPromotionStatus(record, taken); err != nil {
		return nil, err
	}
	if taken.State == "ABORTING" || taken.State == "ABORTED" || taken.State == "COMMITTED" || taken.State == "COMMITTING" {
		return taken, nil
	}
	return nil, fmt.Errorf("promotion owner takeover had no durable winner")
}

func (fsys *Dat9FS) persistRecoveredLocalAbort(opDir string, record *localPromotionRecord, status *promotion.ImportStatus) error {
	if err := observeLocalPromotionStatus(record, status); err != nil {
		return err
	}
	record.Phase = promotionPhaseLocalAborted
	record.FenceState = "RELEASED"
	if err := persistPromotionRecord(opDir, record); err != nil {
		return err
	}
	fsys.acknowledgePromotionResult(opDir, record)
	return nil
}

func (fsys *Dat9FS) recoverCommittedPromotion(opDir string, record *localPromotionRecord) error {
	sourceAbs, err := fsys.localOverlay.abs(record.SourcePath)
	if err != nil {
		return err
	}
	if err := verifyPromotionSourceParent(sourceAbs, record); err != nil {
		return err
	}
	quarantine := filepath.Join(opDir, "source")
	sourceInfo, sourceErr := os.Lstat(sourceAbs)
	quarantineInfo, quarantineErr := os.Lstat(quarantine)
	switch {
	case sourceErr == nil && os.IsNotExist(quarantineErr):
		if !sourceInfo.IsDir() {
			return fmt.Errorf("recovery source is not directory")
		}
		if err := verifyPromotionSourceIdentity(sourceAbs, record.SourceIncarnationUUID); err != nil {
			return err
		}
		if err := os.Rename(sourceAbs, quarantine); err != nil {
			return err
		}
		if err := syncPromotionDirectories(filepath.Dir(sourceAbs), opDir); err != nil {
			return err
		}
	case os.IsNotExist(sourceErr) && quarantineErr == nil:
		if !quarantineInfo.IsDir() {
			return fmt.Errorf("recovery quarantine is not directory")
		}
		if err := verifyPromotionSourceIdentity(quarantine, record.SourceIncarnationUUID); err != nil {
			return err
		}
	default:
		return fmt.Errorf("recovery source/quarantine ownership is ambiguous")
	}
	record.Phase = promotionPhaseSourceQuarantined
	if err := persistPromotionRecord(opDir, record); err != nil {
		return err
	}
	return fsys.releaseRecoveredPromotion(opDir, record)
}

func (fsys *Dat9FS) releaseRecoveredPromotion(opDir string, record *localPromotionRecord) error {
	quarantine := filepath.Join(opDir, "source")
	if err := verifyPromotionSourceIdentity(quarantine, record.SourceIncarnationUUID); err != nil {
		return err
	}
	record.Phase = promotionPhasePublishedQuarantined
	record.FenceState = "RELEASED"
	if err := persistPromotionRecord(opDir, record); err != nil {
		return err
	}
	fsys.acknowledgePromotionResult(opDir, record)
	return nil
}

// recoverRolledBackPromotion handles an emergency restore that made a newer
// server incarnation authoritative after this mount had already observed a
// COMMITTED result. The private quarantine is retained specifically so the
// only local copy can be restored before the mount becomes reachable again.
func (fsys *Dat9FS) recoverRolledBackPromotion(opDir string, record *localPromotionRecord) error {
	if record.FenceState != "RECONCILING" || record.ServerState != "ABORTED" || record.TerminalResultDigest == "" {
		return fmt.Errorf("invalid rolled-back promotion reconciliation state")
	}
	sourceAbs, err := fsys.localOverlay.abs(record.SourcePath)
	if err != nil {
		return err
	}
	if err := verifyPromotionSourceParent(sourceAbs, record); err != nil {
		return err
	}
	quarantine := filepath.Join(opDir, "source")
	sourceInfo, sourceErr := os.Lstat(sourceAbs)
	quarantineInfo, quarantineErr := os.Lstat(quarantine)
	switch {
	case sourceErr == nil && os.IsNotExist(quarantineErr):
		if !sourceInfo.IsDir() {
			return fmt.Errorf("reconciled source is not directory")
		}
		if err := verifyPromotionSourceIdentity(sourceAbs, record.SourceIncarnationUUID); err != nil {
			return err
		}
	case os.IsNotExist(sourceErr) && quarantineErr == nil:
		if !quarantineInfo.IsDir() {
			return fmt.Errorf("reconciliation quarantine is not directory")
		}
		if err := verifyPromotionSourceIdentity(quarantine, record.SourceIncarnationUUID); err != nil {
			return err
		}
		if err := os.Rename(quarantine, sourceAbs); err != nil {
			return err
		}
		if err := syncPromotionDirectories(filepath.Dir(sourceAbs), opDir); err != nil {
			return err
		}
	default:
		return fmt.Errorf("rolled-back promotion source ownership is ambiguous")
	}
	record.Phase = promotionPhaseLocalAborted
	record.FenceState = "RELEASED"
	if err := persistPromotionRecord(opDir, record); err != nil {
		return err
	}
	fsys.acknowledgePromotionResult(opDir, record)
	return nil
}

func writePromotionFileAtomically(dir, name string, value []byte, mode fs.FileMode) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".promotion-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(value); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, name); err != nil {
		return err
	}
	return syncPromotionDirectory(dir)
}

func syncPromotionFileAndParent(name string) error {
	file, err := os.Open(name)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncPromotionDirectory(filepath.Dir(name))
}

func syncPromotionDirectories(names ...string) error {
	for _, name := range names {
		if err := syncPromotionDirectory(name); err != nil {
			return err
		}
	}
	return nil
}

func syncPromotionDirectory(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func randomPromotionSecret(size int) string {
	var value strings.Builder
	for value.Len() < size*2 {
		value.WriteString(strings.ReplaceAll(uuid.NewString(), "-", ""))
	}
	return value.String()[:size*2]
}

func promotionFuseStatus(err error) gofuse.Status {
	if err == nil {
		return gofuse.OK
	}
	if errors.Is(err, promotion.ErrStorageBackendUnsupported) || errors.Is(err, promotion.ErrManifestLimitExceeded) {
		return gofuse.Status(syscall.EXDEV)
	}
	var status *client.StatusError
	if errors.As(err, &status) {
		switch status.Code {
		case "promotion_not_enabled", "promotion_not_supported", "promotion_storage_backend_unsupported":
			return gofuse.Status(syscall.EXDEV)
		case "target_conflict":
			return gofuse.Status(syscall.EEXIST)
		case "target_precondition_changed", "promotion_restore_interrupted", "promotion_restore_rolled_back", "promotion_identity_epoch_unavailable", "promotion_restore_fenced":
			return gofuse.Status(syscall.EAGAIN)
		case "promotion_deadline_exceeded":
			return gofuse.Status(syscall.ETIMEDOUT)
		case "permission_denied":
			return gofuse.EACCES
		case "quota_exceeded", "promotion_identity_budget_exceeded":
			return gofuse.Status(syscall.EDQUOT)
		case "promotion_identity_capacity_unavailable":
			return gofuse.Status(syscall.EAGAIN)
		case "promotion_limit_exceeded":
			return gofuse.Status(syscall.EFBIG)
		default:
			if status.StatusCode == 403 {
				return gofuse.EACCES
			}
		}
	}
	return gofuse.Status(syscall.EIO)
}
