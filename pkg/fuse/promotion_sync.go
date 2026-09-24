package fuse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	stdfs "io/fs"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/promotion"
)

const (
	promotionPhasePrepared  = "prepared"
	promotionPhaseCommitted = "committed"
	promotionPhaseReleased  = "released"
	promotionPhaseAborted   = "aborted"
	promotionRequestTimeout = 10 * time.Minute
)

// Test hooks inject local durability failures at exact boundaries without
// changing the production filesystem operations.
var (
	testHookBeforePromotionDirSync      func(string) error
	testHookBeforePromotionRecordRemove func() error
	testHookBeforeAtomicWriteDirSync    func(string) error
)

type localPromotionRecord struct {
	OperationID          string `json:"operation_id"`
	RemoteIdentitySHA256 string `json:"remote_identity_sha256"`
	Source               string `json:"source"`
	TargetLocal          string `json:"target_local"`
	TargetRemote         string `json:"target_remote"`
	ManifestSHA256       string `json:"manifest_sha256"`
	Phase                string `json:"phase"`
}

// promotionRecoveryRefusal is an operator-action condition, not a retryable
// outage. The journal identifies a publish whose outcome cannot be changed or
// discarded safely from client-side evidence alone.
type promotionRecoveryRefusal struct {
	JournalPath string
	Phase       string
	OperationID string
	Reason      string
}

func (e *promotionRecoveryRefusal) Error() string {
	return fmt.Sprintf("%s (journal=%s phase=%s operation_id=%s)", e.Reason, e.JournalPath, e.Phase, e.OperationID)
}

type promotionStateConflict struct {
	reason string
}

func (e *promotionStateConflict) Error() string { return e.reason }

func newPromotionStateConflict(reason string) error {
	return &promotionStateConflict{reason: reason}
}

func (fs *Dat9FS) refusePromotionRecovery(record *localPromotionRecord, reason string) error {
	return &promotionRecoveryRefusal{
		JournalPath: fs.promotionRecordPath(),
		Phase:       record.Phase,
		OperationID: record.OperationID,
		Reason:      reason,
	}
}

func (fs *Dat9FS) refuseUnreadablePromotionRecovery(err error) error {
	return &promotionRecoveryRefusal{
		JournalPath: fs.promotionRecordPath(),
		Phase:       "unknown",
		OperationID: "unknown",
		Reason:      fmt.Sprintf("promotion journal is invalid or unreadable: %v", err),
	}
}

func isPermanentPromotionRecoveryError(err error) bool {
	var refusal *promotionRecoveryRefusal
	return errors.As(err, &refusal)
}

func (fs *Dat9FS) synchronousPromotionEnabled() bool {
	return fs != nil && fs.opts != nil && fs.opts.EnableSynchronousPromotion
}

// lockPromotionMutation prevents a promotion scan/commit from racing a FUSE
// open or mutation. Gate-off takes no lock and preserves the old path.
func (fs *Dat9FS) lockPromotionMutation() (func(), bool) {
	// promotionBlocked is also the mount-wide fail-closed barrier used when a
	// durable layer-cache rollback cannot complete. Check it even when the
	// synchronous-promotion preview is disabled; otherwise ordinary layer
	// writers could build on a shadow/.meta pair that startup will reject.
	if fs.promotionBlocked.Load() {
		return nil, false
	}
	if !fs.synchronousPromotionEnabled() {
		return func() {}, true
	}
	fs.promotionBarrier.RLock()
	if fs.promotionBlocked.Load() {
		fs.promotionBarrier.RUnlock()
		return nil, false
	}
	return fs.promotionBarrier.RUnlock, true
}

func (fs *Dat9FS) promoteLocalTree(ctx context.Context, input *gofuse.RenameIn, source, targetLocal string) gofuse.Status {
	if !fs.synchronousPromotionEnabled() || fs.localOverlay == nil || fs.layerEnabled() {
		return gofuse.Status(syscall.EXDEV)
	}
	if fs.promotionRootLock != nil {
		if err := validatePromotionRootLock(fs.promotionRootLock); err != nil {
			safeLogPrintf("promotion LocalRoot lock is no longer valid: %v", err)
			fs.promotionBlocked.Store(true)
			return gofuse.EIO
		}
	}
	fs.promotionBarrier.Lock()
	defer fs.promotionBarrier.Unlock()
	fs.promotionLifecycleMu.Lock()
	defer fs.promotionLifecycleMu.Unlock()
	if fs.promotionBlocked.Load() {
		return gofuse.EIO
	}
	if _, err := os.Lstat(fs.promotionRecordPath()); err == nil {
		// A released record is harmless to ordinary namespace work, but this
		// preview has one durable operation slot. Never overwrite it while its
		// private quarantine/receipt cleanup is still pending.
		return gofuse.Status(syscall.EBUSY)
	} else if !errors.Is(err, os.ErrNotExist) {
		return localErrToFuseStatus(err)
	}
	if fs.hasPromotionOpenHandle(source, targetLocal) {
		return gofuse.Status(syscall.EXDEV)
	}
	if pathWithin(source, targetLocal) {
		return gofuse.Status(syscall.EXDEV)
	}
	if _, err := fs.localOverlay.Lstat(targetLocal); err == nil {
		return gofuse.Status(syscall.EEXIST)
	} else if !errors.Is(err, os.ErrNotExist) {
		return localErrToFuseStatus(err)
	}
	targetRemote := fs.remotePath(targetLocal)
	if _, err := fs.client.StatCtx(ctx, targetRemote); err == nil {
		return gofuse.Status(syscall.EEXIST)
	} else if !client.IsNotFound(err) {
		return httpToFuseStatus(err)
	}
	targetRemote, err := pathutil.CanonicalizeDir(targetRemote)
	if err != nil {
		return gofuse.Status(syscall.EXDEV)
	}

	request, err := fs.scanPromotionTree(ctx, source, targetLocal, targetRemote, uuid.NewString())
	if err != nil {
		safeLogPrintf("promotion preflight %s -> %s: %v", source, targetLocal, err)
		return gofuse.Status(syscall.EXDEV)
	}
	requestBody, err := fs.preparePromotionRequest(ctx, request)
	if err != nil {
		safeLogPrintf("promotion request preflight %s -> %s: %v", source, targetLocal, err)
		return gofuse.Status(syscall.EXDEV)
	}
	if err := fs.validatePromotionQuarantineLayout(source); err != nil {
		safeLogPrintf("promotion quarantine preflight %s: %v", source, err)
		return gofuse.Status(syscall.EXDEV)
	}
	record := localPromotionRecord{
		OperationID: request.OperationID, Source: source, TargetLocal: targetLocal,
		RemoteIdentitySHA256: fs.promotionRemoteIdentitySHA256(), TargetRemote: targetRemote,
		ManifestSHA256: request.ManifestSHA256, Phase: promotionPhasePrepared,
	}
	if err := fs.writePromotionRecord(record); err != nil {
		return localErrToFuseStatus(err)
	}
	result, definitive, err := fs.publishPreparedPromotionWithRecovery(ctx, request, requestBody)
	if err != nil {
		if definitive {
			if cleanupErr := fs.abortPromotionRecord(&record); cleanupErr != nil {
				fs.promotionBlocked.Store(true)
				return gofuse.EIO
			}
			return httpToFuseStatus(err)
		}
		// Keep the durable record and fail closed until a remount completes
		// recovery. The current implementation blocks all namespace operations,
		// including opens, so neither reads nor mutations can race an
		// outcome-unknown publish.
		fs.promotionBlocked.Store(true)
		return gofuse.EIO
	}
	if !matchingPromotionResult(request, result) {
		fs.promotionBlocked.Store(true)
		return gofuse.EIO
	}
	record.Phase = promotionPhaseCommitted
	if err := fs.writePromotionRecord(record); err != nil {
		fs.promotionBlocked.Store(true)
		return gofuse.EIO
	}
	if err := fs.finishCommittedPromotion(ctx, &record); err != nil {
		fs.promotionBlocked.Store(true)
		return gofuse.EIO
	}
	fs.finishLocalRename(input, source, targetLocal)
	fs.scheduleReleasedPromotionCleanup(record)
	return gofuse.OK
}

func (fs *Dat9FS) publishPromotionWithRecovery(ctx context.Context, request promotion.PublishRequest) (*promotion.Result, bool, error) {
	body, err := fs.preparePromotionRequest(ctx, request)
	if err != nil {
		return nil, true, err
	}
	return fs.publishPreparedPromotionWithRecovery(ctx, request, body)
}

func (fs *Dat9FS) publishPreparedPromotionWithRecovery(ctx context.Context, request promotion.PublishRequest, body []byte) (*promotion.Result, bool, error) {
	commitCtx, cancel := context.WithTimeout(ctx, promotionRequestTimeout)
	defer cancel()
	lookupCtx, lookupCancel := context.WithTimeout(context.WithoutCancel(ctx), promotionRequestTimeout)
	defer lookupCancel()
	var lastErr error
	mayHaveReachedServer := false
	for attempt := 0; attempt < 3; attempt++ {
		postCtx := commitCtx
		if mayHaveReachedServer {
			// Once a request may have reached the server, resolution must outlive
			// cancellation of the original FUSE request. An exact POST retry is
			// also the server's accounting/event boundary; a side-effect-free GET
			// is evidence only and cannot complete the live syscall by itself.
			postCtx = lookupCtx
		}
		result, err := fs.publishPreparedPromotionRequest(postCtx, body)
		if err == nil {
			return result, true, nil
		}
		definitelyRejected := promotionRequestDefinitelyRejected(err)
		if !definitelyRejected {
			// This bit is deliberately sticky across retries. A later proxy-side
			// rejection cannot prove that an earlier forwarded request will not
			// commit under the same operation ID.
			mayHaveReachedServer = true
		}
		lastErr = err
		// Once POST may have reached the server, a canceled FUSE request is not
		// authority to abandon outcome resolution. Keep request values, detach
		// cancellation, and bound the lookup independently; the durable journal
		// remains the fallback if this bounded lookup cannot decide.
		stored, _, statusErr := fs.getPromotionResult(lookupCtx, request.OperationID, request.Target)
		if statusErr == nil {
			if !matchingPromotionResult(request, stored) {
				return stored, true, nil
			}
			// The receipt proves the namespace commit, but GET is deliberately
			// side-effect-free. Continue with the same operation ID until an exact
			// POST retry succeeds and completes accounting plus the structural
			// reset event. If that never happens, keep the journal fail-closed.
			continue
		}
		if client.IsNotFound(statusErr) {
			if definitelyRejected && !mayHaveReachedServer {
				return nil, true, err
			}
			continue
		}
		if definitelyRejected && !mayHaveReachedServer {
			// A result-lookup outage does not erase a trustworthy rejection when
			// no earlier attempt could have reached the server.
			return nil, true, err
		}
		lastErr = statusErr
		break
	}
	return nil, false, lastErr
}

func promotionRequestDefinitelyRejected(err error) bool {
	if errors.Is(err, promotion.ErrInvalidRequest) || errors.Is(err, promotion.ErrLimitExceeded) {
		return true
	}
	var statusErr *client.StatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	if statusErr.StatusCode == http.StatusInsufficientStorage {
		return true
	}
	if statusErr.StatusCode < 400 || statusErr.StatusCode >= 500 {
		return false
	}
	// A proxy may synthesize these responses after forwarding the request, so
	// they do not prove that the publish transaction did not commit. Resolve
	// them through the durable operation receipt like a dropped connection.
	switch statusErr.StatusCode {
	case 408, 425, 429:
		return false
	default:
		return true
	}
}

func matchingPromotionResult(request promotion.PublishRequest, result *promotion.Result) bool {
	return result != nil && result.Committed && result.OperationID == request.OperationID &&
		result.Target == request.Target && result.ManifestSHA256 == request.ManifestSHA256
}

func (fs *Dat9FS) hasPromotionOpenHandle(source, target string) bool {
	if fs.openHandles.HasPathPrefix(source) || fs.openHandles.HasPathPrefix(target) {
		return true
	}
	for _, handle := range fs.dirHandles.Snapshot() {
		if handle == nil {
			continue
		}
		if pathWithin(source, handle.Path) || pathWithin(target, handle.Path) {
			return true
		}
	}
	return false
}

func pathWithin(root, candidate string) bool {
	return candidate == root || strings.HasPrefix(candidate, strings.TrimSuffix(root, "/")+"/")
}

func (fs *Dat9FS) scanPromotionTree(ctx context.Context, source, targetLocal, targetRemote, operationID string) (promotion.PublishRequest, error) {
	sourceAbs, err := fs.localOverlay.abs(source)
	if err != nil {
		return promotion.PublishRequest{}, err
	}
	rootInfo, err := os.Lstat(sourceAbs)
	if err != nil || !rootInfo.IsDir() {
		return promotion.PublishRequest{}, fmt.Errorf("source is not a directory")
	}
	entries := make([]promotion.Entry, 0)
	var totalBytes int64
	err = filepath.WalkDir(sourceAbs, func(abs string, dirEntry stdfs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		info, err := dirEntry.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(sourceAbs, abs)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel != "." {
			canonicalRel, canonicalErr := pathutil.Canonicalize("/" + rel)
			if canonicalErr != nil || canonicalRel != "/"+rel {
				return fmt.Errorf("non-canonical source path: %s", rel)
			}
		}
		if len(entries) >= promotion.MaxEntries {
			return promotion.ErrLimitExceeded
		}
		if len(rel) > promotion.MaxPathBytes || (rel != "." && strings.Count(rel, "/")+1 > promotion.MaxDepth) {
			return promotion.ErrLimitExceeded
		}
		localPath := source
		if rel != "." {
			localPath = path.Join(source, rel)
		}
		if fs.observePathPolicyWithContext(ctx, localPath) != PathLayerLocalOnly {
			return fmt.Errorf("mixed policy at %s", localPath)
		}
		finalLocalPath := targetLocal
		if rel != "." {
			finalLocalPath = path.Join(targetLocal, rel)
		}
		finalLayer := fs.observePathPolicyWithContext(ctx, finalLocalPath)
		if info.IsDir() {
			finalLayer = fs.observeDirPathPolicyWithContext(ctx, finalLocalPath)
		}
		if finalLayer != PathLayerRemotePersistent {
			return fmt.Errorf("non-remote target policy at %s", finalLocalPath)
		}
		if len(fs.xattrs.List(localPath)) != 0 {
			return fmt.Errorf("xattr: %s", localPath)
		}
		if err := validatePromotionLocalMetadata(abs, info, fs.uid, fs.gid); err != nil {
			return err
		}
		if ino, ok := fs.inodes.GetInode(localPath); ok {
			if visible, ok := fs.inodes.GetEntry(ino); ok &&
				((visible.HasUID && visible.Uid != fs.uid) || (visible.HasGID && visible.Gid != fs.gid)) {
				return fmt.Errorf("foreign visible ownership: %s", localPath)
			}
		}
		if info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
			return fmt.Errorf("special permission bits: %s", localPath)
		}
		entry := promotion.Entry{RelativePath: rel, Mode: uint32(info.Mode().Perm()), MtimeUnixNano: info.ModTime().UnixNano()}
		switch {
		case info.IsDir():
			entry.Type = promotion.EntryDirectory
		case info.Mode().IsRegular():
			entry.Type = promotion.EntryFile
			if info.Size() < 0 || info.Size() > promotion.MaxTotalBytes-totalBytes {
				return promotion.ErrLimitExceeded
			}
			data, err := os.ReadFile(abs)
			if err != nil {
				return err
			}
			after, err := os.Lstat(abs)
			if err != nil || !samePromotionFile(info, after) {
				return fmt.Errorf("source changed while scanning: %s", localPath)
			}
			entry.Data = data
			entry.SizeBytes = int64(len(data))
			totalBytes += entry.SizeBytes
			digest := sha256.Sum256(data)
			entry.ChecksumSHA256 = hex.EncodeToString(digest[:])
		default:
			return fmt.Errorf("unsupported entry at %s", localPath)
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return promotion.PublishRequest{}, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].RelativePath == "." {
			return entries[j].RelativePath != "."
		}
		if entries[j].RelativePath == "." {
			return false
		}
		return entries[i].RelativePath < entries[j].RelativePath
	})
	request := promotion.PublishRequest{OperationID: operationID, Target: targetRemote, Entries: entries}
	request.ManifestSHA256 = promotion.ManifestSHA256(entries)
	return request, nil
}

func (fs *Dat9FS) promotionStateDir() string {
	return filepath.Join(fs.opts.LocalRoot, ".drive9", "promotion")
}

func (fs *Dat9FS) promotionRecordPath() string {
	return filepath.Join(fs.promotionStateDir(), "record.json")
}

func (fs *Dat9FS) promotionRemoteIdentitySHA256() string {
	h := sha256.New()
	_, _ = h.Write([]byte(fs.client.BaseURL()))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(fs.client.APIKey()))
	return hex.EncodeToString(h.Sum(nil))
}

func (fs *Dat9FS) promotionQuarantinePath(operationID string) (string, error) {
	root := filepath.Join(fs.promotionStateDir(), "quarantine")
	joined := filepath.Join(root, operationID)
	rel, err := filepath.Rel(root, joined)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("promotion quarantine path escapes its root")
	}
	if err := validatePromotionDirectoryChain(fs.opts.LocalRoot, joined, true); err != nil {
		return "", err
	}
	return joined, nil
}

func (fs *Dat9FS) validatePromotionQuarantineLayout(source string) error {
	sourceAbs, err := fs.localOverlay.abs(source)
	if err != nil {
		return err
	}
	if err := validatePromotionDirectoryChain(fs.opts.LocalRoot, sourceAbs, false); err != nil {
		return err
	}
	quarantineRoot := filepath.Join(fs.promotionStateDir(), "quarantine")
	if err := ensurePromotionDirDurable(quarantineRoot, 0o700); err != nil {
		return err
	}
	if err := validatePromotionDirectoryChain(fs.opts.LocalRoot, quarantineRoot, false); err != nil {
		return err
	}
	return validatePromotionSameFilesystem(sourceAbs, quarantineRoot)
}

func (fs *Dat9FS) writePromotionRecord(record localPromotionRecord) error {
	if err := ensurePromotionDirDurable(fs.promotionStateDir(), 0o700); err != nil {
		return err
	}
	body, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return atomicWrite(fs.promotionRecordPath(), body)
}

func (fs *Dat9FS) readPromotionRecord() (*localPromotionRecord, error) {
	if err := validatePromotionDirectoryChain(fs.opts.LocalRoot, fs.promotionStateDir(), true); err != nil {
		return nil, err
	}
	info, err := os.Lstat(fs.promotionRecordPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("promotion record is not a regular file")
	}
	body, err := os.ReadFile(fs.promotionRecordPath())
	if err != nil {
		return nil, err
	}
	var record localPromotionRecord
	if err := json.Unmarshal(body, &record); err != nil {
		return nil, err
	}
	if record.OperationID == "" || record.RemoteIdentitySHA256 == "" || record.Source == "" || record.TargetLocal == "" || record.TargetRemote == "" || record.ManifestSHA256 == "" {
		return nil, errors.New("incomplete promotion record")
	}
	parsedOperationID, err := uuid.Parse(record.OperationID)
	if err != nil || parsedOperationID.String() != record.OperationID {
		return nil, errors.New("invalid promotion record operation_id")
	}
	switch record.Phase {
	case promotionPhasePrepared, promotionPhaseCommitted, promotionPhaseReleased, promotionPhaseAborted:
	default:
		return nil, fmt.Errorf("invalid promotion record phase %q", record.Phase)
	}
	canonicalSource, err := pathutil.Canonicalize(record.Source)
	if err != nil || canonicalSource != record.Source || canonicalSource == "/" {
		return nil, errors.New("invalid promotion record source")
	}
	canonicalTarget, err := pathutil.Canonicalize(record.TargetLocal)
	if err != nil || canonicalTarget != record.TargetLocal || canonicalTarget == "/" {
		return nil, errors.New("invalid promotion record local target")
	}
	if pathWithin(record.Source, record.TargetLocal) || pathWithin(record.TargetLocal, record.Source) {
		return nil, errors.New("invalid promotion record overlapping paths")
	}
	canonicalRemote, err := pathutil.CanonicalizeDir(record.TargetRemote)
	if err != nil || canonicalRemote != record.TargetRemote || canonicalRemote == "/" {
		return nil, errors.New("invalid promotion record remote target")
	}
	expectedRemote, err := pathutil.CanonicalizeDir(fs.remotePath(record.TargetLocal))
	if err != nil || expectedRemote != record.TargetRemote {
		return nil, errors.New("promotion record target mapping mismatch")
	}
	if len(record.ManifestSHA256) != sha256.Size*2 || strings.ToLower(record.ManifestSHA256) != record.ManifestSHA256 {
		return nil, errors.New("invalid promotion record manifest digest")
	}
	if _, err := hex.DecodeString(record.ManifestSHA256); err != nil {
		return nil, errors.New("invalid promotion record manifest digest")
	}
	if len(record.RemoteIdentitySHA256) != sha256.Size*2 || strings.ToLower(record.RemoteIdentitySHA256) != record.RemoteIdentitySHA256 {
		return nil, errors.New("invalid promotion record remote identity")
	}
	if _, err := hex.DecodeString(record.RemoteIdentitySHA256); err != nil || record.RemoteIdentitySHA256 != fs.promotionRemoteIdentitySHA256() {
		return nil, errors.New("promotion record remote identity mismatch")
	}
	sourceAbs, err := fs.localOverlay.abs(record.Source)
	if err != nil {
		return nil, errors.New("promotion record source escapes local overlay")
	}
	if err := validatePromotionDirectoryChain(fs.opts.LocalRoot, sourceAbs, true); err != nil {
		return nil, fmt.Errorf("invalid promotion record source path: %w", err)
	}
	targetAbs, err := fs.localOverlay.abs(record.TargetLocal)
	if err != nil {
		return nil, errors.New("promotion record target escapes local overlay")
	}
	if err := validatePromotionDirectoryChain(fs.opts.LocalRoot, targetAbs, true); err != nil {
		return nil, fmt.Errorf("invalid promotion record target path: %w", err)
	}
	if _, err := fs.promotionQuarantinePath(record.OperationID); err != nil {
		return nil, err
	}
	return &record, nil
}

func (fs *Dat9FS) removePromotionRecord() error {
	if err := validatePromotionDirectoryChain(fs.opts.LocalRoot, fs.promotionStateDir(), true); err != nil {
		return err
	}
	if info, err := os.Lstat(fs.promotionRecordPath()); err == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("promotion record is not a regular file")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		return nil
	} else {
		return err
	}
	if testHookBeforePromotionRecordRemove != nil {
		if err := testHookBeforePromotionRecordRemove(); err != nil {
			return err
		}
	}
	err := os.Remove(fs.promotionRecordPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fsyncPromotionDir(fs.promotionStateDir())
}

// abortPromotionRecord durably marks a known non-commit before attempting to
// remove the journal. If removal or its directory fsync fails, startup sees
// "aborted" and can only retry cleanup; it must never publish the request.
func (fs *Dat9FS) abortPromotionRecord(record *localPromotionRecord) error {
	record.Phase = promotionPhaseAborted
	if err := fs.writePromotionRecord(*record); err != nil {
		return err
	}
	return fs.removePromotionRecord()
}

func (fs *Dat9FS) finishCommittedPromotion(ctx context.Context, record *localPromotionRecord) error {
	qPath, err := fs.promotionQuarantinePath(record.OperationID)
	if err != nil {
		return err
	}
	sourceAbs, err := fs.localOverlay.abs(record.Source)
	if err != nil {
		return err
	}
	sourceInfo, sourceErr := os.Lstat(sourceAbs)
	_, qErr := os.Lstat(qPath)
	sourceExists := sourceErr == nil
	qExists := qErr == nil
	if sourceExists && qExists {
		// A cross-directory rename followed by quarantine-parent fsync and a
		// crash before source-parent fsync may replay with both directory names.
		// Forward-complete only when both copies are exactly the committed
		// snapshot; otherwise keep both and fail closed rather than guessing.
		request, scanErr := fs.scanPromotionTree(ctx, record.Source, record.TargetLocal, record.TargetRemote, record.OperationID)
		if scanErr != nil || request.ManifestSHA256 != record.ManifestSHA256 {
			return newPromotionStateConflict("promotion source does not match committed manifest")
		}
		if err := comparePromotionTrees(sourceAbs, qPath); err != nil {
			return newPromotionStateConflict(fmt.Sprintf("promotion source and quarantine differ: %v", err))
		}
		if err := os.RemoveAll(sourceAbs); err != nil {
			return err
		}
		if err := fsyncPromotionDir(filepath.Dir(sourceAbs)); err != nil {
			return err
		}
		sourceExists = false
	}
	if sourceExists {
		if !sourceInfo.IsDir() {
			return newPromotionStateConflict("promotion source is no longer a directory")
		}
		// A committed receipt proves the remote snapshot, not that an offline
		// actor left the still-local source untouched. Revalidate immediately
		// before quarantine so restart can never destroy edits made after the
		// original scan.
		request, scanErr := fs.scanPromotionTree(ctx, record.Source, record.TargetLocal, record.TargetRemote, record.OperationID)
		if scanErr != nil || request.ManifestSHA256 != record.ManifestSHA256 {
			return newPromotionStateConflict("promotion source does not match committed manifest")
		}
		if err := ensurePromotionDirDurable(filepath.Dir(qPath), 0o700); err != nil {
			return err
		}
		if err := os.Rename(sourceAbs, qPath); err != nil {
			return err
		}
		// Persist the quarantine name before the source-name removal. If the
		// second fsync fails or power is lost between them, recovery may see
		// both names, but it must never lose the only durable local copy.
		if err := fsyncPromotionDir(filepath.Dir(qPath)); err != nil {
			return err
		}
		if err := fsyncPromotionDir(filepath.Dir(sourceAbs)); err != nil {
			return err
		}
		qExists = true
	}
	if !qExists && record.Phase != promotionPhaseReleased {
		return newPromotionStateConflict("promotion source and quarantine are both missing")
	}
	// Persist the local winner before deleting the remote receipt. A crash
	// after this write can finish locally without asking the server to prove
	// the already-acknowledged commit again.
	record.Phase = promotionPhaseReleased
	if err := fs.writePromotionRecord(*record); err != nil {
		return err
	}
	return nil
}

// cleanupReleasedPromotion only touches the operation's immutable receipt,
// operation-ID quarantine directory, and journal. In particular it must never
// inspect or remove record.Source: the user may recreate that path immediately
// after rename has returned success.
func (fs *Dat9FS) cleanupReleasedPromotion(ctx context.Context, record localPromotionRecord) error {
	if record.Phase != promotionPhaseReleased {
		return errors.New("promotion cleanup requires released phase")
	}
	result := promotion.Result{OperationID: record.OperationID, Target: record.TargetRemote, ManifestSHA256: record.ManifestSHA256, Committed: true}
	if err := fs.acknowledgePromotionResult(ctx, result); err != nil && !client.IsNotFound(err) {
		return err
	}
	qPath, err := fs.promotionQuarantinePath(record.OperationID)
	if err != nil {
		return err
	}
	fs.promotionLifecycleMu.Lock()
	defer fs.promotionLifecycleMu.Unlock()
	if err := os.RemoveAll(qPath); err != nil {
		return err
	}
	if err := fsyncPromotionDir(filepath.Dir(qPath)); err != nil {
		return err
	}
	return fs.removePromotionRecord()
}

func (fs *Dat9FS) scheduleReleasedPromotionCleanup(record localPromotionRecord) {
	go func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), promotionRequestTimeout)
		defer cancel()
		if err := fs.cleanupReleasedPromotion(cleanupCtx, record); err != nil {
			safeLogPrintf("promotion background cleanup operation_id=%s: %v", record.OperationID, err)
		}
	}()
}

func comparePromotionTrees(sourceRoot, quarantineRoot string) error {
	seen := make(map[string]struct{})
	err := filepath.WalkDir(sourceRoot, func(sourcePath string, entry stdfs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(sourceRoot, sourcePath)
		if err != nil {
			return err
		}
		seen[rel] = struct{}{}
		sourceInfo, err := entry.Info()
		if err != nil {
			return err
		}
		quarantinePath := filepath.Join(quarantineRoot, rel)
		quarantineInfo, err := os.Lstat(quarantinePath)
		if err != nil {
			return err
		}
		if sourceInfo.Mode().Type() != quarantineInfo.Mode().Type() ||
			sourceInfo.Mode().Perm() != quarantineInfo.Mode().Perm() ||
			sourceInfo.ModTime().UnixNano() != quarantineInfo.ModTime().UnixNano() {
			return fmt.Errorf("metadata mismatch at %s", filepath.ToSlash(rel))
		}
		switch {
		case sourceInfo.IsDir():
			return nil
		case sourceInfo.Mode().IsRegular():
			if sourceInfo.Size() != quarantineInfo.Size() {
				return fmt.Errorf("size mismatch at %s", filepath.ToSlash(rel))
			}
			sourceData, err := os.ReadFile(sourcePath)
			if err != nil {
				return err
			}
			quarantineData, err := os.ReadFile(quarantinePath)
			if err != nil {
				return err
			}
			if sha256.Sum256(sourceData) != sha256.Sum256(quarantineData) {
				return fmt.Errorf("content mismatch at %s", filepath.ToSlash(rel))
			}
			return nil
		default:
			return fmt.Errorf("unsupported entry at %s", filepath.ToSlash(rel))
		}
	})
	if err != nil {
		return err
	}
	return filepath.WalkDir(quarantineRoot, func(quarantinePath string, _ stdfs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(quarantineRoot, quarantinePath)
		if err != nil {
			return err
		}
		if _, ok := seen[rel]; !ok {
			return fmt.Errorf("extra quarantine entry at %s", filepath.ToSlash(rel))
		}
		return nil
	})
}

// recoverSynchronousPromotion completes the single durable local operation
// before FUSE begins serving requests.
func (fs *Dat9FS) recoverSynchronousPromotion(ctx context.Context) error {
	record, err := fs.readPromotionRecord()
	if err != nil {
		// A journal that exists but cannot be validated is not a retryable
		// server outage. Refuse startup permanently with an actionable path;
		// recovery must not guess at untrusted phase or operation identifiers.
		return fs.refuseUnreadablePromotionRecovery(err)
	}
	if record == nil {
		return nil
	}
	if record.Phase == promotionPhaseAborted {
		return fs.removePromotionRecord()
	}
	if record.Phase == promotionPhaseReleased {
		fs.scheduleReleasedPromotionCleanup(*record)
		return nil
	}
	result, accountingIncomplete, statusErr := fs.getPromotionResult(ctx, record.OperationID, record.TargetRemote)
	if accountingIncomplete {
		if result == nil || !result.Committed || result.OperationID != record.OperationID ||
			result.Target != record.TargetRemote || result.ManifestSHA256 != record.ManifestSHA256 {
			return fs.refusePromotionRecovery(record, "accounting-incomplete promotion result does not match local record")
		}
		request, scanErr := fs.scanPromotionTree(ctx, record.Source, record.TargetLocal, record.TargetRemote, record.OperationID)
		if scanErr != nil || request.ManifestSHA256 != record.ManifestSHA256 {
			return fs.refusePromotionRecovery(record, "promotion source does not match accounting-incomplete receipt manifest")
		}
		body, prepareErr := fs.preparePromotionRequest(ctx, request)
		if prepareErr != nil {
			return fs.refusePromotionRecovery(record, fmt.Sprintf("promotion source no longer passes request validation: %v", prepareErr))
		}
		var definitive bool
		result, definitive, err = fs.publishPreparedPromotionWithRecovery(ctx, request, body)
		if err != nil {
			if definitive {
				return fs.refusePromotionRecovery(record, fmt.Sprintf("exact POST retry rejected for accounting-incomplete receipt: %v", err))
			}
			return err
		}
		statusErr = nil
	}
	if statusErr != nil && !client.IsNotFound(statusErr) {
		return statusErr
	}
	if client.IsNotFound(statusErr) {
		// A single NotFound does not prove that the old POST cannot still commit:
		// the server transaction may be in flight after this client crashed.
		// Re-POSTing or deleting the journal would respectively create a late
		// success or forget one. Stop permanently and expose the exact journal
		// identity so an operator can follow the documented recovery procedure.
		return fs.refusePromotionRecovery(record, "promotion receipt is absent; refusing unsafe startup replay or journal deletion")
	}
	if result == nil || !result.Committed || result.OperationID != record.OperationID ||
		result.Target != record.TargetRemote || result.ManifestSHA256 != record.ManifestSHA256 {
		return fs.refusePromotionRecovery(record, "promotion result does not match local record")
	}
	record.Phase = promotionPhaseCommitted
	if err := fs.writePromotionRecord(*record); err != nil {
		return err
	}
	err = fs.finishCommittedPromotion(ctx, record)
	var conflict *promotionStateConflict
	if errors.As(err, &conflict) {
		return fs.refusePromotionRecovery(record, conflict.Error())
	}
	if err != nil {
		return err
	}
	fs.scheduleReleasedPromotionCleanup(*record)
	return nil
}

// ensurePromotionDirDurable creates each missing directory component and
// fsyncs its parent before continuing. This makes the complete journal and
// quarantine parent chain survive power loss, not only the innermost entry.
func ensurePromotionDirDurable(dir string, perm os.FileMode) error {
	dir = filepath.Clean(dir)
	missing := make([]string, 0, 3)
	for current := dir; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err == nil {
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("promotion path is not a directory: %s", current)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, current)
		parent := filepath.Dir(current)
		if parent == current {
			return err
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		current := missing[i]
		if err := os.Mkdir(current, perm); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return err
			}
			info, statErr := os.Lstat(current)
			if statErr != nil {
				return statErr
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("promotion path is not a directory: %s", current)
			}
		}
		if err := fsyncPromotionDir(filepath.Dir(current)); err != nil {
			return err
		}
	}
	return nil
}

// validatePromotionDirectoryChain rejects symlinks and non-directories below
// the configured local root before recovery performs any remote or local side
// effect. Lexical containment alone is insufficient because filesystem lookup
// follows symlinked parent components.
func validatePromotionDirectoryChain(root, target string, allowMissing bool) error {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	targetAbs, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootAbs, targetAbs)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("promotion path escapes local root")
	}
	if rel == "." {
		return nil
	}
	rootInfo, err := os.Lstat(rootAbs)
	if err != nil {
		return err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return fmt.Errorf("promotion root is not a real directory: %s", rootAbs)
	}
	current := rootAbs
	for _, component := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) && allowMissing {
			return nil
		}
		if statErr != nil {
			return statErr
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("promotion path is not a real directory: %s", current)
		}
	}
	return nil
}

func fsyncPromotionDir(dir string) error {
	if testHookBeforePromotionDirSync != nil {
		if err := testHookBeforePromotionDirSync(dir); err != nil {
			return err
		}
	}
	return fsyncDir(dir)
}
