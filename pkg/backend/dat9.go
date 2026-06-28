// Package backend implements the Dat9Backend, which satisfies AGFS's FileSystem interface.
// P0 uses TiDB (MySQL protocol) + local blob storage as a stand-in for db9/fs9.
package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/embedding"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"github.com/mem9-ai/drive9/pkg/s3client"
	"github.com/mem9-ai/drive9/pkg/traceid"
	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"
)

const (
	// DefaultInlineThreshold is the default cutoff between DB-inline and S3
	// storage. Operators can override via Options.InlineThreshold; the value
	// is also exposed to clients via /v1/status so they pick a matching
	// upload strategy.
	//
	// This single threshold deliberately governs both decisions:
	//   1. Server-side: store inline in TiDB content_blob vs spill to S3.
	//   2. Client-side: simple PUT to /v1/fs/{path} vs V2 multipart presign.
	//
	// Splitting the two (e.g. "client direct PUT up to 1MB but only inline
	// in TiDB up to 50KB") is tempting for performance but reintroduces the
	// server as a data-plane proxy: the simple-PUT body would have to flow
	// through the server process and PutObject to S3 itself, exactly what
	// the V2 multipart protocol is designed to avoid. Until the simple-PUT
	// path streams into S3 without buffering in server RAM, keep the two
	// decisions tied so raising the threshold has predictable cost
	// (TiDB blob column growth, not server bandwidth + memory).
	DefaultInlineThreshold int64 = 50_000
	// DefaultTextExtractMaxBytes is the default cap for synchronous text
	// extraction. TiDB auto embedding currently rejects inputs over 8192
	// tokens; this byte cap keeps text-heavy small files from failing the
	// whole write through the generated embedding column. Independent of
	// InlineThreshold: extraction can be tightened or relaxed without
	// changing the storage-class boundary.
	DefaultTextExtractMaxBytes int64 = 8_192

	// MaxSymlinkTargetBytes bounds symbolic link targets to a path-sized inline
	// payload instead of allowing generic upload-sized blobs.
	MaxSymlinkTargetBytes = 4 << 10

	symlinkMode        uint32 = 0o120000 | 0o777
	symlinkContentType        = "application/x-symlink"
)

// Dat9Backend implements filesystem.FileSystem with the inode model.
type Dat9Backend struct {
	store         *datastore.Store
	s3            s3client.S3Client // nil when S3 is not configured
	smallInDB     bool
	queryEmbedder embedding.Client
	// databaseAutoEmbedding selects whether this backend uses the TiDB
	// database-managed embedding path instead of the app-managed one for write,
	// upload, image extraction, and grep behavior.
	databaseAutoEmbedding bool
	// appSemanticTasksEnabled gates whether the app-managed embed worker path
	// may enqueue semantic_tasks. False when no DRIVE9_EMBED_* worker is
	// configured, preventing orphaned task rows.
	appSemanticTasksEnabled bool
	maxUploadBytes          int64
	maxTenantStorageBytes   int64
	maxMediaLLMFiles        int64
	// inlineThreshold controls the DB-inline vs S3 storage cutoff and (when
	// surfaced to clients) the simple-PUT vs multipart upload boundary.
	inlineThreshold int64
	// textExtractMaxBytes caps synchronous text extraction input size.
	textExtractMaxBytes int64
	mu                  sync.Mutex
	entropy             io.Reader

	// Central quota enforcement (Rev 4 migration).
	tenantID           string
	storageNamespaceID string
	metaStore          MetaQuotaStore // nil when central quota is not wired (tests, fallback)
	quotaSource        QuotaSource    // "tenant" (default) or "server"
	quotaConfigCache   *quotaConfigCache
	quotaUsageCache    *quotaUsageCache
	quotaPendingCache  *quotaPendingDeltasCache

	// mutationQueue decouples central quota mutations (syncCentralFileCreate,
	// syncCentralFileOverwrite) from the fsync critical path. Mutations are
	// enqueued here and drained by a background worker. The mutation log
	// provides crash recovery via the existing MutationReplayWorker.
	//
	// mutationMu serializes logQuotaMutation + enqueueMutation so that
	// durable log_id order and channel enqueue order cannot diverge under
	// concurrent same-tenant writes.
	mutationMu    sync.Mutex
	mutationQueue chan func()
	mutationWG    sync.WaitGroup
	mutationStop  context.CancelFunc

	quotaOutboxNotify chan struct{}
	quotaOutboxWG     sync.WaitGroup
	quotaOutboxStop   context.CancelFunc
	claimQuotaOutbox  quotaOutboxBatchClaimer

	s3EncryptionPolicy meta.ResolvedS3EncryptionPolicy

	// Async image -> text extraction runtime (durable semantic_tasks delivery;
	// no in-process queue). The semantic worker claims img_extract_text tasks
	// and calls ProcessImageExtractTask via this backend's extractor/S3 client.
	imageExtractEnabled bool
	imageExtractor      ImageTextExtractor
	imageExtractTimeout time.Duration
	imageExtractMaxSize int64
	maxExtractTextBytes int

	// Durable audio transcript extraction (semantic_tasks only; no local queue).
	audioExtractEnabled      bool
	audioExtractor           AudioTextExtractor
	audioExtractTimeout      time.Duration
	audioExtractMaxSize      int64
	maxAudioExtractTextBytes int

	fileGCWorker     *FileGCWorker
	runtimeMetricsID uint64

	// semanticTaskNotifier, when set, runs after a write or upload commit that
	// enqueued at least one durable semantic task, so the semantic worker can
	// claim it immediately instead of waiting for the tenant scan.
	semanticTaskNotifier atomic.Pointer[func()]

	// Monthly LLM cost budget (P1).
	maxMonthlyLLMCostMillicents     int64
	visionCostPerKTokenMillicents   int64
	audioLLMCostPerKTokenMillicents int64
	whisperCostPerMinuteMillicents  int64
	fallbackImageCostMillicents     int64
	fallbackAudioCostMillicents     int64
}

func newBaseBackend(store *datastore.Store) *Dat9Backend {
	return &Dat9Backend{
		store:               store,
		smallInDB:           true,
		queryEmbedder:       embedding.NopClient{},
		entropy:             ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
		inlineThreshold:     DefaultInlineThreshold,
		textExtractMaxBytes: DefaultTextExtractMaxBytes,
		runtimeMetricsID:    globalBackendRuntimeMetrics.allocateID(),
	}
}

// SetSemanticTaskEnqueuedNotifier registers fn to run after a write or upload
// commit that enqueued at least one durable semantic task. fn must be cheap
// and non-blocking: it is invoked inline on the write path. A dropped or
// missing notification is safe — tasks are durable and the semantic worker's
// periodic tenant scan claims them eventually.
func (b *Dat9Backend) SetSemanticTaskEnqueuedNotifier(fn func()) {
	if b == nil {
		return
	}
	b.semanticTaskNotifier.Store(&fn)
}

func (b *Dat9Backend) notifySemanticTaskEnqueued(enqueued bool) {
	if !enqueued {
		return
	}
	if fn := b.semanticTaskNotifier.Load(); fn != nil && *fn != nil {
		(*fn)()
	}
}

func New(store *datastore.Store) (*Dat9Backend, error) {
	return NewWithOptions(store, Options{})
}

func NewWithOptions(store *datastore.Store, opts Options) (*Dat9Backend, error) {
	b := newBaseBackend(store)
	b.configureOptions(opts)
	return b, nil
}

// NewWithS3 creates a Dat9Backend with S3 support for large file uploads.
func NewWithS3(store *datastore.Store, s3 s3client.S3Client) (*Dat9Backend, error) {
	return NewWithS3ModeAndOptions(store, s3, true, Options{})
}

// NewWithS3Mode controls whether files smaller than threshold stay in DB.
func NewWithS3Mode(store *datastore.Store, s3 s3client.S3Client, smallInDB bool) (*Dat9Backend, error) {
	return NewWithS3ModeAndOptions(store, s3, smallInDB, Options{})
}

// NewWithS3ModeAndOptions controls storage mode and backend options.
func NewWithS3ModeAndOptions(store *datastore.Store, s3 s3client.S3Client, smallInDB bool, opts Options) (*Dat9Backend, error) {
	b := newBaseBackend(store)
	b.s3 = s3
	b.smallInDB = smallInDB
	b.configureOptions(opts)
	return b, nil
}

func (b *Dat9Backend) Store() *datastore.Store { return b.store }

// UsesDatabaseAutoEmbedding reports whether this backend instance is
// configured for database-managed semantic embedding.
func (b *Dat9Backend) UsesDatabaseAutoEmbedding() bool {
	return b.databaseAutoEmbedding
}

func (b *Dat9Backend) genID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	id, err := ulid.New(ulid.Timestamp(time.Now()), b.entropy)
	if err != nil {
		// Fallback: reset entropy on exhaustion
		b.entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
		id = ulid.MustNew(ulid.Timestamp(time.Now()), b.entropy)
	}
	return id.String()
}

func (b *Dat9Backend) Create(path string) error {
	return b.CreateCtx(backgroundWithTrace(), path)
}

func (b *Dat9Backend) CreateCtx(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "create", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return err
	}
	if err := rejectRootFileNodePath(path); err != nil {
		return err
	}

	fileID := b.genID()
	now := time.Now()
	storageType := datastore.StorageDB9
	storageRef := "inline"
	storageEncryptionMode := datastore.StorageEncryptionNone
	storageEncryptionKeyID := ""
	var contentBlob []byte
	if b.shouldStoreInDB(0) {
		contentBlob = []byte{}
	} else {
		if b.s3 == nil {
			return fmt.Errorf("s3 client not configured")
		}
		storageType = datastore.StorageS3
		storageRef = "blobs/" + fileID
		encOpts, encMode, encKeyID := b.s3WriteEncryption(storageRef)
		storageEncryptionMode = encMode
		storageEncryptionKeyID = encKeyID
		if err := b.s3.PutObject(ctx, storageRef, bytes.NewReader(nil), 0, encOpts); err != nil {
			logger.Error(ctx, "backend_create_put_object_failed", zap.String("path", path), zap.String("storage_ref", storageRef), zap.Error(err))
			return fmt.Errorf("put object: %w", err)
		}
	}

	var quotaOutboxEnqueued bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.ensureFileCountQuotaServer(ctx, tx, 1); err != nil {
			return err
		}
		if err := b.store.InsertFileTx(tx, &datastore.File{
			FileID: fileID, StorageType: storageType, StorageRef: storageRef,
			StorageEncryptionMode:  storageEncryptionMode,
			StorageEncryptionKeyID: storageEncryptionKeyID,
			ContentBlob:            contentBlob,
			SizeBytes:              0, Revision: 1, Status: datastore.StatusConfirmed,
			CreatedAt: now, ConfirmedAt: &now,
		}); err != nil {
			return err
		}
		if err := b.store.EnsureParentDirsTx(tx, path, b.genID); err != nil {
			return err
		}
		if err := b.store.InsertNodeTx(tx, &datastore.FileNode{
			NodeID: b.genID(), Path: path, ParentPath: pathutil.ParentPath(path),
			Name: pathutil.BaseName(path), FileID: fileID, CreatedAt: now,
		}); err != nil {
			return err
		}
		created, err := b.enqueueQuotaFileCreateOutboxTx(tx, fileID, 0, "")
		if err != nil {
			return err
		}
		quotaOutboxEnqueued = created
		return nil
	})
	if err != nil {
		if storageType == datastore.StorageS3 {
			b.deleteBlobCtx(ctx, storageRef)
		}
		return err
	}
	if quotaOutboxEnqueued {
		b.addLocalQuotaPendingDeltas(0, 1, 0)
		b.notifyQuotaOutbox(true)
	} else {
		b.syncCentralFileCreate(ctx, fileID, 0, "")
	}
	return nil
}

// CreateSymlinkCtx creates a symbolic link whose target is stored as the file
// payload and whose inode mode carries the POSIX symlink file type bits.
func (b *Dat9Backend) CreateSymlinkCtx(ctx context.Context, linkPath, target string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "create_symlink", err, start) }()

	data := []byte(target)
	if target == "" || strings.ContainsRune(target, 0) || len(data) > MaxSymlinkTargetBytes {
		return ErrInvalidSymlinkTarget
	}
	linkPath, err = pathutil.Canonicalize(linkPath)
	if err != nil {
		return err
	}
	if err := rejectRootFileNodePath(linkPath); err != nil {
		return err
	}

	if err := b.ensureUploadSizeAllowed(int64(len(data))); err != nil {
		return err
	}
	if err := b.ensureFileSizeQuota(ctx, int64(len(data))); err != nil {
		return err
	}
	fileID := b.genID()
	now := time.Now()
	checksum := sha256sum(data)

	var quotaOutboxEnqueued bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.ensureStorageQuota(ctx, tx, linkPath, int64(len(data))); err != nil {
			return err
		}
		if err := b.ensureFileCountQuotaServer(ctx, tx, 1); err != nil {
			return err
		}
		if err := b.store.InsertFileTx(tx, &datastore.File{
			FileID:                fileID,
			StorageType:           datastore.StorageDB9,
			StorageRef:            "inline",
			StorageEncryptionMode: datastore.StorageEncryptionNone,
			ContentBlob:           append([]byte(nil), data...),
			ContentType:           symlinkContentType,
			SizeBytes:             int64(len(data)),
			ChecksumSHA256:        checksum,
			Revision:              1,
			Mode:                  symlinkMode,
			Status:                datastore.StatusConfirmed,
			CreatedAt:             now,
			ConfirmedAt:           &now,
		}); err != nil {
			return err
		}
		if err := b.store.EnsureParentDirsTx(tx, linkPath, b.genID); err != nil {
			return err
		}
		if err := b.store.InsertNodeTx(tx, &datastore.FileNode{
			NodeID: b.genID(), Path: linkPath, ParentPath: pathutil.ParentPath(linkPath),
			Name: pathutil.BaseName(linkPath), FileID: fileID, CreatedAt: now,
		}); err != nil {
			return err
		}
		created, err := b.enqueueQuotaFileCreateOutboxTx(tx, fileID, int64(len(data)), symlinkContentType)
		if err != nil {
			return err
		}
		quotaOutboxEnqueued = created
		return nil
	})
	if err != nil {
		return err
	}
	if quotaOutboxEnqueued {
		mediaDelta := int64(0)
		if isQuotaMediaContentType(symlinkContentType) {
			mediaDelta = 1
		}
		b.addLocalQuotaPendingDeltas(int64(len(data)), 1, mediaDelta)
		b.notifyQuotaOutbox(true)
	} else {
		b.syncCentralFileCreate(ctx, fileID, int64(len(data)), symlinkContentType)
	}
	return nil
}

func (b *Dat9Backend) Mkdir(path string, perm uint32) error {
	return b.MkdirCtx(backgroundWithTrace(), path, perm)
}

func (b *Dat9Backend) MkdirCtx(ctx context.Context, path string, perm uint32) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "mkdir", err, start) }()

	dirPath, err := pathutil.CanonicalizeDir(path)
	if err != nil {
		return err
	}
	if dirPath == "/" {
		return nil
	}
	err = b.store.EnsureParentDirs(ctx, dirPath, b.genID)
	if err != nil {
		return err
	}
	now := time.Now()
	nodeID := b.genID()
	if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.store.InsertInodeTx(tx, &datastore.Inode{
			InodeID:   nodeID,
			SizeBytes: 0,
			Revision:  1,
			Mode:      perm,
			Status:    datastore.StatusConfirmed,
			CreatedAt: now,
			Mtime:     now,
		}); err != nil {
			return err
		}
		return b.store.InsertNodeTx(tx, &datastore.FileNode{
			NodeID:      b.genID(),
			Path:        dirPath,
			ParentPath:  pathutil.ParentPath(dirPath),
			Name:        pathutil.BaseName(dirPath),
			IsDirectory: true,
			InodeID:     nodeID,
			CreatedAt:   now,
		})
	}); err != nil {
		return err
	}
	return nil
}

func (b *Dat9Backend) Chmod(path string, mode uint32) error {
	return b.ChmodCtx(backgroundWithTrace(), path, mode)
}

func (b *Dat9Backend) ChmodCtx(ctx context.Context, path string, mode uint32) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "chmod", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return err
	}
	if path == "/" {
		return datastore.ErrInvalidRootDentry
	}
	resolvedPath, _, err := b.resolveNodePath(ctx, path)
	if err != nil {
		return err
	}
	return b.store.Chmod(ctx, resolvedPath, mode)
}

func (b *Dat9Backend) Remove(path string) error {
	return b.RemoveCtx(backgroundWithTrace(), path)
}

func (b *Dat9Backend) RemoveCtx(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "remove", err, start) }()

	path, node, err := b.resolveNodePath(ctx, path)
	if err != nil {
		return err
	}
	if path == "/" {
		return datastore.ErrInvalidRootDentry
	}
	if node.IsDirectory {
		return b.store.DeleteEmptyDir(ctx, path)
	}
	_, err = b.store.DeleteFileWithRefCheck(ctx, path)
	return err
}

func (b *Dat9Backend) RemoveFileCtx(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "remove_file", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return err
	}
	if err := rejectRootFileNodePath(path); err != nil {
		return err
	}
	_, err = b.store.DeleteFileWithRefCheck(ctx, path)
	return err
}

func (b *Dat9Backend) RemoveDirCtx(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "remove_dir", err, start) }()

	path, err = pathutil.CanonicalizeDir(path)
	if err != nil {
		return err
	}
	if path == "/" {
		return datastore.ErrInvalidRootDentry
	}
	return b.store.DeleteEmptyDir(ctx, path)
}

func (b *Dat9Backend) RemoveAll(path string) error {
	return b.RemoveAllCtx(backgroundWithTrace(), path)
}

func (b *Dat9Backend) RemoveAllCtx(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "remove_all", err, start) }()

	path, node, err := b.resolveNodePath(ctx, path)
	if err != nil {
		return err
	}
	if path == "/" {
		return datastore.ErrInvalidRootDentry
	}
	if !node.IsDirectory {
		_, err = b.store.DeleteFileWithRefCheck(ctx, path)
		return err
	}
	_, err = b.store.DeleteDirRecursive(ctx, path)
	return err
}

func (b *Dat9Backend) Read(path string, offset int64, size int64) ([]byte, error) {
	return b.ReadCtx(backgroundWithTrace(), path, offset, size)
}

func (b *Dat9Backend) ReadCtx(ctx context.Context, path string, offset int64, size int64) (data []byte, err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "read", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	if path == "/" {
		return nil, datastore.ErrNotFound
	}
	nf, err := b.store.Stat(ctx, path)
	if err != nil {
		return nil, err
	}
	if nf.Node.IsDirectory {
		return nil, fmt.Errorf("is a directory: %s", path)
	}
	if nf.File == nil {
		return nil, fmt.Errorf("no file entity for path: %s", path)
	}

	data, err = b.readFileDataCtx(ctx, nf.File)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		if offset >= int64(len(data)) {
			return nil, io.EOF
		}
		data = data[offset:]
	}
	if size >= 0 && size < int64(len(data)) {
		data = data[:size]
	}
	return data, nil
}

func (b *Dat9Backend) Write(path string, data []byte, offset int64, flags filesystem.WriteFlag) (int64, error) {
	return b.WriteCtx(backgroundWithTrace(), path, data, offset, flags)
}

func (b *Dat9Backend) WriteCtx(ctx context.Context, path string, data []byte, offset int64, flags filesystem.WriteFlag) (n int64, err error) {
	return b.WriteCtxIfRevisionWithTags(ctx, path, data, offset, flags, -1, nil, "")
}

// WriteCtxIfRevision applies the write only when expectedRevision matches the
// current file revision.
// expectedRevision semantics:
// - negative: unconditional write
// - zero: path must not already exist
// - positive: file must exist at exactly that revision
func (b *Dat9Backend) WriteCtxIfRevision(ctx context.Context, path string, data []byte, offset int64, flags filesystem.WriteFlag, expectedRevision int64) (n int64, err error) {
	return b.WriteCtxIfRevisionWithTags(ctx, path, data, offset, flags, expectedRevision, nil, "")
}

// WriteCtxIfRevisionWithTags applies the write only when expectedRevision
// matches the current file revision, and optionally replaces file tags in the
// same transaction when tags is non-nil. It also accepts an optional
// description for the file.
//
// expectedRevision semantics:
// - negative: unconditional write
// - zero: path must not already exist
// - positive: file must exist at exactly that revision
func (b *Dat9Backend) WriteCtxIfRevisionWithTags(ctx context.Context, path string, data []byte, offset int64, flags filesystem.WriteFlag, expectedRevision int64, tags map[string]string, description string) (n int64, err error) {
	n, _, err = b.WriteCtxIfRevisionWithTagsResult(ctx, path, data, offset, flags, expectedRevision, tags, description)
	return n, err
}

// WriteCtxIfRevisionWithTagsResult is like WriteCtxIfRevisionWithTags but also
// returns the committed revision of the file after a successful write.
func (b *Dat9Backend) WriteCtxIfRevisionWithTagsResult(ctx context.Context, path string, data []byte, offset int64, flags filesystem.WriteFlag, expectedRevision int64, tags map[string]string, description string) (n int64, committedRevision int64, err error) {
	start := time.Now()
	timingEnabled := logger.BenchTimingLogEnabled()
	rawPath := path
	canonicalPath := path
	operation := "unknown"
	result := "ok"
	var canonicalizeDuration time.Duration
	var statDuration time.Duration
	var implementationDuration time.Duration
	defer func() {
		observeBackend(ctx, b.tenantID, "write", err, start)
		if !timingEnabled {
			return
		}
		if err != nil {
			result = backendWriteResult(err)
		}
		fields := []zap.Field{
			zap.String("path", rawPath),
			zap.String("canonical_path", canonicalPath),
			zap.String("operation", operation),
			zap.String("result", result),
			zap.Int("bytes", len(data)),
			zap.Int64("offset", offset),
			zap.Int64("expected_revision", expectedRevision),
			zap.Int64("committed_revision", committedRevision),
			zap.Int("flags", int(flags)),
			zap.Float64("canonicalize_ms", backendDurationMs(canonicalizeDuration)),
			zap.Float64("stat_ms", backendDurationMs(statDuration)),
			zap.Float64("implementation_ms", backendDurationMs(implementationDuration)),
			zap.Float64("total_ms", backendDurationMs(time.Since(start))),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		logger.InfoBenchTiming(ctx, "backend_write_timing", fields...)
	}()

	tags = cloneFileTags(tags)

	canonicalizeStart := time.Time{}
	if timingEnabled {
		canonicalizeStart = time.Now()
	}
	path, err = pathutil.Canonicalize(path)
	canonicalPath = path
	if timingEnabled {
		canonicalizeDuration = time.Since(canonicalizeStart)
	}
	if err != nil {
		return 0, 0, err
	}
	if err := rejectRootFileNodePath(path); err != nil {
		return 0, 0, err
	}

	if isCreateIfAbsentWrite(offset, flags, expectedRevision) {
		operation = "create"
		exists, err := b.store.NodeExists(ctx, path)
		if err != nil {
			return 0, 0, err
		}
		if exists {
			return 0, 0, datastore.ErrRevisionConflict
		}
		implementationStart := time.Time{}
		if timingEnabled {
			implementationStart = time.Now()
		}
		n, err := b.createAndWriteCtx(ctx, path, data, tags, description)
		if timingEnabled {
			implementationDuration = time.Since(implementationStart)
		}
		if errors.Is(err, datastore.ErrPathConflict) {
			return 0, 0, datastore.ErrRevisionConflict
		}
		if err != nil {
			return 0, 0, err
		}
		return n, 1, nil
	}

	statStart := time.Time{}
	if timingEnabled {
		statStart = time.Now()
	}
	existing, err := b.store.Stat(ctx, path)
	if timingEnabled {
		statDuration = time.Since(statStart)
	}
	if err == datastore.ErrNotFound {
		operation = "create"
		if expectedRevision > 0 {
			return 0, 0, datastore.ErrRevisionConflict
		}
		if flags&filesystem.WriteFlagCreate == 0 {
			if expectedRevision == 0 {
				return 0, 0, datastore.ErrRevisionConflict
			}
			return 0, 0, datastore.ErrNotFound
		}
		implementationStart := time.Time{}
		if timingEnabled {
			implementationStart = time.Now()
		}
		n, err := b.createAndWriteCtx(ctx, path, data, tags, description)
		if timingEnabled {
			implementationDuration = time.Since(implementationStart)
		}
		if expectedRevision == 0 && errors.Is(err, datastore.ErrPathConflict) {
			return 0, 0, datastore.ErrRevisionConflict
		}
		if err != nil {
			return 0, 0, err
		}
		return n, 1, nil // create always produces revision 1
	}
	if err != nil {
		return 0, 0, err
	}
	if expectedRevision == 0 {
		return 0, 0, datastore.ErrRevisionConflict
	}
	if existing.Node.IsDirectory {
		return 0, 0, fmt.Errorf("is a directory: %s", path)
	}
	if flags&filesystem.WriteFlagExclusive != 0 {
		return 0, 0, fmt.Errorf("file already exists: %s", path)
	}
	if expectedRevision > 0 && (existing.File == nil || existing.File.Revision != expectedRevision) {
		return 0, 0, datastore.ErrRevisionConflict
	}
	operation = "overwrite"
	implementationStart := time.Time{}
	if timingEnabled {
		implementationStart = time.Now()
	}
	n, rev, err := b.overwriteFileCtxWithRev(ctx, existing, data, offset, flags, expectedRevision, tags, description)
	if timingEnabled {
		implementationDuration = time.Since(implementationStart)
	}
	return n, rev, err
}

func isCreateIfAbsentWrite(offset int64, flags filesystem.WriteFlag, expectedRevision int64) bool {
	return expectedRevision == 0 &&
		offset == 0 &&
		flags&filesystem.WriteFlagCreate != 0 &&
		flags&filesystem.WriteFlagAppend == 0
}

func backendWriteResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, datastore.ErrNotFound):
		return "not_found"
	case errors.Is(err, datastore.ErrRevisionConflict), errors.Is(err, datastore.ErrPathConflict), errors.Is(err, datastore.ErrUploadConflict):
		return "conflict"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "error"
	}
}

func backendDurationMs(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func backendTimingStep(enabled bool, dst *time.Duration) func() {
	if !enabled {
		return func() {}
	}
	start := time.Now()
	return func() {
		*dst = time.Since(start)
	}
}

func cloneFileTags(tags map[string]string) map[string]string {
	if tags == nil {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}

func (b *Dat9Backend) createAndWriteCtx(ctx context.Context, path string, data []byte, tags map[string]string, description string) (written int64, err error) {
	timingEnabled := logger.BenchTimingLogEnabled()
	start := time.Time{}
	if timingEnabled {
		start = time.Now()
	}
	storageType := datastore.StorageDB9
	contentType := ""
	var prepareDuration time.Duration
	var s3PutDuration time.Duration
	var tenantTxDuration time.Duration
	var centralQuotaDuration time.Duration
	var imageEnqueueDuration time.Duration
	var quotaStorageCheckDuration time.Duration
	var quotaFileCountCheckDuration time.Duration
	var insertFileDuration time.Duration
	var ensureParentDirsDuration time.Duration
	var insertNodeDuration time.Duration
	var tagUpdateDuration time.Duration
	var semanticEnqueueDuration time.Duration
	var quotaOutboxEnqueueDuration time.Duration
	defer func() {
		if !timingEnabled {
			return
		}
		fields := []zap.Field{
			zap.String("path", path),
			zap.String("result", backendWriteResult(err)),
			zap.Int("bytes", len(data)),
			zap.Int64("written", written),
			zap.String("storage_type", string(storageType)),
			zap.String("content_type", contentType),
			zap.Float64("prepare_ms", backendDurationMs(prepareDuration)),
			zap.Float64("s3_put_ms", backendDurationMs(s3PutDuration)),
			zap.Float64("tenant_tx_ms", backendDurationMs(tenantTxDuration)),
			zap.Float64("central_quota_ms", backendDurationMs(centralQuotaDuration)),
			zap.Float64("quota_storage_check_ms", backendDurationMs(quotaStorageCheckDuration)),
			zap.Float64("quota_file_count_check_ms", backendDurationMs(quotaFileCountCheckDuration)),
			zap.Float64("insert_file_ms", backendDurationMs(insertFileDuration)),
			zap.Float64("ensure_parent_dirs_ms", backendDurationMs(ensureParentDirsDuration)),
			zap.Float64("insert_node_ms", backendDurationMs(insertNodeDuration)),
			zap.Float64("tag_update_ms", backendDurationMs(tagUpdateDuration)),
			zap.Float64("semantic_enqueue_ms", backendDurationMs(semanticEnqueueDuration)),
			zap.Float64("quota_outbox_enqueue_ms", backendDurationMs(quotaOutboxEnqueueDuration)),
			zap.Float64("image_enqueue_ms", backendDurationMs(imageEnqueueDuration)),
			zap.Float64("total_ms", backendDurationMs(time.Since(start))),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		logger.InfoBenchTiming(ctx, "backend_write_create_timing", fields...)
	}()
	if err := b.ensureUploadSizeAllowed(int64(len(data))); err != nil {
		return 0, err
	}
	if err := b.ensureFileSizeQuota(ctx, int64(len(data))); err != nil {
		return 0, err
	}
	fileID := b.genID()
	now := time.Now()

	prepareStart := time.Time{}
	if timingEnabled {
		prepareStart = time.Now()
	}
	contentType = detectContentType(path, data)
	checksum := sha256sum(data)
	contentText := extractText(data, contentType, b.textExtractMaxBytes)
	if timingEnabled {
		prepareDuration = time.Since(prepareStart)
	}

	storageRef := "inline"
	storageEncryptionMode := datastore.StorageEncryptionNone
	storageEncryptionKeyID := ""
	var contentBlob []byte
	if b.shouldStoreInDB(int64(len(data))) {
		contentBlob = append([]byte(nil), data...)
	} else {
		if b.s3 == nil {
			return 0, fmt.Errorf("s3 client not configured")
		}
		storageType = datastore.StorageS3
		storageRef = "blobs/" + fileID
		encOpts, encMode, encKeyID := b.s3WriteEncryption(storageRef)
		storageEncryptionMode = encMode
		storageEncryptionKeyID = encKeyID
		s3PutStart := time.Time{}
		if timingEnabled {
			s3PutStart = time.Now()
		}
		if err := b.s3.PutObject(ctx, storageRef, bytes.NewReader(data), int64(len(data)), encOpts); err != nil {
			if timingEnabled {
				s3PutDuration = time.Since(s3PutStart)
			}
			logger.Error(ctx, "backend_create_and_write_put_object_failed", zap.String("path", path), zap.String("storage_ref", storageRef), zap.Int("bytes", len(data)), zap.Error(err))
			return 0, fmt.Errorf("put object: %w", err)
		}
		if timingEnabled {
			s3PutDuration = time.Since(s3PutStart)
		}
	}

	var semanticTaskEnqueued bool
	var quotaOutboxEnqueued bool
	txStart := time.Time{}
	if timingEnabled {
		txStart = time.Now()
	}
	if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		semanticTaskEnqueued = false
		quotaOutboxEnqueued = false

		done := backendTimingStep(timingEnabled, &quotaStorageCheckDuration)
		err := b.ensureCreateStorageQuota(ctx, tx, int64(len(data)))
		done()
		if err != nil {
			return err
		}

		done = backendTimingStep(timingEnabled, &quotaFileCountCheckDuration)
		err = b.ensureFileCountQuotaServer(ctx, tx, 1)
		done()
		if err != nil {
			return err
		}

		fileRev := int64(1)
		insertFile := &datastore.File{
			FileID: fileID, StorageType: storageType, StorageRef: storageRef,
			StorageEncryptionMode:  storageEncryptionMode,
			StorageEncryptionKeyID: storageEncryptionKeyID,
			ContentBlob:            contentBlob,
			ContentType:            contentType, SizeBytes: int64(len(data)),
			ChecksumSHA256: checksum, Revision: fileRev, Status: datastore.StatusConfirmed,
			ContentText: contentText, Description: description, CreatedAt: now, ConfirmedAt: &now,
		}
		if b.UsesDatabaseAutoEmbedding() && description != "" {
			insertFile.DescriptionEmbeddingRevision = &fileRev
		}

		done = backendTimingStep(timingEnabled, &insertFileDuration)
		err = b.store.InsertFileTx(tx, insertFile)
		done()
		if err != nil {
			return err
		}

		done = backendTimingStep(timingEnabled, &ensureParentDirsDuration)
		err = b.store.EnsureParentDirsTx(tx, path, b.genID)
		done()
		if err != nil {
			return err
		}

		done = backendTimingStep(timingEnabled, &insertNodeDuration)
		err = b.store.InsertNodeTx(tx, &datastore.FileNode{
			NodeID: b.genID(), Path: path, ParentPath: pathutil.ParentPath(path),
			Name: pathutil.BaseName(path), FileID: fileID, CreatedAt: now,
		})
		done()
		if err != nil {
			return err
		}

		if tags != nil {
			done = backendTimingStep(timingEnabled, &tagUpdateDuration)
			err = b.store.ReplaceFileTagsTx(tx, fileID, tags)
			done()
			if err != nil {
				return err
			}
		}
		currentMediaDelta := quotaMediaDelta(false, isQuotaMediaContentType(contentType))
		done = backendTimingStep(timingEnabled, &semanticEnqueueDuration)
		if b.UsesDatabaseAutoEmbedding() {
			var created bool
			created, err = b.enqueueExtractSemanticTasksTx(ctx, tx, fileID, 1, path, contentType, currentMediaDelta)
			semanticTaskEnqueued = created
			done()
			if err != nil {
				return err
			}
		} else {
			// App-embedding mode: image/audio extract tasks are durable and independent
			// of EMBED_TEXT, so register them in the same transaction. The embed task
			// (if any) is enqueued separately below.
			extractCreated, extractErr := b.enqueueExtractSemanticTasksTx(ctx, tx, fileID, 1, path, contentType, currentMediaDelta)
			err = extractErr
			if err == nil && b.shouldEnqueueEmbedForRevision(path, contentType, contentText, description) {
				var embedCreated bool
				embedCreated, err = b.enqueueEmbedTaskTx(tx, fileID, 1)
				semanticTaskEnqueued = embedCreated || extractCreated
			} else {
				semanticTaskEnqueued = extractCreated
			}
			done()
			if err != nil {
				return err
			}
		}
		if timingEnabled {
			// Keep the legacy field populated for existing dashboards; create
			// uses the semantic enqueue phase as its image enqueue boundary.
			imageEnqueueDuration = semanticEnqueueDuration
		}

		done = backendTimingStep(timingEnabled, &quotaOutboxEnqueueDuration)
		created, err := b.enqueueQuotaFileCreateOutboxTx(tx, fileID, int64(len(data)), contentType)
		done()
		if err != nil {
			return err
		}
		quotaOutboxEnqueued = created
		return nil
	}); err != nil {
		if timingEnabled {
			tenantTxDuration = time.Since(txStart)
		}
		if storageType == datastore.StorageS3 {
			b.deleteBlobCtx(ctx, storageRef)
		}
		return 0, err
	}
	if timingEnabled {
		tenantTxDuration = time.Since(txStart)
	}
	b.notifySemanticTaskEnqueued(semanticTaskEnqueued)
	centralQuotaStart := time.Time{}
	if timingEnabled {
		centralQuotaStart = time.Now()
	}
	if quotaOutboxEnqueued {
		mediaDelta := int64(0)
		if isQuotaMediaContentType(contentType) {
			mediaDelta = 1
		}
		b.addLocalQuotaPendingDeltas(int64(len(data)), 1, mediaDelta)
		b.notifyQuotaOutbox(true)
	} else {
		b.syncCentralFileCreate(ctx, fileID, int64(len(data)), contentType)
	}
	if timingEnabled {
		centralQuotaDuration = time.Since(centralQuotaStart)
	}
	return int64(len(data)), nil
}

func (b *Dat9Backend) overwriteFileCtxWithRev(ctx context.Context, nf *datastore.NodeWithFile, data []byte, offset int64, flags filesystem.WriteFlag, expectedRevision int64, tags map[string]string, description string) (written int64, newRevision int64, err error) {
	timingEnabled := logger.BenchTimingLogEnabled()
	start := time.Time{}
	if timingEnabled {
		start = time.Now()
	}
	path := ""
	fileID := ""
	oldSize := int64(0)
	storageType := datastore.StorageDB9
	contentType := ""
	finalSize := int64(0)
	var readExistingDuration time.Duration
	var prepareDuration time.Duration
	var s3PutDuration time.Duration
	var tenantTxDuration time.Duration
	var centralQuotaDuration time.Duration
	var oldBlobCleanupDuration time.Duration
	var imageEnqueueDuration time.Duration
	defer func() {
		if !timingEnabled {
			return
		}
		fields := []zap.Field{
			zap.String("path", path),
			zap.String("file_id", fileID),
			zap.String("result", backendWriteResult(err)),
			zap.Int("input_bytes", len(data)),
			zap.Int64("final_size", finalSize),
			zap.Int64("old_size", oldSize),
			zap.Int64("written", written),
			zap.Int64("expected_revision", expectedRevision),
			zap.Int64("committed_revision", newRevision),
			zap.Int("flags", int(flags)),
			zap.String("storage_type", string(storageType)),
			zap.String("content_type", contentType),
			zap.Float64("read_existing_ms", backendDurationMs(readExistingDuration)),
			zap.Float64("prepare_ms", backendDurationMs(prepareDuration)),
			zap.Float64("s3_put_ms", backendDurationMs(s3PutDuration)),
			zap.Float64("tenant_tx_ms", backendDurationMs(tenantTxDuration)),
			zap.Float64("central_quota_ms", backendDurationMs(centralQuotaDuration)),
			zap.Float64("old_blob_cleanup_ms", backendDurationMs(oldBlobCleanupDuration)),
			zap.Float64("image_enqueue_ms", backendDurationMs(imageEnqueueDuration)),
			zap.Float64("total_ms", backendDurationMs(time.Since(start))),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		logger.InfoBenchTiming(ctx, "backend_write_overwrite_timing", fields...)
	}()
	if nf.File == nil {
		return 0, 0, fmt.Errorf("no file entity")
	}
	path = nf.Node.Path
	fileID = nf.File.FileID
	oldSize = nf.File.SizeBytes

	var finalData []byte
	if flags&filesystem.WriteFlagAppend != 0 {
		readExistingStart := time.Time{}
		if timingEnabled {
			readExistingStart = time.Now()
		}
		existing, err := b.readFileDataCtx(ctx, nf.File)
		if timingEnabled {
			readExistingDuration = time.Since(readExistingStart)
		}
		if err != nil {
			return 0, 0, fmt.Errorf("read existing data for append: %w", err)
		}
		finalData = append(existing, data...)
	} else if flags&filesystem.WriteFlagTruncate != 0 || offset <= 0 {
		finalData = data
	} else {
		readExistingStart := time.Time{}
		if timingEnabled {
			readExistingStart = time.Now()
		}
		existing, err := b.readFileDataCtx(ctx, nf.File)
		if timingEnabled {
			readExistingDuration = time.Since(readExistingStart)
		}
		if err != nil {
			return 0, 0, fmt.Errorf("read existing data for offset write: %w", err)
		}
		if offset > int64(len(existing)) {
			existing = append(existing, make([]byte, offset-int64(len(existing)))...)
		}
		end := offset + int64(len(data))
		finalData = append(existing[:offset], data...)
		if end < int64(len(existing)) {
			finalData = append(finalData, existing[end:]...)
		}
	}

	if err := b.ensureUploadSizeAllowed(int64(len(finalData))); err != nil {
		return 0, 0, err
	}
	if err := b.ensureFileSizeQuota(ctx, int64(len(finalData))); err != nil {
		return 0, 0, err
	}
	finalSize = int64(len(finalData))
	prepareStart := time.Time{}
	if timingEnabled {
		prepareStart = time.Now()
	}
	contentType = detectContentType(nf.Node.Path, finalData)
	checksum := sha256sum(finalData)
	contentText := extractText(finalData, contentType, b.textExtractMaxBytes)
	if timingEnabled {
		prepareDuration = time.Since(prepareStart)
	}
	storageRef := "inline"
	storageEncryptionMode := datastore.StorageEncryptionNone
	storageEncryptionKeyID := ""
	var contentBlob []byte
	if b.shouldStoreInDB(int64(len(finalData))) {
		contentBlob = append([]byte(nil), finalData...)
	} else {
		if b.s3 == nil {
			return 0, 0, fmt.Errorf("s3 client not configured")
		}
		storageType = datastore.StorageS3
		storageRef = "blobs/" + b.genID()
		encOpts, encMode, encKeyID := b.s3WriteEncryption(storageRef)
		storageEncryptionMode = encMode
		storageEncryptionKeyID = encKeyID
		s3PutStart := time.Time{}
		if timingEnabled {
			s3PutStart = time.Now()
		}
		if err := b.s3.PutObject(ctx, storageRef, bytes.NewReader(finalData), int64(len(finalData)), encOpts); err != nil {
			if timingEnabled {
				s3PutDuration = time.Since(s3PutStart)
			}
			logger.Error(ctx, "backend_overwrite_put_object_failed", zap.String("path", nf.Node.Path), zap.String("storage_ref", storageRef), zap.Int("bytes", len(finalData)), zap.Error(err))
			return 0, 0, fmt.Errorf("put object: %w", err)
		}
		if timingEnabled {
			s3PutDuration = time.Since(s3PutStart)
		}
	}

	var semanticTaskEnqueued bool
	var quotaOutboxEnqueued bool
	txStart := time.Time{}
	if timingEnabled {
		txStart = time.Now()
	}
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		semanticTaskEnqueued = false
		quotaOutboxEnqueued = false
		var oldSizeBytes int64
		var oldContentTypeForQuota string
		currentMeta, err := b.store.GetFileStorageMetaForUpdateTx(tx, nf.File.FileID)
		if err != nil {
			return err
		}
		oldSizeBytes = currentMeta.SizeBytes
		oldContentTypeForQuota = currentMeta.ContentType
		if expectedRevision > 0 && currentMeta.Revision != expectedRevision {
			return datastore.ErrRevisionConflict
		}
		if b.UseServerQuota() {
			if deltaBytes := int64(len(finalData)) - currentMeta.SizeBytes; deltaBytes > 0 {
				if err := b.ensureStorageQuotaServer(ctx, tx, deltaBytes); err != nil {
					return err
				}
			}
		} else if err := b.ensureStorageQuota(ctx, tx, nf.Node.Path, int64(len(finalData))); err != nil {
			return err
		}
		var txErr error
		if b.UsesDatabaseAutoEmbedding() {
			if expectedRevision > 0 {
				newRevision, txErr = b.store.UpdateFileContentAutoEmbeddingIfRevisionTx(tx,
					nf.File.FileID, expectedRevision, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)), description,
				)
			} else {
				newRevision, txErr = b.store.UpdateFileContentAutoEmbeddingTx(tx,
					nf.File.FileID, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)), description,
				)
			}
		} else {
			if expectedRevision > 0 {
				newRevision, txErr = b.store.UpdateFileContentIfRevisionTx(tx,
					nf.File.FileID, expectedRevision, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)), description,
				)
			} else {
				newRevision, txErr = b.store.UpdateFileContentTx(tx,
					nf.File.FileID, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)), description,
				)
			}
		}
		if txErr != nil {
			return txErr
		}
		if err := b.store.UpdateFileStorageEncryptionTx(tx, nf.File.FileID, storageEncryptionMode, storageEncryptionKeyID); err != nil {
			return err
		}
		if tags != nil {
			if err := b.store.ReplaceFileTagsTx(tx, nf.File.FileID, tags); err != nil {
				return err
			}
		}
		currentMediaDelta := quotaMediaDelta(isQuotaMediaContentType(oldContentTypeForQuota), isQuotaMediaContentType(contentType))
		if b.UsesDatabaseAutoEmbedding() {
			created, err := b.enqueueExtractSemanticTasksTx(ctx, tx, nf.File.FileID, newRevision, nf.Node.Path, contentType, currentMediaDelta)
			semanticTaskEnqueued = created
			if err != nil {
				return err
			}
		} else {
			// App-embedding mode: image/audio extract tasks are durable and independent
			// of EMBED_TEXT, so register them in the same transaction. The embed task
			// (if any) is enqueued separately below.
			extractCreated, extractErr := b.enqueueExtractSemanticTasksTx(ctx, tx, nf.File.FileID, newRevision, nf.Node.Path, contentType, currentMediaDelta)
			if extractErr != nil {
				return extractErr
			}
			if b.shouldEnqueueEmbedForRevision(nf.Node.Path, contentType, contentText, description) {
				created, err := b.enqueueEmbedTaskTx(tx, nf.File.FileID, newRevision)
				semanticTaskEnqueued = created || extractCreated
				if err != nil {
					return err
				}
			} else {
				semanticTaskEnqueued = extractCreated
			}
		}
		created, err := b.enqueueQuotaFileOverwriteOutboxTx(tx, nf.File.FileID, oldSizeBytes, oldContentTypeForQuota, int64(len(finalData)), contentType)
		if err != nil {
			return err
		}
		quotaOutboxEnqueued = created
		nf.File.StorageType = currentMeta.StorageType
		nf.File.StorageRef = currentMeta.StorageRef
		nf.File.SizeBytes = oldSizeBytes
		nf.File.ContentType = oldContentTypeForQuota
		return nil
	})
	if timingEnabled {
		tenantTxDuration = time.Since(txStart)
	}
	if err != nil {
		if storageType == datastore.StorageS3 {
			b.deleteBlobCtx(ctx, storageRef)
		}
		return 0, 0, err
	}
	b.notifySemanticTaskEnqueued(semanticTaskEnqueued)
	centralQuotaStart := time.Time{}
	if timingEnabled {
		centralQuotaStart = time.Now()
	}
	if quotaOutboxEnqueued {
		b.addLocalQuotaPendingDeltas(
			int64(len(finalData))-nf.File.SizeBytes,
			0,
			quotaMediaDelta(isQuotaMediaContentType(nf.File.ContentType), isQuotaMediaContentType(contentType)),
		)
		b.notifyQuotaOutbox(true)
	} else {
		b.syncCentralFileOverwrite(ctx, nf.File.FileID, nf.File.SizeBytes, nf.File.ContentType, int64(len(finalData)), contentType)
	}
	if timingEnabled {
		centralQuotaDuration = time.Since(centralQuotaStart)
	}
	// Overwrite cleanup is object-level: file_gc_tasks track deleted file
	// identities, not old blob refs for a still-live file_id.
	oldBlobCleanupStart := time.Time{}
	if timingEnabled {
		oldBlobCleanupStart = time.Now()
	}
	b.deleteBlobIfS3Ctx(ctx, nf.File.StorageType, nf.File.StorageRef, storageRef)
	if timingEnabled {
		oldBlobCleanupDuration = time.Since(oldBlobCleanupStart)
	}
	return int64(len(data)), newRevision, nil
}

func (b *Dat9Backend) ReadDir(path string) ([]filesystem.FileInfo, error) {
	return b.ReadDirCtx(backgroundWithTrace(), path)
}

func (b *Dat9Backend) ReadDirCtx(ctx context.Context, path string) (infos []filesystem.FileInfo, err error) {
	start := time.Now()
	var canonicalPath string
	var listDuration time.Duration
	defer func() {
		observeBackend(ctx, b.tenantID, "read_dir", err, start)
		fields := []zap.Field{
			zap.String("path", path),
			zap.String("canonical_path", canonicalPath),
			zap.Int("entries", len(infos)),
			zap.Float64("list_dir_ms", float64(listDuration.Microseconds())/1000.0),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		logger.InfoBenchTiming(ctx, "backend_read_dir_timing", fields...)
	}()

	dirPath, err := pathutil.CanonicalizeDir(path)
	if err != nil {
		return nil, err
	}
	canonicalPath = dirPath
	logger.InfoBenchTiming(ctx, "backend_read_dir_start",
		zap.String("path", path),
		zap.String("canonical_path", dirPath))
	listStart := time.Now()
	entries, err := b.store.ListDir(ctx, dirPath)
	listDuration = time.Since(listStart)
	if err != nil {
		return nil, err
	}

	fileIDs := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.File != nil && e.File.FileID != "" {
			fileIDs = append(fileIDs, e.File.FileID)
		}
	}
	refCounts, err := b.store.RefCounts(ctx, fileIDs)
	if err != nil {
		return nil, err
	}

	infos = make([]filesystem.FileInfo, 0, len(entries))
	for _, e := range entries {
		info := filesystem.FileInfo{
			Name: e.Node.Name, IsDir: e.Node.IsDirectory, Mode: 0o644,
		}
		meta := make(map[string]string)
		if e.Node.IsDirectory {
			info.Mode = 0o755
		}
		if e.HasMode {
			info.Mode = e.Mode
			meta["hasMode"] = "true"
		}
		if e.File != nil {
			info.Size = e.File.SizeBytes
			info.ModTime = fileMtime(e.File)
			meta["resource_id"] = e.File.FileID
			if e.File.Revision > 0 {
				meta["revision"] = strconv.FormatInt(e.File.Revision, 10)
			}
			count := refCounts[e.File.FileID]
			if count <= 0 {
				count = 1
			}
			if count > int64(^uint32(0)) {
				count = int64(^uint32(0))
			}
			meta["nlink"] = strconv.FormatInt(count, 10)
		} else {
			info.ModTime = e.Node.CreatedAt
			if e.Node.InodeID != "" {
				meta["resource_id"] = e.Node.InodeID
			} else if e.Node.NodeID != "" {
				meta["resource_id"] = e.Node.NodeID
			}
			if e.Node.IsDirectory {
				meta["nlink"] = "2"
			} else {
				meta["nlink"] = "1"
			}
		}
		if len(meta) > 0 {
			info.Meta = filesystem.MetaData{Content: meta}
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (b *Dat9Backend) StatNodeCtx(ctx context.Context, path string) (*datastore.NodeWithFile, error) {
	start := time.Now()
	resolvedPath := normalizePath(path)
	mode := "primary"
	var fallbackPath string
	logger.InfoBenchTiming(ctx, "backend_stat_node_start",
		zap.String("path", path),
		zap.String("resolved_path", resolvedPath))
	var out *datastore.NodeWithFile
	var err error
	if pathutil.IsDir(path) {
		mode = "dir"
		out, err = b.store.Stat(ctx, resolvedPath)
	} else {
		dirPath, dirErr := pathutil.CanonicalizeDir(path)
		if dirErr != nil || dirPath == resolvedPath {
			out, err = b.store.Stat(ctx, resolvedPath)
		} else {
			mode = "fallback"
			fallbackPath = dirPath
			out, err = b.store.StatPathFallback(ctx, resolvedPath, dirPath)
		}
	}
	logStatNodeTiming(ctx, "backend_stat_node_timing", path, resolvedPath, fallbackPath, mode, out, err, start)
	return out, err
}

// StatNodeLiteCtx returns lightweight metadata (no blob/text/description)
// suitable for HEAD/stat operations.
func (b *Dat9Backend) StatNodeLiteCtx(ctx context.Context, path string) (*datastore.NodeWithFile, error) {
	start := time.Now()
	resolvedPath := normalizePath(path)
	mode := "primary"
	var fallbackPath string
	logger.InfoBenchTiming(ctx, "backend_stat_lite_start",
		zap.String("path", path),
		zap.String("resolved_path", resolvedPath))
	var out *datastore.NodeWithFile
	var err error
	if pathutil.IsDir(path) {
		mode = "dir"
		out, err = b.store.StatLite(ctx, resolvedPath)
	} else {
		dirPath, dirErr := pathutil.CanonicalizeDir(path)
		if dirErr != nil || dirPath == resolvedPath {
			out, err = b.store.StatLite(ctx, resolvedPath)
		} else {
			mode = "fallback"
			fallbackPath = dirPath
			out, err = b.store.StatPathFallbackLite(ctx, resolvedPath, dirPath)
		}
	}
	logStatNodeTiming(ctx, "backend_stat_lite_timing", path, resolvedPath, fallbackPath, mode, out, err, start)
	return out, err
}

func logStatNodeTiming(ctx context.Context, message, path, resolvedPath, fallbackPath, mode string, nf *datastore.NodeWithFile, err error, start time.Time) {
	fields := []zap.Field{
		zap.String("path", path),
		zap.String("resolved_path", resolvedPath),
		zap.String("mode", mode),
		zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0),
	}
	if fallbackPath != "" {
		fields = append(fields, zap.String("fallback_path", fallbackPath))
	}
	if nf != nil {
		fields = append(fields, zap.Bool("is_dir", nf.Node.IsDirectory))
	}
	if nf != nil && nf.File != nil {
		fields = append(fields,
			zap.Int64("size", nf.File.SizeBytes),
			zap.Int64("revision", nf.File.Revision))
	}
	if err != nil {
		fields = append(fields, zap.Error(err))
	}
	logger.InfoBenchTiming(ctx, message, fields...)
}

// ReadPlan describes how to serve a GET request after a single metadata query.
type ReadPlan struct {
	// InlineData is non-nil for db9-stored files — the body to return directly.
	InlineData []byte
	// PresignURL is non-empty for S3-stored files — the 302 redirect target.
	PresignURL string
	// Size is the file size in bytes.
	Size int64
	// Revision is the file revision observed by the same metadata query.
	Revision int64
	// Mtime is the confirmed timestamp when available, otherwise file creation time.
	Mtime time.Time
}

// ReadPlanCtx resolves a file path into a ReadPlan with a single metadata query.
// For db9 inline files: returns InlineData (no second stat needed).
// For S3 files: uses the storage_ref from the same query to presign, returns PresignURL.
// Only resolves file-form paths (no directory fallback) to maintain GET /dir → 404 behavior.
func (b *Dat9Backend) ReadPlanCtx(ctx context.Context, path string) (plan *ReadPlan, err error) {
	start := time.Now()
	var statDuration time.Duration
	var presignDuration time.Duration
	storageType := ""
	var size int64
	phase := "canonicalize"
	defer func() {
		observeBackend(ctx, b.tenantID, "read_plan", err, start)
		fields := []zap.Field{
			zap.String("path", path),
			zap.String("phase", phase),
			zap.String("storage_type", storageType),
			zap.Int64("size", size),
			zap.Float64("stat_ms", float64(statDuration.Microseconds())/1000.0),
			zap.Float64("presign_ms", float64(presignDuration.Microseconds())/1000.0),
			zap.Float64("total_ms", float64(time.Since(start).Microseconds())/1000.0),
		}
		if err != nil {
			fields = append(fields, zap.Error(err))
		}
		logger.InfoBenchTiming(ctx, "backend_read_plan_timing", fields...)
	}()
	resolvedPath, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	if resolvedPath == "/" {
		return nil, datastore.ErrNotFound
	}
	phase = "stat_for_read"
	logger.InfoBenchTiming(ctx, "backend_read_plan_start",
		zap.String("path", path),
		zap.String("resolved_path", resolvedPath))

	statStart := time.Now()
	nf, err := b.store.StatForRead(ctx, resolvedPath)
	statDuration = time.Since(statStart)
	if err != nil {
		return nil, err
	}
	phase = "plan"
	if nf.Node.IsDirectory {
		return nil, datastore.ErrNotFound
	}
	if nf.File == nil {
		return nil, datastore.ErrNotFound
	}
	storageType = string(nf.File.StorageType)
	size = nf.File.SizeBytes

	switch nf.File.StorageType {
	case datastore.StorageDB9:
		phase = "inline"
		return &ReadPlan{
			InlineData: nf.File.ContentBlob,
			Size:       nf.File.SizeBytes,
			Revision:   nf.File.Revision,
			Mtime:      fileMtime(nf.File),
		}, nil
	case datastore.StorageS3:
		if b.s3 == nil {
			return nil, ErrS3NotConfigured
		}
		phase = "presign_get_object"
		presignStart := time.Now()
		url, err := b.s3.PresignGetObject(ctx, nf.File.StorageRef, s3client.DownloadTTL)
		presignDuration = time.Since(presignStart)
		if err != nil {
			logger.Error(ctx, "backend_read_plan_presign_failed", zap.String("path", resolvedPath), zap.Error(err))
			return nil, err
		}
		phase = "redirect"
		return &ReadPlan{
			PresignURL: url,
			Size:       nf.File.SizeBytes,
			Revision:   nf.File.Revision,
			Mtime:      fileMtime(nf.File),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported storage type for read plan: %s", nf.File.StorageType)
	}
}

// ReadInlinePlanCtx resolves a file path to inline db9 data without presigning
// S3 objects. Batch read-small uses this to reject S3-backed files cheaply
// without reading or presigning object storage data.
func (b *Dat9Backend) ReadInlinePlanCtx(ctx context.Context, path string) (plan *ReadPlan, err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "read_inline_plan", err, start) }()

	resolvedPath, err := pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
	}
	if resolvedPath == "/" {
		return nil, datastore.ErrNotFound
	}
	nf, err := b.store.StatForRead(ctx, resolvedPath)
	if err != nil {
		return nil, err
	}
	if nf.Node.IsDirectory || nf.File == nil {
		return nil, datastore.ErrNotFound
	}
	if nf.File.StorageType != datastore.StorageDB9 {
		return nil, ErrNotInlineStorage
	}
	return &ReadPlan{
		InlineData: nf.File.ContentBlob,
		Size:       nf.File.SizeBytes,
		Revision:   nf.File.Revision,
		Mtime:      fileMtime(nf.File),
	}, nil
}

func fileMtime(f *datastore.File) time.Time {
	if f == nil {
		return time.Time{}
	}
	if f.ConfirmedAt != nil {
		return *f.ConfirmedAt
	}
	return f.CreatedAt
}

func (b *Dat9Backend) Stat(path string) (*filesystem.FileInfo, error) {
	ctx := backgroundWithTrace()
	nf, err := b.StatNodeCtx(ctx, path)
	if err != nil {
		return nil, err
	}
	info := &filesystem.FileInfo{
		Name: nf.Node.Name, IsDir: nf.Node.IsDirectory,
	}
	if nf.File != nil {
		info.Size = nf.File.SizeBytes
		info.ModTime = fileMtime(nf.File)
		if nf.HasMode {
			info.Mode = nf.Mode
		} else if nf.Node.IsDirectory {
			info.Mode = 0o755
		} else {
			info.Mode = 0o644
		}
	} else {
		info.ModTime = nf.Node.CreatedAt
		if nf.HasMode {
			info.Mode = nf.Mode
		} else if nf.Node.IsDirectory {
			info.Mode = 0o755
		} else {
			info.Mode = 0o644
		}
	}
	return info, nil
}

func (b *Dat9Backend) Rename(oldPath, newPath string) error {
	return b.RenameCtx(backgroundWithTrace(), oldPath, newPath)
}

func (b *Dat9Backend) RenameCtx(ctx context.Context, oldPath, newPath string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "rename", err, start) }()

	oldPath, node, err := b.resolveNodePath(ctx, oldPath)
	if err != nil {
		return err
	}
	newPath = canonicalizePathForKind(newPath, node.IsDirectory)
	if oldPath == "/" || newPath == "/" {
		return datastore.ErrInvalidRootDentry
	}
	if oldPath == newPath {
		return nil
	}
	if node.IsDirectory {
		err = b.store.EnsureParentDirs(ctx, newPath, b.genID)
		if err != nil {
			return err
		}
		_, err = b.store.RenameDir(ctx, oldPath, newPath)
		return err
	}
	err = b.store.EnsureParentDirs(ctx, newPath, b.genID)
	if err != nil {
		return err
	}
	_, err = b.store.RenameFileReplacingTarget(ctx, oldPath, newPath, pathutil.ParentPath(newPath), pathutil.BaseName(newPath))
	return err
}

// RenameFileNoReplaceCtx renames a file and fails with datastore.ErrPathConflict
// if the target path already exists.
func (b *Dat9Backend) RenameFileNoReplaceCtx(ctx context.Context, oldPath, newPath string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "rename_file_no_replace", err, start) }()

	oldPath, node, err := b.resolveNodePath(ctx, oldPath)
	if err != nil {
		return err
	}
	if node.IsDirectory {
		return fmt.Errorf("source is a directory: %s", oldPath)
	}
	newPath, err = pathutil.Canonicalize(newPath)
	if err != nil {
		return err
	}
	if oldPath == "/" || newPath == "/" {
		return datastore.ErrInvalidRootDentry
	}
	if oldPath == newPath {
		return nil
	}
	if err := b.store.EnsureParentDirs(ctx, newPath, b.genID); err != nil {
		return err
	}
	return b.store.RenameFileNoReplace(ctx, oldPath, newPath, pathutil.ParentPath(newPath), pathutil.BaseName(newPath))
}

func (b *Dat9Backend) Open(path string) (io.ReadCloser, error) {
	data, err := b.Read(path, 0, -1)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (b *Dat9Backend) OpenWrite(path string) (io.WriteCloser, error) {
	return &writeCloser{backend: b, path: path}, nil
}

// --- CapabilityProvider ---

func (b *Dat9Backend) GetCapabilities() filesystem.Capabilities {
	caps := filesystem.DefaultCapabilities()
	if b.s3 != nil {
		caps.IsObjectStore = true
	}
	return caps
}

func (b *Dat9Backend) GetPathCapabilities(path string) filesystem.Capabilities {
	return b.GetCapabilities()
}

// Verify interface compliance.
var _ filesystem.CapabilityProvider = (*Dat9Backend)(nil)

// CopyFile performs a zero-copy cp (new file_node pointing to same file_id).
func (b *Dat9Backend) CopyFile(srcPath, dstPath string) error {
	return b.CopyFileCtx(backgroundWithTrace(), srcPath, dstPath)
}

func (b *Dat9Backend) CopyFileCtx(ctx context.Context, srcPath, dstPath string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "copy_file", err, start) }()

	srcPath, err = pathutil.Canonicalize(srcPath)
	if err != nil {
		return err
	}
	if srcPath == "/" {
		return datastore.ErrNotFound
	}
	dstPath, err = pathutil.Canonicalize(dstPath)
	if err != nil {
		return err
	}
	if err := rejectRootFileNodePath(dstPath); err != nil {
		return err
	}
	srcNode, err := b.store.GetNode(ctx, srcPath)
	if err != nil {
		return err
	}
	if srcNode.IsDirectory {
		return fmt.Errorf("cannot copy directory with CopyFile: %s", srcPath)
	}
	if err := b.store.EnsureParentDirs(ctx, dstPath, b.genID); err != nil {
		return err
	}
	return b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.store.InsertNodeTx(tx, &datastore.FileNode{
			NodeID: b.genID(), Path: dstPath, ParentPath: pathutil.ParentPath(dstPath),
			Name: pathutil.BaseName(dstPath), FileID: srcNode.FileID, CreatedAt: time.Now(),
		}); err != nil {
			return fmt.Errorf("insert copied node %q: %w", dstPath, err)
		}
		return nil
	})
}

// HardlinkFile creates dstPath as another directory entry for srcPath's file
// entity. The two paths share content, revision, mode, tags, and storage.
func (b *Dat9Backend) HardlinkFile(srcPath, dstPath string) error {
	return b.HardlinkFileCtx(backgroundWithTrace(), srcPath, dstPath)
}

func (b *Dat9Backend) HardlinkFileCtx(ctx context.Context, srcPath, dstPath string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, b.tenantID, "hardlink_file", err, start) }()

	dstPath, err = pathutil.Canonicalize(dstPath)
	if err != nil {
		return err
	}
	if err := rejectRootFileNodePath(dstPath); err != nil {
		return err
	}
	srcPath, srcNode, err := b.resolveNodePath(ctx, srcPath)
	if err != nil {
		return err
	}
	if srcPath == "/" {
		return datastore.ErrNotFound
	}
	if srcPath == dstPath {
		return datastore.ErrPathConflict
	}
	if srcNode.IsDirectory {
		return ErrInvalidHardlinkTarget
	}
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.store.EnsureParentDirsTx(tx, dstPath, b.genID); err != nil {
			return err
		}
		return b.store.LinkFileNodeTx(ctx, tx, srcPath, dstPath, pathutil.ParentPath(dstPath), pathutil.BaseName(dstPath), b.genID(), time.Now())
	})
	if errors.Is(err, datastore.ErrInvalidLinkTarget) {
		return ErrInvalidHardlinkTarget
	}
	return err
}

func (b *Dat9Backend) deleteBlobCtx(ctx context.Context, ref string) {
	if b.s3 != nil && ref != "" {
		if err := b.s3.DeleteObject(ctx, ref); err != nil {
			logger.Warn(ctx, "backend_delete_blob_failed", zap.String("storage_ref", ref), zap.Error(err))
		}
	}
}

func (b *Dat9Backend) deleteBlobIfS3Ctx(ctx context.Context, storageType datastore.StorageType, storageRef, keepRef string) {
	if storageType != datastore.StorageS3 || storageRef == "" || storageRef == keepRef {
		return
	}
	handled, err := b.enqueueObjectGCCandidateCtx(ctx, storageRef, meta.ObjectGCReasonOverwrite, "")
	if handled && err == nil {
		return
	}
	if err != nil {
		logger.Warn(ctx, "backend_object_gc_candidate_required_but_failed",
			zap.String("storage_ref", storageRef),
			zap.Error(err))
		return
	}
	logger.Warn(ctx, "backend_object_gc_candidate_not_configured",
		zap.String("storage_ref", storageRef))
}

type objectGCCandidateEnqueuer interface {
	EnqueueObjectGCCandidate(ctx context.Context, c *meta.ObjectGCCandidateInput) error
}

func (b *Dat9Backend) enqueueObjectGCCandidateCtx(ctx context.Context, storageRef string, reason meta.ObjectGCCandidateReason, sourceFileID string) (bool, error) {
	if b.metaStore == nil || b.storageNamespaceID == "" || storageRef == "" {
		return false, nil
	}
	enqueuer, ok := b.metaStore.(objectGCCandidateEnqueuer)
	if !ok {
		return false, nil
	}
	err := enqueuer.EnqueueObjectGCCandidate(ctx, &meta.ObjectGCCandidateInput{
		NamespaceID:    b.storageNamespaceID,
		StorageRef:     storageRef,
		StorageRefHash: datastore.StorageRefHash(storageRef),
		Reason:         reason,
		SourceTenantID: b.tenantID,
		SourceFileID:   sourceFileID,
		NotBefore:      time.Now().UTC().Add(7 * 24 * time.Hour),
	})
	if err != nil {
		logger.Warn(ctx, "backend_enqueue_object_gc_candidate_failed",
			zap.String("storage_namespace_id", b.storageNamespaceID),
			zap.String("storage_ref", storageRef),
			zap.String("reason", string(reason)),
			zap.Error(err))
		return true, err
	}
	return true, nil
}

func (b *Dat9Backend) readFileDataCtx(ctx context.Context, f *datastore.File) ([]byte, error) {
	if f == nil {
		return nil, fmt.Errorf("nil file")
	}
	if f.StorageType == datastore.StorageS3 {
		if b.s3 == nil {
			return nil, fmt.Errorf("s3 client not configured")
		}
		rc, err := b.s3.GetObject(ctx, f.StorageRef)
		if err != nil {
			logger.Error(ctx, "backend_read_get_object_failed", zap.String("storage_ref", f.StorageRef), zap.Error(err))
			return nil, err
		}
		defer func() { _ = rc.Close() }()
		return io.ReadAll(rc)
	}
	if f.StorageType == datastore.StorageDB9 {
		return append([]byte(nil), f.ContentBlob...), nil
	}
	return nil, fmt.Errorf("unsupported storage type for direct read: %s", f.StorageType)
}

// InlineThreshold returns the configured DB-inline vs S3 storage cutoff.
func (b *Dat9Backend) InlineThreshold() int64 {
	return b.inlineThreshold
}

// TextExtractMaxBytes returns the configured cap for synchronous text
// extraction inputs.
func (b *Dat9Backend) TextExtractMaxBytes() int64 {
	return b.textExtractMaxBytes
}

func (b *Dat9Backend) shouldStoreInDB(size int64) bool {
	return b.smallInDB && size < b.inlineThreshold
}

// --- writeCloser ---

type writeCloser struct {
	backend *Dat9Backend
	path    string
	buf     bytes.Buffer
}

func (w *writeCloser) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *writeCloser) Close() error {
	_, err := w.backend.Write(w.path, w.buf.Bytes(), 0,
		filesystem.WriteFlagCreate|filesystem.WriteFlagTruncate)
	return err
}

// --- helpers ---

func rejectRootFileNodePath(path string) error {
	if path == "/" {
		return datastore.ErrInvalidRootDentry
	}
	return nil
}

func normalizePath(path string) string {
	if pathutil.IsDir(path) {
		p, err := pathutil.CanonicalizeDir(path)
		if err != nil {
			return path
		}
		return p
	}
	p, err := pathutil.Canonicalize(path)
	if err != nil {
		return path
	}
	return p
}

func canonicalizePathForKind(path string, isDir bool) string {
	if isDir {
		p, err := pathutil.CanonicalizeDir(path)
		if err == nil {
			return p
		}
		return path
	}
	return normalizePath(path)
}

func (b *Dat9Backend) resolveNodePath(ctx context.Context, rawPath string) (string, *datastore.FileNode, error) {
	resolvedPath := normalizePath(rawPath)
	node, err := b.store.GetNode(ctx, resolvedPath)
	if err == nil {
		return resolvedPath, node, nil
	}
	if !errors.Is(err, datastore.ErrNotFound) || pathutil.IsDir(rawPath) {
		return resolvedPath, nil, err
	}

	dirPath, dirErr := pathutil.CanonicalizeDir(rawPath)
	if dirErr != nil || dirPath == resolvedPath {
		return resolvedPath, nil, err
	}
	dirNode, dirLookupErr := b.store.GetNode(ctx, dirPath)
	if dirLookupErr != nil {
		if errors.Is(dirLookupErr, datastore.ErrNotFound) {
			return resolvedPath, nil, err
		}
		return resolvedPath, nil, dirLookupErr
	}
	return dirPath, dirNode, nil
}

func backgroundWithTrace() context.Context {
	return traceid.Background()
}

func sha256sum(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func detectContentType(path string, data []byte) string {
	ext := pathutil.Ext(path)
	if ext != "" {
		if ct := mime.TypeByExtension(ext); ct != "" {
			if isTextualContentType(ct) && !isTextContent(data) {
				return "application/octet-stream"
			}
			return ct
		}
	}
	if len(data) > 0 && isTextContent(data) {
		return "text/plain"
	}
	return "application/octet-stream"
}

func isTextContent(data []byte) bool {
	for _, b := range data {
		if b == 0 {
			return false
		}
	}
	return utf8.Valid(data)
}

func isTextualContentType(contentType string) bool {
	return strings.HasPrefix(contentType, "text/") ||
		contentType == "application/json" ||
		contentType == "application/xml" ||
		contentType == "application/yaml"
}

func extractText(data []byte, contentType string, maxBytes int64) string {
	if !isTextualContentType(contentType) {
		return ""
	}
	if !isTextContent(data) {
		return ""
	}
	if maxBytes <= 0 {
		maxBytes = DefaultTextExtractMaxBytes
	}
	if int64(len(data)) > maxBytes {
		return ""
	}
	return string(data)
}

func (b *Dat9Backend) ExecSQL(ctx context.Context, query string) ([]map[string]interface{}, error) {
	start := time.Now()
	rows, err := b.store.ExecSQL(ctx, query)
	observeBackend(ctx, b.tenantID, "exec_sql", err, start)
	if err != nil {
		logger.Error(ctx, "backend_exec_sql_failed", zap.Int("query_len", len(query)), zap.Error(err))
		return nil, err
	}
	return rows, nil
}

func (b *Dat9Backend) Grep(ctx context.Context, query, pathPrefix string, limit int) ([]datastore.SearchResult, error) {
	start := time.Now()
	var err error
	defer func() { observeBackend(ctx, b.tenantID, "grep", err, start) }()

	if strings.TrimSpace(query) == "" {
		err = fmt.Errorf("empty query")
		return nil, err
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	fetch := limit * 3

	type grepResp struct {
		rows []datastore.SearchResult
		err  error
	}
	ftsCh := make(chan grepResp, 1)
	go func() {
		rows, searchErr := b.store.FTSSearch(ctx, query, pathPrefix, fetch)
		ftsCh <- grepResp{rows: rows, err: searchErr}
	}()

	var vecCh, vecDescCh chan grepResp
	if b.UsesDatabaseAutoEmbedding() {
		vecCh = make(chan grepResp, 1)
		go func() {
			rows, searchErr := b.store.VectorSearchByText(ctx, query, pathPrefix, fetch)
			vecCh <- grepResp{rows: rows, err: searchErr}
		}()
		vecDescCh = make(chan grepResp, 1)
		go func() {
			rows, searchErr := b.store.VectorSearchDescriptionByText(ctx, query, pathPrefix, fetch)
			vecDescCh <- grepResp{rows: rows, err: searchErr}
		}()
	} else if b.queryEmbedder != nil {
		queryVec, embedErr := b.queryEmbedder.EmbedText(ctx, query)
		vecCh = make(chan grepResp, 1)
		go func() {
			if embedErr != nil || len(queryVec) == 0 {
				vecCh <- grepResp{err: embedErr}
				return
			}
			rows, searchErr := b.store.VectorSearch(ctx, queryVec, pathPrefix, fetch)
			vecCh <- grepResp{rows: rows, err: searchErr}
		}()
		vecDescCh = make(chan grepResp, 1)
		go func() {
			if embedErr != nil || len(queryVec) == 0 {
				vecDescCh <- grepResp{err: embedErr}
				return
			}
			rows, searchErr := b.store.VectorSearchDescription(ctx, queryVec, pathPrefix, fetch)
			vecDescCh <- grepResp{rows: rows, err: searchErr}
		}()
	}

	ftsResp := <-ftsCh
	if ftsResp.err != nil {
		logger.Warn(ctx, "backend_grep_fts_failed",
			zap.Int("query_len", len(query)),
			zap.String("path_prefix", pathPrefix),
			zap.Int("limit", fetch),
			zap.Error(ftsResp.err))
	}

	var vecResp, vecDescResp grepResp
	if vecCh != nil {
		vecResp = <-vecCh
		if vecResp.err != nil {
			logger.Warn(ctx, "backend_grep_vector_failed",
				zap.Int("query_len", len(query)),
				zap.String("path_prefix", pathPrefix),
				zap.Int("limit", fetch),
				zap.Error(vecResp.err))
		}
	}
	if vecDescCh != nil {
		vecDescResp = <-vecDescCh
		if vecDescResp.err != nil {
			logger.Warn(ctx, "backend_grep_vector_desc_failed",
				zap.Int("query_len", len(query)),
				zap.String("path_prefix", pathPrefix),
				zap.Int("limit", fetch),
				zap.Error(vecDescResp.err))
		}
	}

	rows := b.grepMerge(ftsResp.rows, vecResp.rows, vecDescResp.rows, limit)
	if rows == nil {
		rows, searchErr := b.store.KeywordSearch(ctx, query, pathPrefix, limit)
		if searchErr != nil {
			logger.Error(ctx, "backend_grep_failed", zap.Int("query_len", len(query)), zap.String("path_prefix", pathPrefix), zap.Int("limit", limit), zap.Error(searchErr))
			err = searchErr
			return nil, err
		}
		return rows, nil
	}
	return rows, nil
}

// grepMerge takes the raw results from FTS, content-vector, and description-vector
// searches and produces the final ranked output. It is extracted for testability.
// When all ranking paths are empty it returns nil to signal the caller to fall back
// to keyword search.
func (b *Dat9Backend) grepMerge(ftsRows, vecRows, vecDescRows []datastore.SearchResult, limit int) []datastore.SearchResult {
	// Merge content vector and description vector results: same file keeps best (min distance) score.
	mergedVec := mergeVectorResults(vecRows, vecDescRows)

	// Decision rule for grep:
	// 1. FTS and vector search are ranking signals; either one may fail independently.
	// 2. If either path returns ranked rows, serve those rows directly after RRF merge.
	// 3. Only fall back to LIKE-based keyword search when both ranking paths produce no rows.
	// This keeps semantic ranking as an enhancement while preserving a text-search safety net.
	hasRankedResults := len(ftsRows) > 0 || len(mergedVec) > 0
	if !hasRankedResults {
		return nil
	}
	return datastore.RRFMerge(ftsRows, mergedVec, limit)
}

// mergeVectorResults merges two vector search result sets, keeping the best score
// (minimum distance / maximum similarity) for each path.
func mergeVectorResults(a, b []datastore.SearchResult) []datastore.SearchResult {
	best := make(map[string]datastore.SearchResult)
	for _, r := range a {
		best[r.Path] = r
	}
	for _, r := range b {
		if existing, ok := best[r.Path]; ok {
			if existing.Score == nil || (r.Score != nil && *r.Score > *existing.Score) {
				best[r.Path] = r
			}
		} else {
			best[r.Path] = r
		}
	}
	out := make([]datastore.SearchResult, 0, len(best))
	for _, r := range best {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return *out[i].Score > *out[j].Score
	})
	return out
}

func (b *Dat9Backend) Find(ctx context.Context, f *datastore.FindFilter) ([]datastore.SearchResult, error) {
	start := time.Now()
	rows, err := b.store.Find(ctx, f)
	observeBackend(ctx, b.tenantID, "find", err, start)
	if err != nil {
		logger.Error(ctx, "backend_find_failed", zap.String("path", f.PathPrefix), zap.String("name", f.NameGlob), zap.Error(err))
		return nil, err
	}
	return rows, nil
}

func observeBackend(ctx context.Context, tenantID, op string, err error, start time.Time) {
	result := "ok"
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			result = "not_found"
		} else {
			result = "error"
		}
	}
	metrics.RecordTenantOperation(tenantID, "backend", op, result, time.Since(start))
}
