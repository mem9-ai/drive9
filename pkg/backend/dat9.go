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
	"strings"
	"sync"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/embedding"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/traceid"
	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"
)

const smallFileThreshold = 50_000 // 50,000 bytes — matches embedding model max input characters

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
	maxUploadBytes        int64
	maxTenantStorageBytes int64
	maxMediaLLMFiles      int64
	mu                    sync.Mutex
	entropy               io.Reader

	// Central quota enforcement (Rev 4 migration).
	tenantID    string
	metaStore   MetaQuotaStore // nil when central quota is not wired (tests, fallback)
	quotaSource QuotaSource    // "tenant" (default) or "server"

	// Async image -> text extraction worker (in-memory queue for P0).
	imageExtractEnabled bool
	imageExtractor      ImageTextExtractor
	imageExtractQueue   chan ImageExtractTaskSpec
	imageExtractWG      sync.WaitGroup
	imageExtractCancel  context.CancelFunc
	imageExtractTimeout time.Duration
	imageExtractMaxSize int64
	maxExtractTextBytes int

	// Durable audio transcript extraction (semantic_tasks only; no local queue).
	audioExtractEnabled      bool
	audioExtractor           AudioTextExtractor
	audioExtractTimeout      time.Duration
	audioExtractMaxSize      int64
	maxAudioExtractTextBytes int

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
		store:         store,
		smallInDB:     true,
		queryEmbedder: embedding.NopClient{},
		entropy:       ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
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
	defer func() { observeBackend(ctx, "create", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return err
	}

	fileID := b.genID()
	now := time.Now()
	storageType := datastore.StorageDB9
	storageRef := "inline"
	var contentBlob []byte
	if b.shouldStoreInDB(0) {
		contentBlob = []byte{}
	} else {
		if b.s3 == nil {
			return fmt.Errorf("s3 client not configured")
		}
		storageType = datastore.StorageS3
		storageRef = "blobs/" + fileID
		if err := b.s3.PutObject(ctx, storageRef, bytes.NewReader(nil), 0); err != nil {
			logger.Error(ctx, "backend_create_put_object_failed", zap.String("path", path), zap.String("storage_ref", storageRef), zap.Error(err))
			return fmt.Errorf("put object: %w", err)
		}
	}

	err = b.store.InsertFile(ctx, &datastore.File{
		FileID: fileID, StorageType: storageType, StorageRef: storageRef,
		ContentBlob: contentBlob,
		SizeBytes:   0, Revision: 1, Status: datastore.StatusConfirmed,
		CreatedAt: now, ConfirmedAt: &now,
	})
	if err != nil {
		return err
	}
	err = b.store.EnsureParentDirs(ctx, path, b.genID)
	if err != nil {
		return err
	}
	err = b.store.InsertNode(ctx, &datastore.FileNode{
		NodeID: b.genID(), Path: path, ParentPath: pathutil.ParentPath(path),
		Name: pathutil.BaseName(path), FileID: fileID, CreatedAt: now,
	})
	if err == nil {
		b.syncCentralFileCreate(ctx, fileID, 0, "")
	}
	return err
}

func (b *Dat9Backend) Mkdir(path string, perm uint32) error {
	return b.MkdirCtx(backgroundWithTrace(), path, perm)
}

func (b *Dat9Backend) MkdirCtx(ctx context.Context, path string, perm uint32) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "mkdir", err, start) }()

	dirPath, err := pathutil.CanonicalizeDir(path)
	if err != nil {
		return err
	}
	err = b.store.EnsureParentDirs(ctx, dirPath, b.genID)
	if err != nil {
		return err
	}
	err = b.store.InsertNode(ctx, &datastore.FileNode{
		NodeID: b.genID(), Path: dirPath, ParentPath: pathutil.ParentPath(dirPath),
		Name: pathutil.BaseName(dirPath), IsDirectory: true, CreatedAt: time.Now(),
	})
	return err
}

func (b *Dat9Backend) Remove(path string) error {
	return b.RemoveCtx(backgroundWithTrace(), path)
}

func (b *Dat9Backend) RemoveCtx(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "remove", err, start) }()

	path = normalizePath(path)
	node, err := b.store.GetNode(ctx, path)
	if err != nil {
		return err
	}
	if node.IsDirectory {
		return b.store.DeleteEmptyDir(ctx, path)
	}
	deleted, err := b.store.DeleteFileWithRefCheck(ctx, path)
	if err != nil {
		return err
	}
	if deleted != nil {
		b.syncCentralFileDelete(ctx, deleted.FileID, deleted.SizeBytes, deleted.ContentType)
		b.deleteBlobCtx(ctx, deleted.StorageRef)
	}
	return nil
}

func (b *Dat9Backend) RemoveAll(path string) error {
	return b.RemoveAllCtx(backgroundWithTrace(), path)
}

func (b *Dat9Backend) RemoveAllCtx(ctx context.Context, path string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "remove_all", err, start) }()

	path = normalizePath(path)
	node, err := b.store.GetNode(ctx, path)
	if err != nil {
		return err
	}
	if !node.IsDirectory {
		return b.RemoveCtx(ctx, path)
	}
	orphaned, err := b.store.DeleteDirRecursive(ctx, path)
	if err != nil {
		return err
	}
	for _, f := range orphaned {
		b.syncCentralFileDelete(ctx, f.FileID, f.SizeBytes, f.ContentType)
		b.deleteBlobCtx(ctx, f.StorageRef)
	}
	return nil
}

func (b *Dat9Backend) Read(path string, offset int64, size int64) ([]byte, error) {
	return b.ReadCtx(backgroundWithTrace(), path, offset, size)
}

func (b *Dat9Backend) ReadCtx(ctx context.Context, path string, offset int64, size int64) (data []byte, err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "read", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return nil, err
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
	return b.WriteCtxIfRevision(ctx, path, data, offset, flags, -1)
}

// WriteCtxIfRevision applies the write only when expectedRevision matches the
// current file revision.
// expectedRevision semantics:
// - negative: unconditional write
// - zero: path must not already exist
// - positive: file must exist at exactly that revision
func (b *Dat9Backend) WriteCtxIfRevision(ctx context.Context, path string, data []byte, offset int64, flags filesystem.WriteFlag, expectedRevision int64) (n int64, err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "write", err, start) }()

	path, err = pathutil.Canonicalize(path)
	if err != nil {
		return 0, err
	}

	existing, err := b.store.Stat(ctx, path)
	if err == datastore.ErrNotFound {
		if expectedRevision > 0 {
			return 0, datastore.ErrRevisionConflict
		}
		if flags&filesystem.WriteFlagCreate == 0 {
			if expectedRevision == 0 {
				return 0, datastore.ErrRevisionConflict
			}
			return 0, datastore.ErrNotFound
		}
		n, err := b.createAndWriteCtx(ctx, path, data)
		if expectedRevision == 0 && errors.Is(err, datastore.ErrPathConflict) {
			return 0, datastore.ErrRevisionConflict
		}
		return n, err
	}
	if err != nil {
		return 0, err
	}
	if expectedRevision == 0 {
		return 0, datastore.ErrRevisionConflict
	}
	if existing.Node.IsDirectory {
		return 0, fmt.Errorf("is a directory: %s", path)
	}
	if flags&filesystem.WriteFlagExclusive != 0 {
		return 0, fmt.Errorf("file already exists: %s", path)
	}
	if expectedRevision > 0 && (existing.File == nil || existing.File.Revision != expectedRevision) {
		return 0, datastore.ErrRevisionConflict
	}
	return b.overwriteFileCtx(ctx, existing, data, offset, flags, expectedRevision)
}

func (b *Dat9Backend) createAndWriteCtx(ctx context.Context, path string, data []byte) (int64, error) {
	if err := b.ensureUploadSizeAllowed(int64(len(data))); err != nil {
		return 0, err
	}
	fileID := b.genID()
	now := time.Now()

	contentType := detectContentType(path, data)
	checksum := sha256sum(data)
	contentText := extractText(data, contentType)

	storageType := datastore.StorageDB9
	storageRef := "inline"
	var contentBlob []byte
	if b.shouldStoreInDB(int64(len(data))) {
		contentBlob = append([]byte(nil), data...)
	} else {
		if b.s3 == nil {
			return 0, fmt.Errorf("s3 client not configured")
		}
		storageType = datastore.StorageS3
		storageRef = "blobs/" + fileID
		if err := b.s3.PutObject(ctx, storageRef, bytes.NewReader(data), int64(len(data))); err != nil {
			logger.Error(ctx, "backend_create_and_write_put_object_failed", zap.String("path", path), zap.String("storage_ref", storageRef), zap.Int("bytes", len(data)), zap.Error(err))
			return 0, fmt.Errorf("put object: %w", err)
		}
	}

	if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.ensureStorageQuota(ctx, tx, path, int64(len(data))); err != nil {
			return err
		}
		if err := b.store.InsertFileTx(tx, &datastore.File{
			FileID: fileID, StorageType: storageType, StorageRef: storageRef,
			ContentBlob: contentBlob,
			ContentType: contentType, SizeBytes: int64(len(data)),
			ChecksumSHA256: checksum, Revision: 1, Status: datastore.StatusConfirmed,
			ContentText: contentText, CreatedAt: now, ConfirmedAt: &now,
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
		if b.UsesDatabaseAutoEmbedding() {
			return b.enqueueTiDBAutoSemanticTasksTx(tx, fileID, 1, path, contentType)
		}
		if b.shouldEnqueueEmbedForRevision(path, contentType, contentText) {
			return b.enqueueEmbedTaskTx(tx, fileID, 1)
		}
		return nil
	}); err != nil {
		if storageType == datastore.StorageS3 {
			b.deleteBlobCtx(ctx, storageRef)
		}
		return 0, err
	}
	// Temporary compatibility: app embedding still relies on the legacy
	// backend-owned image queue until its image task flow also moves to
	// semantic_tasks.
	if b.UsesDatabaseAutoEmbedding() {
		b.syncCentralFileCreate(ctx, fileID, int64(len(data)), contentType)
		return int64(len(data)), nil
	}
	b.syncCentralFileCreate(ctx, fileID, int64(len(data)), contentType)
	b.enqueueImageExtract(fileID, path, contentType, 1)
	return int64(len(data)), nil
}

func (b *Dat9Backend) overwriteFileCtx(ctx context.Context, nf *datastore.NodeWithFile, data []byte, offset int64, flags filesystem.WriteFlag, expectedRevision int64) (int64, error) {
	if nf.File == nil {
		return 0, fmt.Errorf("no file entity")
	}

	var finalData []byte
	if flags&filesystem.WriteFlagAppend != 0 {
		existing, err := b.readFileDataCtx(ctx, nf.File)
		if err != nil {
			return 0, fmt.Errorf("read existing data for append: %w", err)
		}
		finalData = append(existing, data...)
	} else if flags&filesystem.WriteFlagTruncate != 0 || offset <= 0 {
		finalData = data
	} else {
		existing, err := b.readFileDataCtx(ctx, nf.File)
		if err != nil {
			return 0, fmt.Errorf("read existing data for offset write: %w", err)
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
		return 0, err
	}
	contentType := detectContentType(nf.Node.Path, finalData)
	checksum := sha256sum(finalData)
	contentText := extractText(finalData, contentType)
	storageType := datastore.StorageDB9
	storageRef := "inline"
	var contentBlob []byte
	if b.shouldStoreInDB(int64(len(finalData))) {
		contentBlob = append([]byte(nil), finalData...)
	} else {
		if b.s3 == nil {
			return 0, fmt.Errorf("s3 client not configured")
		}
		storageType = datastore.StorageS3
		storageRef = "blobs/" + b.genID()
		if err := b.s3.PutObject(ctx, storageRef, bytes.NewReader(finalData), int64(len(finalData))); err != nil {
			logger.Error(ctx, "backend_overwrite_put_object_failed", zap.String("path", nf.Node.Path), zap.String("storage_ref", storageRef), zap.Int("bytes", len(finalData)), zap.Error(err))
			return 0, fmt.Errorf("put object: %w", err)
		}
	}

	var newRev int64
	err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := b.ensureStorageQuota(ctx, tx, nf.Node.Path, int64(len(finalData))); err != nil {
			return err
		}
		var txErr error
		if b.UsesDatabaseAutoEmbedding() {
			if expectedRevision > 0 {
				newRev, txErr = b.store.UpdateFileContentAutoEmbeddingIfRevisionTx(tx,
					nf.File.FileID, expectedRevision, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)),
				)
			} else {
				newRev, txErr = b.store.UpdateFileContentAutoEmbeddingTx(tx,
					nf.File.FileID, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)),
				)
			}
		} else {
			if expectedRevision > 0 {
				newRev, txErr = b.store.UpdateFileContentIfRevisionTx(tx,
					nf.File.FileID, expectedRevision, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)),
				)
			} else {
				newRev, txErr = b.store.UpdateFileContentTx(tx,
					nf.File.FileID, storageType, storageRef,
					contentType, checksum, contentText, contentBlob, int64(len(finalData)),
				)
			}
		}
		if txErr != nil {
			return txErr
		}
		if b.UsesDatabaseAutoEmbedding() {
			return b.enqueueTiDBAutoSemanticTasksTx(tx, nf.File.FileID, newRev, nf.Node.Path, contentType)
		}
		if b.shouldEnqueueEmbedForRevision(nf.Node.Path, contentType, contentText) {
			return b.enqueueEmbedTaskTx(tx, nf.File.FileID, newRev)
		}
		return nil
	})
	if err != nil {
		if storageType == datastore.StorageS3 {
			b.deleteBlobCtx(ctx, storageRef)
		}
		return 0, err
	}
	b.syncCentralFileOverwrite(ctx, nf.File.FileID, nf.File.SizeBytes, nf.File.ContentType, int64(len(finalData)), contentType)
	b.deleteBlobIfS3Ctx(ctx, nf.File.StorageType, nf.File.StorageRef, storageRef)
	// Temporary compatibility: app embedding still relies on the legacy
	// backend-owned image queue until its image task flow also moves to
	// semantic_tasks.
	if b.UsesDatabaseAutoEmbedding() {
		return int64(len(data)), nil
	}
	b.enqueueImageExtract(nf.File.FileID, nf.Node.Path, contentType, newRev)
	return int64(len(data)), nil
}

func (b *Dat9Backend) ReadDir(path string) ([]filesystem.FileInfo, error) {
	return b.ReadDirCtx(backgroundWithTrace(), path)
}

func (b *Dat9Backend) ReadDirCtx(ctx context.Context, path string) (infos []filesystem.FileInfo, err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "read_dir", err, start) }()

	dirPath, err := pathutil.CanonicalizeDir(path)
	if err != nil {
		return nil, err
	}
	entries, err := b.store.ListDir(ctx, dirPath)
	if err != nil {
		return nil, err
	}

	infos = make([]filesystem.FileInfo, 0, len(entries))
	for _, e := range entries {
		info := filesystem.FileInfo{
			Name: e.Node.Name, IsDir: e.Node.IsDirectory, Mode: 0o644,
		}
		if e.Node.IsDirectory {
			info.Mode = 0o755
		}
		if e.File != nil {
			info.Size = e.File.SizeBytes
			if e.File.ConfirmedAt != nil {
				info.ModTime = *e.File.ConfirmedAt
			} else {
				info.ModTime = e.File.CreatedAt
			}
		} else {
			info.ModTime = e.Node.CreatedAt
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func (b *Dat9Backend) Stat(path string) (*filesystem.FileInfo, error) {
	path = normalizePath(path)
	nf, err := b.store.Stat(backgroundWithTrace(), path)
	if err != nil {
		return nil, err
	}
	info := &filesystem.FileInfo{
		Name: nf.Node.Name, IsDir: nf.Node.IsDirectory, Mode: 0o644,
	}
	if nf.Node.IsDirectory {
		info.Mode = 0o755
	}
	if nf.File != nil {
		info.Size = nf.File.SizeBytes
		if nf.File.ConfirmedAt != nil {
			info.ModTime = *nf.File.ConfirmedAt
		} else {
			info.ModTime = nf.File.CreatedAt
		}
	} else {
		info.ModTime = nf.Node.CreatedAt
	}
	return info, nil
}

func (b *Dat9Backend) Rename(oldPath, newPath string) error {
	return b.RenameCtx(backgroundWithTrace(), oldPath, newPath)
}

func (b *Dat9Backend) RenameCtx(ctx context.Context, oldPath, newPath string) (err error) {
	start := time.Now()
	defer func() { observeBackend(ctx, "rename", err, start) }()

	oldPath = normalizePath(oldPath)
	newPath = normalizePath(newPath)

	node, err := b.store.GetNode(ctx, oldPath)
	if err != nil {
		return err
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
	err = b.store.UpdateNodePath(ctx, oldPath, newPath, pathutil.ParentPath(newPath), pathutil.BaseName(newPath))
	return err
}

func (b *Dat9Backend) Chmod(path string, mode uint32) error { return nil }

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
	defer func() { observeBackend(ctx, "copy_file", err, start) }()

	srcPath, err = pathutil.Canonicalize(srcPath)
	if err != nil {
		return err
	}
	dstPath, err = pathutil.Canonicalize(dstPath)
	if err != nil {
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
	return b.store.InsertNode(ctx, &datastore.FileNode{
		NodeID: b.genID(), Path: dstPath, ParentPath: pathutil.ParentPath(dstPath),
		Name: pathutil.BaseName(dstPath), FileID: srcNode.FileID, CreatedAt: time.Now(),
	})
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
	b.deleteBlobCtx(ctx, storageRef)
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

func (b *Dat9Backend) shouldStoreInDB(size int64) bool {
	return b.smallInDB && size < smallFileThreshold
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
	return true
}

func extractText(data []byte, contentType string) string {
	if !strings.HasPrefix(contentType, "text/") &&
		contentType != "application/json" &&
		contentType != "application/xml" &&
		contentType != "application/yaml" {
		return ""
	}
	if len(data) > smallFileThreshold {
		return ""
	}
	return string(data)
}

func (b *Dat9Backend) ExecSQL(ctx context.Context, query string) ([]map[string]interface{}, error) {
	start := time.Now()
	rows, err := b.store.ExecSQL(ctx, query)
	observeBackend(ctx, "exec_sql", err, start)
	if err != nil {
		logger.Error(ctx, "backend_exec_sql_failed", zap.Int("query_len", len(query)), zap.Error(err))
		return nil, err
	}
	return rows, nil
}

func (b *Dat9Backend) Grep(ctx context.Context, query, pathPrefix string, limit int) ([]datastore.SearchResult, error) {
	start := time.Now()
	var err error
	defer func() { observeBackend(ctx, "grep", err, start) }()

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

	var vecCh chan grepResp
	if b.UsesDatabaseAutoEmbedding() {
		vecCh = make(chan grepResp, 1)
		go func() {
			rows, searchErr := b.store.VectorSearchByText(ctx, query, pathPrefix, fetch)
			vecCh <- grepResp{rows: rows, err: searchErr}
		}()
	} else if b.queryEmbedder != nil {
		vecCh = make(chan grepResp, 1)
		go func() {
			queryVec, embedErr := b.queryEmbedder.EmbedText(ctx, query)
			if embedErr != nil || len(queryVec) == 0 {
				vecCh <- grepResp{err: embedErr}
				return
			}
			rows, searchErr := b.store.VectorSearch(ctx, queryVec, pathPrefix, fetch)
			vecCh <- grepResp{rows: rows, err: searchErr}
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

	var vecResp grepResp
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

	// Decision rule for grep:
	// 1. FTS and vector search are ranking signals; either one may fail independently.
	// 2. If either path returns ranked rows, serve those rows directly after RRF merge.
	// 3. Only fall back to LIKE-based keyword search when both ranking paths produce no rows.
	// This keeps semantic ranking as an enhancement while preserving a text-search safety net.
	hasRankedResults := len(ftsResp.rows) > 0 || len(vecResp.rows) > 0
	if !hasRankedResults {
		rows, searchErr := b.store.KeywordSearch(ctx, query, pathPrefix, limit)
		if searchErr != nil {
			logger.Error(ctx, "backend_grep_failed", zap.Int("query_len", len(query)), zap.String("path_prefix", pathPrefix), zap.Int("limit", limit), zap.Error(searchErr))
			err = searchErr
			return nil, err
		}
		return rows, nil
	}
	return datastore.RRFMerge(ftsResp.rows, vecResp.rows, limit), nil
}

func (b *Dat9Backend) Find(ctx context.Context, f *datastore.FindFilter) ([]datastore.SearchResult, error) {
	start := time.Now()
	rows, err := b.store.Find(ctx, f)
	observeBackend(ctx, "find", err, start)
	if err != nil {
		logger.Error(ctx, "backend_find_failed", zap.String("path", f.PathPrefix), zap.String("name", f.NameGlob), zap.Error(err))
		return nil, err
	}
	return rows, nil
}

func observeBackend(ctx context.Context, op string, err error, start time.Time) {
	result := "ok"
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			result = "not_found"
		} else {
			result = "error"
		}
	}
	metrics.RecordOperation("backend", op, result, time.Since(start))
}
