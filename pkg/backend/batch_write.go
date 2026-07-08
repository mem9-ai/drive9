package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// BatchWriteItem is one inline whole-file conditional write.
type BatchWriteItem struct {
	Path             string
	Data             []byte
	ExpectedRevision int64
	Mode             uint32
	HasMode          bool
}

// BatchWriteResult is the committed revision or per-item error for a batch item.
type BatchWriteResult struct {
	Path     string
	Revision int64
	Err      error
}

// ErrBatchWriteDirectory reports an item attempted to write over a directory.
var ErrBatchWriteDirectory = errors.New("batch write target is a directory")

type preparedBatchWriteItem struct {
	path             string
	data             []byte
	expectedRevision int64
	mode             uint32
	hasMode          bool
	fileID           string
	now              time.Time
	contentType      string
	checksum         string
	contentText      string
	contentBlob      []byte
}

type batchWriteMutation struct {
	resultIndex    int
	fileID         string
	oldSize        int64
	oldContentType string
	newSize        int64
	newContentType string
	create         bool
}

// BatchWriteCtx writes inline whole-file items in one tenant-DB transaction.
//
// The batch primitive is intentionally limited to DB-inline content. Larger
// files keep using the existing multipart path, and callers should fall back
// to single-file writes for unsupported items.
func (b *Dat9Backend) BatchWriteCtx(ctx context.Context, items []BatchWriteItem) ([]BatchWriteResult, error) {
	results := make([]BatchWriteResult, len(items))
	prepared := make([]*preparedBatchWriteItem, len(items))
	for i, item := range items {
		p, err := b.prepareBatchWriteItem(ctx, item)
		if p != nil {
			results[i].Path = p.path
		} else {
			results[i].Path = item.Path
		}
		if err != nil {
			results[i].Err = err
			continue
		}
		prepared[i] = p
	}

	var mutations []batchWriteMutation
	var semanticTaskEnqueued bool
	if err := b.store.InTx(ctx, func(tx *sql.Tx) error {
		for i, item := range prepared {
			if item == nil {
				continue
			}
			if err := b.batchWriteItemTx(ctx, tx, i, item, &results[i], &mutations, &semanticTaskEnqueued); err != nil {
				if isBatchWritePerItemError(err) {
					results[i].Err = err
					continue
				}
				return fmt.Errorf("batch write %s: %w", item.path, err)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if semanticTaskEnqueued {
		b.notifyWorkEnqueued(BackendWorkSemantic)
	}
	quotaCtx, cancelQuota := postCommitQuotaMutationContext()
	defer cancelQuota()
	for _, mutation := range mutations {
		if mutation.create {
			if err := b.recordCentralFileCreateMutation(quotaCtx, mutation.fileID, mutation.newSize, mutation.newContentType); err != nil {
				results[mutation.resultIndex].Err = postCommitQuotaMutationError("record central quota file create", err)
			}
			continue
		}
		if err := b.recordCentralFileOverwriteMutation(quotaCtx, mutation.fileID, mutation.oldSize, mutation.oldContentType, mutation.newSize, mutation.newContentType); err != nil {
			results[mutation.resultIndex].Err = postCommitQuotaMutationError("record central quota file overwrite", err)
		}
	}
	return results, nil
}

func (b *Dat9Backend) prepareBatchWriteItem(ctx context.Context, item BatchWriteItem) (*preparedBatchWriteItem, error) {
	path, err := pathutil.Canonicalize(item.Path)
	if err != nil {
		return nil, err
	}
	if err := rejectRootFileNodePath(path); err != nil {
		return nil, err
	}
	size := int64(len(item.Data))
	if err := b.ensureUploadSizeAllowed(size); err != nil {
		return nil, err
	}
	if err := b.ensureFileSizeQuota(ctx, size); err != nil {
		return nil, err
	}
	if !b.shouldStoreInDB(size) {
		return nil, fmt.Errorf("%w: batch write only supports inline files below %d bytes", ErrUploadTooLarge, b.inlineThreshold)
	}
	data := append([]byte(nil), item.Data...)
	contentType := detectContentType(path, data)
	return &preparedBatchWriteItem{
		path:             path,
		data:             data,
		expectedRevision: item.ExpectedRevision,
		mode:             item.Mode,
		hasMode:          item.HasMode,
		fileID:           b.genID(),
		now:              time.Now(),
		contentType:      contentType,
		checksum:         sha256sum(data),
		contentText:      extractText(data, contentType, b.textExtractMaxBytes),
		contentBlob:      data,
	}, nil
}

func (b *Dat9Backend) batchWriteItemTx(ctx context.Context, tx *sql.Tx, resultIndex int, item *preparedBatchWriteItem, result *BatchWriteResult, mutations *[]batchWriteMutation, semanticTaskEnqueued *bool) error {
	existing, err := b.store.StatTx(ctx, tx, item.path)
	if errors.Is(err, datastore.ErrNotFound) {
		return b.batchWriteCreateTx(ctx, tx, resultIndex, item, result, mutations, semanticTaskEnqueued)
	}
	if err != nil {
		return err
	}
	return b.batchWriteOverwriteTx(ctx, tx, resultIndex, item, existing, result, mutations, semanticTaskEnqueued)
}

func (b *Dat9Backend) batchWriteCreateTx(ctx context.Context, tx *sql.Tx, resultIndex int, item *preparedBatchWriteItem, result *BatchWriteResult, mutations *[]batchWriteMutation, semanticTaskEnqueued *bool) error {
	if item.expectedRevision > 0 {
		return datastore.ErrRevisionConflict
	}
	size := int64(len(item.data))
	if err := b.ensureCreateStorageQuota(ctx, tx, size); err != nil {
		return err
	}
	if err := b.ensureFileCountQuotaServer(ctx, tx, 1); err != nil {
		return err
	}
	revision := int64(1)
	file := &datastore.File{
		FileID:                item.fileID,
		StorageType:           datastore.StorageDB9,
		StorageRef:            "inline",
		ContentBlob:           item.contentBlob,
		ContentType:           item.contentType,
		SizeBytes:             size,
		ChecksumSHA256:        item.checksum,
		Revision:              revision,
		Status:                datastore.StatusConfirmed,
		StorageEncryptionMode: datastore.StorageEncryptionNone,
		ContentText:           item.contentText,
		CreatedAt:             item.now,
		ConfirmedAt:           &item.now,
	}
	if err := b.store.InsertFileTx(tx, file); err != nil {
		return err
	}
	if item.hasMode {
		if err := b.store.UpdateInodeModeTx(tx, item.fileID, item.mode&0o777); err != nil {
			return err
		}
	}
	if err := b.store.EnsureParentDirsTx(tx, item.path, b.genID); err != nil {
		return err
	}
	err := b.store.InsertNodeTx(tx, &datastore.FileNode{
		NodeID:     b.genID(),
		Path:       item.path,
		ParentPath: pathutil.ParentPath(item.path),
		Name:       pathutil.BaseName(item.path),
		FileID:     item.fileID,
		CreatedAt:  item.now,
	})
	if errors.Is(err, datastore.ErrPathConflict) {
		return datastore.ErrRevisionConflict
	}
	if err != nil {
		return err
	}
	if err := b.enqueueBatchWriteSemanticTasks(ctx, tx, item, item.fileID, revision, "", semanticTaskEnqueued); err != nil {
		return err
	}
	result.Revision = revision
	*mutations = append(*mutations, batchWriteMutation{
		resultIndex:    resultIndex,
		fileID:         item.fileID,
		newSize:        size,
		newContentType: item.contentType,
		create:         true,
	})
	return nil
}

func (b *Dat9Backend) batchWriteOverwriteTx(ctx context.Context, tx *sql.Tx, resultIndex int, item *preparedBatchWriteItem, existing *datastore.NodeWithFile, result *BatchWriteResult, mutations *[]batchWriteMutation, semanticTaskEnqueued *bool) error {
	if item.expectedRevision == 0 {
		return datastore.ErrRevisionConflict
	}
	if existing.Node.IsDirectory {
		return fmt.Errorf("%w: %s", ErrBatchWriteDirectory, item.path)
	}
	if existing.File == nil {
		return datastore.ErrNotFound
	}
	if item.expectedRevision > 0 && existing.File.Revision != item.expectedRevision {
		return datastore.ErrRevisionConflict
	}
	meta, err := b.store.GetFileStorageMetaForUpdateTx(tx, existing.File.FileID)
	if err != nil {
		return err
	}
	if item.expectedRevision > 0 && meta.Revision != item.expectedRevision {
		return datastore.ErrRevisionConflict
	}
	size := int64(len(item.data))
	if err := b.ensureStorageQuota(ctx, tx, item.path, size); err != nil {
		return err
	}
	var revision int64
	if b.UsesDatabaseAutoEmbedding() {
		if item.expectedRevision > 0 {
			revision, err = b.store.UpdateFileContentAutoEmbeddingIfRevisionTx(tx, existing.File.FileID, item.expectedRevision, datastore.StorageDB9, "inline", item.contentType, item.checksum, item.contentText, item.contentBlob, size, "")
		} else {
			revision, err = b.store.UpdateFileContentAutoEmbeddingTx(tx, existing.File.FileID, datastore.StorageDB9, "inline", item.contentType, item.checksum, item.contentText, item.contentBlob, size, "")
		}
	} else {
		if item.expectedRevision > 0 {
			revision, err = b.store.UpdateFileContentIfRevisionTx(tx, existing.File.FileID, item.expectedRevision, datastore.StorageDB9, "inline", item.contentType, item.checksum, item.contentText, item.contentBlob, size, "")
		} else {
			revision, err = b.store.UpdateFileContentTx(tx, existing.File.FileID, datastore.StorageDB9, "inline", item.contentType, item.checksum, item.contentText, item.contentBlob, size, "")
		}
	}
	if err != nil {
		return err
	}
	if err := b.store.UpdateFileStorageEncryptionTx(tx, existing.File.FileID, datastore.StorageEncryptionNone, ""); err != nil {
		return err
	}
	if item.hasMode {
		if err := b.store.UpdateInodeModeTx(tx, existing.File.FileID, item.mode&0o777); err != nil {
			return err
		}
	}
	if err := b.enqueueBatchWriteSemanticTasks(ctx, tx, item, existing.File.FileID, revision, meta.ContentType, semanticTaskEnqueued); err != nil {
		return err
	}
	result.Revision = revision
	*mutations = append(*mutations, batchWriteMutation{
		resultIndex:    resultIndex,
		fileID:         existing.File.FileID,
		oldSize:        meta.SizeBytes,
		oldContentType: meta.ContentType,
		newSize:        size,
		newContentType: item.contentType,
	})
	return nil
}

func (b *Dat9Backend) enqueueBatchWriteSemanticTasks(ctx context.Context, tx *sql.Tx, item *preparedBatchWriteItem, fileID string, revision int64, oldContentType string, semanticTaskEnqueued *bool) error {
	currentMediaDelta := quotaMediaDelta(isQuotaMediaContentType(oldContentType), isQuotaMediaContentType(item.contentType))
	if b.UsesDatabaseAutoEmbedding() {
		created, err := b.enqueueExtractSemanticTasksTx(ctx, tx, fileID, revision, item.path, item.contentType, currentMediaDelta)
		*semanticTaskEnqueued = *semanticTaskEnqueued || created
		return err
	}
	extractCreated, err := b.enqueueExtractSemanticTasksTx(ctx, tx, fileID, revision, item.path, item.contentType, currentMediaDelta)
	if err != nil {
		return err
	}
	if b.shouldEnqueueEmbedForRevision(item.path, item.contentType, item.contentText, "") {
		embedCreated, err := b.enqueueEmbedTaskTx(tx, fileID, revision)
		*semanticTaskEnqueued = *semanticTaskEnqueued || extractCreated || embedCreated
		return err
	}
	*semanticTaskEnqueued = *semanticTaskEnqueued || extractCreated
	return nil
}

func isBatchWritePerItemError(err error) bool {
	return errors.Is(err, datastore.ErrNotFound) ||
		errors.Is(err, datastore.ErrRevisionConflict) ||
		errors.Is(err, datastore.ErrPathConflict) ||
		errors.Is(err, ErrUploadTooLarge) ||
		errors.Is(err, ErrFileSizeQuotaExceeded) ||
		errors.Is(err, ErrFileCountQuotaExceeded) ||
		errors.Is(err, ErrStorageQuotaExceeded) ||
		errors.Is(err, ErrMediaLLMQuotaExceeded) ||
		errors.Is(err, datastore.ErrInvalidRootDentry) ||
		errors.Is(err, ErrBatchWriteDirectory)
}
