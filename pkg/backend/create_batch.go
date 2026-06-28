package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

const (
	defaultCreateBatchLinger = time.Millisecond
	maxCreateBatchEntries    = 128
)

type createBatcher struct {
	backend *Dat9Backend
	max     int
	linger  time.Duration
	jobs    chan *createBatchJob
	cancel  context.CancelFunc
	done    chan struct{}
}

type createBatchJob struct {
	ctx    context.Context
	item   *preparedCreateWrite
	result chan createBatchResult
}

type createBatchResult struct {
	written int64
	err     error
}

type preparedCreateWrite struct {
	path                 string
	fileID               string
	tags                 map[string]string
	description          string
	contentType          string
	checksum             string
	contentText          string
	storageRef           string
	contentBlob          []byte
	sizeBytes            int64
	now                  time.Time
	mediaDelta           int64
	semanticTaskEnqueued bool
	quotaOutboxEnqueued  bool
}

func newCreateBatcher(b *Dat9Backend, opts CreateBatchOptions) *createBatcher {
	maxEntries := opts.MaxEntries
	if maxEntries > maxCreateBatchEntries {
		maxEntries = maxCreateBatchEntries
	}
	linger := opts.Linger
	if linger <= 0 {
		linger = defaultCreateBatchLinger
	}
	ctx, cancel := context.WithCancel(backgroundWithTrace())
	c := &createBatcher{
		backend: b,
		max:     maxEntries,
		linger:  linger,
		jobs:    make(chan *createBatchJob, maxEntries*16),
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	go c.run(ctx)
	logger.Info(backgroundWithTrace(), "backend_create_batcher_started",
		zap.String("tenant_id", b.tenantID),
		zap.Int("max_entries", maxEntries),
		zap.Duration("linger", linger))
	return c
}

func (c *createBatcher) stop() {
	c.cancel()
	<-c.done
}

func (c *createBatcher) create(ctx context.Context, path string, data []byte, tags map[string]string, description string) (int64, error) {
	item, err := c.backend.prepareCreateWrite(ctx, path, data, tags, description)
	if err != nil {
		return 0, err
	}
	job := &createBatchJob{
		ctx:    ctx,
		item:   item,
		result: make(chan createBatchResult, 1),
	}
	select {
	case c.jobs <- job:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case res := <-job.result:
		return res.written, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (c *createBatcher) run(ctx context.Context) {
	defer close(c.done)
	var batch []*createBatchJob
	var timer *time.Timer
	var timerC <-chan time.Time
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerC = nil
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		stopTimer()
		c.flush(batch)
		batch = nil
	}
	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case job := <-c.jobs:
			batch = append(batch, job)
			if len(batch) == 1 {
				timer = time.NewTimer(c.linger)
				timerC = timer.C
			}
			if len(batch) >= c.max {
				flush()
			}
		case <-timerC:
			timer = nil
			timerC = nil
			flush()
		}
	}
}

func (c *createBatcher) flush(batch []*createBatchJob) {
	b := c.backend
	start := time.Now()
	active := make([]*createBatchJob, 0, len(batch))
	for _, job := range batch {
		metrics.RecordTenantOperation(b.tenantID, "create_batch", "wait", "ok", time.Since(job.item.now))
		if err := job.ctx.Err(); err != nil {
			job.result <- createBatchResult{err: err}
			continue
		}
		active = append(active, job)
	}
	metrics.RecordTenantGauge(b.tenantID, "create_batch", "batch_size", float64(len(active)))
	if len(active) == 0 {
		return
	}

	type jobOutcome struct {
		ok  bool
		err error
	}
	outcomes := make([]jobOutcome, len(active))
	txErr := b.store.InTx(backgroundWithTrace(), func(tx *sql.Tx) error {
		pendingStorageDelta, pendingFileDelta, _, pendingOK := b.cachedPendingQuotaOutboxDeltasTx(backgroundWithTrace(), tx)
		if !pendingOK {
			metrics.RecordTenantOperation(b.tenantID, "server_quota", "batch_pending_delta", "fail_open", 0)
		}
		var batchStorageDelta int64
		var batchFileDelta int64
		var batchMediaDelta int64
		for i, job := range active {
			item := job.item
			if err := job.ctx.Err(); err != nil {
				outcomes[i].err = err
				continue
			}
			if err := b.ensureCreateBatchAdmission(backgroundWithTrace(), pendingStorageDelta, pendingFileDelta, item, batchStorageDelta, batchFileDelta); err != nil {
				outcomes[i].err = err
				continue
			}
			err := withCreateBatchSavepoint(tx, i, func() error {
				semanticTaskEnqueued, quotaOutboxEnqueued, err := b.insertPreparedCreateTx(backgroundWithTrace(), tx, item, batchMediaDelta+item.mediaDelta)
				if err != nil {
					return err
				}
				item.semanticTaskEnqueued = semanticTaskEnqueued
				item.quotaOutboxEnqueued = quotaOutboxEnqueued
				return nil
			})
			if err != nil {
				outcomes[i].err = err
				continue
			}
			outcomes[i].ok = true
			batchStorageDelta += item.sizeBytes
			batchFileDelta++
			batchMediaDelta += item.mediaDelta
		}
		return nil
	})

	result := "ok"
	if txErr != nil {
		result = metrics.ResultForError(txErr)
	}
	metrics.RecordTenantOperation(b.tenantID, "create_batch", "flush", result, time.Since(start))

	for i, job := range active {
		outcome := outcomes[i]
		if txErr != nil && outcome.ok {
			outcome.ok = false
			outcome.err = txErr
		}
		if !outcome.ok {
			job.result <- createBatchResult{err: outcome.err}
			continue
		}
		item := job.item
		b.notifySemanticTaskEnqueued(item.semanticTaskEnqueued)
		if item.quotaOutboxEnqueued {
			b.addLocalQuotaPendingDeltas(item.sizeBytes, 1, item.mediaDelta)
			b.notifyQuotaOutbox(true)
		}
		job.result <- createBatchResult{written: item.sizeBytes}
	}
}

func withCreateBatchSavepoint(tx *sql.Tx, idx int, fn func() error) error {
	name := fmt.Sprintf("drive9_create_batch_%d", idx)
	if _, err := tx.Exec("SAVEPOINT " + name); err != nil {
		return err
	}
	err := fn()
	if err != nil {
		_, rollbackErr := tx.Exec("ROLLBACK TO SAVEPOINT " + name)
		_, releaseErr := tx.Exec("RELEASE SAVEPOINT " + name)
		if rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		if releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		return err
	}
	_, err = tx.Exec("RELEASE SAVEPOINT " + name)
	return err
}

func (b *Dat9Backend) tryCreateAndWriteBatchedCtx(ctx context.Context, path string, data []byte, tags map[string]string, description string) (int64, error, bool) {
	if b.createBatcher == nil || !b.UseServerQuota() || !b.shouldStoreInDB(int64(len(data))) {
		return 0, nil, false
	}
	if isQuotaMediaContentType(detectContentType(path, data)) {
		return 0, nil, false
	}
	written, err := b.createBatcher.create(ctx, path, data, tags, description)
	return written, err, true
}

func (b *Dat9Backend) prepareCreateWrite(ctx context.Context, path string, data []byte, tags map[string]string, description string) (*preparedCreateWrite, error) {
	if err := b.ensureUploadSizeAllowed(int64(len(data))); err != nil {
		return nil, err
	}
	if err := b.ensureFileSizeQuota(ctx, int64(len(data))); err != nil {
		return nil, err
	}
	contentType := detectContentType(path, data)
	contentBlob := append([]byte(nil), data...)
	mediaDelta := int64(0)
	if isQuotaMediaContentType(contentType) {
		mediaDelta = 1
	}
	return &preparedCreateWrite{
		path:        path,
		fileID:      b.genID(),
		tags:        cloneFileTags(tags),
		description: description,
		contentType: contentType,
		checksum:    sha256sum(data),
		contentText: extractText(data, contentType, b.textExtractMaxBytes),
		storageRef:  "inline",
		contentBlob: contentBlob,
		sizeBytes:   int64(len(data)),
		now:         time.Now(),
		mediaDelta:  mediaDelta,
	}, nil
}

func (b *Dat9Backend) ensureCreateBatchAdmission(ctx context.Context, pendingStorageDelta, pendingFileDelta int64, item *preparedCreateWrite, batchStorageDelta, batchFileDelta int64) error {
	if result, ok := b.checkCreateBatchStorageQuota(ctx, pendingStorageDelta, batchStorageDelta+item.sizeBytes); ok && result.exceeded() {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "storage_check", "exceeded", 0)
		return result.quotaExceededError()
	} else if ok {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "storage_check", "ok", 0)
	}
	return b.ensureCreateBatchFileCountAdmission(ctx, pendingFileDelta, batchFileDelta+1)
}

func (b *Dat9Backend) checkCreateBatchStorageQuota(ctx context.Context, pendingStorageDelta, currentStorageDelta int64) (storageQuotaCheckResult, bool) {
	result := storageQuotaCheckResult{deltaBytes: currentStorageDelta}
	if b.metaStore == nil || currentStorageDelta <= 0 {
		return result, false
	}
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "storage_check", "fail_open", 0)
		return result, false
	}
	if cfg.MaxStorageBytes <= 0 {
		return result, false
	}
	usage := b.cachedQuotaUsage(ctx)
	if usage == nil {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "storage_check", "fail_open", 0)
		return result, false
	}
	recordTenantQuotaSnapshot(b.tenantID, usage, cfg)
	result.checked = true
	result.limitBytes = cfg.MaxStorageBytes
	result.storageBytes = usage.StorageBytes
	result.reservedBytes = usage.ReservedBytes
	result.pendingBytes = pendingStorageDelta
	result.projected = usage.StorageBytes + usage.ReservedBytes + pendingStorageDelta + currentStorageDelta
	return result, true
}

func (b *Dat9Backend) ensureCreateBatchFileCountAdmission(ctx context.Context, pendingFileDelta, currentFileDelta int64) error {
	if !b.UseServerQuota() || currentFileDelta <= 0 {
		return nil
	}
	cfg := b.cachedQuotaConfig(ctx)
	if cfg == nil {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "file_count_check", "fail_open", 0)
		return nil
	}
	if cfg.MaxFileCount <= 0 {
		return nil
	}
	usage := b.cachedQuotaUsage(ctx)
	if usage == nil {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "file_count_check", "fail_open", 0)
		return nil
	}
	recordTenantQuotaSnapshot(b.tenantID, usage, cfg)
	projected := usage.FileCount + pendingFileDelta + currentFileDelta
	if projected > cfg.MaxFileCount {
		metrics.RecordTenantOperation(b.tenantID, "server_quota", "file_count_check", "exceeded", 0)
		return fmt.Errorf("%w: server limit=%d used=%d pending=%d delta=%d",
			ErrFileCountQuotaExceeded, cfg.MaxFileCount, usage.FileCount, pendingFileDelta, currentFileDelta)
	}
	metrics.RecordTenantOperation(b.tenantID, "server_quota", "file_count_check", "ok", 0)
	return nil
}

func (b *Dat9Backend) insertPreparedCreateTx(ctx context.Context, tx *sql.Tx, item *preparedCreateWrite, currentMediaDelta int64) (semanticTaskEnqueued bool, quotaOutboxEnqueued bool, err error) {
	fileRev := int64(1)
	insertFile := &datastore.File{
		FileID:                item.fileID,
		StorageType:           datastore.StorageDB9,
		StorageRef:            item.storageRef,
		StorageEncryptionMode: datastore.StorageEncryptionNone,
		ContentBlob:           item.contentBlob,
		ContentType:           item.contentType,
		SizeBytes:             item.sizeBytes,
		ChecksumSHA256:        item.checksum,
		Revision:              fileRev,
		Status:                datastore.StatusConfirmed,
		ContentText:           item.contentText,
		Description:           item.description,
		CreatedAt:             item.now,
		ConfirmedAt:           &item.now,
	}
	if b.UsesDatabaseAutoEmbedding() && item.description != "" {
		insertFile.DescriptionEmbeddingRevision = &fileRev
	}
	if err := b.store.InsertFileTx(tx, insertFile); err != nil {
		return false, false, err
	}
	if err := b.store.EnsureParentDirsTx(tx, item.path, b.genID); err != nil {
		return false, false, err
	}
	if err := b.store.InsertNodeTx(tx, &datastore.FileNode{
		NodeID:     b.genID(),
		Path:       item.path,
		ParentPath: pathutil.ParentPath(item.path),
		Name:       pathutil.BaseName(item.path),
		FileID:     item.fileID,
		CreatedAt:  item.now,
	}); err != nil {
		return false, false, err
	}
	if item.tags != nil {
		if err := b.store.ReplaceFileTagsTx(tx, item.fileID, item.tags); err != nil {
			return false, false, err
		}
	}
	if b.UsesDatabaseAutoEmbedding() {
		semanticTaskEnqueued, err = b.enqueueExtractSemanticTasksTx(ctx, tx, item.fileID, 1, item.path, item.contentType, currentMediaDelta)
		if err != nil {
			return false, false, err
		}
	} else {
		extractCreated, extractErr := b.enqueueExtractSemanticTasksTx(ctx, tx, item.fileID, 1, item.path, item.contentType, currentMediaDelta)
		err = extractErr
		if err == nil && b.shouldEnqueueEmbedForRevision(item.path, item.contentType, item.contentText, item.description) {
			var embedCreated bool
			embedCreated, err = b.enqueueEmbedTaskTx(tx, item.fileID, 1)
			semanticTaskEnqueued = embedCreated || extractCreated
		} else {
			semanticTaskEnqueued = extractCreated
		}
		if err != nil {
			return false, false, err
		}
	}
	quotaOutboxEnqueued, err = b.enqueueQuotaFileCreateOutboxTx(tx, item.fileID, item.sizeBytes, item.contentType)
	if err != nil {
		return false, false, err
	}
	return semanticTaskEnqueued, quotaOutboxEnqueued, nil
}
