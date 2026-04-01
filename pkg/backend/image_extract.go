package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
)

// ImageExtractRequest is the input to a pluggable image->text extractor.
type ImageExtractRequest struct {
	FileID      string
	Path        string
	ContentType string
	Data        []byte
}

// ImageTextExtractor extracts searchable text from image bytes.
type ImageTextExtractor interface {
	ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, error)
}

type imageExtractTask struct {
	FileID      string
	Path        string
	ContentType string
	Revision    int64
}

func isImageContentType(contentType string) bool {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	return strings.HasPrefix(contentType, "image/")
}

var imageExtensions = map[string]string{
	".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
	".gif": "image/gif", ".bmp": "image/bmp", ".webp": "image/webp",
	".svg": "image/svg+xml", ".tiff": "image/tiff", ".tif": "image/tiff",
	".ico": "image/x-icon",
}

func contentTypeFromPath(path string) string {
	path = strings.ToLower(path)
	for ext, ct := range imageExtensions {
		if strings.HasSuffix(path, ext) {
			return ct
		}
	}
	return ""
}

func (b *Dat9Backend) enqueueImageExtract(fileID, path, contentType string, revision int64) {
	if !b.imageExtractEnabled || b.imageExtractor == nil {
		return
	}
	if !isImageContentType(contentType) {
		contentType = contentTypeFromPath(path)
		if !isImageContentType(contentType) {
			return
		}
	}
	task := imageExtractTask{
		FileID:      fileID,
		Path:        path,
		ContentType: contentType,
		Revision:    revision,
	}
	select {
	case b.imageExtractQueue <- task:
		metrics.RecordOperation("image_extract", "enqueue", "ok", 0)
		metrics.RecordGauge("image_extract", "queue_depth", float64(len(b.imageExtractQueue)))
	default:
		metrics.RecordOperation("image_extract", "enqueue", "queue_full", 0)
		metrics.RecordGauge("image_extract", "queue_depth", float64(len(b.imageExtractQueue)))
		logger.Warn(backgroundWithTrace(), "backend_image_extract_queue_full_drop",
			zap.String("file_id", fileID),
			zap.String("path", path),
			zap.Int("queue_size", cap(b.imageExtractQueue)))
	}
}

func (b *Dat9Backend) enqueueImageExtractForUpload(ctx context.Context, upload *datastore.Upload, isOverwrite bool) {
	if !b.imageExtractEnabled || b.imageExtractor == nil {
		return
	}
	fileID := upload.FileID
	if isOverwrite {
		nf, err := b.store.Stat(ctx, upload.TargetPath)
		if err != nil || nf.File == nil {
			logger.Warn(ctx, "backend_image_extract_upload_enqueue_stat_failed",
				zap.String("upload_id", upload.UploadID),
				zap.String("path", upload.TargetPath),
				zap.Error(err))
			return
		}
		fileID = nf.File.FileID
	}
	f, err := b.store.GetFile(ctx, fileID)
	if err != nil {
		logger.Warn(ctx, "backend_image_extract_upload_enqueue_get_file_failed",
			zap.String("upload_id", upload.UploadID),
			zap.String("file_id", fileID),
			zap.String("path", upload.TargetPath),
			zap.Error(err))
		return
	}
	ct := f.ContentType
	if ct == "" {
		ct = contentTypeFromPath(upload.TargetPath)
		if ct == "" {
			logger.Warn(ctx, "backend_image_extract_upload_enqueue_skipped_unknown_content_type",
				zap.String("upload_id", upload.UploadID),
				zap.String("file_id", fileID),
				zap.String("path", upload.TargetPath),
				zap.String("reason", "content_type_missing_and_extension_unknown"))
			return
		}
	}
	b.enqueueImageExtract(fileID, upload.TargetPath, ct, f.Revision)
}

func (b *Dat9Backend) runImageExtractWorker(ctx context.Context, workerID int) {
	defer b.imageExtractWG.Done()
	logger.Info(ctx, "backend_image_extract_worker_started", zap.Int("worker_id", workerID))
	for {
		select {
		case <-ctx.Done():
			logger.Info(ctx, "backend_image_extract_worker_stopped", zap.Int("worker_id", workerID), zap.String("reason", "context_canceled"))
			return
		case task := <-b.imageExtractQueue:
			metrics.RecordGauge("image_extract", "queue_depth", float64(len(b.imageExtractQueue)))
			b.processImageExtractTask(ctx, task)
		}
	}
}

func (b *Dat9Backend) processImageExtractTask(ctx context.Context, task imageExtractTask) {
	start := time.Now()
	f, err := b.store.GetFile(ctx, task.FileID)
	if err != nil {
		if !errors.Is(err, datastore.ErrNotFound) {
			logger.Warn(ctx, "backend_image_extract_get_file_failed",
				zap.String("file_id", task.FileID), zap.Error(err))
		}
		metrics.RecordOperation("image_extract", "process", "get_file_error", time.Since(start))
		return
	}
	if f.Status != datastore.StatusConfirmed {
		metrics.RecordOperation("image_extract", "process", "not_confirmed", time.Since(start))
		return
	}
	ct := f.ContentType
	if ct == "" {
		ct = task.ContentType
	}
	if !isImageContentType(ct) {
		metrics.RecordOperation("image_extract", "process", "not_image", time.Since(start))
		return
	}
	if task.Revision > 0 && f.Revision != task.Revision {
		metrics.RecordOperation("image_extract", "process", "stale_precheck", time.Since(start))
		return
	}

	data, err := b.loadImageBytesForExtract(ctx, f)
	if err != nil {
		logger.Warn(ctx, "backend_image_extract_load_bytes_failed",
			zap.String("file_id", task.FileID), zap.Error(err))
		metrics.RecordOperation("image_extract", "process", "load_error", time.Since(start))
		return
	}
	if len(data) == 0 {
		logger.Warn(ctx, "backend_image_extract_skipped_too_large_or_empty",
			zap.String("file_id", task.FileID),
			zap.String("path", task.Path),
			zap.Int64("file_size", f.SizeBytes),
			zap.Int64("max_bytes", b.imageExtractMaxSize))
		metrics.RecordOperation("image_extract", "process", "skip_too_large", time.Since(start))
		return
	}

	taskCtx, cancel := context.WithTimeout(ctx, b.imageExtractTimeout)
	text, err := b.imageExtractor.ExtractImageText(taskCtx, ImageExtractRequest{
		FileID:      task.FileID,
		Path:        task.Path,
		ContentType: ct,
		Data:        data,
	})
	cancel()
	if err != nil {
		logger.Warn(ctx, "backend_image_extract_failed",
			zap.String("file_id", task.FileID), zap.String("path", task.Path), zap.Error(err))
		metrics.RecordOperation("image_extract", "process", "extract_error", time.Since(start))
		return
	}
	text = sanitizeExtractedText(text, b.maxExtractTextBytes)
	if text == "" {
		logger.Warn(ctx, "backend_image_extract_empty_text",
			zap.String("file_id", task.FileID),
			zap.String("path", task.Path))
		metrics.RecordOperation("image_extract", "process", "empty_text", time.Since(start))
		return
	}

	var updated bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		var txErr error
		updated, txErr = b.store.UpdateFileSearchTextTx(tx, task.FileID, task.Revision, text)
		if txErr != nil {
			return txErr
		}
		if !updated || b.UsesDatabaseAutoEmbedding() {
			return nil
		}
		_, txErr = b.store.EnsureSemanticTaskQueuedTx(tx, newEmbedTask(b.genID(), task.FileID, task.Revision, time.Now().UTC()))
		return txErr
	})
	if err != nil {
		logger.Warn(ctx, "backend_image_extract_update_file_failed",
			zap.String("file_id", task.FileID), zap.Error(err))
		metrics.RecordOperation("image_extract", "process", "update_error", time.Since(start))
		return
	}
	if !updated {
		logger.Info(ctx, "backend_image_extract_skipped_stale",
			zap.String("file_id", task.FileID), zap.String("path", task.Path))
		metrics.RecordOperation("image_extract", "process", "stale", time.Since(start))
		return
	}
	logger.Info(ctx, "backend_image_extract_ok",
		zap.String("file_id", task.FileID), zap.String("path", task.Path), zap.Int("text_len", len(text)))
	metrics.RecordOperation("image_extract", "process", "ok", time.Since(start))
}

func (b *Dat9Backend) loadImageBytesForExtract(ctx context.Context, f *datastore.File) ([]byte, error) {
	if b.imageExtractMaxSize > 0 && f.SizeBytes > b.imageExtractMaxSize {
		return nil, nil
	}
	if f.StorageType == datastore.StorageDB9 {
		if b.imageExtractMaxSize > 0 && int64(len(f.ContentBlob)) > b.imageExtractMaxSize {
			return nil, nil
		}
		return append([]byte(nil), f.ContentBlob...), nil
	}
	if f.StorageType != datastore.StorageS3 {
		return nil, fmt.Errorf("unsupported storage type: %s", f.StorageType)
	}
	if b.s3 == nil {
		return nil, fmt.Errorf("s3 client not configured")
	}
	rc, err := b.s3.GetObject(ctx, f.StorageRef)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	reader := io.Reader(rc)
	if b.imageExtractMaxSize > 0 {
		reader = io.LimitReader(rc, b.imageExtractMaxSize+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if b.imageExtractMaxSize > 0 && int64(len(data)) > b.imageExtractMaxSize {
		return nil, nil
	}
	return data, nil
}

func sanitizeExtractedText(in string, maxBytes int) string {
	in = strings.TrimSpace(in)
	if in == "" {
		return ""
	}
	in = strings.Join(strings.Fields(in), " ")
	if maxBytes <= 0 || len(in) <= maxBytes {
		return in
	}
	var (
		total int
		out   strings.Builder
	)
	for _, r := range in {
		rb := utf8.RuneLen(r)
		if rb < 0 {
			continue
		}
		if total+rb > maxBytes {
			break
		}
		out.WriteRune(r)
		total += rb
	}
	return strings.TrimSpace(out.String())
}
