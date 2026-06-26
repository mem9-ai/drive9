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

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

// ImageExtractRequest is the input to a pluggable image->text extractor.
type ImageExtractRequest struct {
	TenantID    string
	FileID      string
	Path        string
	ContentType string
	Data        []byte
}

// ImageTextExtractor extracts searchable text from image bytes.
type ImageTextExtractor interface {
	ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, ImageExtractUsage, error)
}

// ImageExtractTaskSpec carries the revision-scoped inputs needed to extract
// image text for one file version.
type ImageExtractTaskSpec struct {
	FileID      string
	Path        string
	ContentType string
	Revision    int64
}

// ImageExtractResult reports the outcome of one image extraction attempt.
type ImageExtractResult string

const (
	ImageExtractResultRuntimeNotConfigured ImageExtractResult = "runtime_not_configured"
	ImageExtractResultGetFileError         ImageExtractResult = "get_file_error"
	ImageExtractResultFileNotFound         ImageExtractResult = "file_not_found"
	ImageExtractResultNotConfirmed         ImageExtractResult = "not_confirmed"
	ImageExtractResultNotImage             ImageExtractResult = "not_image"
	ImageExtractResultStale                ImageExtractResult = "stale"
	ImageExtractResultLoadError            ImageExtractResult = "load_error"
	ImageExtractResultTooLarge             ImageExtractResult = "too_large"
	ImageExtractResultExtractError         ImageExtractResult = "extract_error"
	ImageExtractResultEmptyText            ImageExtractResult = "empty_text"
	ImageExtractResultUpdateError          ImageExtractResult = "update_error"
	ImageExtractResultWritten              ImageExtractResult = "written"
	ImageExtractResultBudgetExhausted      ImageExtractResult = "budget_exhausted"
)

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

// SupportsAsyncImageExtract reports whether this backend instance has the
// runtime dependencies needed to extract image text.
func (b *Dat9Backend) SupportsAsyncImageExtract() bool {
	return b.imageExtractEnabled && b.imageExtractor != nil
}

// ProcessImageExtractTask runs the backend-owned image extraction logic for one
// revision-scoped task. Normal terminal business outcomes return a nil error
// and a non-empty result label. Runtime misconfiguration, misrouting, and
// transient failures return a retryable error.
func (b *Dat9Backend) ProcessImageExtractTask(ctx context.Context, task ImageExtractTaskSpec) (ImageExtractResult, error) {
	if !b.SupportsAsyncImageExtract() {
		return ImageExtractResultRuntimeNotConfigured, fmt.Errorf("async image extract runtime not configured")
	}
	if b.monthlyLLMCostExceededCheck(ctx) {
		metrics.RecordTenantOperation(b.tenantID, "llm_cost_budget", "process_skip", "budget_exhausted", 0)
		return ImageExtractResultBudgetExhausted, nil
	}

	f, err := b.store.GetFile(ctx, task.FileID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return ImageExtractResultFileNotFound, nil
		}
		return ImageExtractResultGetFileError, fmt.Errorf("get file: %w", err)
	}
	if f.Status != datastore.StatusConfirmed {
		return ImageExtractResultNotConfirmed, nil
	}
	ct := f.ContentType
	if ct == "" {
		ct = task.ContentType
	}
	if ct == "" {
		ct = contentTypeFromPath(task.Path)
	}
	if !isImageContentType(ct) {
		return ImageExtractResultNotImage, nil
	}
	if task.Revision > 0 && f.Revision != task.Revision {
		return ImageExtractResultStale, nil
	}

	data, err := b.loadImageBytesForExtract(ctx, f)
	if err != nil {
		return ImageExtractResultLoadError, fmt.Errorf("load image bytes: %w", err)
	}
	if len(data) == 0 {
		return ImageExtractResultTooLarge, nil
	}

	taskCtx, cancel := context.WithTimeout(ctx, b.imageExtractTimeout)
	text, imageUsage, err := b.imageExtractor.ExtractImageText(taskCtx, ImageExtractRequest{
		TenantID:    b.tenantID,
		FileID:      task.FileID,
		Path:        task.Path,
		ContentType: ct,
		Data:        data,
	})
	cancel()
	if err != nil {
		return ImageExtractResultExtractError, fmt.Errorf("extract image text: %w", err)
	}
	b.recordImageExtractUsage(task.FileID, imageUsage)
	writeback := prepareImageExtractWriteback(text, b.maxExtractTextBytes)
	if writeback.text == "" {
		return ImageExtractResultEmptyText, nil
	}

	var updated bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := injectedImageExtractWritebackError("imageExtractWritebackUpdateFileSearchTextError"); err != nil {
			return err
		}
		var txErr error
		updated, txErr = b.store.UpdateFileSearchTextTx(tx, task.FileID, task.Revision, writeback.text)
		if txErr != nil {
			return txErr
		}
		if !updated {
			return nil
		}
		if txErr = b.store.ReplaceFileTagsByPrefixTx(tx, task.FileID, imageExtractTagPrefix, writeback.tags); txErr != nil {
			return txErr
		}
		if b.UsesDatabaseAutoEmbedding() || !b.appSemanticTasksEnabled {
			return nil
		}
		if err := injectedImageExtractWritebackError("imageExtractWritebackQueueEmbedTaskError"); err != nil {
			return err
		}
		_, txErr = b.store.EnsureSemanticTaskQueuedTx(tx, newEmbedTask(b.genID(), task.FileID, task.Revision, time.Now().UTC()))
		return txErr
	})
	if err != nil {
		return ImageExtractResultUpdateError, fmt.Errorf("update file search text: %w", err)
	}
	if !updated {
		return ImageExtractResultStale, nil
	}
	return ImageExtractResultWritten, nil
}

func injectedImageExtractWritebackError(name string) error {
	var injected error
	failpoint.Inject(name, func(val failpoint.Value) {
		switch v := val.(type) {
		case string:
			injected = errors.New(v)
		case bool:
			if v {
				injected = fmt.Errorf("injected failpoint %s", name)
			}
		}
	})
	return injected
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
	return truncateUTF8Bytes(in, maxBytes)
}

func truncateUTF8Bytes(in string, maxBytes int) string {
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
