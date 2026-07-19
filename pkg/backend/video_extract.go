package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/metrics"
)

// VideoExtractRequest is the input to a pluggable video->text extractor.
type VideoExtractRequest struct {
	TenantID    string
	FileID      string
	Path        string
	ContentType string
	Data        []byte
}

// VideoExtractUsage reports the resource consumption of one video visual extraction call.
type VideoExtractUsage struct {
	PromptTokens     int
	CompletionTokens int
	FramesExtracted  int
}

// TotalTokens returns the sum of prompt and completion tokens.
func (u VideoExtractUsage) TotalTokens() int { return u.PromptTokens + u.CompletionTokens }

// VideoTextExtractor extracts searchable text from video bytes by sampling
// frames and describing them via a Vision model.
type VideoTextExtractor interface {
	ExtractVideoText(ctx context.Context, req VideoExtractRequest) (string, VideoExtractUsage, error)
}

// VideoExtractTaskSpec carries the revision-scoped inputs needed to extract
// video visual text for one file version.
type VideoExtractTaskSpec struct {
	FileID      string
	Path        string
	ContentType string
	Revision    int64
}

// VideoExtractResult reports the outcome of one video extraction attempt.
type VideoExtractResult string

const (
	VideoExtractResultRuntimeNotConfigured VideoExtractResult = "runtime_not_configured"
	VideoExtractResultGetFileError         VideoExtractResult = "get_file_error"
	VideoExtractResultFileNotFound         VideoExtractResult = "file_not_found"
	VideoExtractResultNotConfirmed         VideoExtractResult = "not_confirmed"
	VideoExtractResultNotVideo             VideoExtractResult = "not_video"
	VideoExtractResultStale                VideoExtractResult = "stale"
	VideoExtractResultLoadError            VideoExtractResult = "load_error"
	VideoExtractResultTooLarge             VideoExtractResult = "too_large"
	VideoExtractResultExtractError         VideoExtractResult = "extract_error"
	VideoExtractResultEmptyText            VideoExtractResult = "empty_text"
	VideoExtractResultUpdateError          VideoExtractResult = "update_error"
	VideoExtractResultWritten              VideoExtractResult = "written"
	VideoExtractResultBudgetExhausted      VideoExtractResult = "budget_exhausted"
)

// allowedVideoMIME is the closed set of MIME types for video visual extraction.
var allowedVideoMIME = map[string]struct{}{
	"video/mp4":       {},
	"video/quicktime": {},
	"video/x-msvideo": {},
	"video/webm":      {},
	"video/x-matroska": {},
}

// videoExtensionMIME maps file extensions to canonical video MIME types.
var videoExtensionMIME = map[string]string{
	".mp4":  "video/mp4",
	".mov":  "video/quicktime",
	".avi":  "video/x-msvideo",
	".webm": "video/webm",
	".mkv":  "video/x-matroska",
}

func isAllowedVideoMIME(ct string) bool {
	_, ok := allowedVideoMIME[stripMIMEParams(ct)]
	return ok
}

func effectiveVideoMIME(path, contentType string) string {
	ct := stripMIMEParams(contentType)
	if isAllowedVideoMIME(ct) {
		return ct
	}
	// When content_type is absent or generic, try path extension.
	switch ct {
	case "", "application/octet-stream", "text/plain":
		path = strings.ToLower(path)
		for ext, mime := range videoExtensionMIME {
			if strings.HasSuffix(path, ext) {
				return mime
			}
		}
	}
	return ""
}

func isVideoContentType(contentType, path string) bool {
	return effectiveVideoMIME(path, contentType) != ""
}

func isSupportedVideoForSemanticTask(path, contentType string) bool {
	return isVideoContentType(contentType, path)
}

// SupportsAsyncVideoExtract reports whether this backend instance has the
// runtime dependencies needed to extract video visual text.
func (b *Dat9Backend) SupportsAsyncVideoExtract() bool {
	return b != nil && b.videoExtractEnabled && b.videoExtractor != nil
}

// errVideoExtractSourceTooLarge signals the video exceeds MaxVideoBytes.
var errVideoExtractSourceTooLarge = errors.New("video extract source exceeds configured max bytes")

// ProcessVideoExtractTask runs visual extraction for one durable
// video_extract_visual task. Terminal business outcomes return a nil error;
// runtime misconfiguration and transient failures return a retryable error.
func (b *Dat9Backend) ProcessVideoExtractTask(ctx context.Context, task VideoExtractTaskSpec) (VideoExtractResult, error) {
	start := time.Now()
	var result VideoExtractResult
	defer func() {
		metrics.RecordTenantOperation(b.tenantID, "video_extract", "process", string(result), time.Since(start))
	}()

	if !b.SupportsAsyncVideoExtract() {
		result = VideoExtractResultRuntimeNotConfigured
		return result, fmt.Errorf("async video extract runtime not configured")
	}
	if b.monthlyLLMCostExceededCheck(ctx) {
		metrics.RecordTenantOperation(b.tenantID, "llm_cost_budget", "process_skip", "budget_exhausted", 0)
		result = VideoExtractResultBudgetExhausted
		return result, nil
	}

	f, err := b.store.GetFile(ctx, task.FileID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			result = VideoExtractResultFileNotFound
			return result, nil
		}
		result = VideoExtractResultGetFileError
		return result, fmt.Errorf("get file: %w", err)
	}
	if f.Status != datastore.StatusConfirmed {
		result = VideoExtractResultNotConfirmed
		return result, nil
	}
	if task.Revision > 0 && f.Revision != task.Revision {
		result = VideoExtractResultStale
		return result, nil
	}

	ct := f.ContentType
	if ct == "" {
		ct = task.ContentType
	}
	resolvedMIME := effectiveVideoMIME(task.Path, ct)
	if resolvedMIME == "" {
		result = VideoExtractResultNotVideo
		return result, nil
	}

	data, err := b.loadVideoBytesForExtract(ctx, f)
	if err != nil {
		if errors.Is(err, errVideoExtractSourceTooLarge) {
			result = VideoExtractResultTooLarge
			return result, nil
		}
		result = VideoExtractResultLoadError
		return result, fmt.Errorf("load video bytes: %w", err)
	}

	taskCtx, cancel := context.WithTimeout(ctx, b.videoExtractTimeout)
	text, videoUsage, err := b.videoExtractor.ExtractVideoText(taskCtx, VideoExtractRequest{
		TenantID:    b.tenantID,
		FileID:      task.FileID,
		Path:        task.Path,
		ContentType: resolvedMIME,
		Data:        data,
	})
	cancel()
	if err != nil {
		result = VideoExtractResultExtractError
		return result, fmt.Errorf("extract video text: %w", err)
	}
	b.recordVideoExtractUsage(task.FileID, videoUsage)
	text = sanitizeExtractedText(text, b.maxVideoExtractTextBytes)
	if text == "" {
		result = VideoExtractResultEmptyText
		return result, nil
	}

	expectedRevision := task.Revision
	if expectedRevision == 0 {
		expectedRevision = f.Revision
	}

	var updated bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := injectedVideoExtractWritebackError("videoExtractWritebackUpdateFileSearchTextError"); err != nil {
			return err
		}
		var txErr error
		updated, txErr = b.store.UpdateFileSearchTextTx(tx, task.FileID, expectedRevision, text)
		if txErr != nil {
			return txErr
		}
		if !updated {
			return nil
		}
		if b.UsesDatabaseAutoEmbedding() || !b.appSemanticTasksEnabled {
			return nil
		}
		_, txErr = b.store.EnsureSemanticTaskQueuedTx(tx, newEmbedTask(b.genID(), task.FileID, expectedRevision, time.Now().UTC()))
		return txErr
	})
	if err != nil {
		result = VideoExtractResultUpdateError
		return result, fmt.Errorf("update file search text: %w", err)
	}
	if !updated {
		result = VideoExtractResultStale
		return result, nil
	}
	result = VideoExtractResultWritten
	return result, nil
}

func injectedVideoExtractWritebackError(name string) error {
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

func (b *Dat9Backend) loadVideoBytesForExtract(ctx context.Context, f *datastore.File) ([]byte, error) {
	if b.videoExtractMaxSize > 0 && f.SizeBytes > b.videoExtractMaxSize {
		return nil, errVideoExtractSourceTooLarge
	}
	if f.StorageType == datastore.StorageDB9 {
		if b.videoExtractMaxSize > 0 && int64(len(f.ContentBlob)) > b.videoExtractMaxSize {
			return nil, errVideoExtractSourceTooLarge
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
	if b.videoExtractMaxSize > 0 {
		reader = io.LimitReader(rc, b.videoExtractMaxSize+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if b.videoExtractMaxSize > 0 && int64(len(data)) > b.videoExtractMaxSize {
		return nil, errVideoExtractSourceTooLarge
	}
	return data, nil
}
