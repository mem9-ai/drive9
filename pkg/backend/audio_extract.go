package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/dat9/pkg/datastore"
)

// AudioExtractRequest is the input to a pluggable audio->text extractor.
type AudioExtractRequest struct {
	FileID      string
	Path        string
	ContentType string
	Data        []byte
}

// AudioTextExtractor extracts searchable transcript text from audio bytes.
type AudioTextExtractor interface {
	ExtractAudioText(ctx context.Context, req AudioExtractRequest) (string, error)
}

// AudioExtractTaskSpec carries the revision-scoped inputs needed to extract
// audio transcript text for one file version.
type AudioExtractTaskSpec struct {
	FileID      string
	Path        string
	ContentType string
	Revision    int64
}

// AudioExtractResult reports the outcome of one audio extraction attempt.
type AudioExtractResult string

const (
	AudioExtractResultRuntimeNotConfigured AudioExtractResult = "runtime_not_configured"
	AudioExtractResultGetFileError         AudioExtractResult = "get_file_error"
	AudioExtractResultFileNotFound         AudioExtractResult = "file_not_found"
	AudioExtractResultNotConfirmed         AudioExtractResult = "not_confirmed"
	AudioExtractResultNotAudio             AudioExtractResult = "not_audio"
	AudioExtractResultStale                AudioExtractResult = "stale"
	AudioExtractResultLoadError            AudioExtractResult = "load_error"
	AudioExtractResultTooLarge             AudioExtractResult = "too_large"
	AudioExtractResultExtractError         AudioExtractResult = "extract_error"
	AudioExtractResultEmptyText            AudioExtractResult = "empty_text"
	AudioExtractResultUpdateError          AudioExtractResult = "update_error"
	AudioExtractResultWritten              AudioExtractResult = "written"
)

// SupportsAsyncAudioExtract reports whether this backend instance has a fully
// wired audio transcript runtime (enabled flag and non-nil extractor). Phase 2
// does not supply an implicit default extractor when Extractor is nil.
func (b *Dat9Backend) SupportsAsyncAudioExtract() bool {
	return b != nil && b.audioExtractEnabled && b.audioExtractor != nil
}

// audioExtensionMIME maps path suffixes to canonical MIME types for the MVP
// allowlist. Used only when content_type is missing or too generic.
//
// TODO(WebM): Phase 2 MVP excludes both audio/webm and video/webm. Go's
// mime.TypeByExtension(".webm") reports video/webm; supporting WebM means
// deciding extractor behavior for muxed vs audio-only payloads and possibly
// normalizing aliases—revisit after MVP.
var audioExtensionMIME = map[string]string{
	".mp3":  "audio/mpeg",
	".wav":  "audio/wav",
	".m4a":  "audio/mp4",
	".aac":  "audio/aac",
	".ogg":  "audio/ogg",
	".flac": "audio/flac",
}

// allowedAudioMIME is the MVP closed set for durable audio_extract_text. Keep in
// sync with audioExtensionMIME canonical types; do not add audio/webm or
// video/webm until post-MVP WebM work (see TODO on audioExtensionMIME).
var allowedAudioMIME = map[string]struct{}{
	"audio/mpeg":  {},
	"audio/wav":   {},
	"audio/x-wav": {},
	"audio/mp4":   {},
	"audio/x-m4a": {},
	"audio/aac":   {},
	"audio/ogg":   {},
	"audio/flac":  {},
}

func stripMIMEParams(ct string) string {
	ct = strings.TrimSpace(strings.ToLower(ct))
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct
}

func isAllowedAudioMIME(ct string) bool {
	_, ok := allowedAudioMIME[stripMIMEParams(ct)]
	return ok
}

// audioMIMEAllowsPathFallback mirrors the conservative contract: only when
// content_type is absent or not meaningful do we consult the path extension.
func audioMIMEAllowsPathFallback(ct string) bool {
	switch stripMIMEParams(ct) {
	case "", "application/octet-stream", "text/plain":
		return true
	default:
		return false
	}
}

// effectiveAudioMIME resolves an allowlisted audio MIME from stored content_type
// and/or path. It does not sniff file bytes; detectContentType elsewhere is unchanged.
func effectiveAudioMIME(path, contentType string) string {
	ct := stripMIMEParams(contentType)
	if isAllowedAudioMIME(ct) {
		return ct
	}
	if !audioMIMEAllowsPathFallback(ct) {
		return ""
	}
	path = strings.ToLower(path)
	for ext, mime := range audioExtensionMIME {
		if strings.HasSuffix(path, ext) {
			return mime
		}
	}
	return ""
}

// isAudioContentType reports whether contentType (and optional path fallback) is
// in the Phase-2 audio allowlist.
func isAudioContentType(contentType, path string) bool {
	return effectiveAudioMIME(path, contentType) != ""
}

func isSupportedAudioForSemanticTask(path, contentType string) bool {
	return isAudioContentType(contentType, path)
}

// errAudioExtractSourceTooLarge signals the object exceeds MaxAudioBytes; it is
// distinct from an empty payload so the handler does not report oversize for 0-byte inputs.
var errAudioExtractSourceTooLarge = errors.New("audio extract source exceeds configured max bytes")

// resolvedAudioMIMEForHandler combines the stored file Content-Type with the durable
// task payload hint. When the DB value is generic (empty, octet-stream, text/plain),
// we may still resolve audio via path extension or the payload MIME—so a task enqueued
// with an audio hint is not dropped just because the inode row kept a generic type.
func resolvedAudioMIMEForHandler(path, storedContentType, payloadContentType string) string {
	stored := strings.TrimSpace(storedContentType)
	payload := strings.TrimSpace(payloadContentType)
	if m := effectiveAudioMIME(path, stored); m != "" {
		return m
	}
	if audioMIMEAllowsPathFallback(stored) {
		return effectiveAudioMIME(path, payload)
	}
	return ""
}

// ProcessAudioExtractTask runs transcript extraction for one durable
// audio_extract_text task. Terminal business outcomes return a nil error; runtime
// misconfiguration and transient failures return a retryable error.
func (b *Dat9Backend) ProcessAudioExtractTask(ctx context.Context, task AudioExtractTaskSpec) (AudioExtractResult, error) {
	if !b.SupportsAsyncAudioExtract() {
		return AudioExtractResultRuntimeNotConfigured, fmt.Errorf("async audio extract runtime not configured")
	}

	f, err := b.store.GetFile(ctx, task.FileID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return AudioExtractResultFileNotFound, nil
		}
		return AudioExtractResultGetFileError, fmt.Errorf("get file: %w", err)
	}
	if f.Status != datastore.StatusConfirmed {
		return AudioExtractResultNotConfirmed, nil
	}
	resolvedMIME := resolvedAudioMIMEForHandler(task.Path, f.ContentType, task.ContentType)
	if resolvedMIME == "" {
		return AudioExtractResultNotAudio, nil
	}
	if task.Revision > 0 && f.Revision != task.Revision {
		return AudioExtractResultStale, nil
	}

	data, err := b.loadAudioBytesForExtract(ctx, f)
	if err != nil {
		if errors.Is(err, errAudioExtractSourceTooLarge) {
			return AudioExtractResultTooLarge, nil
		}
		return AudioExtractResultLoadError, fmt.Errorf("load audio bytes: %w", err)
	}

	taskCtx, cancel := context.WithTimeout(ctx, b.audioExtractTimeout)
	text, err := b.audioExtractor.ExtractAudioText(taskCtx, AudioExtractRequest{
		FileID:      task.FileID,
		Path:        task.Path,
		ContentType: resolvedMIME,
		Data:        data,
	})
	cancel()
	if err != nil {
		return AudioExtractResultExtractError, fmt.Errorf("extract audio text: %w", err)
	}
	text = sanitizeExtractedText(text, b.maxAudioExtractTextBytes)
	if text == "" {
		return AudioExtractResultEmptyText, nil
	}

	var updated bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := injectedAudioExtractWritebackError("audioExtractWritebackUpdateFileSearchTextError"); err != nil {
			return err
		}
		var txErr error
		updated, txErr = b.store.UpdateFileSearchTextTx(tx, task.FileID, task.Revision, text)
		return txErr
	})
	if err != nil {
		return AudioExtractResultUpdateError, fmt.Errorf("update file search text: %w", err)
	}
	if !updated {
		return AudioExtractResultStale, nil
	}
	return AudioExtractResultWritten, nil
}

func injectedAudioExtractWritebackError(name string) error {
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

func (b *Dat9Backend) loadAudioBytesForExtract(ctx context.Context, f *datastore.File) ([]byte, error) {
	if b.audioExtractMaxSize > 0 && f.SizeBytes > b.audioExtractMaxSize {
		return nil, errAudioExtractSourceTooLarge
	}
	if f.StorageType == datastore.StorageDB9 {
		if b.audioExtractMaxSize > 0 && int64(len(f.ContentBlob)) > b.audioExtractMaxSize {
			return nil, errAudioExtractSourceTooLarge
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
	if b.audioExtractMaxSize > 0 {
		reader = io.LimitReader(rc, b.audioExtractMaxSize+1)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if b.audioExtractMaxSize > 0 && int64(len(data)) > b.audioExtractMaxSize {
		return nil, errAudioExtractSourceTooLarge
	}
	return data, nil
}
