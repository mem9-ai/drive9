package backend

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pingcap/failpoint"

	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/metrics"
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
	ExtractAudioText(ctx context.Context, req AudioExtractRequest) (string, AudioExtractUsage, error)
}

// AudioExtractAPIError describes an HTTP/API failure returned by an audio
// transcription provider.
type AudioExtractAPIError struct {
	Provider   string
	StatusCode int
	Message    string
}

func (e *AudioExtractAPIError) Error() string {
	provider := strings.TrimSpace(e.Provider)
	if provider == "" {
		provider = "audio extract"
	}
	if e.Message != "" {
		return fmt.Sprintf("%s api status %d: %s", provider, e.StatusCode, e.Message)
	}
	return fmt.Sprintf("%s api status %d", provider, e.StatusCode)
}

// NonRetryableAudioExtract reports whether retrying the same audio bytes cannot
// reasonably succeed without changing the request, file, or provider account.
func (e *AudioExtractAPIError) NonRetryableAudioExtract() bool {
	switch e.StatusCode {
	case http.StatusBadRequest,
		http.StatusForbidden:
		return true
	default:
		return false
	}
}

type nonRetryableAudioExtractError interface {
	NonRetryableAudioExtract() bool
}

// IsNonRetryableAudioExtractError reports whether err should bypass normal
// semantic worker retry scheduling for an audio_extract_text task.
func IsNonRetryableAudioExtractError(err error) bool {
	var permanent nonRetryableAudioExtractError
	if errors.As(err, &permanent) {
		return permanent.NonRetryableAudioExtract()
	}
	return false
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
	AudioExtractResultBudgetExhausted      AudioExtractResult = "budget_exhausted"
)

// SupportsAsyncAudioExtract reports whether this backend instance has a fully
// wired audio transcript runtime (enabled flag and non-nil extractor). Phase 2
// does not supply an implicit default extractor when Extractor is nil.
func (b *Dat9Backend) SupportsAsyncAudioExtract() bool {
	return b != nil && b.audioExtractEnabled && b.audioExtractor != nil
}

// audioExtensionMIME maps path suffixes to canonical MIME types for the current
// audio_extract_text closed set. It is consulted only when content_type is
// missing or too generic.
//
// The shipped set stays small: MP3, WAV, MP4, and M4A (audio in MP4
// container). Broader formats remain deferred; see TODO(post-MVP audio) and
// TODO(WebM) below.
//
// TODO(post-MVP audio): Restore AAC, FLAC, OGG extensions (and MIME /
// alias allowlist entries) once the extractor and tests cover them.
//
//	".aac":  "audio/aac",
//	".ogg":  "audio/ogg",
//	".flac": "audio/flac",
//
// TODO(WebM): Phase 2 MVP excludes both audio/webm and video/webm. Go's
// mime.TypeByExtension(".webm") reports video/webm; supporting WebM means
// deciding extractor behavior for muxed vs audio-only payloads and possibly
// normalizing aliases—revisit after MVP.
var audioExtensionMIME = map[string]string{
	".mp3": "audio/mpeg",
	".wav": "audio/wav",
	".mp4": "audio/mp4",
	".m4a": "audio/mp4",
}

// allowedAudioMIME is the current closed set for durable audio_extract_text.
// MP4 container uploads are admitted here even though detectContentType()
// commonly reports video/mp4 for .mp4 paths.
//
// TODO(post-MVP audio): Re-add closed-set entries for AAC, OGG, FLAC
// (and further aliases) when those formats return to scope.
//
//	"audio/aac":   {},
//	"audio/ogg":   {},
//	"audio/flac":  {},
//
// Do not add audio/webm or video/webm until post-MVP WebM work (TODO(WebM)).
var allowedAudioMIME = map[string]struct{}{
	"audio/mpeg":  {},
	"audio/wav":   {},
	"audio/x-wav": {},
	"audio/mp4":   {},
	"audio/x-m4a": {},
}

func stripMIMEParams(ct string) string {
	ct = strings.TrimSpace(strings.ToLower(ct))
	if i := strings.Index(ct, ";"); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	return ct
}

// normalizeStdlibAudioMIMEAliases maps Go mime.TypeByExtension and common
// platform MIME-info aliases onto the current allowlist keys (see
// allowedAudioMIME).
//
// Linux loads extension MIME from freedesktop globs2 before /etc/mime.types.
// Typical mappings include audio/vnd.wave (preferred in shared-mime-info2.3+),
// audio/wave, and audio/x-wav; only some of those are in allowedAudioMIME.
// Canonicalize WAV synonyms to audio/wav so enqueue and task payloads match
// across macOS vs Ubuntu CI. Also canonicalize MP4 container MIME variants so
// semantic routing can treat .mp4 uploads as audio_extract_text candidates
// without changing detectContentType().
//
// TODO(post-MVP audio): When AAC and FLAC are allowlisted again, restore
// further alias normalization (e.g. audio/x-aac -> audio/aac).
func normalizeStdlibAudioMIMEAliases(ct string) string {
	ct = stripMIMEParams(ct)
	switch ct {
	case "audio/wave", "audio/vnd.wave", "audio/x-pn-wav":
		return "audio/wav"
	case "video/mp4", "application/mp4":
		return "audio/mp4"
	case "audio/mp4a-latm":
		return "audio/mp4"
	default:
		return ct
	}
}

func isAllowedAudioMIME(ct string) bool {
	_, ok := allowedAudioMIME[normalizeStdlibAudioMIMEAliases(ct)]
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
	rawStripped := stripMIMEParams(contentType)
	if isAllowedAudioMIME(contentType) {
		return normalizeStdlibAudioMIMEAliases(contentType)
	}
	if !audioMIMEAllowsPathFallback(rawStripped) {
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

// resolvedAudioExtractContentType chooses the MIME sent to the pluggable
// extractor after the handler has already decided the revision is a supported
// audio source.
//
// Semantic routing canonicalizes MP4 containers to audio/mp4 for stable
// eligibility checks, but OpenAI-compatible ASR providers often expect the
// original part-level container MIME (commonly video/mp4) for .mp4 uploads.
func resolvedAudioExtractContentType(storedContentType, payloadContentType, resolvedAudioMIME string) string {
	resolved := strings.TrimSpace(resolvedAudioMIME)
	if resolved == "" {
		return ""
	}
	for _, candidate := range []string{storedContentType, payloadContentType} {
		switch stripMIMEParams(candidate) {
		case "video/mp4", "application/mp4", "audio/x-m4a":
			return stripMIMEParams(candidate)
		}
	}
	return resolved
}

// ProcessAudioExtractTask runs transcript extraction for one durable
// audio_extract_text task. Terminal business outcomes return a nil error; runtime
// misconfiguration and transient failures return a retryable error.
func (b *Dat9Backend) ProcessAudioExtractTask(ctx context.Context, task AudioExtractTaskSpec) (AudioExtractResult, error) {
	if !b.SupportsAsyncAudioExtract() {
		return AudioExtractResultRuntimeNotConfigured, fmt.Errorf("async audio extract runtime not configured")
	}
	if b.monthlyLLMCostExceededCheck(ctx) {
		metrics.RecordOperation("llm_cost_budget", "process_skip", "budget_exhausted", 0)
		return AudioExtractResultBudgetExhausted, nil
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
	if task.Revision > 0 && f.Revision != task.Revision {
		return AudioExtractResultStale, nil
	}
	resolvedMIME := resolvedAudioMIMEForHandler(task.Path, f.ContentType, task.ContentType)
	if resolvedMIME == "" {
		return AudioExtractResultNotAudio, nil
	}
	extractorContentType := resolvedAudioExtractContentType(f.ContentType, task.ContentType, resolvedMIME)
	if extractorContentType == "" {
		extractorContentType = resolvedMIME
	}

	data, err := b.loadAudioBytesForExtract(ctx, f)
	if err != nil {
		if errors.Is(err, errAudioExtractSourceTooLarge) {
			return AudioExtractResultTooLarge, nil
		}
		return AudioExtractResultLoadError, fmt.Errorf("load audio bytes: %w", err)
	}

	taskCtx, cancel := context.WithTimeout(ctx, b.audioExtractTimeout)
	text, audioUsage, err := b.audioExtractor.ExtractAudioText(taskCtx, AudioExtractRequest{
		FileID:      task.FileID,
		Path:        task.Path,
		ContentType: extractorContentType,
		Data:        data,
	})
	cancel()
	if err != nil {
		return AudioExtractResultExtractError, fmt.Errorf("extract audio text: %w", err)
	}
	b.recordAudioExtractUsage(task.FileID, audioUsage)
	text = sanitizeExtractedText(text, b.maxAudioExtractTextBytes)
	if text == "" {
		return AudioExtractResultEmptyText, nil
	}

	// Always revision-gate writeback: UpdateFileSearchTextTx treats expectedRevision<=0
	// as an unscoped UPDATE: use the loaded row revision when the task omits ResourceVersion.
	expectedRevision := task.Revision
	if expectedRevision == 0 {
		expectedRevision = f.Revision
	}

	var updated bool
	err = b.store.InTx(ctx, func(tx *sql.Tx) error {
		if err := injectedAudioExtractWritebackError("audioExtractWritebackUpdateFileSearchTextError"); err != nil {
			return err
		}
		var txErr error
		updated, txErr = b.store.UpdateFileSearchTextTx(tx, task.FileID, expectedRevision, text)
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
