package backend

import (
	"context"
	"strings"
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
	AudioExtractResultGetFileError           AudioExtractResult = "get_file_error"
	AudioExtractResultFileNotFound           AudioExtractResult = "file_not_found"
	AudioExtractResultNotConfirmed           AudioExtractResult = "not_confirmed"
	AudioExtractResultNotAudio               AudioExtractResult = "not_audio"
	AudioExtractResultStale                  AudioExtractResult = "stale"
	AudioExtractResultLoadError              AudioExtractResult = "load_error"
	AudioExtractResultTooLarge               AudioExtractResult = "too_large"
	AudioExtractResultExtractError           AudioExtractResult = "extract_error"
	AudioExtractResultEmptyText              AudioExtractResult = "empty_text"
	AudioExtractResultUpdateError            AudioExtractResult = "update_error"
	AudioExtractResultWritten                AudioExtractResult = "written"
)

// SupportsAsyncAudioExtract reports whether this backend instance has a fully
// wired audio transcript runtime (enabled flag and non-nil extractor). Phase 2
// does not supply an implicit default extractor when Extractor is nil.
func (b *Dat9Backend) SupportsAsyncAudioExtract() bool {
	return b != nil && b.audioExtractEnabled && b.audioExtractor != nil
}

// audioExtensionMIME maps known file suffixes to Phase-2 allowlisted audio MIME
// types. Used only when content_type is missing or too generic to identify audio.
var audioExtensionMIME = map[string]string{
	".mp3":  "audio/mpeg",
	".wav":  "audio/wav",
	".m4a":  "audio/mp4",
	".aac":  "audio/aac",
	".ogg":  "audio/ogg",
	".flac": "audio/flac",
}

var allowedAudioMIME = map[string]struct{}{
	"audio/mpeg":  {},
	"audio/wav":   {},
	"audio/x-wav": {},
	"audio/mp4":   {},
	"audio/x-m4a": {},
	"audio/aac":   {},
	"audio/ogg":   {},
	"audio/webm":  {},
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

func audioContentTypeFromPath(path string) string {
	return effectiveAudioMIME(path, "")
}

// isAudioContentType reports whether contentType (and optional path fallback) is
// in the Phase-2 audio allowlist.
func isAudioContentType(contentType, path string) bool {
	return effectiveAudioMIME(path, contentType) != ""
}

func isSupportedAudioForSemanticTask(path, contentType string) bool {
	return isAudioContentType(contentType, path)
}
