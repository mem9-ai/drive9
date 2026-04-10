package backend

import (
	"context"
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
