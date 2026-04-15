package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/backend"
)

// Environment variable names for drive9-server-local async audio extract wiring.
// These are read only by buildLocalAudioExtractOptionsFromEnv.
const (
	envAudioExtractEnabled        = "DRIVE9_AUDIO_EXTRACT_ENABLED"
	envAudioExtractMode           = "DRIVE9_AUDIO_EXTRACT_MODE"
	envAudioExtractMaxBytes       = "DRIVE9_AUDIO_EXTRACT_MAX_BYTES"
	envAudioExtractTimeoutSeconds = "DRIVE9_AUDIO_EXTRACT_TIMEOUT_SECONDS"
	envAudioExtractMaxTextBytes   = "DRIVE9_AUDIO_EXTRACT_MAX_TEXT_BYTES"
	envAudioExtractAPIBase        = "DRIVE9_AUDIO_EXTRACT_API_BASE"
	envAudioExtractAPIKey         = "DRIVE9_AUDIO_EXTRACT_API_KEY"
	envAudioExtractModel          = "DRIVE9_AUDIO_EXTRACT_MODEL"
	envAudioExtractPrompt         = "DRIVE9_AUDIO_EXTRACT_PROMPT"
	localAudioExtractStubMode     = "stub"
	localAudioExtractOpenAIMode   = "openai"
)

// localStubAudioTextExtractor implements backend.AudioTextExtractor for local e2e.
// Transcript text is derived only from the logical path basename (no decoding of
// audio bytes, no external services). The same path always yields the same string,
// so overwrite and upload-completion flows remain deterministic.
type localStubAudioTextExtractor struct{}

// ExtractAudioText implements [backend.AudioTextExtractor].
func (localStubAudioTextExtractor) ExtractAudioText(ctx context.Context, req backend.AudioExtractRequest) (string, backend.AudioExtractUsage, error) {
	if err := ctx.Err(); err != nil {
		return "", backend.AudioExtractUsage{}, err
	}
	p := strings.TrimSpace(req.Path)
	base := path.Base(p)
	if base == "." || base == "/" || base == "" {
		base = "unknown"
	}
	return "audio transcript for " + base, backend.AudioExtractUsage{}, nil
}

// buildLocalAudioExtractOptionsFromEnv returns [backend.AsyncAudioExtractOptions] for
// drive9-server-local. When DRIVE9_AUDIO_EXTRACT_ENABLED is false, the zero value is
// returned. When true, DRIVE9_AUDIO_EXTRACT_MODE must be either "stub" or
// "openai". The local entrypoint keeps stub mode for deterministic e2e while
// also allowing real provider smoke checks against OpenAI-compatible ASR.
func buildLocalAudioExtractOptionsFromEnv() (backend.AsyncAudioExtractOptions, error) {
	if !envBool(envAudioExtractEnabled, false) {
		return backend.AsyncAudioExtractOptions{}, nil
	}
	mode := strings.ToLower(strings.TrimSpace(os.Getenv(envAudioExtractMode)))
	if mode == "" {
		return backend.AsyncAudioExtractOptions{}, fmt.Errorf("%s is required when %s is true", envAudioExtractMode, envAudioExtractEnabled)
	}
	switch mode {
	case localAudioExtractStubMode:
		// Zero MaxAudioBytes / TaskTimeout / MaxExtractTextBytes lets backend.configureOptions apply defaults.
		return backend.AsyncAudioExtractOptions{
			Enabled:             true,
			MaxAudioBytes:       envInt64(envAudioExtractMaxBytes, 0),
			TaskTimeout:         time.Duration(envInt(envAudioExtractTimeoutSeconds, 0)) * time.Second,
			MaxExtractTextBytes: envInt(envAudioExtractMaxTextBytes, 0),
			Extractor:           localStubAudioTextExtractor{},
		}, nil
	case localAudioExtractOpenAIMode:
		extractor, err := backend.NewOpenAIAudioTextExtractor(backend.OpenAIAudioTextExtractorConfig{
			BaseURL: strings.TrimSpace(os.Getenv(envAudioExtractAPIBase)),
			APIKey:  strings.TrimSpace(os.Getenv(envAudioExtractAPIKey)),
			Model:   strings.TrimSpace(os.Getenv(envAudioExtractModel)),
			Prompt:  strings.TrimSpace(os.Getenv(envAudioExtractPrompt)),
			Timeout: time.Duration(envInt(envAudioExtractTimeoutSeconds, 0)) * time.Second,
		})
		if err != nil {
			return backend.AsyncAudioExtractOptions{}, fmt.Errorf("init local openai audio extractor: %w", err)
		}
		return backend.AsyncAudioExtractOptions{
			Enabled:             true,
			MaxAudioBytes:       envInt64(envAudioExtractMaxBytes, 0),
			TaskTimeout:         time.Duration(envInt(envAudioExtractTimeoutSeconds, 0)) * time.Second,
			MaxExtractTextBytes: envInt(envAudioExtractMaxTextBytes, 0),
			Extractor:           extractor,
		}, nil
	default:
		return backend.AsyncAudioExtractOptions{}, fmt.Errorf("%s must be %q or %q for drive9-server-local (got %q)", envAudioExtractMode, localAudioExtractStubMode, localAudioExtractOpenAIMode, mode)
	}
}
