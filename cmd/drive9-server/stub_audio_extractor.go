package main

import (
	"context"
	"path"
	"strings"

	"github.com/mem9-ai/drive9/pkg/backend"
)

// stubAudioTextExtractor is a deterministic ASR stand-in for local e2e.
type stubAudioTextExtractor struct{}

func (stubAudioTextExtractor) ExtractAudioText(ctx context.Context, req backend.AudioExtractRequest) (string, backend.AudioExtractUsage, error) {
	if err := ctx.Err(); err != nil {
		return "", backend.AudioExtractUsage{}, err
	}
	base := path.Base(strings.TrimSpace(req.Path))
	if base == "." || base == "/" || base == "" {
		base = "unknown"
	}
	return "audio transcript for " + base, backend.AudioExtractUsage{}, nil
}
