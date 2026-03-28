package captioner

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidOutput is returned when the captioner produces empty, whitespace-only,
// or excessively long output. This is a non-retryable error.
var ErrInvalidOutput = errors.New("captioner: invalid output")

// ErrImageTooLarge is returned when the image exceeds the configured max size.
// This is a non-retryable error.
var ErrImageTooLarge = errors.New("captioner: image too large")

// MaxCaptionLen is the maximum allowed caption length in bytes.
const MaxCaptionLen = 32 * 1024 // 32 KB

// Captioner generates a text description of an image.
type Captioner interface {
	Caption(ctx context.Context, imageBytes []byte, contentType string) (string, error)
}

// validate checks the captioner output for empty, whitespace-only, or excessively long text.
func validate(text string) (string, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return "", fmt.Errorf("%w: empty or whitespace-only", ErrInvalidOutput)
	}
	if len(trimmed) > MaxCaptionLen {
		return "", fmt.Errorf("%w: length %d exceeds max %d", ErrInvalidOutput, len(trimmed), MaxCaptionLen)
	}
	return trimmed, nil
}
