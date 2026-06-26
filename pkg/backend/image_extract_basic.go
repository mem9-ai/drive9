package backend

import (
	"bytes"
	"context"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"strings"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"github.com/mem9-ai/drive9/pkg/pathutil"
	"go.uber.org/zap"
)

// BasicImageTextExtractor is a deterministic fallback extractor used when no
// external vision model is configured. It emits lightweight metadata text.
type BasicImageTextExtractor struct{}

func NewBasicImageTextExtractor() *BasicImageTextExtractor { return &BasicImageTextExtractor{} }

func (e *BasicImageTextExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, ImageExtractUsage, error) {
	select {
	case <-ctx.Done():
		return "", ImageExtractUsage{}, ctx.Err()
	default:
	}

	name := pathutil.BaseName(req.Path)
	cfg, format, err := image.DecodeConfig(bytes.NewReader(req.Data))
	if err == nil {
		return strings.TrimSpace(fmt.Sprintf("image file %s format %s width %d height %d", name, strings.ToLower(format), cfg.Width, cfg.Height)), ImageExtractUsage{}, nil
	}
	if req.ContentType != "" {
		return strings.TrimSpace(fmt.Sprintf("image file %s content type %s", name, req.ContentType)), ImageExtractUsage{}, nil
	}
	return strings.TrimSpace(fmt.Sprintf("image file %s", name)), ImageExtractUsage{}, nil
}

type fallbackImageTextExtractor struct {
	primary  ImageTextExtractor
	fallback ImageTextExtractor
}

// NewFallbackImageTextExtractor wraps an extractor with a fallback extractor.
// The fallback only covers primary success with empty text; primary errors
// propagate to the caller so they surface in semantic_tasks.last_error and
// retry with backoff, instead of being permanently masked by placeholder
// metadata text that no log retains.
func NewFallbackImageTextExtractor(primary, fallback ImageTextExtractor) ImageTextExtractor {
	if primary == nil {
		return fallback
	}
	if fallback == nil {
		return primary
	}
	return &fallbackImageTextExtractor{primary: primary, fallback: fallback}
}

func (e *fallbackImageTextExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, ImageExtractUsage, error) {
	text, usage, err := e.primary.ExtractImageText(ctx, req)
	if err != nil {
		logger.Warn(ctx, "backend_image_extract_primary_failed", zap.String("tenant_id", req.TenantID),
			zap.String("file_id", req.FileID),
			zap.String("path", req.Path),
			zap.String("content_type", req.ContentType),
			zap.Error(err))
		metrics.RecordTenantOperation(req.TenantID, "image_extract", "fallback", "primary_error", 0)
		return "", usage, fmt.Errorf("primary image extractor: %w", err)
	}
	if strings.TrimSpace(text) != "" {
		return text, usage, nil
	}
	logger.Warn(ctx, "backend_image_extract_primary_empty_use_fallback", zap.String("tenant_id", req.TenantID),
		zap.String("file_id", req.FileID),
		zap.String("path", req.Path))
	metrics.RecordTenantOperation(req.TenantID, "image_extract", "fallback", "primary_empty", 0)
	return e.fallback.ExtractImageText(ctx, req)
}
