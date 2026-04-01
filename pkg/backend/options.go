package backend

import (
	"context"
	"time"

	"github.com/mem9-ai/dat9/pkg/embedding"
	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
	"go.uber.org/zap"
)

const (
	defaultImageExtractQueueSize = 128
	defaultImageExtractWorkers   = 1
	defaultImageExtractMaxSize   = int64(8 << 20) // 8 MiB
	defaultImageExtractTimeout   = 20 * time.Second
	defaultMaxExtractedTextBytes = 8 << 10 // 8 KiB
)

// Options configures Dat9Backend behavior.
type Options struct {
	AsyncImageExtract AsyncImageExtractOptions
	QueryEmbedding    QueryEmbeddingOptions
	// DatabaseAutoEmbedding controls whether semantic text is embedded by the
	// database itself rather than by the app-managed embed worker. When enabled,
	// runtime write/query paths rely on database-side embedding behavior.
	DatabaseAutoEmbedding bool
}

// AsyncImageExtractOptions controls async image->text extraction.
type AsyncImageExtractOptions struct {
	Enabled             bool
	QueueSize           int
	Workers             int
	MaxImageBytes       int64
	TaskTimeout         time.Duration
	MaxExtractTextBytes int
	Extractor           ImageTextExtractor
}

// QueryEmbeddingOptions controls app-side query embedding for semantic search.
type QueryEmbeddingOptions struct {
	Client embedding.Client
}

func (b *Dat9Backend) configureOptions(opts Options) {
	b.databaseAutoEmbedding = opts.DatabaseAutoEmbedding

	if opts.QueryEmbedding.Client != nil {
		b.queryEmbedder = opts.QueryEmbedding.Client
	} else {
		b.queryEmbedder = embedding.NopClient{}
	}

	cfg := opts.AsyncImageExtract
	if !cfg.Enabled {
		return
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultImageExtractQueueSize
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultImageExtractWorkers
	}
	if cfg.MaxImageBytes <= 0 {
		cfg.MaxImageBytes = defaultImageExtractMaxSize
	}
	if cfg.TaskTimeout <= 0 {
		cfg.TaskTimeout = defaultImageExtractTimeout
	}
	if cfg.MaxExtractTextBytes <= 0 {
		cfg.MaxExtractTextBytes = defaultMaxExtractedTextBytes
	}
	if cfg.Extractor == nil {
		cfg.Extractor = NewBasicImageTextExtractor()
	}

	b.imageExtractEnabled = true
	b.imageExtractor = cfg.Extractor
	b.imageExtractTimeout = cfg.TaskTimeout
	b.imageExtractMaxSize = cfg.MaxImageBytes
	b.maxExtractTextBytes = cfg.MaxExtractTextBytes
	b.imageExtractQueue = make(chan ImageExtractTaskSpec, cfg.QueueSize)
	metrics.RecordGauge("image_extract", "queue_capacity", float64(cfg.QueueSize))
	metrics.RecordGauge("image_extract", "workers", float64(cfg.Workers))
	metrics.RecordGauge("image_extract", "queue_depth", 0)

	ctx, cancel := context.WithCancel(backgroundWithTrace())
	b.imageExtractCancel = cancel
	for i := 0; i < cfg.Workers; i++ {
		b.imageExtractWG.Add(1)
		go b.runImageExtractWorker(ctx, i+1)
	}
	logger.Info(ctx, "backend_image_extract_workers_started",
		zap.Int("workers", cfg.Workers), zap.Int("queue_size", cfg.QueueSize))
}

// Close stops background workers owned by this backend instance.
func (b *Dat9Backend) Close() {
	if b.imageExtractCancel == nil {
		return
	}
	b.imageExtractCancel()
	b.imageExtractWG.Wait()
	b.imageExtractCancel = nil
	metrics.RecordGauge("image_extract", "workers", 0)
	metrics.RecordGauge("image_extract", "queue_depth", 0)
	logger.Info(backgroundWithTrace(), "backend_image_extract_workers_stopped",
		zap.Int("queue_depth", len(b.imageExtractQueue)),
		zap.Int("queue_size", cap(b.imageExtractQueue)))
}
