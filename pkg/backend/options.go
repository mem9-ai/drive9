package backend

import (
	"context"
	"fmt"
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
	defaultMaxExtractedTextBytes = 8 << 10               // 8 KiB
	defaultAudioExtractMaxSize   = int64(32 << 20)       // 32 MiB
	defaultAudioExtractTimeout   = 2 * time.Minute
	defaultMaxAudioExtractedTextBytes = 8 << 10          // 8 KiB
	defaultMaxUploadBytes        = int64(10 * (1 << 30)) // 10 GiB
	defaultMaxTenantStorageBytes = int64(50 * (1 << 30)) // 50 GiB
	defaultMaxMediaLLMFiles      = int64(500)            // 500 media files per tenant
)

// QuotaSource controls where quota checks read authoritative state from.
// During migration from per-tenant DB to server DB, this flag selects the
// active source.
type QuotaSource string

const (
	// QuotaSourceTenant reads quota state from the per-tenant TiDB cluster
	// (current/legacy behavior). This is the default.
	QuotaSourceTenant QuotaSource = "tenant"
	// QuotaSourceServer reads quota state from the drive9 server DB (meta).
	// Requires that central quota tables are populated (backfill complete).
	QuotaSourceServer QuotaSource = "server"
)

// Options configures Dat9Backend behavior.
type Options struct {
	AsyncImageExtract AsyncImageExtractOptions
	// AsyncAudioExtract configures durable audio transcript extraction for the
	// TiDB auto-embedding path. Unlike async image extract, there is no in-process
	// queue; work is delivered only via semantic_tasks when runtime is wired.
	AsyncAudioExtract AsyncAudioExtractOptions
	QueryEmbedding    QueryEmbeddingOptions
	MaxUploadBytes    int64
	// MaxTenantStorageBytes caps the total logical storage a single tenant may
	// occupy across confirmed files plus in-flight upload reservations.
	MaxTenantStorageBytes int64
	// DatabaseAutoEmbedding controls whether semantic text is embedded by the
	// database itself rather than by the app-managed embed worker. When enabled,
	// runtime write/query paths rely on database-side embedding behavior.
	DatabaseAutoEmbedding bool
	// MaxMediaLLMFiles caps the number of confirmed image+audio files per tenant
	// that trigger LLM extraction tasks (img_extract_text, audio_extract_text).
	// Files beyond this limit are still stored but their LLM tasks are not enqueued.
	// Zero or negative means use the default (500).
	MaxMediaLLMFiles int64
	// LLMCostBudget configures the monthly LLM cost budget for this tenant.
	LLMCostBudget LLMCostBudgetOptions
	// QuotaSource selects where quota enforcement reads authoritative state.
	// "tenant" (default) uses per-tenant DB; "server" uses the central server DB.
	QuotaSource QuotaSource
}

// LLMCostBudgetOptions configures the monthly LLM cost budget.
type LLMCostBudgetOptions struct {
	// MaxMonthlyMillicents is the monthly cost cap in millicents (0.001 cents).
	// Zero or negative disables the monthly cost budget gate.
	MaxMonthlyMillicents int64
	// VisionCostPerKTokenMillicents is the cost per 1K tokens for Vision API calls.
	VisionCostPerKTokenMillicents int64
	// AudioLLMCostPerKTokenMillicents is the cost per 1K tokens for token-based
	// audio models (e.g. gpt-4o-transcribe).
	AudioLLMCostPerKTokenMillicents int64
	// WhisperCostPerMinuteMillicents is the cost per minute for duration-based
	// audio models (e.g. whisper-1).
	WhisperCostPerMinuteMillicents int64
	// FallbackImageCostMillicents is used when the Vision API does not return
	// token usage. Must be > 0 for cost tracking to work with such providers.
	FallbackImageCostMillicents int64
	// FallbackAudioCostMillicents is used when the audio API returns neither
	// duration nor token usage. Must be > 0 for cost tracking to work.
	FallbackAudioCostMillicents int64
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

// AsyncImageExtractWillWireRuntime reports whether async image extraction will be
// wired on a Dat9Backend built from opts. When Enabled is true, configureOptions
// always assigns a concrete ImageTextExtractor (NewBasicImageTextExtractor when nil),
// so this matches effective SupportsAsyncImageExtract after backend construction.
func AsyncImageExtractWillWireRuntime(opts AsyncImageExtractOptions) bool {
	return opts.Enabled
}

// AsyncAudioExtractOptions configures audio transcript extraction for the database
// auto-embedding path. Delivery uses semantic_tasks only; no local worker queue.
type AsyncAudioExtractOptions struct {
	Enabled             bool
	MaxAudioBytes       int64
	TaskTimeout         time.Duration
	MaxExtractTextBytes int
	Extractor           AudioTextExtractor
}

// AsyncAudioExtractWillWireRuntime reports whether async audio extraction should be
// treated as fully configured. Unlike image extract, Phase 2 does not substitute a
// default extractor: both Enabled and a non-nil Extractor are required.
func AsyncAudioExtractWillWireRuntime(opts AsyncAudioExtractOptions) bool {
	return opts.Enabled && opts.Extractor != nil
}

// QueryEmbeddingOptions controls app-side query embedding for semantic search.
type QueryEmbeddingOptions struct {
	Client embedding.Client
}

func (b *Dat9Backend) configureOptions(opts Options) {
	b.databaseAutoEmbedding = opts.DatabaseAutoEmbedding
	if opts.QuotaSource == QuotaSourceServer {
		b.quotaSource = QuotaSourceServer
	} else {
		b.quotaSource = QuotaSourceTenant
	}
	if opts.MaxUploadBytes > 0 {
		b.maxUploadBytes = opts.MaxUploadBytes
	} else {
		b.maxUploadBytes = defaultMaxUploadBytes
	}
	if opts.MaxMediaLLMFiles > 0 {
		b.maxMediaLLMFiles = opts.MaxMediaLLMFiles
	} else {
		b.maxMediaLLMFiles = defaultMaxMediaLLMFiles
	}
	if opts.MaxTenantStorageBytes > 0 {
		b.maxTenantStorageBytes = opts.MaxTenantStorageBytes
	} else {
		b.maxTenantStorageBytes = defaultMaxTenantStorageBytes
	}

	cb := opts.LLMCostBudget
	b.maxMonthlyLLMCostMillicents = cb.MaxMonthlyMillicents
	b.visionCostPerKTokenMillicents = cb.VisionCostPerKTokenMillicents
	b.audioLLMCostPerKTokenMillicents = cb.AudioLLMCostPerKTokenMillicents
	b.whisperCostPerMinuteMillicents = cb.WhisperCostPerMinuteMillicents
	b.fallbackImageCostMillicents = cb.FallbackImageCostMillicents
	b.fallbackAudioCostMillicents = cb.FallbackAudioCostMillicents

	if opts.QueryEmbedding.Client != nil {
		b.queryEmbedder = opts.QueryEmbedding.Client
	} else {
		b.queryEmbedder = embedding.NopClient{}
	}

	cfg := opts.AsyncImageExtract
	if cfg.Enabled {
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
			zap.Int("workers", cfg.Workers),
			zap.Int("queue_size", cfg.QueueSize),
			zap.Duration("task_timeout", cfg.TaskTimeout),
			zap.Int64("max_image_bytes", cfg.MaxImageBytes),
			zap.Int("max_extract_text_bytes", cfg.MaxExtractTextBytes),
			zap.String("extractor_type", fmt.Sprintf("%T", cfg.Extractor)))
	}

	a := opts.AsyncAudioExtract
	if AsyncAudioExtractWillWireRuntime(a) {
		if a.MaxAudioBytes <= 0 {
			a.MaxAudioBytes = defaultAudioExtractMaxSize
		}
		if a.TaskTimeout <= 0 {
			a.TaskTimeout = defaultAudioExtractTimeout
		}
		if a.MaxExtractTextBytes <= 0 {
			a.MaxExtractTextBytes = defaultMaxAudioExtractedTextBytes
		}
		b.audioExtractEnabled = true
		b.audioExtractor = a.Extractor
		b.audioExtractTimeout = a.TaskTimeout
		b.audioExtractMaxSize = a.MaxAudioBytes
		b.maxAudioExtractTextBytes = a.MaxExtractTextBytes
		logger.Info(backgroundWithTrace(), "backend_audio_extract_runtime_configured",
			zap.Duration("task_timeout", a.TaskTimeout),
			zap.Int64("max_audio_bytes", a.MaxAudioBytes),
			zap.Int("max_extract_text_bytes", a.MaxExtractTextBytes),
			zap.String("extractor_type", fmt.Sprintf("%T", a.Extractor)))
	}
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
