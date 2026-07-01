package backend

import (
	"fmt"
	"time"

	"github.com/mem9-ai/drive9/pkg/embedding"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/meta"
	"go.uber.org/zap"
)

const (
	defaultImageExtractMaxSize = int64(8 << 20) // 8 MiB
	defaultImageExtractTimeout = 20 * time.Second
	// DefaultImageExtractMaxTextBytes is the default stored semantic text cap
	// for image extraction. The image writeback path also enforces an
	// estimated-token cap before this byte cap is applied.
	DefaultImageExtractMaxTextBytes   = 32 << 10 // 32 KiB
	defaultMaxExtractedTextBytes      = DefaultImageExtractMaxTextBytes
	defaultAudioExtractMaxSize        = int64(32 << 20) // 32 MiB
	defaultAudioExtractTimeout        = 2 * time.Minute
	defaultMaxAudioExtractedTextBytes = 8 << 10               // 8 KiB
	defaultMaxUploadBytes             = int64(10 * (1 << 30)) // 10 GiB
	defaultMaxTenantStorageBytes      = int64(50 * (1 << 30)) // 50 GiB
	defaultMaxMediaLLMFiles           = int64(500)            // 500 media files per tenant
	// defaultMaxMonthlyLLMCostMillicents is the per-tenant monthly LLM spend
	// cap applied when no explicit budget is configured. $10.00 USD, expressed
	// in millicents (0.001 cents; $10 = 1000 cents = 1_000_000 millicents).
	// This is a defense-in-depth floor, not a product pricing tier: high
	// enough for a reasonable trial workload (hundreds of images or a few
	// hours of audio) and low enough that a runaway tenant is noticed before
	// meaningful financial impact. Operators who need a higher baseline raise
	// this constant; operators who want to disable it globally pass a
	// negative MaxMonthlyMillicents in Options.LLMCostBudget. Per-tenant
	// overrides via meta.QuotaConfig.MaxMonthlyCostMC continue to win.
	defaultMaxMonthlyLLMCostMillicents = int64(1_000_000) // $10.00
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
	// AppSemanticTasksEnabled controls whether the app-managed embed worker
	// path may enqueue semantic_tasks for text and description embedding.
	// When false, shouldEnqueueEmbedForRevision short-circuits to false,
	// preventing orphaned task rows when no DRIVE9_EMBED_* worker is
	// configured. This does not affect the DatabaseAutoEmbedding (TiDB
	// auto) path or image/audio extract tasks.
	AppSemanticTasksEnabled bool
	// MaxMediaLLMFiles caps the number of confirmed image+audio files per tenant
	// that trigger LLM extraction tasks (img_extract_text, audio_extract_text).
	// Files beyond this limit are still stored but their LLM tasks are not enqueued.
	// Zero or negative means use the default (500).
	MaxMediaLLMFiles int64
	// LLMCostBudget configures the monthly LLM cost budget for this tenant.
	LLMCostBudget LLMCostBudgetOptions
	// TenantID is used for per-write S3 encryption context and audit metadata.
	TenantID string
	// StorageNamespaceID is the control-plane namespace for S3 object lifecycle.
	StorageNamespaceID string
	// S3EncryptionPolicy is the already-resolved policy for this backend.
	// The zero value is normalized to the global default of explicit no encryption.
	S3EncryptionPolicy meta.ResolvedS3EncryptionPolicy
	// InlineThreshold overrides the DB-inline vs S3 storage cutoff. When 0,
	// DefaultInlineThreshold is used. The same value is surfaced via
	// /v1/status so clients can pick a matching upload strategy.
	InlineThreshold int64
	// TextExtractMaxBytes overrides the synchronous text extraction cap. When
	// 0, DefaultTextExtractMaxBytes is used. Independent of InlineThreshold.
	TextExtractMaxBytes int64
}

// LLMCostBudgetOptions configures the monthly LLM cost budget.
type LLMCostBudgetOptions struct {
	// MaxMonthlyMillicents is the monthly cost cap in millicents (0.001 cents).
	//
	// Tri-state:
	//   > 0  — explicit per-tenant cap in millicents.
	//   == 0 — unset; the default defense-in-depth cap
	//          (defaultMaxMonthlyLLMCostMillicents, currently $10) is applied.
	//   < 0  — explicit opt-out; disables the monthly cost budget gate.
	//
	// The zero-value meaning changed intentionally: leaving this field
	// unset no longer yields "unlimited". Operators that truly need no
	// monthly cap must pass a negative value. Per-tenant overrides via
	// meta.QuotaConfig.MaxMonthlyCostMC still win over this value.
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

// AsyncImageExtractOptions controls async image->text extraction. Delivery is
// fully durable via semantic_tasks; the semantic worker claims and processes
// img_extract_text tasks using this backend's extractor and S3 client.
type AsyncImageExtractOptions struct {
	Enabled bool
	// QueueSize and Workers are deprecated: image extraction no longer uses an
	// in-process channel. They remain for config compatibility but are ignored.
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
	if opts.TenantID != "" {
		b.tenantID = opts.TenantID
	}
	if opts.StorageNamespaceID != "" {
		b.storageNamespaceID = opts.StorageNamespaceID
	}
	b.databaseAutoEmbedding = opts.DatabaseAutoEmbedding
	b.appSemanticTasksEnabled = opts.AppSemanticTasksEnabled
	b.s3EncryptionPolicy = opts.S3EncryptionPolicy
	if b.s3EncryptionPolicy.Mode == "" {
		resolved, err := meta.ResolveS3EncryptionPolicy(meta.DefaultS3EncryptionPolicy(), meta.S3EncryptionPolicy{Mode: meta.S3EncryptionModeInherit})
		if err == nil {
			b.s3EncryptionPolicy = resolved
		}
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
	if opts.InlineThreshold > 0 {
		b.inlineThreshold = opts.InlineThreshold
	} else {
		b.inlineThreshold = DefaultInlineThreshold
	}
	if opts.TextExtractMaxBytes > 0 {
		b.textExtractMaxBytes = opts.TextExtractMaxBytes
	} else {
		b.textExtractMaxBytes = DefaultTextExtractMaxBytes
	}

	cb := opts.LLMCostBudget
	switch {
	case cb.MaxMonthlyMillicents > 0:
		b.maxMonthlyLLMCostMillicents = cb.MaxMonthlyMillicents
	case cb.MaxMonthlyMillicents < 0:
		b.maxMonthlyLLMCostMillicents = 0 // explicit opt-out
	default:
		b.maxMonthlyLLMCostMillicents = defaultMaxMonthlyLLMCostMillicents
	}
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
		globalBackendRuntimeMetrics.activateImage(b.runtimeMetricsID, 0, 0)

		logger.Info(backgroundWithTrace(), "backend_image_extract_runtime_configured",
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
		globalBackendRuntimeMetrics.activateAudio(b.runtimeMetricsID, a.MaxAudioBytes, a.MaxExtractTextBytes, a.TaskTimeout)
		logger.Info(backgroundWithTrace(), "backend_audio_extract_runtime_configured",
			zap.Duration("task_timeout", a.TaskTimeout),
			zap.Int64("max_audio_bytes", a.MaxAudioBytes),
			zap.Int("max_extract_text_bytes", a.MaxExtractTextBytes),
			zap.String("extractor_type", fmt.Sprintf("%T", a.Extractor)))
	}
}

// Close stops background workers owned by this backend instance.
func (b *Dat9Backend) Close() {
	b.StopFileGCWorker()
	if b.imageExtractEnabled {
		globalBackendRuntimeMetrics.deactivateImage(b.runtimeMetricsID)
		b.imageExtractEnabled = false
	}
	if b.audioExtractEnabled {
		globalBackendRuntimeMetrics.deactivateAudio(b.runtimeMetricsID)
		b.audioExtractEnabled = false
	}
	b.stopMutationWorker()
	if b.quotaConfigCache != nil {
		b.quotaConfigCache.stop()
		b.quotaConfigCache = nil
	}
}
