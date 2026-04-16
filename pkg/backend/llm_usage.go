package backend

import (
	"go.uber.org/zap"

	"github.com/mem9-ai/dat9/pkg/logger"
	"github.com/mem9-ai/dat9/pkg/metrics"
)

// ImageExtractUsage reports the resource consumption of one Vision API call.
type ImageExtractUsage struct {
	PromptTokens     int
	CompletionTokens int
}

// TotalTokens returns the sum of prompt and completion tokens.
func (u ImageExtractUsage) TotalTokens() int { return u.PromptTokens + u.CompletionTokens }

// AudioExtractUsage reports the resource consumption of one audio transcription call.
type AudioExtractUsage struct {
	DurationSeconds float64 // whisper-1: from verbose_json duration field
	InputTokens     int     // gpt-4o-transcribe: from usage.input_tokens
	OutputTokens    int     // gpt-4o-transcribe: from usage.output_tokens
}

func (b *Dat9Backend) recordImageExtractUsage(taskID string, usage ImageExtractUsage) {
	totalTokens := int64(usage.TotalTokens())
	cost := b.imageTokenCostMillicents(totalTokens)
	if cost == 0 && totalTokens == 0 {
		cost = b.fallbackImageCostMillicents
	}
	if cost <= 0 {
		return
	}
	if err := b.store.InsertLLMUsage("img_extract_text", taskID, cost, totalTokens, "tokens"); err != nil {
		logger.Warn(backgroundWithTrace(), "llm_usage_insert_failed",
			zap.String("task_type", "img_extract_text"),
			zap.String("task_id", taskID),
			zap.Error(err))
		metrics.RecordOperation("llm_cost_budget", "usage_insert", "error", 0)
	}
	b.syncCentralLLMCostRecord(backgroundWithTrace(), "img_extract_text", taskID, cost, totalTokens, "tokens")
}

func (b *Dat9Backend) recordAudioExtractUsage(taskID string, usage AudioExtractUsage) {
	var cost int64
	var rawUnits int64
	var rawUnitType string

	totalTokens := int64(usage.InputTokens + usage.OutputTokens)
	if totalTokens > 0 {
		cost = b.audioTokenCostMillicents(totalTokens)
		rawUnits = totalTokens
		rawUnitType = "tokens"
	} else if usage.DurationSeconds > 0 {
		cost = b.audioDurationCostMillicents(usage.DurationSeconds)
		rawUnits = int64(usage.DurationSeconds * 1000) // store as milliseconds
		rawUnitType = "audio_ms"
	} else {
		cost = b.fallbackAudioCostMillicents
		rawUnitType = "fallback"
	}
	if cost <= 0 {
		return
	}
	if err := b.store.InsertLLMUsage("audio_extract_text", taskID, cost, rawUnits, rawUnitType); err != nil {
		logger.Warn(backgroundWithTrace(), "llm_usage_insert_failed",
			zap.String("task_type", "audio_extract_text"),
			zap.String("task_id", taskID),
			zap.Error(err))
		metrics.RecordOperation("llm_cost_budget", "usage_insert", "error", 0)
	}
	b.syncCentralLLMCostRecord(backgroundWithTrace(), "audio_extract_text", taskID, cost, rawUnits, rawUnitType)
}

func (b *Dat9Backend) imageTokenCostMillicents(totalTokens int64) int64 {
	if b.visionCostPerKTokenMillicents <= 0 || totalTokens <= 0 {
		return 0
	}
	return (totalTokens * b.visionCostPerKTokenMillicents) / 1000
}

func (b *Dat9Backend) audioTokenCostMillicents(totalTokens int64) int64 {
	if b.audioLLMCostPerKTokenMillicents <= 0 || totalTokens <= 0 {
		return 0
	}
	return (totalTokens * b.audioLLMCostPerKTokenMillicents) / 1000
}

func (b *Dat9Backend) audioDurationCostMillicents(durationSeconds float64) int64 {
	if b.whisperCostPerMinuteMillicents <= 0 || durationSeconds <= 0 {
		return 0
	}
	return int64(durationSeconds / 60.0 * float64(b.whisperCostPerMinuteMillicents))
}

// monthlyLLMCostExceeded checks whether the tenant has exceeded its monthly
// LLM cost budget. Returns true when the total settled cost exceeds the limit.
func (b *Dat9Backend) monthlyLLMCostExceeded() bool {
	if b.maxMonthlyLLMCostMillicents <= 0 {
		return false
	}
	if b.metaStore != nil && b.tenantID != "" {
		total, err := b.metaStore.MonthlyLLMCostMillicents(backgroundWithTrace(), b.tenantID)
		if err != nil {
			logger.Warn(backgroundWithTrace(), "llm_cost_budget_check_fail_open", zap.Error(err))
			metrics.RecordOperation("llm_cost_budget", "quota_check", "fail_open", 0)
			return false
		}
		return total > b.maxMonthlyLLMCostMillicents
	}
	total, err := b.store.MonthlyLLMCostMillicents()
	if err != nil {
		logger.Warn(backgroundWithTrace(), "llm_cost_budget_check_fail_open", zap.Error(err))
		metrics.RecordOperation("llm_cost_budget", "quota_check", "fail_open", 0)
		return false
	}
	return total > b.maxMonthlyLLMCostMillicents
}
