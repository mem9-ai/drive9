package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/semantic"
	"go.uber.org/zap"
)

// The enqueue helpers report whether a task row was actually created (the
// store dedupes identical queued tasks), so callers can fire the post-commit
// semantic task notifier only when there is new work to claim.
func (b *Dat9Backend) enqueueEmbedTaskTx(tx *sql.Tx, fileID string, revision int64) (bool, error) {
	now := time.Now().UTC()
	return b.store.EnqueueSemanticTaskTx(tx, newEmbedTask(b.genID(), fileID, revision, now))
}

func (b *Dat9Backend) enqueueImgExtractTaskTx(tx *sql.Tx, fileID string, revision int64, path, contentType string) (bool, error) {
	now := time.Now().UTC()
	task, err := newImgExtractTask(b.genID(), fileID, revision, path, contentType, now)
	if err != nil {
		return false, err
	}
	return b.store.EnqueueSemanticTaskTx(tx, task)
}

func (b *Dat9Backend) enqueueAudioExtractTaskTx(tx *sql.Tx, fileID string, revision int64, path, contentType string) (bool, error) {
	now := time.Now().UTC()
	task, err := newAudioExtractTask(b.genID(), fileID, revision, path, contentType, now)
	if err != nil {
		return false, err
	}
	return b.store.EnqueueSemanticTaskTx(tx, task)
}

func (b *Dat9Backend) enqueueVideoExtractTaskTx(tx *sql.Tx, fileID string, revision int64, path, contentType string) (bool, error) {
	now := time.Now().UTC()
	task, err := newVideoExtractTask(b.genID(), fileID, revision, path, contentType, now)
	if err != nil {
		return false, err
	}
	return b.store.EnqueueSemanticTaskTx(tx, task)
}

// enqueueExtractSemanticTasksTx registers durable img_extract_text and/or
// audio_extract_text tasks for one confirmed file revision. It applies in both
// database auto-embedding and app-embedding modes: image/audio extraction does
// not depend on EMBED_TEXT. When the tenant's media LLM file quota is exceeded,
// no extraction tasks are enqueued but the file write itself succeeds normally.
// currentMediaDelta accounts for the current transaction when server quota
// usage has not converged yet.
func (b *Dat9Backend) enqueueExtractSemanticTasksTx(ctx context.Context, tx *sql.Tx, fileID string, revision int64, path, contentType string, currentMediaDelta int64) (bool, error) {
	isImage := b.hasAsyncImageTextSource(path, contentType)
	isVideo := b.shouldEnqueueVideoExtractTask(path, contentType)
	// When video visual extraction is enabled for this file, skip audio
	// extraction to avoid dual content_text overwrites on the same revision.
	// Video extraction captures visual content; the audio transcript path
	// (which normalizes video/mp4 → audio/mp4) would race on content_text.
	isAudio := !isVideo && b.shouldEnqueueAudioExtractTask(path, contentType)
	if !isImage && !isAudio && !isVideo {
		return false, nil
	}
	enqueued := false
	// Image and audio share the general media LLM quota.
	if (isImage || isAudio) && b.mediaLLMQuotaExceededCheckTx(ctx, tx, currentMediaDelta) {
		b.recordTenantOperation("media_llm_budget", "enqueue_skip", "quota_exceeded", 0)
	} else {
		if isImage {
			created, err := b.enqueueImgExtractTaskTx(tx, fileID, revision, path, contentType)
			if err != nil {
				return enqueued, err
			}
			enqueued = enqueued || created
		}
		if isAudio {
			created, err := b.enqueueAudioExtractTaskTx(tx, fileID, revision, path, contentType)
			if err != nil {
				return enqueued, err
			}
			enqueued = enqueued || created
		}
	}
	// Video has its own independent quota.
	if isVideo {
		if b.videoLLMQuotaExceededTx(ctx, tx) {
			b.recordTenantOperation("video_llm_budget", "enqueue_skip", "quota_exceeded", 0)
		} else {
			created, err := b.enqueueVideoExtractTaskTx(tx, fileID, revision, path, contentType)
			if err != nil {
				return enqueued, err
			}
			if created && b.metaStore != nil {
				if err := b.metaStore.IncrVideoFileCount(ctx, b.tenantID, 1); err != nil {
					logger.Warn(ctx, "video_llm_central_count_failed",
						zap.String("tenant_id", b.tenantID), zap.Error(err))
				}
			}
			enqueued = enqueued || created
		}
	}
	return enqueued, nil
}

func (b *Dat9Backend) shouldEnqueueAudioExtractTask(path, contentType string) bool {
	if !b.SupportsAsyncAudioExtract() {
		return false
	}
	return isSupportedAudioForSemanticTask(path, contentType)
}

func (b *Dat9Backend) shouldEnqueueVideoExtractTask(path, contentType string) bool {
	if !b.SupportsAsyncVideoExtract() {
		return false
	}
	// Tenant check: "*" allows all; otherwise only explicitly listed tenants.
	if !b.videoExtractAllTenants {
		if _, ok := b.videoExtractTenantAllowlist[b.tenantID]; !ok {
			return false
		}
	}
	return isSupportedVideoForSemanticTask(path, contentType)
}

func newEmbedTask(taskID, fileID string, revision int64, now time.Time) *semantic.Task {
	now = now.UTC()
	return &semantic.Task{
		TaskID:          taskID,
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      fileID,
		ResourceVersion: revision,
		Status:          semantic.TaskQueued,
		MaxAttempts:     5,
		AvailableAt:     now,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func newImgExtractTask(taskID, fileID string, revision int64, path, contentType string, now time.Time) (*semantic.Task, error) {
	now = now.UTC()
	payload := semantic.ImgExtractTaskPayload{Path: path, ContentType: contentType}
	var payloadJSON []byte
	if payload.Path != "" || payload.ContentType != "" {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		payloadJSON = encoded
	}
	return &semantic.Task{
		TaskID:          taskID,
		TaskType:        semantic.TaskTypeImgExtractText,
		ResourceID:      fileID,
		ResourceVersion: revision,
		Status:          semantic.TaskQueued,
		MaxAttempts:     5,
		AvailableAt:     now,
		PayloadJSON:     payloadJSON,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

func newAudioExtractTask(taskID, fileID string, revision int64, path, contentType string, now time.Time) (*semantic.Task, error) {
	now = now.UTC()
	payload := semantic.AudioExtractTaskPayload{Path: path, ContentType: contentType}
	var payloadJSON []byte
	if payload.Path != "" || payload.ContentType != "" {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		payloadJSON = encoded
	}
	return &semantic.Task{
		TaskID:          taskID,
		TaskType:        semantic.TaskTypeAudioExtractText,
		ResourceID:      fileID,
		ResourceVersion: revision,
		Status:          semantic.TaskQueued,
		MaxAttempts:     5,
		AvailableAt:     now,
		PayloadJSON:     payloadJSON,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

func newVideoExtractTask(taskID, fileID string, revision int64, path, contentType string, now time.Time) (*semantic.Task, error) {
	now = now.UTC()
	payload := semantic.VideoExtractTaskPayload{Path: path, ContentType: contentType}
	var payloadJSON []byte
	if payload.Path != "" || payload.ContentType != "" {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		payloadJSON = encoded
	}
	return &semantic.Task{
		TaskID:          taskID,
		TaskType:        semantic.TaskTypeVideoExtractVisual,
		ResourceID:      fileID,
		ResourceVersion: revision,
		Status:          semantic.TaskQueued,
		MaxAttempts:     3,
		AvailableAt:     now,
		PayloadJSON:     payloadJSON,
		CreatedAt:       now,
		UpdatedAt:       now,
	}, nil
}

func (b *Dat9Backend) shouldEnqueueEmbedForRevision(path, contentType, contentText, description string) bool {
	if !b.appSemanticTasksEnabled {
		return false
	}
	if strings.TrimSpace(contentText) != "" {
		return true
	}
	if strings.TrimSpace(description) != "" {
		return true
	}
	return b.hasAsyncImageTextSource(path, contentType)
}

func (b *Dat9Backend) hasAsyncImageTextSource(path, contentType string) bool {
	if !b.imageExtractEnabled || b.imageExtractor == nil {
		return false
	}
	if isImageContentType(contentType) {
		return true
	}
	return isImageContentType(contentTypeFromPath(path))
}

// AutoSemanticTaskTypes returns durable semantic task types executed on the
// database auto-embedding (TiDB auto) path: img_extract_text and/or
// audio_extract_text when the corresponding async runtimes are configured.
//
// It does not include app-managed embed work; embed routing uses the worker
// embedder, not the backend. A nil return means this backend contributes no
// auto semantic tasks. The returned slice must be treated as read-only.
func (b *Dat9Backend) AutoSemanticTaskTypes() []semantic.TaskType {
	if b == nil || !b.UsesDatabaseAutoEmbedding() {
		return nil
	}
	var out []semantic.TaskType
	if b.SupportsAsyncImageExtract() {
		out = append(out, semantic.TaskTypeImgExtractText)
	}
	if b.SupportsAsyncAudioExtract() {
		out = append(out, semantic.TaskTypeAudioExtractText)
	}
	if b.SupportsAsyncVideoExtract() {
		out = append(out, semantic.TaskTypeVideoExtractVisual)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
