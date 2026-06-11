package backend

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/metrics"
	"github.com/mem9-ai/dat9/pkg/semantic"
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

// enqueueTiDBAutoSemanticTasksTx registers durable img_extract_text and/or
// audio_extract_text tasks for one confirmed file revision in TiDB auto-embedding mode.
// When the tenant's media LLM file quota is exceeded, no extraction tasks are
// enqueued but the file write itself succeeds normally.
func (b *Dat9Backend) enqueueTiDBAutoSemanticTasksTx(ctx context.Context, tx *sql.Tx, fileID string, revision int64, path, contentType string) (bool, error) {
	isImage := b.hasAsyncImageTextSource(path, contentType)
	isAudio := b.shouldEnqueueAudioExtractTask(path, contentType)
	if !isImage && !isAudio {
		return false, nil
	}
	if b.mediaLLMQuotaExceededCheckTx(ctx, tx) {
		metrics.RecordOperation("media_llm_budget", "enqueue_skip", "quota_exceeded", 0)
		return false, nil
	}
	enqueued := false
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
	return enqueued, nil
}

func (b *Dat9Backend) shouldEnqueueAudioExtractTask(path, contentType string) bool {
	if !b.SupportsAsyncAudioExtract() {
		return false
	}
	return isSupportedAudioForSemanticTask(path, contentType)
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

func (b *Dat9Backend) shouldEnqueueEmbedForRevision(path, contentType, contentText, description string) bool {
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
	if len(out) == 0 {
		return nil
	}
	return out
}
