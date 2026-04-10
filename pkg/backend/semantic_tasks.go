package backend

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/semantic"
)

func (b *Dat9Backend) enqueueEmbedTaskTx(tx *sql.Tx, fileID string, revision int64) error {
	now := time.Now().UTC()
	_, err := b.store.EnqueueSemanticTaskTx(tx, newEmbedTask(b.genID(), fileID, revision, now))
	return err
}

func (b *Dat9Backend) enqueueImgExtractTaskTx(tx *sql.Tx, fileID string, revision int64, path, contentType string) error {
	now := time.Now().UTC()
	task, err := newImgExtractTask(b.genID(), fileID, revision, path, contentType, now)
	if err != nil {
		return err
	}
	_, err = b.store.EnqueueSemanticTaskTx(tx, task)
	return err
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

func (b *Dat9Backend) shouldEnqueueEmbedForRevision(path, contentType, contentText string) bool {
	if strings.TrimSpace(contentText) != "" {
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

// dat9AutoSemanticTaskTypes is the immutable slice returned when this backend
// exposes auto-managed img_extract_text work. Callers must not mutate it.
var dat9AutoSemanticTaskTypes = []semantic.TaskType{semantic.TaskTypeImgExtractText}

// AutoSemanticTaskTypes returns durable semantic task types executed on the
// database auto-embedding (TiDB auto) path — today only img_extract_text when
// async image extraction runtime is configured.
//
// It does not include app-managed embed work; embed routing uses the worker
// embedder, not the backend. A nil return means this backend contributes no
// auto semantic tasks. The returned slice must be treated as read-only.
func (b *Dat9Backend) AutoSemanticTaskTypes() []semantic.TaskType {
	if b == nil || !b.UsesDatabaseAutoEmbedding() || !b.SupportsAsyncImageExtract() {
		return nil
	}
	return dat9AutoSemanticTaskTypes
}
