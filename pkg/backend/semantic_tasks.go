package backend

import (
	"database/sql"
	"strings"
	"time"

	"github.com/mem9-ai/dat9/pkg/semantic"
)

func (b *Dat9Backend) enqueueEmbedTaskTx(tx *sql.Tx, fileID string, revision int64) error {
	now := time.Now().UTC()
	_, err := b.store.EnqueueSemanticTaskTx(tx, newEmbedTask(b.genID(), fileID, revision, now))
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
