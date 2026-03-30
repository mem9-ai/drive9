package backend

import (
	"database/sql"
	"time"

	"github.com/mem9-ai/dat9/pkg/semantic"
)

func (b *Dat9Backend) enqueueEmbedTaskTx(tx *sql.Tx, fileID string, revision int64) error {
	now := time.Now().UTC()
	_, err := b.store.EnqueueSemanticTaskTx(tx, &semantic.Task{
		TaskID:          b.genID(),
		TaskType:        semantic.TaskTypeEmbed,
		ResourceID:      fileID,
		ResourceVersion: revision,
		Status:          semantic.TaskQueued,
		MaxAttempts:     5,
		AvailableAt:     now,
		CreatedAt:       now,
		UpdatedAt:       now,
	})
	return err
}
