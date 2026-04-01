package datastore

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/mem9-ai/dat9/pkg/semantic"
)

func TestInsertFileTx(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()

	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.InsertFileTx(tx, &File{
			FileID:      "f1",
			StorageType: StorageDB9,
			StorageRef:  "/blobs/f1",
			ContentType: "text/plain",
			SizeBytes:   12,
			Revision:    1,
			Status:      StatusConfirmed,
			CreatedAt:   now,
			ConfirmedAt: &now,
		})
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.StorageRef != "/blobs/f1" || got.Revision != 1 {
		t.Fatalf("unexpected inserted file: %+v", got)
	}
}

func TestUpdateFileContentTxClearsEmbeddingState(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		ContentType: "text/plain",
		SizeBytes:   10,
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 9)

	var newRev int64
	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		newRev, err = s.UpdateFileContentTx(tx, "f1", StorageDB9, "/blobs/f1-v2", "text/markdown", "sum-2", "new text", []byte("blob-2"), 22)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if newRev != 2 {
		t.Fatalf("new revision=%d, want 2", newRev)
	}

	got, err := s.GetFile(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 2 || got.StorageRef != "/blobs/f1-v2" || got.ContentText != "new text" {
		t.Fatalf("unexpected updated file: %+v", got)
	}
	if got.EmbeddingRevision != nil {
		t.Fatalf("embedding revision should be cleared, got %v", *got.EmbeddingRevision)
	}
}

func TestClearFileEmbeddingStateTx(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 12)

	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		return s.ClearFileEmbeddingStateTx(tx, "f1")
	}); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetFile(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.EmbeddingRevision != nil {
		t.Fatalf("embedding revision should be nil after clear, got %v", *got.EmbeddingRevision)
	}
}

func TestUpdateFileContentAutoEmbeddingTxPreservesEmbeddingState(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	if err := s.InsertFile(context.Background(), &File{
		FileID:      "f1",
		StorageType: StorageDB9,
		StorageRef:  "/blobs/f1",
		ContentType: "text/plain",
		SizeBytes:   10,
		Revision:    1,
		Status:      StatusConfirmed,
		CreatedAt:   now,
		ConfirmedAt: &now,
	}); err != nil {
		t.Fatal(err)
	}
	setEmbeddingRevision(t, s, "f1", 9)

	var newRev int64
	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		newRev, err = s.UpdateFileContentAutoEmbeddingTx(tx, "f1", StorageDB9, "/blobs/f1-v2", "text/markdown", "sum-2", "new text", []byte("blob-2"), 22)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if newRev != 2 {
		t.Fatalf("new revision=%d, want 2", newRev)
	}

	got, err := s.GetFile(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision != 2 || got.StorageRef != "/blobs/f1-v2" || got.ContentText != "new text" {
		t.Fatalf("unexpected updated file: %+v", got)
	}
	if got.EmbeddingRevision == nil || *got.EmbeddingRevision != 9 {
		t.Fatalf("embedding revision should be preserved, got %v", got.EmbeddingRevision)
	}
}

func TestEnqueueSemanticTaskTxRollsBackWithOuterTransaction(t *testing.T) {
	s := newTestStore(t)
	now := time.Now().UTC()
	task := newSemanticTask("task-tx", "file-1", 1, now, now)

	err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		created, err := s.EnqueueSemanticTaskTx(tx, task)
		if err != nil {
			return err
		}
		if !created {
			t.Fatalf("expected transactional enqueue to create task")
		}
		return context.Canceled
	})
	if err != context.Canceled {
		t.Fatalf("rollback error=%v, want %v", err, context.Canceled)
	}
	if count := countSemanticTasks(t, s); count != 0 {
		t.Fatalf("semantic task count after rollback=%d, want 0", count)
	}

	if err := s.InTx(context.Background(), func(tx *sql.Tx) error {
		created, err := s.EnqueueSemanticTaskTx(tx, &semantic.Task{
			TaskID:          "task-commit",
			TaskType:        semantic.TaskTypeEmbed,
			ResourceID:      "file-1",
			ResourceVersion: 2,
			Status:          semantic.TaskQueued,
			MaxAttempts:     3,
			AvailableAt:     now,
			CreatedAt:       now,
			UpdatedAt:       now,
		})
		if err != nil {
			return err
		}
		if !created {
			t.Fatalf("expected committed transactional enqueue to create task")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count := countSemanticTasks(t, s); count != 1 {
		t.Fatalf("semantic task count after commit=%d, want 1", count)
	}
}
