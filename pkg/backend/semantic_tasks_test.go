package backend

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/s3client"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

type backendSemanticTaskRow struct {
	TaskType        string
	Status          string
	ResourceVersion int64
}

func loadSemanticTasksForFile(t *testing.T, b *Dat9Backend, fileID string) []backendSemanticTaskRow {
	t.Helper()
	rows, err := b.Store().DB().Query(`SELECT task_type, status, resource_version
		FROM semantic_tasks WHERE resource_id = ?
		ORDER BY resource_version, created_at, task_id`, fileID)
	if err != nil {
		t.Fatalf("query semantic tasks for %s: %v", fileID, err)
	}
	defer func() { _ = rows.Close() }()

	var tasks []backendSemanticTaskRow
	for rows.Next() {
		var task backendSemanticTaskRow
		if err := rows.Scan(&task.TaskType, &task.Status, &task.ResourceVersion); err != nil {
			t.Fatalf("scan semantic task for %s: %v", fileID, err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate semantic tasks for %s: %v", fileID, err)
	}
	return tasks
}

func setStoredEmbeddingState(t *testing.T, b *Dat9Backend, fileID string, revision int64) {
	t.Helper()
	if _, err := b.Store().DB().Exec(`UPDATE files SET embedding = ?, embedding_revision = ? WHERE file_id = ?`, "old-vector", revision, fileID); err != nil {
		t.Fatalf("set embedding state for %s: %v", fileID, err)
	}
}

func uploadAllPartsForPlan(t *testing.T, b *Dat9Backend, plan *UploadPlan, uploadID string, totalSize int64) {
	t.Helper()
	ctx := context.Background()
	upload, err := b.GetUpload(ctx, uploadID)
	if err != nil {
		t.Fatalf("get upload %s: %v", uploadID, err)
	}
	partData := make([]byte, totalSize)
	for i := range partData {
		partData[i] = byte(i % 251)
	}
	for _, part := range plan.Parts {
		start := int64(part.Number-1) * s3client.PartSize
		end := start + part.Size
		if end > totalSize {
			end = totalSize
		}
		if _, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, part.Number, bytes.NewReader(partData[start:end])); err != nil {
			t.Fatalf("upload part %d: %v", part.Number, err)
		}
	}
}

func mustFileForPath(t *testing.T, b *Dat9Backend, path string) (string, int64, *int64, string) {
	t.Helper()
	nf, err := b.Store().Stat(context.Background(), path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if nf.File == nil {
		t.Fatalf("path %s has no file entity", path)
	}
	return nf.File.FileID, nf.File.Revision, nf.File.EmbeddingRevision, nf.File.ContentType
}

func TestWriteCreateEnqueuesEmbedTask(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/data/file.txt", []byte("hello world"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	fileID, revision, embeddingRevision, _ := mustFileForPath(t, b, "/data/file.txt")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should be nil before worker, got %v", *embeddingRevision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != string(semantic.TaskTypeEmbed) || tasks[0].Status != string(semantic.TaskQueued) || tasks[0].ResourceVersion != 1 {
		t.Fatalf("unexpected semantic task: %+v", tasks[0])
	}
}

func TestWriteCreateSkipsEmbedTaskWithoutTextSource(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/data/blob.bin", []byte{0, 1, 2, 3}, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	fileID, revision, embeddingRevision, _ := mustFileForPath(t, b, "/data/blob.bin")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should be nil before worker, got %v", *embeddingRevision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
	}
}

func TestWriteOverwriteEnqueuesNextRevisionAndClearsEmbeddingState(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f.txt", []byte("old"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/f.txt")
	setStoredEmbeddingState(t, b, fileID, 1)

	if _, err := b.Write("/f.txt", []byte("new"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}

	_, revision, embeddingRevision, _ := mustFileForPath(t, b, "/f.txt")
	if revision != 2 {
		t.Fatalf("revision=%d, want 2", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should be cleared, got %v", *embeddingRevision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 2 {
		t.Fatalf("semantic task count=%d, want 2", len(tasks))
	}
	if tasks[1].ResourceVersion != 2 || tasks[1].TaskType != string(semantic.TaskTypeEmbed) {
		t.Fatalf("unexpected overwrite semantic task: %+v", tasks[1])
	}
}

func TestWriteOverwriteSkipsEmbedTaskWithoutTextSource(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f", []byte("old"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/f")
	setStoredEmbeddingState(t, b, fileID, 1)

	if _, err := b.Write("/f", []byte{0, 1, 2, 3}, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}

	_, revision, embeddingRevision, _ := mustFileForPath(t, b, "/f")
	if revision != 2 {
		t.Fatalf("revision=%d, want 2", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should be cleared, got %v", *embeddingRevision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].ResourceVersion != 1 {
		t.Fatalf("unexpected semantic task history: %+v", tasks)
	}
}

func TestWriteOverwriteDoesNotDeleteInlineMarkerObject(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/f", []byte("old"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	if err := b.S3().PutObject(context.Background(), "inline", bytes.NewReader([]byte("marker")), int64(len("marker"))); err != nil {
		t.Fatal(err)
	}
	large := bytes.Repeat([]byte("a"), 2<<20)
	if _, err := b.Write("/f", large, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	rc, err := b.S3().GetObject(context.Background(), "inline")
	if err != nil {
		t.Fatalf("inline marker object should survive overwrite cleanup: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "marker" {
		t.Fatalf("marker object=%q, want %q", data, "marker")
	}
}

func TestConfirmUploadSkipsEmbedTaskWithoutTextSource(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/bigfile.txt", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)

	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}

	fileID, revision, embeddingRevision, contentType := mustFileForPath(t, b, "/bigfile.txt")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should be nil before worker, got %v", *embeddingRevision)
	}
	if contentType != detectContentType("/bigfile.txt", nil) {
		t.Fatalf("content_type=%q, want %q", contentType, detectContentType("/bigfile.txt", nil))
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
	}
}

func TestConfirmUploadOverwriteSkipsEmbedTaskWithoutTextSourceAndRebindsUpload(t *testing.T) {
	b := newTestBackendWithS3(t)
	ctx := context.Background()
	if _, err := b.Write("/report.txt", []byte("old body"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/report.txt")
	setStoredEmbeddingState(t, b, fileID, 1)

	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/report.txt", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	pendingUpload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	pendingFileID := pendingUpload.FileID
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)

	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}

	confirmedFileID, revision, embeddingRevision, contentType := mustFileForPath(t, b, "/report.txt")
	if confirmedFileID != fileID {
		t.Fatalf("overwrite should preserve inode file_id=%q, got %q", fileID, confirmedFileID)
	}
	if revision != 2 {
		t.Fatalf("revision=%d, want 2", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should be cleared, got %v", *embeddingRevision)
	}
	if contentType != detectContentType("/report.txt", nil) {
		t.Fatalf("content_type=%q, want %q", contentType, detectContentType("/report.txt", nil))
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].ResourceVersion != 1 || tasks[0].TaskType != string(semantic.TaskTypeEmbed) {
		t.Fatalf("unexpected overwrite upload semantic tasks: %+v", tasks)
	}
	upload, err := b.GetUpload(ctx, plan.UploadID)
	if err != nil {
		t.Fatal(err)
	}
	if upload.FileID != fileID {
		t.Fatalf("upload file_id=%q, want surviving inode %q", upload.FileID, fileID)
	}
	var deletedStatus string
	if err := b.Store().DB().QueryRow(`SELECT status FROM files WHERE file_id = ?`, pendingFileID).Scan(&deletedStatus); err != nil {
		t.Fatal(err)
	}
	if deletedStatus != "DELETED" {
		t.Fatalf("pending upload file status=%q, want DELETED", deletedStatus)
	}
}

func TestShouldEnqueueEmbedForRevisionWithSynchronousText(t *testing.T) {
	b := newTestBackend(t)
	if !b.shouldEnqueueEmbedForRevision("/docs/a.txt", "text/plain", "hello world") {
		t.Fatal("expected synchronous text content to enqueue embed work")
	}
}

func TestShouldEnqueueEmbedForRevisionWithAsyncImageSource(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "caption"},
		},
	})
	if !b.shouldEnqueueEmbedForRevision("/img/a.png", "application/octet-stream", "") {
		t.Fatal("expected image path with async extractor to enqueue embed work")
	}
}

func TestShouldEnqueueEmbedForRevisionWithoutTextSource(t *testing.T) {
	b := newTestBackend(t)
	if b.shouldEnqueueEmbedForRevision("/bin/a.bin", "application/octet-stream", "") {
		t.Fatal("generic binary object should not enqueue embed work without text source")
	}
}
