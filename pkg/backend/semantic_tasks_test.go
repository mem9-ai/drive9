package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

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
		start := int64(part.Number-1) * upload.PartSize
		end := start + part.Size
		if end > totalSize {
			end = totalSize
		}
		if _, err := b.S3().(*s3client.LocalS3Client).UploadPart(ctx, upload.S3UploadID, part.Number, bytes.NewReader(partData[start:end])); err != nil {
			t.Fatalf("upload part %d: %v", part.Number, err)
		}
	}
}

// completePartsForConfirmV2 builds the client part list for ConfirmUploadV2 after
// parts have been uploaded to S3 (same bytes as uploadAllPartsForPlan).
func completePartsForConfirmV2(t *testing.T, b *Dat9Backend, uploadID string) []CompletePart {
	t.Helper()
	ctx := context.Background()
	u, err := b.GetUpload(ctx, uploadID)
	if err != nil {
		t.Fatalf("get upload: %v", err)
	}
	parts, err := b.S3().ListParts(ctx, u.S3Key, u.S3UploadID)
	if err != nil {
		t.Fatalf("list parts: %v", err)
	}
	out := make([]CompletePart, len(parts))
	for i, p := range parts {
		out[i] = CompletePart{Number: p.Number, ETag: p.ETag}
	}
	return out
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

func TestWriteCreateAutoEmbeddingSkipsEmbedTaskEvenWithTextSource(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{DatabaseAutoEmbedding: true})
	if _, err := b.Write("/data/file.txt", []byte("hello world"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	fileID, revision, embeddingRevision, _ := mustFileForPath(t, b, "/data/file.txt")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should remain nil on create, got %v", *embeddingRevision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
	}
}

func TestWriteCreateAutoEmbeddingImageEnqueuesImgExtractTaskWithoutLegacyQueue(t *testing.T) {
	extractor := &gatedImageExtractor{started: make(chan struct{}, 1), release: make(chan struct{})}
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	if _, err := b.Write("/img/create-auto.png", []byte("fake-png"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	select {
	case <-extractor.started:
		close(extractor.release)
		t.Fatal("legacy image queue should stay idle for auto create path")
	case <-time.After(200 * time.Millisecond):
	}

	fileID, revision, embeddingRevision, _ := mustFileForPath(t, b, "/img/create-auto.png")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should remain nil before durable worker, got %v", *embeddingRevision)
	}
	nf, err := b.Store().Stat(context.Background(), "/img/create-auto.png")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /img/create-auto.png: %v", err)
	}
	if nf.File.ContentText != "" {
		t.Fatalf("content_text=%q, want empty before durable worker", nf.File.ContentText)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != string(semantic.TaskTypeImgExtractText) || tasks[0].Status != string(semantic.TaskQueued) || tasks[0].ResourceVersion != 1 {
		t.Fatalf("unexpected semantic task: %+v", tasks[0])
	}
}

func TestWriteCreateAutoEmbeddingAudioEnqueuesAudioExtractTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	if _, err := b.Write("/tracks/create.mp3", []byte{0xff, 0xf3}, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, _ := mustFileForPath(t, b, "/tracks/create.mp3")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text among %+v", tasks)
	}
	nf, err := b.Store().Stat(context.Background(), "/tracks/create.mp3")
	if err != nil || nf.File == nil {
		t.Fatal(err)
	}
	if nf.File.ContentText != "" {
		t.Fatalf("content_text should be empty before worker, got %q", nf.File.ContentText)
	}
}

func TestWriteCreateAutoEmbeddingMP4EnqueuesAudioExtractTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	if _, err := b.Write("/tracks/create.mp4", []byte{0x00, 0x00, 0x00, 0x18}, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, contentType := mustFileForPath(t, b, "/tracks/create.mp4")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	wantCT := detectContentType("/tracks/create.mp4", nil)
	if contentType != wantCT {
		t.Fatalf("content_type=%q, want %q", contentType, wantCT)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text among %+v", tasks)
	}
}

func TestWriteCreateAutoEmbeddingM4AEnqueuesAudioExtractTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	if _, err := b.Write("/tracks/create.m4a", []byte{0x00, 0x00, 0x00}, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, contentType := mustFileForPath(t, b, "/tracks/create.m4a")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	wantCT := detectContentType("/tracks/create.m4a", nil)
	if contentType != wantCT {
		t.Fatalf("content_type=%q, want %q", contentType, wantCT)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text among %+v", tasks)
	}
}

func TestWriteOverwriteAutoEmbeddingAudioEnqueuesAudioExtractTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/tracks/ow.mp3", "audio/mpeg", []byte{1, 2, 3})
	setStoredEmbeddingState(t, b, fileID, 1)

	if _, err := b.Write("/tracks/ow.mp3", []byte{4, 5, 6}, 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	_, revision, _, _ := mustFileForPath(t, b, "/tracks/ow.mp3")
	if revision != 2 {
		t.Fatalf("revision=%d, want 2", revision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) && tsk.ResourceVersion == 2 {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text for revision 2 among %+v", tasks)
	}
}

func TestWriteCreateAutoEmbeddingSkipsAudioWhenExtractorUnset(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled: true,
			// Extractor nil: Phase 2 does not wire a default extractor.
			Extractor: nil,
		},
	})
	if _, err := b.Write("/tracks/no-extractor.mp3", []byte{0xff, 0xf3}, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/tracks/no-extractor.mp3")
	tasks := loadSemanticTasksForFile(t, b, fileID)
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			t.Fatalf("unexpected audio task: %+v", tsk)
		}
	}
}

func TestWriteCreateAutoEmbeddingSkipsAudioForNonAudioFile(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	if _, err := b.Write("/notes/plain.txt", []byte("hello"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/notes/plain.txt")
	tasks := loadSemanticTasksForFile(t, b, fileID)
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			t.Fatalf("unexpected audio task: %+v", tsk)
		}
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

func TestWriteOverwriteAutoEmbeddingSkipsEmbedTaskAndPreservesEmbeddingState(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{DatabaseAutoEmbedding: true})
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
	if embeddingRevision == nil || *embeddingRevision != 1 {
		t.Fatalf("embedding revision should be preserved, got %v", embeddingRevision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
	}
}

func TestWriteOverwriteAutoEmbeddingImageEnqueuesImgExtractTaskWithoutLegacyQueue(t *testing.T) {
	extractor := &gatedImageExtractor{started: make(chan struct{}, 1), release: make(chan struct{})}
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/img/overwrite-auto.png", "image/png", []byte("old-image"))
	setStoredEmbeddingState(t, b, fileID, 1)

	if _, err := b.Write("/img/overwrite-auto.png", []byte("new-image"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}

	select {
	case <-extractor.started:
		close(extractor.release)
		t.Fatal("legacy image queue should stay idle for auto overwrite path")
	case <-time.After(200 * time.Millisecond):
	}

	_, revision, embeddingRevision, _ := mustFileForPath(t, b, "/img/overwrite-auto.png")
	if revision != 2 {
		t.Fatalf("revision=%d, want 2", revision)
	}
	if embeddingRevision == nil || *embeddingRevision != 1 {
		t.Fatalf("embedding revision should be preserved, got %v", embeddingRevision)
	}
	nf, err := b.Store().Stat(context.Background(), "/img/overwrite-auto.png")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /img/overwrite-auto.png: %v", err)
	}
	if nf.File.ContentText != "" {
		t.Fatalf("content_text=%q, want empty before durable worker", nf.File.ContentText)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != string(semantic.TaskTypeImgExtractText) || tasks[0].Status != string(semantic.TaskQueued) || tasks[0].ResourceVersion != 2 {
		t.Fatalf("unexpected semantic task: %+v", tasks[0])
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

func TestConfirmUploadOverwriteAutoEmbeddingSkipsEmbedTaskAndPreservesEmbeddingState(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{DatabaseAutoEmbedding: true})
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
	if embeddingRevision == nil || *embeddingRevision != 1 {
		t.Fatalf("embedding revision should be preserved, got %v", embeddingRevision)
	}
	if contentType != detectContentType("/report.txt", nil) {
		t.Fatalf("content_type=%q, want %q", contentType, detectContentType("/report.txt", nil))
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
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

func TestConfirmUploadAutoEmbeddingImageEnqueuesImgExtractTaskWithoutLegacyQueue(t *testing.T) {
	extractor := &gatedImageExtractor{started: make(chan struct{}, 1), release: make(chan struct{})}
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/img/upload-auto.png", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)

	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}

	select {
	case <-extractor.started:
		close(extractor.release)
		t.Fatal("legacy image queue should stay idle for auto upload create path")
	case <-time.After(200 * time.Millisecond):
	}

	fileID, revision, embeddingRevision, contentType := mustFileForPath(t, b, "/img/upload-auto.png")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	if embeddingRevision != nil {
		t.Fatalf("embedding revision should be nil before durable worker, got %v", *embeddingRevision)
	}
	if contentType != detectContentType("/img/upload-auto.png", nil) {
		t.Fatalf("content_type=%q, want %q", contentType, detectContentType("/img/upload-auto.png", nil))
	}
	nf, err := b.Store().Stat(ctx, "/img/upload-auto.png")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /img/upload-auto.png: %v", err)
	}
	if nf.File.ContentText != "" {
		t.Fatalf("content_text=%q, want empty before durable worker", nf.File.ContentText)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != string(semantic.TaskTypeImgExtractText) || tasks[0].Status != string(semantic.TaskQueued) || tasks[0].ResourceVersion != 1 {
		t.Fatalf("unexpected semantic task: %+v", tasks[0])
	}
}

func TestConfirmUploadAutoEmbeddingEnqueuesAudioExtractTaskForMP3(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/upload/clip.mp3", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, _ := mustFileForPath(t, b, "/upload/clip.mp3")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text for upload completion .mp3 among %+v", tasks)
	}
}

func TestConfirmUploadAutoEmbeddingEnqueuesAudioExtractTaskForWAV(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/upload/clip.wav", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, contentType := mustFileForPath(t, b, "/upload/clip.wav")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	wantCT := detectContentType("/upload/clip.wav", nil)
	if contentType != wantCT {
		t.Fatalf("content_type=%q, want %q", contentType, wantCT)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text for upload completion .wav among %+v", tasks)
	}
}

func TestConfirmUploadAutoEmbeddingEnqueuesAudioExtractTaskForMP4(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/upload/clip.mp4", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, contentType := mustFileForPath(t, b, "/upload/clip.mp4")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	wantCT := detectContentType("/upload/clip.mp4", nil)
	if contentType != wantCT {
		t.Fatalf("content_type=%q, want %q", contentType, wantCT)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text for upload completion .mp4 among %+v", tasks)
	}
}

func TestConfirmUploadOverwriteAutoEmbeddingEnqueuesAudioExtractTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	ctx := context.Background()
	fileID := insertImageFileForExtractTest(t, b, "/upload/ow.mp3", "audio/mpeg", []byte{1, 2, 3})
	setStoredEmbeddingState(t, b, fileID, 1)

	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/upload/ow.mp3", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}
	confirmedID, revision, _, _ := mustFileForPath(t, b, "/upload/ow.mp3")
	if confirmedID != fileID {
		t.Fatalf("overwrite should preserve file_id=%q, got %q", fileID, confirmedID)
	}
	if revision != 2 {
		t.Fatalf("revision=%d, want 2", revision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioRev2 bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) && tsk.ResourceVersion == 2 {
			audioRev2 = true
			if tsk.Status != string(semantic.TaskQueued) {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioRev2 {
		t.Fatalf("expected audio_extract_text for revision 2 among %+v", tasks)
	}
}

func TestConfirmUploadAutoEmbeddingEnqueuesAudioExtractTaskForM4A(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/upload/clip.m4a", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, contentType := mustFileForPath(t, b, "/upload/clip.m4a")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	wantCT := detectContentType("/upload/clip.m4a", nil)
	if contentType != wantCT {
		t.Fatalf("content_type=%q, want %q", contentType, wantCT)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("expected audio_extract_text for upload completion .m4a among %+v", tasks)
	}
}

func TestConfirmUploadAutoEmbeddingSkipsAudioWhenAsyncAudioDisabled(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   false,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/upload/no-audio-async.mp3", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	if err := b.ConfirmUpload(ctx, plan.UploadID); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/upload/no-audio-async.mp3")
	tasks := loadSemanticTasksForFile(t, b, fileID)
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			t.Fatalf("unexpected audio task when async audio disabled: %+v", tsk)
		}
	}
}

// TestConfirmUploadV2AutoEmbeddingEnqueuesAudioViaSharedFinalize checks that
// ConfirmUploadV2 enqueues audio_extract_text for TiDB auto-embedding by going
// through the shared finalizeUpload path.
//
// The fixture intentionally uses InitiateUpload (v1) to create the multipart
// upload and upload parts; only the completion step uses ConfirmUploadV2. That
// does not exercise the full InitiateUploadV2 / v2 presign lifecycle (covered
// in upload_test.go); it specifically locks shared finalize behavior so v1 and
// v2 completion stay aligned for semantic enqueue.
func TestConfirmUploadV2AutoEmbeddingEnqueuesAudioViaSharedFinalize(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	ctx := context.Background()
	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/upload/shared-finalize-v2-clip.mp3", totalSize)
	if err != nil {
		t.Fatal(err)
	}
	uploadAllPartsForPlan(t, b, plan, plan.UploadID, totalSize)
	parts := completePartsForConfirmV2(t, b, plan.UploadID)
	if err := b.ConfirmUploadV2(ctx, plan.UploadID, parts); err != nil {
		t.Fatal(err)
	}
	fileID, revision, _, _ := mustFileForPath(t, b, "/upload/shared-finalize-v2-clip.mp3")
	if revision != 1 {
		t.Fatalf("revision=%d, want 1", revision)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	var audioSeen bool
	for _, tsk := range tasks {
		if tsk.TaskType == string(semantic.TaskTypeAudioExtractText) {
			audioSeen = true
			if tsk.Status != string(semantic.TaskQueued) || tsk.ResourceVersion != 1 {
				t.Fatalf("unexpected audio task: %+v", tsk)
			}
		}
	}
	if !audioSeen {
		t.Fatalf("ConfirmUploadV2 should enqueue audio via shared finalize; got %+v", tasks)
	}
}

func TestConfirmUploadOverwriteAutoEmbeddingImageEnqueuesImgExtractTaskWithoutLegacyQueue(t *testing.T) {
	extractor := &gatedImageExtractor{started: make(chan struct{}, 1), release: make(chan struct{})}
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	ctx := context.Background()
	fileID := insertImageFileForExtractTest(t, b, "/img/upload-overwrite-auto.png", "image/png", []byte("old-image"))
	setStoredEmbeddingState(t, b, fileID, 1)

	totalSize := int64(2 << 20)
	plan, err := b.InitiateUpload(ctx, "/img/upload-overwrite-auto.png", totalSize)
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

	select {
	case <-extractor.started:
		close(extractor.release)
		t.Fatal("legacy image queue should stay idle for auto upload overwrite path")
	case <-time.After(200 * time.Millisecond):
	}

	confirmedFileID, revision, embeddingRevision, contentType := mustFileForPath(t, b, "/img/upload-overwrite-auto.png")
	if confirmedFileID != fileID {
		t.Fatalf("overwrite should preserve inode file_id=%q, got %q", fileID, confirmedFileID)
	}
	if revision != 2 {
		t.Fatalf("revision=%d, want 2", revision)
	}
	if embeddingRevision == nil || *embeddingRevision != 1 {
		t.Fatalf("embedding revision should be preserved, got %v", embeddingRevision)
	}
	if contentType != detectContentType("/img/upload-overwrite-auto.png", nil) {
		t.Fatalf("content_type=%q, want %q", contentType, detectContentType("/img/upload-overwrite-auto.png", nil))
	}
	nf, err := b.Store().Stat(ctx, "/img/upload-overwrite-auto.png")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /img/upload-overwrite-auto.png: %v", err)
	}
	if nf.File.ContentText != "" {
		t.Fatalf("content_text=%q, want empty before durable worker", nf.File.ContentText)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != string(semantic.TaskTypeImgExtractText) || tasks[0].Status != string(semantic.TaskQueued) || tasks[0].ResourceVersion != 2 {
		t.Fatalf("unexpected semantic task: %+v", tasks[0])
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

func TestRenameDoesNotCreateAdditionalSemanticTasks(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/old.txt", []byte("data"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/old.txt")
	if err := b.Rename("/old.txt", "/new.txt"); err != nil {
		t.Fatal(err)
	}

	newFileID, _, _, _ := mustFileForPath(t, b, "/new.txt")
	if newFileID != fileID {
		t.Fatalf("rename should preserve file_id=%q, got %q", fileID, newFileID)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
}

func TestCopyFileDoesNotCreateAdditionalSemanticTasks(t *testing.T) {
	b := newTestBackend(t)
	if _, err := b.Write("/src.txt", []byte("shared"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, _, _, _ := mustFileForPath(t, b, "/src.txt")
	if err := b.CopyFile("/src.txt", "/dst.txt"); err != nil {
		t.Fatal(err)
	}

	dstFileID, _, _, _ := mustFileForPath(t, b, "/dst.txt")
	if dstFileID != fileID {
		t.Fatalf("copy should preserve file_id=%q, got %q", fileID, dstFileID)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
}

func TestShouldEnqueueEmbedForRevisionWithSynchronousText(t *testing.T) {
	b := newTestBackend(t)
	if !b.shouldEnqueueEmbedForRevision("/docs/a.txt", "text/plain", "hello world", "") {
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
	if !b.shouldEnqueueEmbedForRevision("/img/a.png", "application/octet-stream", "", "") {
		t.Fatal("expected image path with async extractor to enqueue embed work")
	}
}

func TestShouldEnqueueEmbedForRevisionWithoutTextSource(t *testing.T) {
	b := newTestBackend(t)
	if b.shouldEnqueueEmbedForRevision("/bin/a.bin", "application/octet-stream", "", "") {
		t.Fatal("generic binary object should not enqueue embed work without text source")
	}
}

func TestNewImgExtractTaskCarriesPayloadHints(t *testing.T) {
	now := time.Now().UTC()
	task, err := newImgExtractTask("task-1", "file-1", 7, "/img/a.png", "image/png", now)
	if err != nil {
		t.Fatal(err)
	}
	if task.TaskType != semantic.TaskTypeImgExtractText {
		t.Fatalf("task type=%q, want %q", task.TaskType, semantic.TaskTypeImgExtractText)
	}
	if task.ResourceID != "file-1" || task.ResourceVersion != 7 {
		t.Fatalf("unexpected task identity: %+v", task)
	}
	var payload semantic.ImgExtractTaskPayload
	if err := json.Unmarshal(task.PayloadJSON, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Path != "/img/a.png" || payload.ContentType != "image/png" {
		t.Fatalf("unexpected payload: %+v", payload)
	}
}

func TestShouldEnqueueEmbedForRevisionWithDescriptionOnly(t *testing.T) {
	b := newTestBackend(t)
	if !b.shouldEnqueueEmbedForRevision("/bin/a.bin", "application/octet-stream", "", "some description") {
		t.Fatal("expected non-empty description to enqueue embed work")
	}
}
