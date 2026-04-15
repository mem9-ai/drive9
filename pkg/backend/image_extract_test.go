package backend

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
	"github.com/mem9-ai/dat9/pkg/datastore"
	"github.com/mem9-ai/dat9/pkg/pathutil"
	"github.com/mem9-ai/dat9/pkg/semantic"
)

type staticImageExtractor struct {
	text string
}

func (e *staticImageExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, ImageExtractUsage, error) {
	return e.text, ImageExtractUsage{}, nil
}

type gatedImageExtractor struct {
	started chan struct{}
	release chan struct{}

	mu    sync.Mutex
	calls int
}

func (e *gatedImageExtractor) ExtractImageText(ctx context.Context, req ImageExtractRequest) (string, ImageExtractUsage, error) {
	e.mu.Lock()
	e.calls++
	call := e.calls
	e.mu.Unlock()
	if call == 1 {
		select {
		case e.started <- struct{}{}:
		default:
		}
		select {
		case <-e.release:
		case <-ctx.Done():
			return "", ImageExtractUsage{}, ctx.Err()
		}
		return "old caption", ImageExtractUsage{}, nil
	}
	return "new caption", ImageExtractUsage{}, nil
}

func TestAsyncImageExtractUpdatesContentText(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})

	if _, err := b.Write("/img/a.png", []byte("fake-png"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	got := waitForContentText(t, b, "/img/a.png", 2*time.Second)
	if !strings.Contains(got, "cat on sofa") {
		t.Fatalf("unexpected extracted text: %q", got)
	}
}

func TestAsyncImageExtractAutoEmbeddingUpdatesContentTextWithoutSemanticTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/img/auto.png", "image/png", []byte("fake-png"))
	result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
		FileID:      fileID,
		Path:        "/img/auto.png",
		ContentType: "image/png",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != ImageExtractResultWritten {
		t.Fatalf("result=%q, want %q", result, ImageExtractResultWritten)
	}

	got := waitForContentText(t, b, "/img/auto.png", time.Second)
	if !strings.Contains(got, "cat on sofa") {
		t.Fatalf("unexpected extracted text: %q", got)
	}

	if tasks := loadSemanticTasksForFile(t, b, fileID); len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
	}
}

func TestAsyncImageExtractSkipsStaleChecksum(t *testing.T) {
	extractor := &gatedImageExtractor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})

	if _, err := b.Write("/img/b.png", []byte("first-image-bytes"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}

	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first extraction did not start")
	}

	if _, err := b.Write("/img/b.png", []byte("second-image-bytes"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	close(extractor.release)

	got := waitForContentText(t, b, "/img/b.png", 3*time.Second)
	if got != "new caption" {
		t.Fatalf("expected final extracted text %q, got %q", "new caption", got)
	}
}

func TestAsyncImageExtractAutoEmbeddingStaleResultDoesNotQueueOrOverwriteCurrentText(t *testing.T) {
	extractor := &gatedImageExtractor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/img/auto-stale.png", "image/png", []byte("first-image-bytes"))
	resultCh := make(chan ImageExtractResult, 1)
	errCh := make(chan error, 1)
	go func() {
		result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
			FileID:      fileID,
			Path:        "/img/auto-stale.png",
			ContentType: "image/png",
			Revision:    1,
		})
		resultCh <- result
		errCh <- err
	}()
	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first extraction did not start")
	}

	if _, err := b.Write("/img/auto-stale.png", []byte("second-image-bytes"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
		FileID:      fileID,
		Path:        "/img/auto-stale.png",
		ContentType: "image/png",
		Revision:    2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != ImageExtractResultWritten {
		t.Fatalf("result=%q, want %q", result, ImageExtractResultWritten)
	}
	close(extractor.release)
	if gotErr := <-errCh; gotErr != nil {
		t.Fatal(gotErr)
	}
	if got := <-resultCh; got != ImageExtractResultStale {
		t.Fatalf("stale result=%q, want %q", got, ImageExtractResultStale)
	}

	got := waitForContentText(t, b, "/img/auto-stale.png", time.Second)
	if got != "new caption" {
		t.Fatalf("expected final extracted text %q, got %q", "new caption", got)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != string(semantic.TaskTypeImgExtractText) || tasks[0].Status != "queued" || tasks[0].ResourceVersion != 2 {
		t.Fatalf("unexpected semantic task history: %+v", tasks)
	}
}

func TestAsyncImageExtractRequeuesSucceededEmbedTask(t *testing.T) {
	extractor := &gatedImageExtractor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})

	if _, err := b.Write("/img/requeue.png", []byte("first-image-bytes"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("image extraction did not start")
	}

	fileID, _, _, _ := mustFileForPath(t, b, "/img/requeue.png")
	claimed, found, err := b.Store().ClaimSemanticTask(context.Background(), time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected initial embed task to exist")
	}
	if err := b.Store().AckSemanticTask(context.Background(), claimed.TaskID, claimed.Receipt); err != nil {
		t.Fatal(err)
	}
	close(extractor.release)

	got := waitForContentText(t, b, "/img/requeue.png", 3*time.Second)
	if got != "old caption" {
		t.Fatalf("expected extracted text %q, got %q", "old caption", got)
	}

	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].ResourceVersion != 1 || tasks[0].Status != "queued" {
		t.Fatalf("expected image bridge to requeue embed task, got %+v", tasks[0])
	}
}

func TestAsyncImageExtractStaleResultDoesNotRequeueOldRevision(t *testing.T) {
	extractor := &gatedImageExtractor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: extractor,
		},
	})

	if _, err := b.Write("/img/stale.png", []byte("first-image-bytes"), 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	select {
	case <-extractor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first extraction did not start")
	}

	fileID, _, _, _ := mustFileForPath(t, b, "/img/stale.png")
	claimed, found, err := b.Store().ClaimSemanticTask(context.Background(), time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected initial embed task to exist")
	}
	if err := b.Store().AckSemanticTask(context.Background(), claimed.TaskID, claimed.Receipt); err != nil {
		t.Fatal(err)
	}

	if _, err := b.Write("/img/stale.png", []byte("second-image-bytes"), 0, filesystem.WriteFlagTruncate); err != nil {
		t.Fatal(err)
	}
	close(extractor.release)

	got := waitForContentText(t, b, "/img/stale.png", 3*time.Second)
	if got != "new caption" {
		t.Fatalf("expected final extracted text %q, got %q", "new caption", got)
	}

	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 2 {
		t.Fatalf("semantic task count=%d, want 2", len(tasks))
	}
	if tasks[0].ResourceVersion != 1 || tasks[0].Status != "succeeded" {
		t.Fatalf("stale revision task should stay terminal, got %+v", tasks[0])
	}
	if tasks[1].ResourceVersion != 2 || tasks[1].Status != "queued" {
		t.Fatalf("current revision task should remain queued, got %+v", tasks[1])
	}
}

func TestProcessImageExtractTaskWritesContentTextAndQueuesEmbedTask(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})

	fileID := insertImageFileForExtractTest(t, b, "/img/direct.png", "image/png", []byte("fake-png"))
	result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
		FileID:      fileID,
		Path:        "/img/direct.png",
		ContentType: "image/png",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != ImageExtractResultWritten {
		t.Fatalf("result=%q, want %q", result, ImageExtractResultWritten)
	}

	nf, err := b.Store().Stat(context.Background(), "/img/direct.png")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /img/direct.png: %v", err)
	}
	if !strings.Contains(nf.File.ContentText, "cat on sofa") {
		t.Fatalf("unexpected extracted text: %q", nf.File.ContentText)
	}
	tasks := loadSemanticTasksForFile(t, b, fileID)
	if len(tasks) != 1 {
		t.Fatalf("semantic task count=%d, want 1", len(tasks))
	}
	if tasks[0].TaskType != "embed" || tasks[0].ResourceVersion != 1 || tasks[0].Status != "queued" {
		t.Fatalf("unexpected semantic task: %+v", tasks[0])
	}
}

func TestProcessImageExtractTaskAutoEmbeddingSkipsEmbedBridge(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})

	fileID := insertImageFileForExtractTest(t, b, "/img/direct-auto.png", "image/png", []byte("fake-png"))
	result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
		FileID:      fileID,
		Path:        "/img/direct-auto.png",
		ContentType: "image/png",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != ImageExtractResultWritten {
		t.Fatalf("result=%q, want %q", result, ImageExtractResultWritten)
	}

	nf, err := b.Store().Stat(context.Background(), "/img/direct-auto.png")
	if err != nil || nf.File == nil {
		t.Fatalf("stat /img/direct-auto.png: %v", err)
	}
	if !strings.Contains(nf.File.ContentText, "cat on sofa") {
		t.Fatalf("unexpected extracted text: %q", nf.File.ContentText)
	}
	if tasks := loadSemanticTasksForFile(t, b, fileID); len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0", len(tasks))
	}
}

func TestProcessImageExtractTaskTooLargeReturnsTerminalResult(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:       true,
			Workers:       1,
			QueueSize:     8,
			MaxImageBytes: 4,
			Extractor:     &staticImageExtractor{text: "should not run"},
		},
	})

	fileID := insertImageFileForExtractTest(t, b, "/img/large.png", "image/png", []byte("12345"))
	result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
		FileID:      fileID,
		Path:        "/img/large.png",
		ContentType: "image/png",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != ImageExtractResultTooLarge {
		t.Fatalf("result=%q, want %q", result, ImageExtractResultTooLarge)
	}
}

func insertImageFileForExtractTest(t *testing.T, b *Dat9Backend, path, contentType string, data []byte) string {
	t.Helper()
	fileID := b.genID()
	now := time.Now().UTC()
	err := b.Store().InTx(context.Background(), func(tx *sql.Tx) error {
		if err := b.Store().InsertFileTx(tx, &datastore.File{
			FileID:         fileID,
			StorageType:    datastore.StorageDB9,
			StorageRef:     "inline",
			ContentBlob:    append([]byte(nil), data...),
			ContentType:    contentType,
			SizeBytes:      int64(len(data)),
			Revision:       1,
			Status:         datastore.StatusConfirmed,
			CreatedAt:      now,
			ConfirmedAt:    &now,
			ChecksumSHA256: "",
		}); err != nil {
			return err
		}
		if err := b.Store().EnsureParentDirsTx(tx, path, b.genID); err != nil {
			return err
		}
		return b.Store().InsertNodeTx(tx, &datastore.FileNode{
			NodeID:     b.genID(),
			Path:       path,
			ParentPath: pathutil.ParentPath(path),
			Name:       pathutil.BaseName(path),
			FileID:     fileID,
			CreatedAt:  now,
		})
	})
	if err != nil {
		t.Fatalf("insert image file %s: %v", path, err)
	}
	return fileID
}

func waitForContentText(t *testing.T, b *Dat9Backend, path string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nf, err := b.store.Stat(backgroundWithTrace(), path)
		if err == nil && nf.File != nil {
			if strings.TrimSpace(nf.File.ContentText) != "" {
				return nf.File.ContentText
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	nf, err := b.store.Stat(backgroundWithTrace(), path)
	if err != nil || nf.File == nil {
		t.Fatalf("file not found while waiting for extracted text: path=%s err=%v", path, err)
	}
	t.Fatalf("timed out waiting for extracted text, last value=%q", nf.File.ContentText)
	return ""
}
