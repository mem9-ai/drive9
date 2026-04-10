//go:build failpoint

package backend

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
)

const imageExtractWritebackUpdateFileSearchTextErrorFailpoint = "github.com/mem9-ai/dat9/pkg/backend/imageExtractWritebackUpdateFileSearchTextError"
const imageExtractWritebackQueueEmbedTaskErrorFailpoint = "github.com/mem9-ai/dat9/pkg/backend/imageExtractWritebackQueueEmbedTaskError"

func TestProcessImageExtractTaskWritebackUpdateFailsWithFailpoint(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})

	fileID := insertImageFileForExtractTest(t, b, "/img/writeback-update-fail.png", "image/png", []byte("fake-png"))
	if err := failpoint.Enable(imageExtractWritebackUpdateFileSearchTextErrorFailpoint, `return("injected update failure")`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(imageExtractWritebackUpdateFileSearchTextErrorFailpoint)
	})

	result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
		FileID:      fileID,
		Path:        "/img/writeback-update-fail.png",
		ContentType: "image/png",
		Revision:    1,
	})
	if result != ImageExtractResultUpdateError {
		t.Fatalf("result=%q, want %q", result, ImageExtractResultUpdateError)
	}
	if err == nil || !strings.Contains(err.Error(), "injected update failure") {
		t.Fatalf("err=%v, want injected update failure", err)
	}

	nf, statErr := b.Store().Stat(context.Background(), "/img/writeback-update-fail.png")
	if statErr != nil || nf.File == nil {
		t.Fatalf("stat /img/writeback-update-fail.png: %v", statErr)
	}
	if strings.TrimSpace(nf.File.ContentText) != "" {
		t.Fatalf("content_text=%q, want empty after rolled back writeback", nf.File.ContentText)
	}
	if tasks := loadSemanticTasksForFile(t, b, fileID); len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0 after writeback update failure", len(tasks))
	}
}

func TestProcessImageExtractTaskWritebackQueueEmbedFailsWithFailpoint(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		AsyncImageExtract: AsyncImageExtractOptions{
			Enabled:   true,
			Workers:   1,
			QueueSize: 8,
			Extractor: &staticImageExtractor{text: "cat on sofa screenshot invoice"},
		},
	})

	fileID := insertImageFileForExtractTest(t, b, "/img/writeback-queue-fail.png", "image/png", []byte("fake-png"))
	if err := failpoint.Enable(imageExtractWritebackQueueEmbedTaskErrorFailpoint, `return("injected queue failure")`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(imageExtractWritebackQueueEmbedTaskErrorFailpoint)
	})

	result, err := b.ProcessImageExtractTask(context.Background(), ImageExtractTaskSpec{
		FileID:      fileID,
		Path:        "/img/writeback-queue-fail.png",
		ContentType: "image/png",
		Revision:    1,
	})
	if result != ImageExtractResultUpdateError {
		t.Fatalf("result=%q, want %q", result, ImageExtractResultUpdateError)
	}
	if err == nil || !strings.Contains(err.Error(), "injected queue failure") {
		t.Fatalf("err=%v, want injected queue failure", err)
	}

	nf, statErr := b.Store().Stat(context.Background(), "/img/writeback-queue-fail.png")
	if statErr != nil || nf.File == nil {
		t.Fatalf("stat /img/writeback-queue-fail.png: %v", statErr)
	}
	if strings.TrimSpace(nf.File.ContentText) != "" {
		t.Fatalf("content_text=%q, want empty after rolled back embed-bridge failure", nf.File.ContentText)
	}
	if tasks := loadSemanticTasksForFile(t, b, fileID); len(tasks) != 0 {
		t.Fatalf("semantic task count=%d, want 0 after embed-bridge failure", len(tasks))
	}
}
