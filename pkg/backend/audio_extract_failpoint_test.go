//go:build failpoint

package backend

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
)

const audioExtractWritebackUpdateFileSearchTextErrorFailpoint = "github.com/mem9-ai/dat9/pkg/backend/audioExtractWritebackUpdateFileSearchTextError"

func TestProcessAudioExtractTaskWritebackUpdateFailsWithFailpoint(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "spoken line"},
		},
	})

	fileID := insertImageFileForExtractTest(t, b, "/rec/writeback-fail.mp3", "audio/mpeg", []byte{1, 2, 3})
	if err := failpoint.Enable(audioExtractWritebackUpdateFileSearchTextErrorFailpoint, `return("injected update failure")`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = failpoint.Disable(audioExtractWritebackUpdateFileSearchTextErrorFailpoint)
	})

	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/writeback-fail.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if result != AudioExtractResultUpdateError {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultUpdateError)
	}
	if err == nil || !strings.Contains(err.Error(), "injected update failure") {
		t.Fatalf("err=%v, want injected update failure", err)
	}

	nf, statErr := b.Store().Stat(context.Background(), "/rec/writeback-fail.mp3")
	if statErr != nil || nf.File == nil {
		t.Fatalf("stat /rec/writeback-fail.mp3: %v", statErr)
	}
	if strings.TrimSpace(nf.File.ContentText) != "" {
		t.Fatalf("content_text=%q, want empty after rolled back writeback", nf.File.ContentText)
	}
}
