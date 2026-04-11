package backend

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/c4pt0r/agfs/agfs-server/pkg/filesystem"
)

type staticAudioExtractor struct {
	text string
	err  error
}

func (e *staticAudioExtractor) ExtractAudioText(ctx context.Context, req AudioExtractRequest) (string, error) {
	if e.err != nil {
		return "", e.err
	}
	return e.text, nil
}

func TestProcessAudioExtractTaskWritesContentText(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "spoken keyword in transcript"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/clip.mp3", "audio/mpeg", []byte{0xff, 0xf3, 0x80})
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/clip.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultWritten {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultWritten)
	}
	got := waitForContentText(t, b, "/rec/clip.mp3", time.Second)
	if !strings.Contains(got, "keyword") {
		t.Fatalf("unexpected content_text: %q", got)
	}
}

func TestEffectiveAudioMIMEAllowlistedMP3AndWAV(t *testing.T) {
	tests := []struct {
		path, ct, want string
	}{
		{"/x.mp3", "audio/mpeg", "audio/mpeg"},
		{"/x.wav", "audio/wav", "audio/wav"},
		{"/x.wav", "audio/x-wav", "audio/x-wav"},
		{"/sub/clip.mp3", "application/octet-stream", "audio/mpeg"},
		{"/sub/clip.wav", "application/octet-stream", "audio/wav"},
		{"/x.m4a", "application/octet-stream", ""},
		{"/x.m4a", "audio/mp4a-latm", ""},
	}
	for _, tc := range tests {
		got := effectiveAudioMIME(tc.path, tc.ct)
		if got != tc.want {
			t.Fatalf("effectiveAudioMIME(%q,%q)=%q, want %q", tc.path, tc.ct, got, tc.want)
		}
	}
}

func TestProcessAudioExtractTaskStaleRevision(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "should not apply"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/stale.mp3", "audio/mpeg", []byte{1, 2, 3})
	if _, err := b.Store().DB().Exec(`UPDATE files SET revision = 2 WHERE file_id = ?`, fileID); err != nil {
		t.Fatal(err)
	}
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/stale.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultStale {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultStale)
	}
}

func TestProcessAudioExtractTaskStaleBeforeNotAudioWhenTypeChanged(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "must not apply"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/type-changed.mp3", "audio/mpeg", []byte{1, 2, 3})
	if _, err := b.Store().DB().Exec(`UPDATE files SET revision = 2, content_type = ? WHERE file_id = ?`, "text/plain", fileID); err != nil {
		t.Fatal(err)
	}
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/type-changed.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultStale {
		t.Fatalf("result=%q, want %q (revision mismatch must win over not_audio)", result, AudioExtractResultStale)
	}
}

func TestProcessAudioExtractTaskUsesPayloadMIMEWhenStoredGenericAndExtensionless(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "payload hint transcript"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/extensionless-audio", "application/octet-stream", []byte{0xff, 0xf3, 0x80})
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/extensionless-audio",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultWritten {
		t.Fatalf("result=%q, want %q (stored generic MIME must combine with payload hint)", result, AudioExtractResultWritten)
	}
	got := waitForContentText(t, b, "/rec/extensionless-audio", time.Second)
	if !strings.Contains(got, "payload hint") {
		t.Fatalf("unexpected content_text: %q", got)
	}
}

func TestProcessAudioExtractTaskNonGenericStoredIgnoresConflictingPayloadMIME(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "must not run"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/wrong-type.bin", "image/png", []byte{1, 2, 3, 4})
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/wrong-type.bin",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultNotAudio {
		t.Fatalf("result=%q, want %q (specific non-generic stored type must not be overridden by payload)", result, AudioExtractResultNotAudio)
	}
}

func TestProcessAudioExtractTaskZeroByteSourceNotReportedAsTooLarge(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "empty blob ok"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/zero.mp3", "audio/mpeg", []byte{})
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/zero.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultWritten {
		t.Fatalf("result=%q, want %q (0-byte payload is not oversize)", result, AudioExtractResultWritten)
	}
}

func TestProcessAudioExtractTaskNotAudio(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "x"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/note.txt", "text/plain", []byte("hello"))
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/note.txt",
		ContentType: "text/plain",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultNotAudio {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultNotAudio)
	}
}

func TestProcessAudioExtractTaskTooLarge(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:       true,
			MaxAudioBytes: 4,
			Extractor:     &staticAudioExtractor{text: "nope"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/big.mp3", "audio/mpeg", []byte("12345"))
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/big.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultTooLarge {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultTooLarge)
	}
}

func TestProcessAudioExtractTaskEmptyText(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "   \n  "},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/empty.mp3", "audio/mpeg", []byte{1})
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/empty.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultEmptyText {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultEmptyText)
	}
}

func TestProcessAudioExtractTaskRuntimeNotConfigured(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{DatabaseAutoEmbedding: true})
	fileID := insertImageFileForExtractTest(t, b, "/rec/nort.mp3", "audio/mpeg", []byte{1})
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/nort.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if result != AudioExtractResultRuntimeNotConfigured {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultRuntimeNotConfigured)
	}
	if err == nil {
		t.Fatal("expected retryable error")
	}
}

func TestProcessAudioExtractTaskExtractErrorRetries(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{err: context.Canceled},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/rec/fail.mp3", "audio/mpeg", []byte{1})
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/fail.mp3",
		ContentType: "audio/mpeg",
		Revision:    1,
	})
	if result != AudioExtractResultExtractError {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultExtractError)
	}
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestProcessAudioExtractTaskLoadsFromS3(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "s3 keyword"},
		},
	})
	data := make([]byte, 60_000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if _, err := b.Write("/rec/huge.mp3", data, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, rev, _, _ := mustFileForPath(t, b, "/rec/huge.mp3")
	result, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID:      fileID,
		Path:        "/rec/huge.mp3",
		ContentType: "audio/mpeg",
		Revision:    rev,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != AudioExtractResultWritten {
		t.Fatalf("result=%q, want %q", result, AudioExtractResultWritten)
	}
	got := waitForContentText(t, b, "/rec/huge.mp3", 2*time.Second)
	if !strings.Contains(got, "s3 keyword") {
		t.Fatalf("unexpected content_text: %q", got)
	}
}

func TestGrepFindsAudioTranscript(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "unique transcript token zyx"},
		},
	})
	if _, err := b.Write("/vo/clip.mp3", []byte{0xff, 0xf3, 0x80}, 0, filesystem.WriteFlagCreate); err != nil {
		t.Fatal(err)
	}
	fileID, rev, _, _ := mustFileForPath(t, b, "/vo/clip.mp3")
	if _, err := b.ProcessAudioExtractTask(context.Background(), AudioExtractTaskSpec{
		FileID: fileID, Path: "/vo/clip.mp3", ContentType: "audio/mpeg", Revision: rev,
	}); err != nil {
		t.Fatal(err)
	}
	results, err := b.Grep(context.Background(), "zyx", "/vo/", 20)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range results {
		if r.Path == "/vo/clip.mp3" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("grep should find audio transcript path, got %+v", results)
	}
}
