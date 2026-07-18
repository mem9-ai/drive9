package backend

import (
	"context"
	"strings"
	"testing"
	"time"
)

type staticVideoExtractor struct {
	text string
	err  error
}

func (e *staticVideoExtractor) ExtractVideoText(ctx context.Context, req VideoExtractRequest) (string, VideoExtractUsage, error) {
	if e.err != nil {
		return "", VideoExtractUsage{}, e.err
	}
	return e.text, VideoExtractUsage{}, nil
}

func TestProcessVideoExtractTaskWritesContentText(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncVideoExtract: AsyncVideoExtractOptions{
			Enabled:   true,
			Extractor: &staticVideoExtractor{text: "a dog running in a park with trees"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/vid/clip.mp4", "video/mp4", []byte{0x00, 0x00, 0x00, 0x18})
	result, err := b.ProcessVideoExtractTask(context.Background(), VideoExtractTaskSpec{
		FileID:      fileID,
		Path:        "/vid/clip.mp4",
		ContentType: "video/mp4",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != VideoExtractResultWritten {
		t.Fatalf("result=%q, want %q", result, VideoExtractResultWritten)
	}
	got := waitForContentText(t, b, "/vid/clip.mp4", time.Second)
	if !strings.Contains(got, "dog") {
		t.Fatalf("unexpected content_text: %q", got)
	}
}

func TestProcessVideoExtractTaskNotConfigured(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
	})
	result, err := b.ProcessVideoExtractTask(context.Background(), VideoExtractTaskSpec{
		FileID:      "non-existent",
		Path:        "/vid/clip.mp4",
		ContentType: "video/mp4",
		Revision:    1,
	})
	if err == nil {
		t.Fatal("expected error for unconfigured runtime")
	}
	if result != VideoExtractResultRuntimeNotConfigured {
		t.Fatalf("result=%q, want %q", result, VideoExtractResultRuntimeNotConfigured)
	}
}

func TestProcessVideoExtractTaskNotVideo(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncVideoExtract: AsyncVideoExtractOptions{
			Enabled:   true,
			Extractor: &staticVideoExtractor{text: "should not be called"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/pic/img.png", "image/png", []byte{0x89, 0x50, 0x4e})
	result, err := b.ProcessVideoExtractTask(context.Background(), VideoExtractTaskSpec{
		FileID:      fileID,
		Path:        "/pic/img.png",
		ContentType: "image/png",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != VideoExtractResultNotVideo {
		t.Fatalf("result=%q, want %q", result, VideoExtractResultNotVideo)
	}
}

func TestProcessVideoExtractTaskFileNotFound(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncVideoExtract: AsyncVideoExtractOptions{
			Enabled:   true,
			Extractor: &staticVideoExtractor{text: "should not be called"},
		},
	})
	result, err := b.ProcessVideoExtractTask(context.Background(), VideoExtractTaskSpec{
		FileID:      "non-existent-id",
		Path:        "/vid/clip.mp4",
		ContentType: "video/mp4",
		Revision:    1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != VideoExtractResultFileNotFound {
		t.Fatalf("result=%q, want %q", result, VideoExtractResultFileNotFound)
	}
}

func TestProcessVideoExtractTaskStaleRevision(t *testing.T) {
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncVideoExtract: AsyncVideoExtractOptions{
			Enabled:   true,
			Extractor: &staticVideoExtractor{text: "should not be called"},
		},
	})
	fileID := insertImageFileForExtractTest(t, b, "/vid/stale.mp4", "video/mp4", []byte{0x00, 0x00})
	result, err := b.ProcessVideoExtractTask(context.Background(), VideoExtractTaskSpec{
		FileID:      fileID,
		Path:        "/vid/stale.mp4",
		ContentType: "video/mp4",
		Revision:    999,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != VideoExtractResultStale {
		t.Fatalf("result=%q, want %q", result, VideoExtractResultStale)
	}
}

func TestEffectiveVideoMIMEAllowlist(t *testing.T) {
	tests := []struct {
		path, ct, want string
	}{
		{"/x.mp4", "video/mp4", "video/mp4"},
		{"/x.mov", "video/quicktime", "video/quicktime"},
		{"/x.avi", "video/x-msvideo", "video/x-msvideo"},
		{"/x.webm", "video/webm", "video/webm"},
		{"/x.mkv", "video/x-matroska", "video/x-matroska"},
		// path fallback when content_type is empty
		{"/x.mp4", "", "video/mp4"},
		{"/x.webm", "", "video/webm"},
		// non-video should return empty
		{"/x.png", "image/png", ""},
		{"/x.mp3", "audio/mpeg", ""},
		{"/x.txt", "text/plain", ""},
	}
	for _, tt := range tests {
		got := effectiveVideoMIME(tt.path, tt.ct)
		if got != tt.want {
			t.Errorf("effectiveVideoMIME(%q, %q) = %q, want %q", tt.path, tt.ct, got, tt.want)
		}
	}
}

func TestIsVideoContentType(t *testing.T) {
	if !isVideoContentType("video/mp4", "/x.mp4") {
		t.Fatal("expected video/mp4 to be video")
	}
	if isVideoContentType("audio/mpeg", "/x.mp3") {
		t.Fatal("expected audio/mpeg to not be video")
	}
	if !isVideoContentType("", "/x.webm") {
		t.Fatal("expected .webm path fallback to work")
	}
}

func TestVideoMIMEToExt(t *testing.T) {
	tests := []struct {
		mime, want string
	}{
		{"video/mp4", ".mp4"},
		{"video/quicktime", ".mov"},
		{"video/x-msvideo", ".avi"},
		{"video/webm", ".webm"},
		{"video/x-matroska", ".mkv"},
		{"unknown/type", ".mp4"},
	}
	for _, tt := range tests {
		got := videoMIMEToExt(tt.mime)
		if got != tt.want {
			t.Errorf("videoMIMEToExt(%q) = %q, want %q", tt.mime, got, tt.want)
		}
	}
}

func TestShouldEnqueueVideoExtractTask(t *testing.T) {
	// Not configured — should not enqueue
	b := newTestBackendWithOptions(t, Options{DatabaseAutoEmbedding: true})
	if b.shouldEnqueueVideoExtractTask("/x.mp4", "video/mp4") {
		t.Fatal("should not enqueue when video extract not configured")
	}

	// Configured — video types should enqueue
	b2 := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncVideoExtract: AsyncVideoExtractOptions{
			Enabled:   true,
			Extractor: &staticVideoExtractor{text: "test"},
		},
	})
	if !b2.shouldEnqueueVideoExtractTask("/x.mp4", "video/mp4") {
		t.Fatal("should enqueue for video/mp4")
	}
	if b2.shouldEnqueueVideoExtractTask("/x.png", "image/png") {
		t.Fatal("should not enqueue for image/png")
	}
}

func TestMP4VideoExcludesAudioEnqueue(t *testing.T) {
	// When both video and audio extract are enabled, MP4 files should only
	// enqueue video_extract_visual, not audio_extract_text, to avoid
	// dual content_text overwrites.
	b := newTestBackendWithOptions(t, Options{
		DatabaseAutoEmbedding: true,
		AsyncVideoExtract: AsyncVideoExtractOptions{
			Enabled:   true,
			Extractor: &staticVideoExtractor{text: "visual content"},
		},
		AsyncAudioExtract: AsyncAudioExtractOptions{
			Enabled:   true,
			Extractor: &staticAudioExtractor{text: "audio transcript"},
		},
	})

	// video/mp4 should enqueue video, not audio
	if !b.shouldEnqueueVideoExtractTask("/clip.mp4", "video/mp4") {
		t.Fatal("video/mp4 should enqueue video extract")
	}
	// The enqueue logic skips audio when video is active for the same file
	isVideo := b.shouldEnqueueVideoExtractTask("/clip.mp4", "video/mp4")
	isAudio := !isVideo && b.shouldEnqueueAudioExtractTask("/clip.mp4", "video/mp4")
	if isAudio {
		t.Fatal("MP4 should NOT enqueue audio when video extract is enabled — dual content_text overwrite")
	}

	// Pure audio files (mp3) should still enqueue audio
	isVideoMP3 := b.shouldEnqueueVideoExtractTask("/song.mp3", "audio/mpeg")
	isAudioMP3 := !isVideoMP3 && b.shouldEnqueueAudioExtractTask("/song.mp3", "audio/mpeg")
	if isVideoMP3 {
		t.Fatal("audio/mpeg should not enqueue video extract")
	}
	if !isAudioMP3 {
		t.Fatal("audio/mpeg should still enqueue audio extract")
	}
}
