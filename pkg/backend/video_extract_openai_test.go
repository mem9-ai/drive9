package backend

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The demuxer pin only exists if extractFrames puts it on ffmpeg's command line: a table test
// over mediaInputArgs keeps passing when the call site stops using the helper and hands ffmpeg
// a bare "-i <path>" again. This drives the real function with a stub executable that records
// its argv, so the assertion is on what would actually be executed.
func TestExtractFramesPinsDemuxerOnFfmpegArgv(t *testing.T) {
	dir := t.TempDir()
	argvFile := filepath.Join(dir, "argv")
	stub := filepath.Join(dir, "ffmpeg")
	const script = `#!/bin/sh
for arg in "$@"; do printf '%s\n' "$arg" >> "$D9_TEST_FFMPEG_ARGV"; done
exit 0
`
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub ffmpeg: %v", err)
	}
	t.Setenv("D9_TEST_FFMPEG_ARGV", argvFile)

	e, err := NewOpenAIVideoTextExtractor(OpenAIVideoTextExtractorConfig{
		BaseURL:       "http://127.0.0.1:1",
		APIKey:        "test-key",
		Model:         "test-model",
		FFmpegPath:    stub,
		FrameInterval: 2,
		MaxFrames:     3,
	})
	if err != nil {
		t.Fatalf("new extractor: %v", err)
	}

	// The stub writes no frames, which is fine: this test is about the command that runs, not
	// about the frames a real decoder would produce.
	frames, err := e.extractFrames(context.Background(), []byte("not really an mp4"), "video/mp4")
	if err != nil {
		t.Fatalf("extractFrames: %v", err)
	}
	if len(frames) != 0 {
		t.Fatalf("stub produced no frames, got %d", len(frames))
	}

	raw, err := os.ReadFile(argvFile)
	if err != nil {
		t.Fatalf("ffmpeg was not executed with a recorded argv: %v", err)
	}
	argv := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	// ffmpeg selects a demuxer by probing unless one is named, so these five arguments are the
	// whole fix: protocols restricted to the staged file and the demuxer pinned to the routed
	// container, with the input path after -i.
	wantPrefix := []string{"-protocol_whitelist", "file", "-f", "mov", "-i"}
	if len(argv) < len(wantPrefix) {
		t.Fatalf("ffmpeg argv = %q, want at least %d arguments", argv, len(wantPrefix))
	}
	for i, want := range wantPrefix {
		if argv[i] != want {
			t.Fatalf("ffmpeg argv[%d] = %q, want %q (argv %q)", i, argv[i], want, argv)
		}
	}
	if !strings.HasSuffix(argv[len(wantPrefix)], "input.mp4") {
		t.Fatalf("ffmpeg input = %q, want the staged video file", argv[len(wantPrefix)])
	}
	// The extraction arguments must still follow the input, and frames must go to image2.
	joined := strings.Join(argv[len(wantPrefix)+1:], " ")
	if !strings.Contains(joined, "-vf fps=1/2") || !strings.Contains(joined, "-f image2") {
		t.Fatalf("ffmpeg argv lost its extraction arguments: %q", argv)
	}
}

// A content type outside the allowlist must be refused before ffmpeg runs at all, rather than
// falling back to probing whatever the bytes happen to look like.
func TestExtractFramesRefusesUnpinnedContentTypeBeforeRunningFfmpeg(t *testing.T) {
	dir := t.TempDir()
	argvFile := filepath.Join(dir, "argv")
	stub := filepath.Join(dir, "ffmpeg")
	const script = `#!/bin/sh
for arg in "$@"; do printf '%s\n' "$arg" >> "$D9_TEST_FFMPEG_ARGV"; done
exit 0
`
	if err := os.WriteFile(stub, []byte(script), 0o755); err != nil {
		t.Fatalf("write stub ffmpeg: %v", err)
	}
	t.Setenv("D9_TEST_FFMPEG_ARGV", argvFile)

	e, err := NewOpenAIVideoTextExtractor(OpenAIVideoTextExtractorConfig{
		BaseURL:    "http://127.0.0.1:1",
		APIKey:     "test-key",
		Model:      "test-model",
		FFmpegPath: stub,
	})
	if err != nil {
		t.Fatalf("new extractor: %v", err)
	}

	for _, contentType := range []string{"application/vnd.apple.mpegurl", "application/octet-stream", ""} {
		if _, err := e.extractFrames(context.Background(), []byte("#EXTM3U"), contentType); err == nil {
			t.Fatalf("content type %q was accepted, want a refusal", contentType)
		}
	}
	if _, err := os.Stat(argvFile); err == nil {
		t.Fatalf("ffmpeg ran for a refused content type; argv = %q", mustReadFile(t, argvFile))
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat argv file: %v", err)
	}
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}
