package backend

import (
	"strings"
	"testing"
)

// TestMediaInputDemuxerCoversEveryAllowedMIME keeps the demuxer pin in sync
// with the routing allowlists: every MIME the extractors accept must resolve to
// a pinned ffmpeg demuxer, otherwise a file could fall back to content probing
// (hls/concat) — the behavior the pin exists to prevent.
func TestMediaInputDemuxerCoversEveryAllowedMIME(t *testing.T) {
	for mime := range allowedVideoMIME {
		if _, err := mediaInputDemuxer(mime); err != nil {
			t.Errorf("video allowlist MIME %q has no pinned demuxer: %v", mime, err)
		}
	}
	for mime := range allowedAudioMIME {
		if _, err := mediaInputDemuxer(mime); err != nil {
			t.Errorf("audio allowlist MIME %q has no pinned demuxer: %v", mime, err)
		}
	}
}

func TestMediaInputDemuxerMappings(t *testing.T) {
	for _, tc := range []struct{ mime, want string }{
		{"video/mp4", "mov"},
		{"video/quicktime; codecs=h264", "mov"},
		{"application/mp4", "mov"},
		{"video/x-msvideo", "avi"},
		{"video/webm", "matroska"},
		{"video/x-matroska", "matroska"},
		{"audio/mpeg", "mp3"},
		{"audio/wav", "wav"},
		{"audio/x-wav", "wav"},
		{"audio/mp4", "mov"},
		{"audio/x-m4a", "mov"},
	} {
		got, err := mediaInputDemuxer(tc.mime)
		if err != nil {
			t.Fatalf("mediaInputDemuxer(%q): %v", tc.mime, err)
		}
		if got != tc.want {
			t.Fatalf("mediaInputDemuxer(%q) = %q, want %q", tc.mime, got, tc.want)
		}
	}
}

func TestMediaInputDemuxerRejectsUnpinnedFormats(t *testing.T) {
	for _, mime := range []string{"", "application/vnd.apple.mpegurl", "video/mp2t", "text/plain"} {
		if got, err := mediaInputDemuxer(mime); err == nil {
			t.Fatalf("mediaInputDemuxer(%q) = %q, want error (unpinned fallback)", mime, got)
		}
	}
}

// TestMediaInputArgsRestrictProtocolsAndPinFormat pins the argv contract: the
// local file protocol is the only one allowed and the demuxer is named before
// -i, so a playlist payload cannot switch ffmpeg to hls and reach the network.
func TestMediaInputArgsRestrictProtocolsAndPinFormat(t *testing.T) {
	got := strings.Join(mediaInputArgs("mov", "/tmp/input.mp4"), " ")
	want := "-protocol_whitelist file -f mov -i /tmp/input.mp4"
	if got != want {
		t.Fatalf("mediaInputArgs = %q, want %q", got, want)
	}
}
