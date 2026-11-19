package backend

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The demuxer is pinned by content type, so a payload cannot choose ffmpeg's
// parser by looking like something else. Every MIME the routers accept must
// resolve, and anything outside the allowlists must be refused rather than
// falling back to content probing.
func TestMediaInputDemuxerPinsAcceptedFormats(t *testing.T) {
	for mime := range allowedVideoMIME {
		if _, err := mediaInputDemuxer(mime); err != nil {
			t.Fatalf("allowed video MIME %q has no pinned demuxer: %v", mime, err)
		}
	}
	for mime := range allowedAudioMIME {
		if _, err := mediaInputDemuxer(mime); err != nil {
			t.Fatalf("allowed audio MIME %q has no pinned demuxer: %v", mime, err)
		}
	}
	for _, tc := range []struct{ mime, want string }{
		{"video/mp4", "mov"},
		{"video/quicktime", "mov"},
		{"application/mp4", "mov"},
		{"video/x-msvideo", "avi"},
		{"video/webm", "matroska"},
		{"video/x-matroska", "matroska"},
		{"audio/mpeg", "mp3"},
		{"audio/wav", "wav"},
		{"audio/x-wav", "wav"},
		{"audio/mp4", "mov"},
		{"audio/x-m4a", "mov"},
		// Parameters and casing must not defeat the pin.
		{"Video/MP4; codecs=avc1", "mov"},
		{" audio/mpeg ", "mp3"},
	} {
		got, err := mediaInputDemuxer(tc.mime)
		if err != nil {
			t.Fatalf("mediaInputDemuxer(%q): %v", tc.mime, err)
		}
		if got != tc.want {
			t.Fatalf("mediaInputDemuxer(%q) = %q, want %q", tc.mime, got, tc.want)
		}
	}
	// A playlist or concat script routed as video must not reach ffmpeg at all.
	for _, mime := range []string{"application/vnd.apple.mpegurl", "application/x-mpegURL", "text/plain", "", "video/x-flv"} {
		if got, err := mediaInputDemuxer(mime); err == nil {
			t.Fatalf("mediaInputDemuxer(%q) = %q, want an error (unpinned fallback)", mime, got)
		}
	}
}

// Every media argv must restrict protocols and pin the demuxer before the input
// path, or ffmpeg is free to probe a container and follow nested references.
func TestMediaInputArgsRestrictProtocolsAndPinDemuxer(t *testing.T) {
	args := mediaInputArgs("mov", "/tmp/staged/input.mp4")
	want := []string{"-protocol_whitelist", "file", "-f", "mov", "-i", "/tmp/staged/input.mp4"}
	if len(args) != len(want) {
		t.Fatalf("mediaInputArgs = %#v, want %#v", args, want)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("mediaInputArgs[%d] = %q, want %q", i, args[i], want[i])
		}
	}
	// The whitelist must be exactly the local file protocol: hls, concat, http and
	// friends are what let a payload read files or fetch URLs.
	for i, arg := range args {
		if arg != "-protocol_whitelist" {
			continue
		}
		if got := args[i+1]; got != "file" {
			t.Fatalf("protocol whitelist = %q, want only \"file\"", got)
		}
	}
}

// A media tool must not inherit the API server's environment: master keys and
// tenant credentials live there, and /proc/self/environ is exactly what a
// decoder escape would read.
func TestMediaToolEnvIsMinimal(t *testing.T) {
	env := mediaToolEnv("/tmp/staged")
	joined := strings.Join(env, "\n")
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		switch name {
		case "PATH", "HOME", "TMPDIR", "LANG", "LC_ALL":
		default:
			t.Fatalf("mediaToolEnv exposed %q", entry)
		}
	}
	if strings.Contains(joined, "DRIVE9_") {
		t.Fatalf("mediaToolEnv carries server configuration: %s", joined)
	}
	// Temp and home paths point at the staging directory, so a tool that writes
	// a report or temporary file cannot reach outside it.
	for _, want := range []string{"HOME=/tmp/staged", "TMPDIR=/tmp/staged"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("mediaToolEnv missing %q: %s", want, joined)
		}
	}
}

// The tool runs with cmd.Dir inside the staging directory, so a configured relative path must
// be resolved against the server's working directory first — otherwise "./tools/ffmpeg" is
// looked up inside the per-request temp dir and the extraction cannot start.
func TestResolveMediaToolPath(t *testing.T) {
	if got := resolveMediaToolPath("ffmpeg"); got != "ffmpeg" {
		t.Fatalf("bare name = %q, want it left for PATH lookup", got)
	}
	absolute := filepath.Join(t.TempDir(), "ffmpeg")
	if got := resolveMediaToolPath(absolute); got != absolute {
		t.Fatalf("absolute path = %q, want %q", got, absolute)
	}
	relative := "." + string(os.PathSeparator) + "tools" + string(os.PathSeparator) + "ffmpeg"
	got := resolveMediaToolPath(relative)
	if !filepath.IsAbs(got) {
		t.Fatalf("relative path = %q, want an absolute path", got)
	}
	if filepath.Base(got) != "ffmpeg" {
		t.Fatalf("relative path = %q, want it to keep the tool name", got)
	}
}
