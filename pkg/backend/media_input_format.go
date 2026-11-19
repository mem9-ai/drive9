package backend

import "fmt"

// mediaInputDemuxer pins the ffmpeg demuxer for one of the MIME types the
// extraction routers accept. Pinning matters because ffmpeg selects a demuxer
// by content probing, not by the file extension we chose: a payload that is
// actually an HLS playlist (or a concat script) would otherwise be handled by
// the hls/concat demuxers, which can read unexpected local files and fetch
// remote URLs while the process holds tenant bytes.
//
// The set is closed to the audio and video allowlists (allowedAudioMIME,
// allowedVideoMIME). Anything else is an error, never an unpinned fallback.
//
// This mirrors the contract `fs` already ships: the same mapping, so a format
// change lands in both repositories instead of drifting.
func mediaInputDemuxer(mime string) (string, error) {
	switch stripMIMEParams(mime) {
	case "video/mp4", "video/quicktime", "application/mp4":
		// resolvedAudioExtractContentType keeps application/mp4 for MP4 audio.
		return "mov", nil
	case "video/x-msvideo":
		return "avi", nil
	case "video/webm", "video/x-matroska":
		return "matroska", nil
	case "audio/mpeg":
		return "mp3", nil
	case "audio/wav", "audio/x-wav":
		return "wav", nil
	case "audio/mp4", "audio/x-m4a":
		// m4a is in the mov/mp4 demuxer family.
		return "mov", nil
	default:
		return "", fmt.Errorf("media input format %q has no pinned demuxer", stripMIMEParams(mime))
	}
}

// mediaInputArgs is the argv slice every media probe/decode shares: protocols
// restricted to the local file we staged, the demuxer pinned to the routed
// container, then the input path. Restricting protocols is defense in depth:
// a pinned demuxer should not open a nested protocol, but if one does it can
// only reach the filesystem, never the network.
func mediaInputArgs(demuxer, path string) []string {
	return []string{"-protocol_whitelist", "file", "-f", demuxer, "-i", path}
}

// mediaToolEnv is the environment a media tool runs with: the directory the
// tenant bytes were staged in, a fixed locale, and nothing inherited from the
// API server. ffmpeg reads a few environment variables (report files, temporary
// paths, resource limits), and a server that carries master keys and tenant
// credentials in its own environment has no reason to hand them to a decoder.
func mediaToolEnv(dir string) []string {
	return []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"HOME=" + dir,
		"TMPDIR=" + dir,
		"LANG=C",
		"LC_ALL=C",
	}
}
