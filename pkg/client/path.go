package client

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// windowsDriveSegment matches a Windows drive-letter prefix ("C:\" or "D:/")
// at the start of the path or at the start of any path segment. The segment
// form catches the observed failure mode where a client joins a Windows local
// absolute path onto a drive9 directory, e.g. "/remote/dir/C:\stock".
var windowsDriveSegment = regexp.MustCompile(`(?:^|/)[A-Za-z]:[\\/]`)

// validateFSPath rejects paths the server would refuse with HTTP 400 so the
// caller fails fast with an actionable message instead of a server round
// trip. Validation only: the server remains the canonicalization authority,
// and the original path is sent unchanged when valid.
func validateFSPath(path string) error {
	if _, err := pathutil.Canonicalize(path); err != nil {
		return fmt.Errorf("invalid drive9 path %q: %v%s", path, err, windowsPathHint(path))
	}
	return nil
}

// windowsPathHint returns actionable guidance when an invalid path looks like
// it came from a Windows local filesystem.
func windowsPathHint(path string) string {
	switch {
	case windowsDriveSegment.MatchString(path):
		return "; path looks like a Windows local path — drive9 paths are absolute POSIX paths with no drive letter; strip the drive prefix and convert separators (e.g. filepath.ToSlash) before calling"
	case strings.ContainsRune(path, '\\'):
		return "; drive9 paths use forward slashes — convert Windows-style separators (e.g. filepath.ToSlash) before calling"
	default:
		return ""
	}
}
