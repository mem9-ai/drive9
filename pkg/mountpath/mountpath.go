// Package mountpath provides path mapping between a local mount namespace
// and a remote drive9 subtree. It is used by both FUSE and WebDAV mount
// backends to translate local paths to remote paths and vice versa.
package mountpath

import (
	"fmt"
	"path"
	"strings"
)

// NormalizeRoot canonicalizes a remote root path. It must be an absolute
// path with no ".." components that escape above "/". An empty input
// defaults to "/".
func NormalizeRoot(root string) (string, error) {
	if root == "" {
		return "/", nil
	}
	if !strings.HasPrefix(root, "/") {
		return "", fmt.Errorf("remote root must be an absolute path: %q", root)
	}
	cleaned := path.Clean(root)
	if cleaned == "." {
		cleaned = "/"
	}
	return cleaned, nil
}

// ToRemote maps a local path (relative to the mount root) to a remote
// drive9 path by joining remoteRoot and localPath. The local path is
// canonicalized first; any attempt to escape above "/" via ".." is
// clamped to "/", which maps to remoteRoot itself.
//
// remoteRoot must already be normalized via NormalizeRoot.
func ToRemote(remoteRoot, localPath string) string {
	local := path.Clean("/" + localPath)
	if remoteRoot == "/" {
		return local
	}
	if local == "/" {
		return remoteRoot
	}
	return remoteRoot + local
}

// ToLocal maps a remote drive9 path back to a local path relative to
// the mount root. It returns the local path and true if the remote path
// is within the subtree, or ("", false) if it is outside scope.
//
// remoteRoot must already be normalized via NormalizeRoot.
func ToLocal(remoteRoot, remotePath string) (string, bool) {
	remote := path.Clean(remotePath)
	if remoteRoot == "/" {
		return remote, true
	}
	if remote == remoteRoot {
		return "/", true
	}
	prefix := remoteRoot + "/"
	if strings.HasPrefix(remote, prefix) {
		return "/" + remote[len(prefix):], true
	}
	return "", false
}
