package gitcache

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// WorkspacePendingDir returns the directory that holds pre-registration
// markers. A pending marker means "a git workspace is being created at this
// mount-local root but is not registered yet".
func WorkspacePendingDir(localRoot string) string {
	return filepath.Join(strings.TrimSpace(localRoot), "git-workspaces", "pending")
}

// WorkspacePendingMarkerPath returns the pending marker for a mount-local
// workspace root path (for example "/repo", or "/" for the mount root).
//
// The marker name is a flat key derived from the canonical root, not a
// per-segment layout: segment layouts cannot distinguish the mount root "/"
// from a workspace root named "root", and parent/child roots ("/repo" and
// "/repo/sub") collide as file-vs-directory. The canonical root is stored in
// the marker body and re-validated by WorkspacePending, so a hash collision
// cannot misroute `.git` to the wrong root.
func WorkspacePendingMarkerPath(localRoot, mountRoot string) string {
	name := fmt.Sprintf("pending-%016x", pendingMarkerKey(canonicalMountRoot(mountRoot)))
	return filepath.Join(WorkspacePendingDir(localRoot), name)
}

// canonicalMountRoot normalizes a mount-local workspace root to the canonical
// form used for the pending-marker key and body: absolute and slash-clean with
// no trailing slash.
func canonicalMountRoot(mountRoot string) string {
	trimmed := strings.TrimSpace(mountRoot)
	if trimmed == "" {
		return "/"
	}
	if !strings.HasPrefix(trimmed, "/") {
		trimmed = "/" + trimmed
	}
	return path.Clean(trimmed)
}

func pendingMarkerKey(canonicalRoot string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(canonicalRoot))
	return h.Sum64()
}

// MarkWorkspacePending records that a git workspace is being created at
// mountRoot (a mount-local path) before it is registered, so a live mount
// keeps `.git` under that root local instead of uploading it during
// `git clone --fast`.
func MarkWorkspacePending(ctx context.Context, localRoot, mountRoot string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return nil
	}
	marker := WorkspacePendingMarkerPath(localRoot, mountRoot)
	if err := os.MkdirAll(filepath.Dir(marker), 0o755); err != nil {
		return fmt.Errorf("create git workspace pending dir %q: %w", filepath.Dir(marker), err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	body := canonicalMountRoot(mountRoot) + "\n" + time.Now().UTC().Format(time.RFC3339Nano) + "\n"
	if err := os.WriteFile(marker, []byte(body), 0o644); err != nil {
		return fmt.Errorf("write git workspace pending marker %q: %w", marker, err)
	}
	return nil
}

// ClearWorkspacePending removes the pending marker for mountRoot.
func ClearWorkspacePending(ctx context.Context, localRoot, mountRoot string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return nil
	}
	marker := WorkspacePendingMarkerPath(localRoot, mountRoot)
	if err := os.Remove(marker); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove git workspace pending marker %q: %w", marker, err)
	}
	return nil
}

// WorkspacePending reports whether a pending marker exists for mountRoot. The
// marker body must carry the canonical root: a flat hash key could in
// principle collide with a different root, and a body mismatch is treated as
// not pending rather than misrouting `.git`.
func WorkspacePending(ctx context.Context, localRoot, mountRoot string) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return false
	}
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return false
	}
	marker := WorkspacePendingMarkerPath(localRoot, mountRoot)
	info, err := os.Stat(marker)
	if err != nil || info.IsDir() {
		return false
	}
	data, err := os.ReadFile(marker)
	if err != nil {
		return false
	}
	root := canonicalMountRoot(mountRoot)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// The body's first non-empty line is the canonical root.
		return line == root
	}
	return false
}
