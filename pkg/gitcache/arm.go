package gitcache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// WorkspaceArmedPath returns the local armed marker for a LocalRoot.
// Presence (or refresh/ directory activity) tells a running FUSE mount to arm
// git-workspace discovery without polling the backend.
func WorkspaceArmedPath(localRoot string) string {
	return filepath.Join(strings.TrimSpace(localRoot), "git-workspaces", "armed")
}

// WorkspaceRefreshDir returns the directory that holds per-workspace refresh markers.
func WorkspaceRefreshDir(localRoot string) string {
	return filepath.Join(strings.TrimSpace(localRoot), "git-workspaces", "refresh")
}

// WorkspaceDeletedDir returns the directory that holds per-workspace deleted markers.
func WorkspaceDeletedDir(localRoot string) string {
	return filepath.Join(strings.TrimSpace(localRoot), "git-workspaces", "deleted")
}

// TouchWorkspaceArmed writes/updates the local armed marker so a live FUSE
// mount can discover a newly registered git workspace without remote polling.
func TouchWorkspaceArmed(ctx context.Context, localRoot string) error {
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
	path := WorkspaceArmedPath(localRoot)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create git workspace armed dir %q: %w", filepath.Dir(path), err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.WriteFile(path, []byte(time.Now().UTC().Format(time.RFC3339Nano)+"\n"), 0o644); err != nil {
		return fmt.Errorf("write git workspace armed marker %q: %w", path, err)
	}
	return nil
}

// LocalArmSignal reports whether local markers indicate git workspaces should
// be armed. This is directory-level and works with an empty loaded workspace
// list (unlike per-ID refresh marker scans).
//
// lastScanMtime is the previously observed max mtime of armed/refresh signals
// (zero if never scanned). Returns (armed, newMaxMtime).
func LocalArmSignal(ctx context.Context, localRoot string, lastScanMtime time.Time) (bool, time.Time) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return false, lastScanMtime
	}
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return false, lastScanMtime
	}

	var maxMtime time.Time
	consider := func(p string) {
		info, err := os.Stat(p)
		if err != nil {
			return
		}
		mt := info.ModTime()
		if mt.After(maxMtime) {
			maxMtime = mt
		}
	}

	// armed file
	consider(WorkspaceArmedPath(localRoot))

	// any refresh/<id> marker or the refresh directory itself
	refreshDir := WorkspaceRefreshDir(localRoot)
	consider(refreshDir)
	if entries, err := os.ReadDir(refreshDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			info, err := e.Info()
			if err != nil {
				continue
			}
			mt := info.ModTime()
			if mt.After(maxMtime) {
				maxMtime = mt
			}
		}
		if len(entries) > 0 && maxMtime.IsZero() {
			// Directory exists with entries but mtimes unreadable — still a signal.
			maxMtime = time.Now()
		}
	}

	if maxMtime.IsZero() {
		return false, lastScanMtime
	}
	if lastScanMtime.IsZero() {
		// First observation of any marker is an arm signal.
		return true, maxMtime
	}
	if maxMtime.After(lastScanMtime) {
		return true, maxMtime
	}
	// Marker still present (armed file or non-empty refresh/) → stay armed-capable.
	if _, err := os.Stat(WorkspaceArmedPath(localRoot)); err == nil {
		return true, maxMtime
	}
	if entries, err := os.ReadDir(refreshDir); err == nil && len(entries) > 0 {
		return true, maxMtime
	}
	return false, maxMtime
}

// MarkWorkspaceRegistered updates local signals after a successful remote
// git-workspace registration: refresh marker for the id, clear deleted, touch armed.
func MarkWorkspaceRegistered(ctx context.Context, localRoot, workspaceID string) error {
	if err := ClearWorkspaceDeleted(ctx, localRoot, workspaceID); err != nil {
		return err
	}
	return TouchWorkspaceArmed(ctx, localRoot)
}
