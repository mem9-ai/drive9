package gitcache

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
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
// be armed, and an opaque generation token for the marker *set*.
//
// Generation changes when the armed marker body or the set of refresh/<id>
// marker names/bodies changes — not only when FS max mtime advances. That way
// a second MarkWorkspaceRegistered for a new workspace id still forces a FUSE
// re-list on coarse-mtime filesystems (same-second mtimes).
//
// gen is empty when !armed. Compare gen with == only; do not parse it.
func LocalArmSignal(ctx context.Context, localRoot string) (armed bool, gen string) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return false, ""
	}
	localRoot = strings.TrimSpace(localRoot)
	if localRoot == "" {
		return false, ""
	}

	type markerPart struct {
		name string
		body string
	}
	var parts []markerPart

	armedPath := WorkspaceArmedPath(localRoot)
	if data, err := os.ReadFile(armedPath); err == nil {
		parts = append(parts, markerPart{name: "armed", body: string(data)})
	}

	// Any refresh/<id> file marker. An empty refresh/ directory alone is not a
	// signal (avoids re-arming after ClearLocalArmSignals left a bare dir).
	refreshDir := WorkspaceRefreshDir(localRoot)
	if entries, err := os.ReadDir(refreshDir); err == nil {
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			data, err := os.ReadFile(filepath.Join(refreshDir, name))
			if err != nil {
				// Unreadable entry still contributes identity so a new name is seen.
				parts = append(parts, markerPart{name: "refresh/" + name, body: "?"})
				continue
			}
			parts = append(parts, markerPart{name: "refresh/" + name, body: string(data)})
		}
	}

	if len(parts) == 0 {
		return false, ""
	}

	sort.Slice(parts, func(i, j int) bool { return parts[i].name < parts[j].name })
	h := fnv.New64a()
	for _, p := range parts {
		_, _ = h.Write([]byte(p.name))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(p.body))
		_, _ = h.Write([]byte{0})
	}
	return true, fmt.Sprintf("%016x", h.Sum64())
}

// MarkWorkspaceRegistered updates local signals after a successful remote
// git-workspace registration: refresh marker for the id, clear deleted, touch armed.
func MarkWorkspaceRegistered(ctx context.Context, localRoot, workspaceID string) error {
	if err := ClearWorkspaceDeleted(ctx, localRoot, workspaceID); err != nil {
		return err
	}
	return TouchWorkspaceArmed(ctx, localRoot)
}

// ClearLocalArmSignals removes the LocalRoot armed file and refresh markers so a
// mount can stay dormant after the last workspace is deleted. Deleted markers
// under git-workspaces/deleted/ are left intact.
func ClearLocalArmSignals(ctx context.Context, localRoot string) error {
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
	var errs []error
	armed := WorkspaceArmedPath(localRoot)
	if err := os.Remove(armed); err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("remove git workspace armed marker %q: %w", armed, err))
	}
	refreshDir := WorkspaceRefreshDir(localRoot)
	entries, err := os.ReadDir(refreshDir)
	if err != nil && !os.IsNotExist(err) {
		errs = append(errs, fmt.Errorf("read git workspace refresh dir %q: %w", refreshDir, err))
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		p := filepath.Join(refreshDir, e.Name())
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove git workspace refresh marker %q: %w", p, err))
		}
	}
	// Drop the refresh directory itself so LocalArmSignal does not treat an empty
	// dir mtime as an arm signal.
	_ = os.Remove(refreshDir)
	if len(errs) == 0 {
		return nil
	}
	return errors.Join(errs...)
}
