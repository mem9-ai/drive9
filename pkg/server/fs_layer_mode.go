package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	backendpkg "github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
)

// The owner-only chmod policy for the fs layer surface.
//
// A layer entry can carry permission bits, so staging entries and committing them is a
// second way to change a path's mode. The policy is the one handleChmod enforces: only a
// token that may chmod may change an *existing* path's bits. A mode on a path that does
// not exist yet is a creation mode, which the same token may already set through create
// and mkdir.
//
// The decision is made in three places, all through the predicates below so they cannot
// disagree: at entry write (with the layer's stored plan as context), over the whole plan
// at commit, and per entry at apply time. Plan order matters — an upsert at a path an
// earlier entry vacated (rename source or whiteout) creates a new file, it does not
// rewrite the live inode's mode.

// errChmodForbidden is the apply-time twin of the HTTP-layer chmod denial: a layer entry
// reached the backend that would change an existing path's permission bits and the
// caller is not allowed to chmod. The handlers reject such a plan before applying
// anything, so this fires only if one of those checks is ever bypassed.
var errChmodForbidden = errors.New(chmodOwnerOnlyMessage)

// defaultFSLayerEntryMode stamps the mode a layer entry gets when the caller names none.
// It is decided here, before any authorization check, so the entry-write gate, the stored
// row and the commit-time scan all see the same value. datastore uses the same defaults
// when a row reaches it without a mode.
func defaultFSLayerEntryMode(entry *datastore.FSLayerEntry) {
	if entry == nil || entry.Mode != 0 {
		return
	}
	entry.Mode = datastore.FSLayerDefaultFileMode
	if entry.Kind == datastore.FSLayerEntryKindDir || entry.Op == datastore.FSLayerEntryOpMkdir {
		entry.Mode = datastore.FSLayerDefaultDirMode
	}
}

// fsLayerEntryCarriesMode reports whether the entry can carry permission bits at all.
// One spelling for all three call sites, so a future op that carries a mode is added in
// one place instead of three.
func fsLayerEntryCarriesMode(entry *datastore.FSLayerEntry) bool {
	if entry == nil {
		return false
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpChmod:
		return entry.Mode != 0
	case datastore.FSLayerEntryOpUpsert, datastore.FSLayerEntryOpMkdir:
		return entry.Mode != 0
	default:
		return false
	}
}

// fsLayerEntryModeChange reports whether plan[index] would change an existing path's
// permission bits.
//
// Only entries *before* index matter: they are the ones that can have vacated the path or
// moved the live inode it names. An upsert placed before a rename still overwrites the
// live inode, and stays a mode change.
func fsLayerEntryModeChange(ctx context.Context, b *backendpkg.Dat9Backend, plan []datastore.FSLayerEntry, index int) (bool, error) {
	if b == nil || index < 0 || index >= len(plan) {
		return false, nil
	}
	entry := &plan[index]
	if !fsLayerEntryCarriesMode(entry) {
		return false, nil
	}
	if entry.Op == datastore.FSLayerEntryOpChmod {
		// An explicit mode-only entry targets whatever path it names.
		return true, nil
	}
	earlier := plan[:index]
	if fsLayerPathVacatedBy(earlier, entry.Path) {
		// The path an earlier entry vacated is created fresh here, so the mode is a
		// creation mode.
		return false, nil
	}
	// A file moved by a directory rename is still the same inode: resolve the
	// destination back to its source before falling back to live main.
	if source, ok := fsLayerRenameSourceForPath(earlier, entry.Path); ok {
		nf, err := b.StatNodeCtx(ctx, source)
		if err == nil {
			return fsLayerNodeMode(nf)&0o777 != entry.Mode&0o777, nil
		}
		if !errors.Is(err, datastore.ErrNotFound) {
			return false, err
		}
	}
	nf, err := b.StatNodeCtx(ctx, entry.Path)
	if errors.Is(err, datastore.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return fsLayerNodeMode(nf)&0o777 != entry.Mode&0o777, nil
}

// fsLayerPlanChangesExistingMode reports whether committing the plan would change an
// existing path's permission bits.
func fsLayerPlanChangesExistingMode(ctx context.Context, b *backendpkg.Dat9Backend, plan []datastore.FSLayerEntry) (bool, error) {
	for i := range plan {
		changes, err := fsLayerEntryModeChange(ctx, b, plan, i)
		if err != nil {
			return false, err
		}
		if changes {
			return true, nil
		}
	}
	return false, nil
}

// fsLayerPathVacatedBy reports whether an earlier plan entry removed the path (or an
// ancestor directory) from live main: a whiteout at or above it, or a rename whose source
// is it or an ancestor.
func fsLayerPathVacatedBy(earlier []datastore.FSLayerEntry, path string) bool {
	path = strings.TrimSuffix(path, "/")
	for i := range earlier {
		entry := &earlier[i]
		switch entry.Op {
		case datastore.FSLayerEntryOpWhiteout:
			source := strings.TrimSuffix(entry.Path, "/")
			if source == path || strings.HasPrefix(path, source+"/") {
				return true
			}
		case datastore.FSLayerEntryOpRename:
			source := strings.TrimSuffix(entry.Path, "/")
			if source == path || strings.HasPrefix(path, source+"/") {
				return true
			}
		}
	}
	return false
}

// fsLayerRenameSourceForPath maps a rename destination back to the live source path it
// originated from, so a descendant of a renamed directory is treated as an existing
// inode rather than a new file created under the destination name.
func fsLayerRenameSourceForPath(entries []datastore.FSLayerEntry, path string) (string, bool) {
	for i := range entries {
		entry := &entries[i]
		if entry.Op != datastore.FSLayerEntryOpRename {
			continue
		}
		target := strings.TrimSuffix(fsLayerRenameTarget(entry), "/")
		if target == "" {
			continue
		}
		if path != target && !strings.HasPrefix(path, target+"/") {
			continue
		}
		source := strings.TrimSuffix(entry.Path, "/") + strings.TrimPrefix(path, target)
		return source, true
	}
	return "", false
}

// fsLayerNodeMode is the permission bits a stat row reports, preferring the file row
// when it carries one.
func fsLayerNodeMode(nf *datastore.NodeWithFile) uint32 {
	if nf == nil {
		return 0
	}
	if nf.File != nil && nf.File.Mode != 0 {
		return nf.File.Mode
	}
	return nf.Mode
}

// authorizeFSLayerEntryMode rejects staging an entry that would change an existing
// path's permission bits unless the caller may chmod. The layer's stored entries are the
// plan prefix, so a path an earlier rename or whiteout vacated is treated as a creation
// here exactly as it will be at commit.
func (s *Server) authorizeFSLayerEntryMode(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store, layer *datastore.FSLayer, candidate *datastore.FSLayerEntry) bool {
	if !fsLayerEntryCarriesMode(candidate) || ScopeFromContext(r.Context()).CanChmod() {
		return true
	}
	stored, err := store.ListFSLayerEntryLog(r.Context(), layer.LayerID)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return false
	}
	plan := make([]datastore.FSLayerEntry, 0, len(stored)+1)
	plan = append(plan, stored...)
	plan = append(plan, *candidate)
	changes, err := fsLayerEntryModeChange(r.Context(), b, plan, len(plan)-1)
	if err != nil {
		writeBackendError(w, r, err)
		return false
	}
	if changes {
		errJSON(w, http.StatusForbidden, chmodOwnerOnlyMessage)
		return false
	}
	return true
}
