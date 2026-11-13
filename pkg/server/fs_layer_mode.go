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
// What a path names at a given point in the plan is not live main: a rename moves an
// inode and takes its name away, a whiteout removes one, and a rename into a path an
// earlier entry vacated re-occupies it. So the policy replays the plan's rename/whiteout
// entries — and only those; their content is irrelevant — into a map from path to the
// live-main inode that name now refers to, and decides each mode-carrying entry against
// that view. Entry write, commit scan and apply-time guard all use this one simulation so
// they cannot disagree.

// errChmodForbidden is the apply-time twin of the HTTP-layer chmod denial: a layer entry
// reached the backend that would change an existing path's permission bits and the
// caller is not allowed to chmod. The handlers reject such a plan before applying
// anything, so this fires only if one of those checks is ever bypassed.
var errChmodForbidden = errors.New(chmodOwnerOnlyMessage)

// defaultFSLayerEntryMode stamps the mode a layer entry gets when the caller names none.
// It is decided here, before any authorization check, so the entry-write gate, the stored
// row and the commit-time scan all see the same value. The defaults are the store's own
// constants, and the directory condition matches normalizeFSLayerEntry's.
func defaultFSLayerEntryMode(entry *datastore.FSLayerEntry) {
	if entry == nil || entry.Mode != 0 {
		return
	}
	entry.Mode = datastore.FSLayerDefaultFileMode
	// The store derives Kind from Op before its own default, so `op=mkdir` with no kind is
	// a directory there and must be one here too.
	if entry.Kind == datastore.FSLayerEntryKindDir || entry.Op == datastore.FSLayerEntryOpMkdir {
		entry.Mode = datastore.FSLayerDefaultDirMode
	}
}

// fsLayerEntryCarriesMode reports whether the entry can carry permission bits at all.
func fsLayerEntryCarriesMode(entry *datastore.FSLayerEntry) bool {
	if entry == nil {
		return false
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpChmod, datastore.FSLayerEntryOpUpsert, datastore.FSLayerEntryOpMkdir:
		return entry.Mode != 0
	default:
		return false
	}
}

// fsLayerPlanNode is what a path names after the plan entries replayed so far.
type fsLayerPlanNode struct {
	// livePath is the live-main path of the inode this name refers to, or "" when the
	// name holds an inode the plan itself created.
	livePath string
	// created marks an inode the plan made; vacated marks a name the plan emptied.
	created bool
	vacated bool
}

// fsLayerPlanState replays rename/whiteout entries into the path -> inode view.
type fsLayerPlanState map[string]fsLayerPlanNode

// resolve reports what rawPath names, following the longest mapped ancestor so a moved
// directory keeps its identity for the paths below it.
func (st fsLayerPlanState) resolve(rawPath string) (fsLayerPlanNode, bool) {
	path := strings.TrimSuffix(rawPath, "/")
	node, ok := st[path]
	if ok {
		return node, true
	}
	for ancestor := path; ; {
		idx := strings.LastIndex(ancestor, "/")
		if idx <= 0 {
			return fsLayerPlanNode{}, false
		}
		ancestor = ancestor[:idx]
		if node, ok := st[ancestor]; ok {
			if node.vacated || node.created {
				return node, true
			}
			return fsLayerPlanNode{livePath: node.livePath + strings.TrimPrefix(path, ancestor)}, true
		}
	}
}

// prune drops the name and every name below it: after a move or a whiteout, the entries
// recorded for its descendants describe paths that no longer exist.
func (st fsLayerPlanState) prune(rawPath string) {
	path := strings.TrimSuffix(rawPath, "/")
	for key := range st {
		if key == path || strings.HasPrefix(key, path+"/") {
			delete(st, key)
		}
	}
}

// applyMove replays one rename or whiteout.
func (st fsLayerPlanState) applyMove(ctx context.Context, b *backendpkg.Dat9Backend, move datastore.FSLayerPlanMove) error {
	source := strings.TrimSuffix(move.Path, "/")
	switch move.Op {
	case datastore.FSLayerEntryOpWhiteout:
		st.prune(source)
		st[source] = fsLayerPlanNode{vacated: true}
		return nil
	case datastore.FSLayerEntryOpRename:
	default:
		return nil
	}
	target := strings.TrimSuffix(move.Target, "/")
	if target == "" {
		// A rename without a target moves nothing.
		return nil
	}
	if target == source {
		// Every layer rename target is validated to be a canonical path, so source ==
		// target is a legal request. The backend answers it as a successful no-op
		// (RenameFileNoReplaceCtx returns early, preflightFSLayerRename allows it), so
		// the path keeps its inode: marking it vacated here would classify the next
		// upsert as a creation and let a scoped token chmod a live file.
		return nil
	}
	resolved, ok := st.resolve(source)
	if !ok {
		// First mention of the source: ask live main what it holds. A source the plan
		// created (or already vacated) needs no stat.
		nf, err := b.StatNodeCtx(ctx, source)
		switch {
		case err == nil:
			resolved = fsLayerPlanNode{livePath: fsLayerNodePath(nf, source)}
		case errors.Is(err, datastore.ErrNotFound):
			resolved = fsLayerPlanNode{created: true}
		default:
			return err
		}
	}
	st.prune(source)
	st.prune(target)
	st[target] = resolved
	st[source] = fsLayerPlanNode{vacated: true}
	return nil
}

// fsLayerNodePath reports the live path a stat row belongs to. The node is authoritative
// when it carries one, because a path can already name a newer inode than the caller's
// snapshot.
func fsLayerNodePath(nf *datastore.NodeWithFile, fallback string) string {
	if nf != nil && nf.Node.Path != "" {
		return strings.TrimSuffix(nf.Node.Path, "/")
	}
	return fallback
}

// classify reports what path names after the moves replayed so far, falling back to live
// main once. A path live main does not have is a plan creation, which is the fact the
// commit loop also records when the entry actually creates it.
func (st fsLayerPlanState) classify(ctx context.Context, b *backendpkg.Dat9Backend, rawPath string) (fsLayerPlanNode, error) {
	if node, ok := st.resolve(rawPath); ok {
		return node, nil
	}
	path := strings.TrimSuffix(rawPath, "/")
	nf, err := b.StatNodeCtx(ctx, path)
	if errors.Is(err, datastore.ErrNotFound) {
		return fsLayerPlanNode{created: true}, nil
	}
	if err != nil {
		return fsLayerPlanNode{}, err
	}
	return fsLayerPlanNode{livePath: fsLayerNodePath(nf, path)}, nil
}

// recordCreated marks a name the plan itself owns, so a later entry at the same path is a
// creation-mode write rather than a chmod of the inode the commit just made.
func (st fsLayerPlanState) recordCreated(rawPath string) {
	st[strings.TrimSuffix(rawPath, "/")] = fsLayerPlanNode{created: true}
}

// entryChangesMode reports whether entry, executed after the moves replayed so far,
// would change an existing path's permission bits. This is the only place the decision is
// written: the entry gate, the commit scan and the apply-time re-check all call it with
// the same state, so they cannot drift.
func (st fsLayerPlanState) entryChangesMode(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) (bool, error) {
	if b == nil || !fsLayerEntryCarriesMode(entry) {
		return false, nil
	}
	node, err := st.classify(ctx, b, entry.Path)
	if err != nil {
		return false, err
	}
	if node.created || node.vacated {
		return false, nil
	}
	if entry.Op == datastore.FSLayerEntryOpChmod {
		// A mode-only entry names a path the plan has not created, so it changes that
		// live inode's bits.
		return true, nil
	}
	return fsLayerModesDiffer(ctx, b, node.livePath, entry.Mode)
}

// replayMove advances the state past one entry, for callers walking a plan.
func (st fsLayerPlanState) replayMove(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) error {
	if entry == nil {
		return nil
	}
	return st.applyMove(ctx, b, fslayerMoveFor(entry))
}

// fsLayerEntryModeChange reports whether entry, executed after moves, would change an
// existing path's permission bits.
func fsLayerEntryModeChange(ctx context.Context, b *backendpkg.Dat9Backend, moves []datastore.FSLayerPlanMove, entry *datastore.FSLayerEntry) (bool, error) {
	state := fsLayerPlanState{}
	for _, move := range moves {
		if err := state.applyMove(ctx, b, move); err != nil {
			return false, err
		}
	}
	return state.entryChangesMode(ctx, b, entry)
}

// fsLayerModesDiffer reports whether the live inode at livePath has permission bits other
// than mode. A missing path is a creation, so it does not differ.
func fsLayerModesDiffer(ctx context.Context, b *backendpkg.Dat9Backend, livePath string, mode uint32) (bool, error) {
	nf, err := b.StatNodeCtx(ctx, livePath)
	if errors.Is(err, datastore.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return fsLayerNodeMode(nf)&0o777 != mode&0o777, nil
}

// fsLayerPlanChangesExistingMode reports whether committing the plan would change an
// existing path's permission bits, replaying the plan once in order.
func fsLayerPlanChangesExistingMode(ctx context.Context, b *backendpkg.Dat9Backend, entries []datastore.FSLayerEntry) (bool, error) {
	state := fsLayerPlanState{}
	for i := range entries {
		entry := &entries[i]
		changes, err := state.entryChangesMode(ctx, b, entry)
		if err != nil {
			return false, err
		}
		if changes {
			return true, nil
		}
		if err := state.replayMove(ctx, b, entry); err != nil {
			return false, err
		}
	}
	return false, nil
}

// fslayerMoveFor projects an entry onto the path-identity move it performs, if any.
func fslayerMoveFor(entry *datastore.FSLayerEntry) datastore.FSLayerPlanMove {
	if entry == nil {
		return datastore.FSLayerPlanMove{}
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpRename:
		return datastore.FSLayerPlanMove{Op: entry.Op, Path: entry.Path, Target: fsLayerRenameTarget(entry)}
	case datastore.FSLayerEntryOpWhiteout:
		return datastore.FSLayerPlanMove{Op: entry.Op, Path: entry.Path}
	default:
		return datastore.FSLayerPlanMove{}
	}
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
// path's permission bits unless the caller may chmod. The layer's rename/whiteout
// entries are the plan prefix — metadata only, so this does not reload staged content —
// and the decision is the same one commit will make.
func (s *Server) authorizeFSLayerEntryMode(w http.ResponseWriter, r *http.Request, b *backendpkg.Dat9Backend, store *datastore.Store, layer *datastore.FSLayer, candidate *datastore.FSLayerEntry) bool {
	if !fsLayerEntryCarriesMode(candidate) || ScopeFromContext(r.Context()).CanChmod() {
		return true
	}
	moves, err := store.ListFSLayerPlanMoves(r.Context(), layer.LayerID)
	if err != nil {
		writeFSLayerStoreError(w, r, err)
		return false
	}
	changes, err := fsLayerEntryModeChange(r.Context(), b, moves, candidate)
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
