package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	backendpkg "github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// The owner-only chmod policy for the fs layer surface.
//
// A layer entry can carry permission bits, so staging entries and committing them is a
// second way to change a path's mode. The policy is the one handleChmod enforces: only a
// token that may chmod may change an *existing* path's bits. A mode on a path that does
// not exist yet is a creation mode, which the same token may already set through create
// and mkdir.
//
// There are two worlds, and conflating them is how the checks drift:
//
//   - Before anything applies, a rename moves an inode and takes its name away, a
//     whiteout removes one, and a rename into a path an earlier entry vacated re-occupies
//     it. The entry gate and the commit scan both decide in this world, replaying the
//     plan's rename/whiteout entries — only those; their content is irrelevant — into
//     fsLayerPlanState, a map from path to the live-main inode that name refers to.
//   - The commit loop runs in a world it is itself mutating. A live-main path recorded
//     above is stale there, so the apply-time guard does not reuse it: it asks only
//     whether the commit already owns the name (fsLayerPlanOwnership) and, when it does
//     not, compares against the path as it is right now.
//
// Both are needed: the first authorizes the plan before any mutation, the second refuses
// an entry whose live inode changed since that decision.

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
	// A directory rename carries everything below it: re-key the descendants instead of
	// discarding them, or their live identity is lost and a later mode entry under the
	// target reads as a creation. The pre-existing target subtree goes first, so the
	// re-key is not pruned away.
	st.prune(target)
	st.rekey(source, target)
	st[target] = resolved
	st[source] = fsLayerPlanNode{vacated: true}
	return nil
}

// rekey moves every name strictly below oldPrefix to the same relative place below
// newPrefix, preserving each node's live-main identity.
func (st fsLayerPlanState) rekey(oldPrefix, newPrefix string) {
	if oldPrefix == "" || newPrefix == "" || oldPrefix == newPrefix {
		return
	}
	moved := make(map[string]fsLayerPlanNode)
	for key, node := range st {
		suffix, ok := strings.CutPrefix(key, oldPrefix+"/")
		if !ok {
			continue
		}
		moved[newPrefix+"/"+suffix] = node
		delete(st, key)
	}
	for key, node := range moved {
		st[key] = node
	}
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
// main once. A path live main does not have is a creation.
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

// entryChangesMode reports whether entry, executed after the moves replayed so far, would
// change an existing path's permission bits. The entry gate and the commit scan decide
// with it. It is *not* the apply-time guard: that one runs in a world this commit is
// mutating, and uses fsLayerPlanOwnership instead.
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
		// A mode-only entry names a path the plan has not created, so whatever live inode
		// is there loses its bits. A path live main does not have is not a live inode, and
		// the apply step tolerates the missing target.
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

// fsLayerPlanOwnership is the apply-time view: for the names the commit touched, whether
// this commit owns them (it created the name or an ancestor) or whether they hold a live
// inode the plan moved there, plus the inode identity of every name the commit created.
//
// The identity map is what makes the guard race-safe. Names are not stable — the commit
// does not lock the tenant against ordinary FS writes, so a concurrent rename can move a
// victim inode onto a plan-owned name between two entries. A name the commit created is
// therefore only treated as the plan's own while the inode there is still the one it
// recorded, and the planned mode is applied to that inode id rather than to the name.
type fsLayerOwnershipKind uint8

const (
	ownershipPlan fsLayerOwnershipKind = iota + 1
	ownershipLive
)

type fsLayerPlanOwnership struct {
	kinds  map[string]fsLayerOwnershipKind
	inodes map[string]string
}

func newFSLayerPlanOwnership() fsLayerPlanOwnership {
	return fsLayerPlanOwnership{kinds: map[string]fsLayerOwnershipKind{}, inodes: map[string]string{}}
}

func trimLayerPath(rawPath string) string {
	return strings.TrimSuffix(rawPath, "/")
}

// owns reports whether the commit owns the name: it created the name or an ancestor, and
// the name is not a live inode the plan moved underneath an owned directory.
func (o fsLayerPlanOwnership) owns(rawPath string) bool {
	path := trimLayerPath(rawPath)
	for name := path; ; {
		switch o.kinds[name] {
		case ownershipLive:
			return false
		case ownershipPlan:
			return true
		}
		idx := strings.LastIndex(name, "/")
		if idx <= 0 {
			return false
		}
		name = name[:idx]
	}
}

// own records a name this commit created.
func (o fsLayerPlanOwnership) own(rawPath string) {
	path := trimLayerPath(rawPath)
	delete(o.inodes, path)
	o.kinds[path] = ownershipPlan
}

// noteCreated records a name this commit created together with the inode it created there.
func (o fsLayerPlanOwnership) noteCreated(rawPath, inodeID string) {
	path := trimLayerPath(rawPath)
	o.kinds[path] = ownershipPlan
	if inodeID != "" {
		o.inodes[path] = inodeID
	}
}

// markLive records a name that holds a live inode even though an owned ancestor exists.
func (o fsLayerPlanOwnership) markLive(rawPath string) {
	path := trimLayerPath(rawPath)
	delete(o.inodes, path)
	o.kinds[path] = ownershipLive
}

// drop forgets the name and everything below it.
func (o fsLayerPlanOwnership) drop(rawPath string) {
	path := trimLayerPath(rawPath)
	for name := range o.kinds {
		if name == path || strings.HasPrefix(name, path+"/") {
			delete(o.kinds, name)
		}
	}
	for name := range o.inodes {
		if name == path || strings.HasPrefix(name, path+"/") {
			delete(o.inodes, name)
		}
	}
}

// rekey moves every recorded name strictly below oldPrefix to the same relative place
// below newPrefix. A directory rename carries its subtree, so ownership and live
// exclusions have to move with it; dropping them is how a live inode gets laundered into a
// plan-owned directory.
func (o fsLayerPlanOwnership) rekey(oldPrefix, newPrefix string) {
	if oldPrefix == "" || newPrefix == "" || oldPrefix == newPrefix {
		return
	}
	movedKinds := make(map[string]fsLayerOwnershipKind)
	for name, kind := range o.kinds {
		suffix, ok := strings.CutPrefix(name, oldPrefix+"/")
		if !ok {
			continue
		}
		movedKinds[newPrefix+"/"+suffix] = kind
		delete(o.kinds, name)
	}
	for name, kind := range movedKinds {
		o.kinds[name] = kind
	}
	movedInodes := make(map[string]string)
	for name, id := range o.inodes {
		suffix, ok := strings.CutPrefix(name, oldPrefix+"/")
		if !ok {
			continue
		}
		movedInodes[newPrefix+"/"+suffix] = id
		delete(o.inodes, name)
	}
	for name, id := range movedInodes {
		o.inodes[name] = id
	}
}

// noteMove applies a rename or whiteout to the ownership set. A rename carries the names
// below the source to the target and keeps each one's kind: a name this commit owns stays
// owned, a moved live inode stays live (so later entries there are compared against it as
// it is now).
func (o fsLayerPlanOwnership) noteMove(entry *datastore.FSLayerEntry) {
	if entry == nil {
		return
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpRename, datastore.FSLayerEntryOpWhiteout:
	default:
		// Not a move: leave the name alone. Dropping here would forget the creation the
		// same entry just recorded.
		return
	}
	source := trimLayerPath(entry.Path)
	target := ""
	if entry.Op == datastore.FSLayerEntryOpRename {
		target = trimLayerPath(fsLayerRenameTarget(entry))
	}
	// A self-rename is a backend no-op, so it must not disturb ownership of the name.
	if target == source {
		return
	}
	sourceKind := o.kinds[source]
	// A pre-existing target is replaced by the rename; drop it before re-keying so the
	// moved subtree is not erased.
	o.drop(target)
	o.rekey(source, target)
	if entry.Op != datastore.FSLayerEntryOpRename || target == "" {
		o.drop(source)
		return
	}
	// The source name itself is now empty; the target holds the source's inode. A source
	// the commit does not own is a live inode (or absent from live main), so the target is
	// marked live and later entries there are compared against it.
	o.drop(source)
	if sourceKind == ownershipPlan {
		o.own(target)
	} else {
		o.markLive(target)
	}
}

// createsPath reports whether applying entry will create its path: the commit does not own
// the name and live main has nothing there. A whiteout/rename/chmod never creates.
func (o fsLayerPlanOwnership) createsPath(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) (bool, []string, error) {
	if entry == nil || b == nil {
		return false, nil, nil
	}
	switch entry.Op {
	case datastore.FSLayerEntryOpUpsert, datastore.FSLayerEntryOpMkdir, datastore.FSLayerEntryOpSymlink:
	default:
		return false, nil, nil
	}
	path := trimLayerPath(entry.Path)
	if o.owns(path) {
		return false, nil, nil
	}
	_, err := b.StatNodeCtx(ctx, path)
	if err == nil {
		return false, nil, nil
	}
	if !errors.Is(err, datastore.ErrNotFound) {
		return false, nil, err
	}
	// The entry creates its path; any ancestor that is missing right now will be created
	// implicitly by the same apply, so the commit owns those names too.
	var missing []string
	for ancestor := pathutil.ParentPath(path); ancestor != "" && ancestor != "/"; ancestor = pathutil.ParentPath(ancestor) {
		if o.owns(ancestor) {
			break
		}
		if _, statErr := b.StatNodeCtx(ctx, trimLayerPath(ancestor)); statErr == nil {
			break
		} else if !errors.Is(statErr, datastore.ErrNotFound) {
			return false, nil, statErr
		}
		missing = append(missing, ancestor)
	}
	return true, missing, nil
}

// appliedModeChange reports whether applying entry now would change the permission bits of
// an inode this commit does not own — the drift check for an owner chmod landing between
// the plan scan and this entry. A path this commit created, or that sits under one, has no
// live inode to protect unless a rename moved a live inode there.
func (o fsLayerPlanOwnership) appliedModeChange(ctx context.Context, b *backendpkg.Dat9Backend, entry *datastore.FSLayerEntry) (bool, string, error) {
	if b == nil || !fsLayerEntryCarriesMode(entry) {
		return false, "", nil
	}
	path := trimLayerPath(entry.Path)
	nf, err := b.StatNodeCtx(ctx, path)
	if errors.Is(err, datastore.ErrNotFound) {
		// Nothing at the name yet: an upsert creates it with this mode, a chmod is a
		// no-op.
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	inodeID := fsLayerNodeIdentity(nf)
	// A name this commit created is only its own while the inode there is still the one it
	// recorded; a concurrent rename can park a live inode on the name.
	if o.owns(path) {
		if recorded := o.inodes[path]; recorded != "" && recorded == inodeID {
			return false, inodeID, nil
		}
	}
	if entry.Op == datastore.FSLayerEntryOpChmod {
		// A live inode holds the name: changing its bits needs chmod authority.
		return true, inodeID, nil
	}
	differs, err := fsLayerModesDiffer(ctx, b, path, entry.Mode)
	if err != nil {
		return false, "", err
	}
	if differs {
		return true, inodeID, nil
	}
	// The requested mode already matches; it is safe to re-apply to this inode.
	return false, inodeID, nil
}

// fsLayerNodeIdentity is a stable id for the inode a stat row describes: the file id when
// there is one, else the directory inode id.
func fsLayerNodeIdentity(nf *datastore.NodeWithFile) string {
	if nf == nil {
		return ""
	}
	if nf.File != nil && nf.File.FileID != "" {
		return nf.File.FileID
	}
	return nf.Node.InodeID
}
