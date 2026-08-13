package datastore

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/mem9-ai/drive9/pkg/pathutil"
)

// FSLayerOverlayNode is the final overlay state of one path after replaying
// the chain log in entry_seq order (D19). Chmod/rename are folded in.
type FSLayerOverlayNode struct {
	Path             string
	Whiteout         bool
	ConsumedByRename bool // src whiteout already represented by dest rename draft
	RenameFrom       string
	Kind             FSLayerEntryKind
	Mode             uint32
	ModeSet          bool
	HasBody          bool
	Entry            FSLayerEntry
}

// MaterializeFSLayerOverlay replays the chain effective log into a path→node map.
// tipMaxSeq nil means tip is unbounded; non-nil limits only the tip layer.
func (s *Store) MaterializeFSLayerOverlay(ctx context.Context, layerID string, tipMaxSeq *int64) (map[string]FSLayerOverlayNode, error) {
	log, err := s.listFSLayerChainEffectiveLog(ctx, layerID, tipMaxSeq)
	if err != nil {
		return nil, err
	}
	return replayFSLayerOverlay(log), nil
}

func replayFSLayerOverlay(log []FSLayerEntry) map[string]FSLayerOverlayNode {
	tree := make(map[string]FSLayerOverlayNode, len(log))
	for i := range log {
		applyFSLayerOverlayOp(tree, &log[i])
	}
	return tree
}

func applyFSLayerOverlayOp(tree map[string]FSLayerOverlayNode, op *FSLayerEntry) {
	if op == nil || op.Path == "" {
		return
	}
	switch op.Op {
	case FSLayerEntryOpWhiteout:
		if prev, ok := tree[op.Path]; ok {
			unconsumeOverlayRenameSource(tree, prev.RenameFrom)
		}
		node := overlayFromEntry(op)
		node.Whiteout = true
		node.HasBody = false
		node.RenameFrom = ""
		// Dir whiteout hides overlay descendants.
		if op.Kind == FSLayerEntryKindDir || strings.HasSuffix(op.Path, "/") {
			clearOverlayDescendants(tree, op.Path)
		}
		tree[op.Path] = node
	case FSLayerEntryOpRename:
		target := overlayRenameTarget(op)
		if target == "" {
			return
		}
		src, ok := tree[op.Path]
		if overlayRenameMovesDir(op, src, ok, tree) && !overlayRenameDestInsideSrc(op.Path, target) {
			clearOverlayDescendants(tree, target)
			moveOverlayDescendants(tree, op.Path, target)
			src, ok = tree[op.Path]
		}
		if ok && !src.Whiteout && src.HasBody {
			if prev, destOK := tree[target]; destOK {
				unconsumeOverlayRenameSource(tree, prev.RenameFrom)
			}
			moved := src
			moved.Path = target
			moved.Entry.Path = target
			moved.RenameFrom = ""
			tree[target] = moved
			wo := overlayFromEntry(op)
			wo.Path = op.Path
			wo.Whiteout = true
			wo.HasBody = false
			wo.RenameFrom = ""
			wo.Entry.Op = FSLayerEntryOpWhiteout
			wo.Entry.Path = op.Path
			tree[op.Path] = wo
			return
		}
		// Main-backed rename: keep a real rename draft on dest, mark src consumed.
		// If dest already carried a rename, inherit the original source.
		renameFrom := op.Path
		if ok && !src.Whiteout && src.RenameFrom != "" {
			renameFrom = src.RenameFrom
		}
		if prev, destOK := tree[target]; destOK && prev.RenameFrom != "" && prev.RenameFrom != renameFrom {
			unconsumeOverlayRenameSource(tree, prev.RenameFrom)
		}
		dst := overlayFromEntry(op)
		dst.Path = target
		dst.RenameFrom = renameFrom
		dst.HasBody = false
		dst.Whiteout = false
		dst.Mode = src.Mode
		dst.ModeSet = src.ModeSet
		dst.Entry.Op = FSLayerEntryOpRename
		dst.Entry.Path = renameFrom
		dst.Entry.ContentText = target
		if src.ModeSet {
			dst.Entry.Mode = src.Mode
		}
		tree[target] = dst
		wo := overlayFromEntry(op)
		wo.Path = op.Path
		wo.Whiteout = true
		// Intermediate hops (mv a b; mv b c) must keep a real whiteout at b.
		// Only the original source is implied by the dest rename draft.
		wo.ConsumedByRename = renameFrom == op.Path
		wo.HasBody = false
		wo.RenameFrom = ""
		wo.Entry.Op = FSLayerEntryOpWhiteout
		wo.Entry.Path = op.Path
		tree[op.Path] = wo
		if renameFrom != op.Path {
			if orig, origOK := tree[renameFrom]; origOK {
				orig.ConsumedByRename = true
				orig.Whiteout = true
				orig.HasBody = false
				tree[renameFrom] = orig
			}
		}
	case FSLayerEntryOpChmod:
		cur, ok := tree[op.Path]
		if !ok || cur.Whiteout {
			node := overlayFromEntry(op)
			node.ModeSet = true
			node.HasBody = false
			tree[op.Path] = node
			return
		}
		cur.Mode = op.Mode
		cur.ModeSet = true
		cur.Entry.Mode = op.Mode
		tree[op.Path] = cur
	default:
		prev, hadPrev := tree[op.Path]
		node := overlayFromEntry(op)
		if op.Op == FSLayerEntryOpUpsert || op.Op == FSLayerEntryOpSymlink || op.Op == FSLayerEntryOpMkdir {
			node.HasBody = true
			node.Whiteout = false
			node.RenameFrom = ""
			if hadPrev {
				unconsumeOverlayRenameSource(tree, prev.RenameFrom)
			}
		}
		tree[op.Path] = node
	}
}

func unconsumeOverlayRenameSource(tree map[string]FSLayerOverlayNode, src string) {
	if src == "" {
		return
	}
	n, ok := tree[src]
	if !ok || !n.ConsumedByRename {
		return
	}
	n.ConsumedByRename = false
	tree[src] = n
}

func clearOverlayDescendants(tree map[string]FSLayerOverlayNode, dir string) {
	prefix := dir
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var toDelete []string
	var sources []string
	for p, n := range tree {
		if strings.HasPrefix(p, prefix) {
			toDelete = append(toDelete, p)
			if n.RenameFrom != "" {
				sources = append(sources, n.RenameFrom)
			}
		}
	}
	for _, src := range sources {
		unconsumeOverlayRenameSource(tree, src)
	}
	for _, p := range toDelete {
		delete(tree, p)
	}
}

func overlayFromEntry(op *FSLayerEntry) FSLayerOverlayNode {
	return FSLayerOverlayNode{
		Path: op.Path,
		Kind: op.Kind,
		Mode: op.Mode,
		// ModeSet is only flipped by chmod so default stored modes (0644)
		// on rename/upsert do not emit a follow-up chmod.
		ModeSet:  false,
		HasBody:  op.Op == FSLayerEntryOpUpsert || op.Op == FSLayerEntryOpSymlink || op.Op == FSLayerEntryOpMkdir,
		Entry:    *op,
		Whiteout: op.Op == FSLayerEntryOpWhiteout,
	}
}

func overlayRenameTarget(op *FSLayerEntry) string {
	if op == nil {
		return ""
	}
	target := op.ContentText
	if target == "" && len(op.ContentBlob) > 0 {
		target = string(op.ContentBlob)
	}
	if target == "" {
		return ""
	}
	isDir := op.Kind == FSLayerEntryKindDir || strings.HasSuffix(op.Path, "/") || strings.HasSuffix(target, "/")
	var (
		can string
		err error
	)
	if isDir {
		can, err = pathutil.CanonicalizeDir(target)
	} else {
		can, err = pathutil.Canonicalize(target)
	}
	if err != nil {
		return target
	}
	return can
}

func overlayDirPrefix(p string) string {
	if strings.HasSuffix(p, "/") {
		return p
	}
	return p + "/"
}

func overlayRenameDestInsideSrc(src, dest string) bool {
	srcPrefix := overlayDirPrefix(src)
	destPrefix := overlayDirPrefix(dest)
	return destPrefix != srcPrefix && strings.HasPrefix(destPrefix, srcPrefix)
}

func overlayRenameMovesDir(op *FSLayerEntry, src FSLayerOverlayNode, srcOK bool, tree map[string]FSLayerOverlayNode) bool {
	if op.Kind == FSLayerEntryKindDir || strings.HasSuffix(op.Path, "/") {
		return true
	}
	if srcOK && (src.Kind == FSLayerEntryKindDir || strings.HasSuffix(src.Path, "/")) {
		return true
	}
	prefix := overlayDirPrefix(op.Path)
	for p := range tree {
		if strings.HasPrefix(p, prefix) {
			return true
		}
	}
	return false
}

func moveOverlayDescendants(tree map[string]FSLayerOverlayNode, src, dest string) {
	srcPrefix := overlayDirPrefix(src)
	destPrefix := overlayDirPrefix(dest)
	if srcPrefix == destPrefix {
		return
	}
	type item struct {
		path string
		node FSLayerOverlayNode
	}
	var moved []item
	for p, n := range tree {
		if p == src || p == strings.TrimSuffix(src, "/") {
			continue
		}
		if strings.HasPrefix(p, srcPrefix) {
			moved = append(moved, item{path: p, node: n})
		}
	}
	for _, it := range moved {
		delete(tree, it.path)
	}
	for _, it := range moved {
		newP := destPrefix + strings.TrimPrefix(it.path, srcPrefix)
		n := it.node
		n.Path = newP
		n.Entry.Path = newP
		if n.RenameFrom != "" {
			if n.RenameFrom == src {
				n.RenameFrom = dest
			} else if strings.HasPrefix(n.RenameFrom, srcPrefix) {
				n.RenameFrom = destPrefix + strings.TrimPrefix(n.RenameFrom, srcPrefix)
			}
		}
		if n.Entry.Op == FSLayerEntryOpRename && n.Entry.ContentText != "" {
			if n.Entry.ContentText == src || strings.HasPrefix(n.Entry.ContentText, srcPrefix) {
				n.Entry.ContentText = destPrefix + strings.TrimPrefix(n.Entry.ContentText, srcPrefix)
			}
		}
		tree[newP] = n
	}
}

// OverlayNodeEntry returns the folded effective entry for reads (GET/diff/FUSE).
func OverlayNodeEntry(n FSLayerOverlayNode) FSLayerEntry {
	e := n.Entry
	e.Path = n.Path
	if n.Whiteout {
		e.Op = FSLayerEntryOpWhiteout
		e.Path = n.Path
	} else if n.RenameFrom != "" && !n.HasBody {
		e.Op = FSLayerEntryOpRename
		e.Path = n.RenameFrom
		e.ContentText = n.Path
	} else if n.HasBody {
		if e.Op == FSLayerEntryOpChmod || e.Op == FSLayerEntryOpRename {
			e.Op = FSLayerEntryOpUpsert
		}
		e.Path = n.Path
	} else {
		e.Op = FSLayerEntryOpChmod
		e.Path = n.Path
	}
	e.Kind = n.Kind
	if n.ModeSet {
		e.Mode = n.Mode
	}
	return e
}

// OverlayCommitDrafts converts overlay nodes to apply-shaped entries.
// Creates/renames come first (shallow paths), then whiteouts (deep paths first)
// so directory deletes see an empty tree (D19 / V1 apply order).
func OverlayCommitDrafts(tree map[string]FSLayerOverlayNode) []FSLayerEntry {
	var creates, deletes []FSLayerEntry
	for p, n := range tree {
		if n.ConsumedByRename {
			continue
		}
		e := OverlayNodeEntry(n)
		e.Path = p
		e.BaseInodeID = ""
		e.BaseRevision = 0
		if n.RenameFrom != "" && !n.HasBody {
			e.Op = FSLayerEntryOpRename
			e.Path = n.RenameFrom
			e.ContentText = n.Path
			creates = append(creates, e)
			if n.ModeSet {
				chmod := e
				chmod.Op = FSLayerEntryOpChmod
				chmod.Path = n.Path
				chmod.ContentText = ""
				chmod.Mode = n.Mode
				creates = append(creates, chmod)
			}
			continue
		}
		if n.Whiteout {
			e.Op = FSLayerEntryOpWhiteout
			e.Path = n.Path
			deletes = append(deletes, e)
			continue
		}
		if n.HasBody {
			if e.Op == FSLayerEntryOpChmod || e.Op == FSLayerEntryOpRename {
				e.Op = FSLayerEntryOpUpsert
			}
			e.Path = n.Path
			creates = append(creates, e)
			continue
		}
		e.Op = FSLayerEntryOpChmod
		e.Path = n.Path
		creates = append(creates, e)
	}
	renameDestBySource := make(map[string]string)
	for i := range creates {
		if creates[i].Op == FSLayerEntryOpRename && creates[i].Path != "" && creates[i].ContentText != "" {
			renameDestBySource[creates[i].Path] = creates[i].ContentText
		}
	}
	sort.Slice(creates, func(i, j int) bool {
		si, sj := overlayDraftCreateSortPath(creates[i], renameDestBySource), overlayDraftCreateSortPath(creates[j], renameDestBySource)
		di, dj := pathDepth(si), pathDepth(sj)
		if di != dj {
			return di < dj
		}
		if si != sj {
			return si < sj
		}
		ri, rj := overlayDraftCreateRank(creates[i], renameDestBySource), overlayDraftCreateRank(creates[j], renameDestBySource)
		if ri != rj {
			return ri < rj
		}
		return creates[i].Path < creates[j].Path
	})
	sort.Slice(deletes, func(i, j int) bool {
		di, dj := pathDepth(deletes[i].Path), pathDepth(deletes[j].Path)
		if di != dj {
			return di > dj
		}
		return deletes[i].Path > deletes[j].Path
	})
	return append(creates, deletes...)
}

func overlayDraftCreateSortPath(e FSLayerEntry, renameDestBySource map[string]string) string {
	if e.Op == FSLayerEntryOpRename {
		return e.ContentText
	}
	// Recreate of a rename source (mv a b; echo > a) must sort with the dest
	// so dest-parent mkdir stays first and the rename still precedes the upsert.
	if dest := renameDestBySource[e.Path]; dest != "" {
		return dest
	}
	return e.Path
}

func overlayDraftCreateRank(e FSLayerEntry, renameDestBySource map[string]string) int {
	// Recreate of a rename source (mv dir new; mkdir dir) must follow the
	// rename even if the recreate is a mkdir (default rank 0).
	if e.Op != FSLayerEntryOpRename && renameDestBySource[e.Path] != "" {
		return 4
	}
	switch e.Op {
	case FSLayerEntryOpMkdir:
		return 0
	case FSLayerEntryOpRename:
		return 1
	case FSLayerEntryOpChmod:
		return 3
	default:
		return 2
	}
}

func pathDepth(p string) int {
	n := 0
	for _, c := range p {
		if c == '/' {
			n++
		}
	}
	return n
}

// listFSLayerChainEffectiveLog is the shared implementation with optional tip cap.
func (s *Store) listFSLayerChainEffectiveLog(ctx context.Context, layerID string, tipMaxSeq *int64) ([]FSLayerEntry, error) {
	chain, err := s.ListFSLayerChain(ctx, layerID)
	if err != nil {
		return nil, fmt.Errorf("list fs layer chain %s: %w", layerID, err)
	}
	if tipMaxSeq != nil && *tipMaxSeq < 0 {
		return nil, fmt.Errorf("fs layer max seq must be non-negative")
	}
	chain = applyTipLimit(chain, tipMaxSeq)
	var out []FSLayerEntry
	for _, frame := range chain {
		var log []FSLayerEntry
		if frame.LimitSeq < 0 {
			log, err = s.ListFSLayerEntryLog(ctx, frame.Layer.LayerID)
		} else {
			log, err = s.ListFSLayerEntryLogAtSeq(ctx, frame.Layer.LayerID, frame.LimitSeq)
		}
		if err != nil {
			return nil, fmt.Errorf("list fs layer entry log %s: %w", frame.Layer.LayerID, err)
		}
		out = append(out, log...)
	}
	return out, nil
}
