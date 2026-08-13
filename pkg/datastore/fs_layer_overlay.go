package datastore

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
		target := strings.TrimSpace(op.ContentText)
		if target == "" && len(op.ContentBlob) > 0 {
			target = strings.TrimSpace(string(op.ContentBlob))
		}
		if target == "" {
			return
		}
		src, ok := tree[op.Path]
		if ok && !src.Whiteout && src.HasBody {
			moved := src
			moved.Path = target
			moved.Entry.Path = target
			moved.RenameFrom = ""
			tree[target] = moved
			wo := overlayFromEntry(op)
			wo.Whiteout = true
			wo.HasBody = false
			wo.RenameFrom = ""
			wo.Entry.Op = FSLayerEntryOpWhiteout
			tree[op.Path] = wo
			return
		}
		// Main-backed rename: keep a real rename draft on dest, mark src consumed.
		dst := overlayFromEntry(op)
		dst.Path = target
		dst.RenameFrom = op.Path
		dst.HasBody = false
		dst.Whiteout = false
		dst.Entry.Op = FSLayerEntryOpRename
		dst.Entry.Path = op.Path
		dst.Entry.ContentText = target
		tree[target] = dst
		wo := overlayFromEntry(op)
		wo.Whiteout = true
		wo.ConsumedByRename = true
		wo.HasBody = false
		wo.RenameFrom = ""
		wo.Entry.Op = FSLayerEntryOpWhiteout
		tree[op.Path] = wo
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
		node := overlayFromEntry(op)
		if op.Op == FSLayerEntryOpUpsert || op.Op == FSLayerEntryOpSymlink || op.Op == FSLayerEntryOpMkdir {
			node.HasBody = true
			node.Whiteout = false
			node.RenameFrom = ""
		}
		tree[op.Path] = node
	}
}

func clearOverlayDescendants(tree map[string]FSLayerOverlayNode, dir string) {
	prefix := dir
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	for p := range tree {
		if strings.HasPrefix(p, prefix) {
			delete(tree, p)
		}
	}
}

func overlayFromEntry(op *FSLayerEntry) FSLayerOverlayNode {
	return FSLayerOverlayNode{
		Path:     op.Path,
		Kind:     op.Kind,
		Mode:     op.Mode,
		ModeSet:  op.Mode != 0,
		HasBody:  op.Op == FSLayerEntryOpUpsert || op.Op == FSLayerEntryOpSymlink || op.Op == FSLayerEntryOpMkdir,
		Entry:    *op,
		Whiteout: op.Op == FSLayerEntryOpWhiteout,
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
	sort.Slice(creates, func(i, j int) bool {
		di, dj := pathDepth(creates[i].Path), pathDepth(creates[j].Path)
		if di != dj {
			return di < dj
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
		return nil, err
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
			return nil, err
		}
		out = append(out, log...)
	}
	return out, nil
}
