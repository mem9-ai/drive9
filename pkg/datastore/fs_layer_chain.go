package datastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// FSLayerForkOptions controls ForkFSLayer.
type FSLayerForkOptions struct {
	// ChildLayerID is required (caller allocates id).
	ChildLayerID string
	// ParentLayerID is the layer to fork from (required).
	ParentLayerID string
	// Name optional display name for the child.
	Name string
	// ActorID optional.
	ActorID string
	// CheckpointID optional; when set, pin to that checkpoint's durable_seq (D2).
	CheckpointID string
	// MaxDepth soft cap; 0 means FSLayerDefaultMaxDepth.
	MaxDepth int
}

// FSLayerChainFrame is one hop on the parent chain from root toward tip.
type FSLayerChainFrame struct {
	Layer     FSLayer
	// LimitSeq is the max entry_seq visible on this layer for the requesting child.
	// For the tip layer itself, LimitSeq is -1 meaning tip (no max).
	LimitSeq int64
}

// ForkFSLayer creates a child layer pinned to the parent's tip (or checkpoint).
// D1: tip pin = MAX(entry_seq) under parent FOR UPDATE.
// D2: checkpoint pin when CheckpointID set.
// D13: parent must be active or sealed.
// D14/D15: inherit base_root/durability; depth check.
func (s *Store) ForkFSLayer(ctx context.Context, opts FSLayerForkOptions) (*FSLayer, error) {
	childID := strings.TrimSpace(opts.ChildLayerID)
	parentID := strings.TrimSpace(opts.ParentLayerID)
	if childID == "" {
		return nil, fmt.Errorf("fs layer child id is required")
	}
	if parentID == "" {
		return nil, fmt.Errorf("fs layer parent id is required")
	}
	if len(childID) > 50 {
		return nil, fmt.Errorf("fs layer id too long (%d chars, max 50)", len(childID))
	}
	if strings.Contains(childID, ":") {
		return nil, fmt.Errorf("fs layer id must not contain \":\"")
	}
	maxDepth := opts.MaxDepth
	if maxDepth <= 0 {
		maxDepth = FSLayerDefaultMaxDepth
	}
	if maxDepth > FSLayerHardMaxDepth {
		maxDepth = FSLayerHardMaxDepth
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin fork fs layer: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		baseRoot, state, durability, actorID, parentRoot, parentParent string
		durableSeq                                                     int64
		parentDepth                                                    int
	)
	err = tx.QueryRowContext(ctx, `
SELECT base_root_path, state, durability_mode, actor_id, durable_seq, parent_layer_id, root_layer_id, depth
FROM fs_layers WHERE `+s.scope.And(`layer_id = ?`)+` FOR UPDATE`,
		s.scope.Args(parentID)...).Scan(
		&baseRoot, &state, &durability, &actorID, &durableSeq, &parentParent, &parentRoot, &parentDepth,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("read parent fs layer %s: %w", parentID, err)
	}
	st := FSLayerState(state)
	if st != FSLayerStateActive && st != FSLayerStateSealed {
		return nil, fmt.Errorf("%w: parent layer %s state %s cannot be forked", ErrFSLayerStateConflict, parentID, state)
	}
	childDepth := parentDepth + 1
	if childDepth > maxDepth {
		return nil, fmt.Errorf("%w: depth %d exceeds max %d", ErrFSLayerDepthExceeded, childDepth, maxDepth)
	}
	if parentRoot == "" {
		parentRoot = parentID
	}

	var originSeq int64
	var originCheckpointID string
	checkpointID := strings.TrimSpace(opts.CheckpointID)
	if checkpointID != "" {
		var cpLayer string
		var cpSeq int64
		if err := tx.QueryRowContext(ctx, `
SELECT layer_id, durable_seq FROM fs_layer_checkpoints
WHERE `+s.scope.And(`checkpoint_id = ?`), s.scope.Args(checkpointID)...).Scan(&cpLayer, &cpSeq); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: checkpoint %s", ErrNotFound, checkpointID)
			}
			return nil, fmt.Errorf("read checkpoint %s: %w", checkpointID, err)
		}
		if cpLayer != parentID {
			return nil, fmt.Errorf("checkpoint %s does not belong to parent layer %s", checkpointID, parentID)
		}
		originSeq = cpSeq
		originCheckpointID = checkpointID
	} else {
		// D1 tip pin: MAX(entry_seq), not durable_seq.
		var maxSeq sql.NullInt64
		if err := tx.QueryRowContext(ctx, `
SELECT MAX(entry_seq) FROM fs_layer_entries WHERE `+s.scope.And(`layer_id = ?`),
			s.scope.Args(parentID)...).Scan(&maxSeq); err != nil {
			return nil, fmt.Errorf("read parent max entry_seq: %w", err)
		}
		if maxSeq.Valid {
			originSeq = maxSeq.Int64
		} else {
			originSeq = 0
		}
	}

	name := strings.TrimSpace(opts.Name)
	actor := strings.TrimSpace(opts.ActorID)
	child := FSLayer{
		LayerID:            childID,
		BaseRootPath:       baseRoot,
		Name:               name,
		State:              FSLayerStateActive,
		DurabilityMode:     FSLayerDurabilityMode(durability),
		ActorID:            actor,
		ParentLayerID:      parentID,
		OriginSeq:          originSeq,
		OriginCheckpointID: originCheckpointID,
		RootLayerID:        parentRoot,
		Depth:              childDepth,
		Origin:             FSLayerOriginFork,
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO fs_layers (
	`+s.scope.InsCols(`layer_id, base_root_path, name, state, durability_mode, actor_id, durable_seq,
	parent_layer_id, origin_seq, origin_checkpoint_id, root_layer_id, depth, origin,
	created_at, updated_at, sealed_at`)+`
) VALUES (`+s.scope.InsVals(`?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, UTC_TIMESTAMP(3), UTC_TIMESTAMP(3), NULL`)+`)`,
		s.scope.Args(child.LayerID, child.BaseRootPath, child.Name, string(child.State), string(child.DurabilityMode), child.ActorID,
			child.ParentLayerID, child.OriginSeq, child.OriginCheckpointID, child.RootLayerID, child.Depth, child.Origin)...)
	if err != nil {
		return nil, fmt.Errorf("insert forked fs layer %s: %w", childID, err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit fork fs layer %s: %w", childID, err)
	}
	return s.GetFSLayer(ctx, childID)
}

// ListFSLayerChain returns frames from root to tip (inclusive).
// Ancestor LimitSeq is the origin_seq of the next hop toward tip.
// Tip LimitSeq is -1 (unbounded).
func (s *Store) ListFSLayerChain(ctx context.Context, layerID string) ([]FSLayerChainFrame, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return nil, fmt.Errorf("fs layer id is required")
	}
	var rev []FSLayer
	seen := map[string]struct{}{}
	for curID := layerID; curID != ""; {
		if _, ok := seen[curID]; ok {
			return nil, fmt.Errorf("fs layer chain cycle at %s", curID)
		}
		seen[curID] = struct{}{}
		layer, err := s.GetFSLayer(ctx, curID)
		if err != nil {
			return nil, err
		}
		rev = append(rev, *layer)
		if layer.ParentLayerID == "" {
			break
		}
		curID = layer.ParentLayerID
		if len(rev) > FSLayerHardMaxDepth+2 {
			return nil, fmt.Errorf("fs layer chain too deep")
		}
	}
	frames := make([]FSLayerChainFrame, 0, len(rev))
	for i := len(rev) - 1; i >= 0; i-- {
		frames = append(frames, FSLayerChainFrame{Layer: rev[i], LimitSeq: -1})
	}
	for i := 0; i < len(frames)-1; i++ {
		frames[i].LimitSeq = frames[i+1].Layer.OriginSeq
	}
	return frames, nil
}

func applyTipLimit(chain []FSLayerChainFrame, tipMaxSeq *int64) []FSLayerChainFrame {
	if tipMaxSeq == nil || len(chain) == 0 {
		return chain
	}
	chain[len(chain)-1].LimitSeq = *tipMaxSeq
	return chain
}

// ResolveFSLayerPath walks the chain for a path (D8).
// Returns (entry, true, nil) on hit (including whiteout), (nil, false, nil) on miss → main.
func (s *Store) ResolveFSLayerPath(ctx context.Context, layerID, path string) (*FSLayerEntry, bool, error) {
	return s.ResolveFSLayerPathAtSeq(ctx, layerID, path, nil)
}

// ResolveFSLayerPathAtSeq is ResolveFSLayerPath with an optional tip max_seq.
func (s *Store) ResolveFSLayerPathAtSeq(ctx context.Context, layerID, path string, tipMaxSeq *int64) (*FSLayerEntry, bool, error) {
	canonical, err := canonicalFSLayerLookupPath(path)
	if err != nil {
		return nil, false, err
	}
	tree, err := s.MaterializeFSLayerOverlay(ctx, layerID, tipMaxSeq)
	if err != nil {
		return nil, false, err
	}
	n, ok := tree[canonical]
	if !ok {
		// Directory lookup may have been stored with trailing slash.
		if !strings.HasSuffix(canonical, "/") {
			if n, ok = tree[canonical+"/"]; !ok {
				return nil, false, nil
			}
		} else {
			return nil, false, nil
		}
	}
	e := OverlayNodeEntry(n)
	return &e, true, nil
}

// ListFSLayerChainMergedEntries returns latest-per-path effective overlay entries
// across the chain (tip wins). Used for diff/status and FUSE-friendly listing.
// Whiteouts are included so callers can hide base paths.
func (s *Store) ListFSLayerChainMergedEntries(ctx context.Context, layerID string) ([]FSLayerEntry, error) {
	return s.listFSLayerChainMergedEntries(ctx, layerID, nil)
}

func (s *Store) ListFSLayerChainMergedEntriesAtSeq(ctx context.Context, layerID string, tipMaxSeq int64) ([]FSLayerEntry, error) {
	return s.listFSLayerChainMergedEntries(ctx, layerID, &tipMaxSeq)
}

func (s *Store) listFSLayerChainMergedEntries(ctx context.Context, layerID string, tipMaxSeq *int64) ([]FSLayerEntry, error) {
	tree, err := s.MaterializeFSLayerOverlay(ctx, layerID, tipMaxSeq)
	if err != nil {
		return nil, err
	}
	out := make([]FSLayerEntry, 0, len(tree))
	for _, n := range tree {
		if n.ConsumedByRename {
			continue
		}
		out = append(out, OverlayNodeEntry(n))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path == out[j].Path {
			return out[i].EntrySeq < out[j].EntrySeq
		}
		return out[i].Path < out[j].Path
	})
	return out, nil
}

// ListFSLayerChainEffectiveLog returns ordered ops (root→tip, origin limits). D19.
func (s *Store) ListFSLayerChainEffectiveLog(ctx context.Context, layerID string) ([]FSLayerEntry, error) {
	return s.listFSLayerChainEffectiveLog(ctx, layerID, nil)
}

func (s *Store) ListFSLayerChainEffectiveLogAtSeq(ctx context.Context, layerID string, tipMaxSeq int64) ([]FSLayerEntry, error) {
	return s.listFSLayerChainEffectiveLog(ctx, layerID, &tipMaxSeq)
}

// MaterializeFSLayerCommitLog builds the ordered entry list to apply to main
// for commit (D19). For root layers (no parent), returns the single-layer log.
// For child layers, returns the full chain effective log (ordered materialize).
// Caller (server) still applies DiffFSLayerVsMain / live main claim (D18).
func (s *Store) MaterializeFSLayerCommitLog(ctx context.Context, layerID string) ([]FSLayerEntry, error) {
	layer, err := s.GetFSLayer(ctx, layerID)
	if err != nil {
		return nil, err
	}
	if layer.ParentLayerID == "" {
		return s.ListFSLayerEntryLog(ctx, layerID)
	}
	return s.ListFSLayerChainEffectiveLog(ctx, layerID)
}

// FSLayerStillPins reports whether any descendant still needs this layer (D17).
// True if any descendant is active/sealed/committing/conflicted, or is
// abandoned/committed but itself still_pins (e.g. abandoned middle + active grandchild).
func (s *Store) FSLayerStillPins(ctx context.Context, layerID string) (bool, error) {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return false, fmt.Errorf("fs layer id is required")
	}
	return s.fsLayerStillPinsRec(ctx, layerID, map[string]struct{}{})
}

func (s *Store) fsLayerStillPinsRec(ctx context.Context, layerID string, seen map[string]struct{}) (bool, error) {
	if _, ok := seen[layerID]; ok {
		return false, nil
	}
	seen[layerID] = struct{}{}
	rows, err := s.db.QueryContext(ctx, `
SELECT layer_id, state FROM fs_layers WHERE `+s.scope.And(`parent_layer_id = ?`),
		s.scope.Args(layerID)...)
	if err != nil {
		return false, fmt.Errorf("list child layers of %s: %w", layerID, err)
	}
	defer func() { _ = rows.Close() }()
	type child struct {
		id    string
		state FSLayerState
	}
	var children []child
	for rows.Next() {
		var id, state string
		if err := rows.Scan(&id, &state); err != nil {
			return false, err
		}
		children = append(children, child{id: id, state: FSLayerState(state)})
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	for _, c := range children {
		switch c.state {
		case FSLayerStateActive, FSLayerStateSealed, FSLayerStateCommitting, FSLayerStateConflicted:
			return true, nil
		}
		// abandoned/committed: pin if their subtree still needs them
		sub, err := s.fsLayerStillPinsRec(ctx, c.id, seen)
		if err != nil {
			return false, err
		}
		if sub {
			return true, nil
		}
	}
	return false, nil
}

// ListFSLayerDirectChildren returns layers whose parent_layer_id = layerID.
func (s *Store) ListFSLayerDirectChildren(ctx context.Context, layerID string) ([]FSLayer, error) {
	layerID = strings.TrimSpace(layerID)
	rows, err := s.db.QueryContext(ctx, `
SELECT `+fsLayerSelectCols+`
FROM fs_layers WHERE `+s.scope.And(`parent_layer_id = ?`)+`
ORDER BY layer_id`, s.scope.Args(layerID)...)
	if err != nil {
		return nil, fmt.Errorf("list child layers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return s.scanFSLayersWithTags(ctx, rows, "list child layers")
}

// DeleteFSLayerOptions controls DeleteFSLayer (logical abandon).
type DeleteFSLayerOptions struct {
	// Cascade abandons the whole descendant tree first (depth-first).
	Cascade bool
}

// DeleteFSLayer logically abandons a layer (D17). Physical row/entry delete is
// not performed while still_pins. Without cascade, still_pins → ErrFSLayerHasChildren.
func (s *Store) DeleteFSLayer(ctx context.Context, layerID string, opts DeleteFSLayerOptions) error {
	layerID = strings.TrimSpace(layerID)
	if layerID == "" {
		return fmt.Errorf("fs layer id is required")
	}
	layer, err := s.GetFSLayer(ctx, layerID)
	if err != nil {
		return err
	}
	if layer.State == FSLayerStateAbandoned {
		return nil
	}
	pins, err := s.FSLayerStillPins(ctx, layerID)
	if err != nil {
		return err
	}
	if pins {
		if !opts.Cascade {
			return fmt.Errorf("%w: layer %s has descendants; use cascade", ErrFSLayerHasChildren, layerID)
		}
		children, err := s.ListFSLayerDirectChildren(ctx, layerID)
		if err != nil {
			return err
		}
		// Depth-first: delete children first.
		for i := range children {
			if err := s.DeleteFSLayer(ctx, children[i].LayerID, DeleteFSLayerOptions{Cascade: true}); err != nil {
				return err
			}
		}
	}
	// Logical abandon; preserve entries for any residual pin safety.
	if err := s.SetFSLayerStateIf(ctx, layerID,
		[]FSLayerState{FSLayerStateActive, FSLayerStateSealed, FSLayerStateConflicted, FSLayerStateCommitted, FSLayerStateCommitting},
		FSLayerStateAbandoned); err != nil {
		// Already abandoned or wrong state — re-check.
		cur, gerr := s.GetFSLayer(ctx, layerID)
		if gerr != nil {
			return gerr
		}
		if cur.State == FSLayerStateAbandoned {
			return nil
		}
		return err
	}
	return nil
}

// HasFSLayerParent returns true if the layer is a non-root (forked) layer.
func (l *FSLayer) HasParent() bool {
	return l != nil && strings.TrimSpace(l.ParentLayerID) != ""
}
