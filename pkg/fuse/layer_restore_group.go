package fuse

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// layerRestoreRenameMove describes one committed retained file that must move
// with a peer directory rename. All moves in one group share one durable
// rollback marker: exposing a partially moved subtree is never valid.
type layerRestoreRenameMove struct {
	oldPath string
	newPath string

	expectedOldPendingGen uint64
	expectedNewPendingGen uint64
	expectedOldShadowGen  uint64
	expectedNewShadowGen  uint64
}

type heldPendingPathLock struct {
	path string
	lock *pendingPathLock
}

type heldShadowPathLock struct {
	path string
	lock *shadowPathLock
}

func uniqueSortedLayerRestorePaths(moves []layerRestoreRenameMove) ([]string, error) {
	paths := make(map[string]struct{}, len(moves)*2)
	oldPaths := make(map[string]struct{}, len(moves))
	newPaths := make(map[string]struct{}, len(moves))
	for _, move := range moves {
		if move.oldPath == "" || move.newPath == "" || move.oldPath == move.newPath {
			return nil, fmt.Errorf("invalid layer restore group move %q to %q", move.oldPath, move.newPath)
		}
		if _, exists := oldPaths[move.oldPath]; exists {
			return nil, fmt.Errorf("duplicate layer restore group source %q", move.oldPath)
		}
		if _, exists := newPaths[move.newPath]; exists {
			return nil, fmt.Errorf("duplicate layer restore group target %q", move.newPath)
		}
		oldPaths[move.oldPath] = struct{}{}
		newPaths[move.newPath] = struct{}{}
		paths[move.oldPath] = struct{}{}
		paths[move.newPath] = struct{}{}
	}
	// A directory rename maps disjoint source and target subtrees. Reject an
	// overlapping file graph instead of depending on os.Rename ordering.
	for oldPath := range oldPaths {
		if _, overlaps := newPaths[oldPath]; overlaps {
			return nil, fmt.Errorf("overlapping layer restore group path %q", oldPath)
		}
	}
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	return ordered, nil
}

func acquirePendingPathLocks(idx *PendingIndex, paths []string) []heldPendingPathLock {
	held := make([]heldPendingPathLock, 0, len(paths))
	for _, path := range paths {
		held = append(held, heldPendingPathLock{path: path, lock: idx.acquirePathLock(path)})
	}
	return held
}

func releasePendingPathLocks(idx *PendingIndex, held []heldPendingPathLock) {
	for i := len(held) - 1; i >= 0; i-- {
		idx.releasePathLock(held[i].path, held[i].lock)
	}
}

func acquireShadowPathLocks(shadows *ShadowStore, paths []string) []heldShadowPathLock {
	held := make([]heldShadowPathLock, 0, len(paths))
	for _, path := range paths {
		held = append(held, heldShadowPathLock{path: path, lock: shadows.acquirePathLock(path)})
	}
	return held
}

func releaseShadowPathLocks(shadows *ShadowStore, held []heldShadowPathLock) {
	for i := len(held) - 1; i >= 0; i-- {
		shadows.releasePathLock(held[i].path, held[i].lock)
	}
}

type preparedLayerRestoreRenameMove struct {
	layerRestoreRenameMove
	oldMeta  *WriteBackMeta
	newMeta  *WriteBackMeta
	metaJSON []byte

	shadow       *ShadowFile
	replaced     *ShadowFile
	oldActiveGen uint64
	replacedGen  uint64
	replacedSize int64
}

// restoreLayerRenameGroupIfGenerations moves every retained committed file in
// a directory subtree as one crash-recoverable local transaction. The pending
// and shadow path locks cover the complete group, one marker snapshots every
// source and target file, and no in-memory state is published until the marker
// has reached its final commit point.
func restoreLayerRenameGroupIfGenerations(shadows *ShadowStore, pending *PendingIndex, moves []layerRestoreRenameMove) (bool, error) {
	if len(moves) == 0 {
		return true, nil
	}
	paths, err := uniqueSortedLayerRestorePaths(moves)
	if err != nil {
		return false, err
	}
	pendingLocks := acquirePendingPathLocks(pending, paths)
	defer releasePendingPathLocks(pending, pendingLocks)
	shadowLocks := acquireShadowPathLocks(shadows, paths)
	defer releaseShadowPathLocks(shadows, shadowLocks)

	prepared := make([]preparedLayerRestoreRenameMove, 0, len(moves))
	for _, move := range moves {
		oldMeta, ok := pending.restoreLayerGenerationLocked(move.oldPath, move.expectedOldPendingGen)
		if !ok || oldMeta == nil {
			return false, nil
		}
		if _, ok := pending.restoreLayerGenerationLocked(move.newPath, move.expectedNewPendingGen); !ok {
			return false, nil
		}
		prepared = append(prepared, preparedLayerRestoreRenameMove{
			layerRestoreRenameMove: move,
			oldMeta:                oldMeta,
		})
	}

	shadows.mu.Lock()
	defer shadows.mu.Unlock()
	for i := range prepared {
		move := &prepared[i]
		if shadows.writeGen[move.oldPath] != move.expectedOldShadowGen || shadows.writeGen[move.newPath] != move.expectedNewShadowGen {
			return false, nil
		}
		sf := shadows.files[move.oldPath]
		if sf == nil {
			return false, fmt.Errorf("restore layer rename group: committed shadow missing for %s", move.oldPath)
		}
		move.shadow = sf
		move.replaced = shadows.files[move.newPath]
		move.oldActiveGen = shadows.active[move.oldPath]
		move.replacedGen = shadows.active[move.newPath]
		move.replacedSize = shadows.recoveredSizes[shadows.shadowPath(move.newPath)]
		if move.replaced != nil {
			move.replacedSize = move.replaced.size
		}

		now := time.Now()
		newMeta := cloneWriteBackMeta(move.oldMeta)
		newMeta.Path = move.newPath
		newMeta.Size = sf.size
		newMeta.Mtime = now
		newMeta.Kind = PendingOverwrite
		newMeta.LayerCommitted = true
		newMeta.Generation = pending.nextGen.Add(1)
		metaJSON, marshalErr := json.Marshal(&newMeta)
		if marshalErr != nil {
			return false, fmt.Errorf("pending index marshal restored layer group rename: %w", marshalErr)
		}
		move.newMeta = &newMeta
		move.metaJSON = metaJSON
	}

	txnPaths := make([]string, 0, len(prepared)*4)
	for _, move := range prepared {
		txnPaths = append(txnPaths,
			filepath.Join(pending.dir, hashPath(move.oldPath)+".meta"),
			filepath.Join(pending.dir, hashPath(move.newPath)+".meta"),
			shadows.shadowPath(move.oldPath),
			shadows.shadowPath(move.newPath),
		)
	}
	tx, err := beginLayerRestoreTxn(pending.dir, txnPaths...)
	if err != nil {
		return false, err
	}
	tx.removeMarker = pending.restoreTxnMarkerRemove
	fail := func(primary error) (bool, error) {
		return false, rollbackLayerRestoreFailure(tx, primary)
	}

	for _, move := range prepared {
		if err := os.Rename(shadows.shadowPath(move.oldPath), shadows.shadowPath(move.newPath)); err != nil {
			return fail(fmt.Errorf("shadow layer group rename %s to %s: %w", move.oldPath, move.newPath, err))
		}
	}
	if err := fsyncDir(shadows.dir); err != nil {
		return fail(fmt.Errorf("shadow layer group rename dir sync: %w", err))
	}
	for _, move := range prepared {
		newMetaPath := filepath.Join(pending.dir, hashPath(move.newPath)+".meta")
		if err := pending.writeLayerRestoreMeta(newMetaPath, move.metaJSON); err != nil {
			return fail(fmt.Errorf("pending index restore layer group rename %s: %w", move.newPath, err))
		}
	}
	for _, move := range prepared {
		oldMetaPath := filepath.Join(pending.dir, hashPath(move.oldPath)+".meta")
		if err := os.Remove(oldMetaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fail(fmt.Errorf("pending index remove layer group source %s: %w", move.oldPath, err))
		}
	}
	if err := fsyncDir(pending.dir); err != nil {
		return fail(fmt.Errorf("pending index sync layer group rename: %w", err))
	}
	if err := tx.commit(); err != nil {
		return fail(err)
	}

	// The durable group is committed. Publish both in-memory indexes without
	// any remaining fallible work so readers can observe only the whole move.
	pending.mu.Lock()
	for _, move := range prepared {
		delete(pending.items, move.oldPath)
		pending.items[move.newPath] = move.newMeta
	}
	pending.mu.Unlock()

	var replacedBytes int64
	for _, move := range prepared {
		delete(shadows.recoveredSizes, shadows.shadowPath(move.oldPath))
		delete(shadows.recoveredSizes, shadows.shadowPath(move.newPath))
		if move.replaced != nil {
			shadows.resizeWrittenLocked(move.replaced, 0)
		}
		delete(shadows.files, move.oldPath)
		shadows.files[move.newPath] = move.shadow
		oldWriteGen := shadows.writeGen[move.oldPath]
		if oldWriteGen != 0 {
			shadows.writeGen[move.newPath] = oldWriteGen
			delete(shadows.writeGen, move.oldPath)
		} else {
			delete(shadows.writeGen, move.newPath)
		}
		if move.oldActiveGen != 0 {
			shadows.active[move.newPath] = move.oldActiveGen
			delete(shadows.active, move.oldPath)
		} else if move.replacedGen != 0 {
			delete(shadows.active, move.newPath)
		}
		if move.replacedGen != 0 && move.replacedGen != move.oldActiveGen {
			delete(shadows.genFile, move.replacedGen)
			if shadows.refs[move.replacedGen] > 0 && move.replaced != nil {
				shadows.retired[move.replacedGen] = &retiredShadow{
					fd:     move.replaced.fd,
					size:   move.replaced.size,
					reason: shadowRetiredSnapshot,
				}
			} else {
				delete(shadows.refs, move.replacedGen)
				if move.replaced != nil {
					_ = move.replaced.fd.Close()
				}
			}
		} else if move.replaced != nil && move.replaced != move.shadow {
			_ = move.replaced.fd.Close()
		}
		replacedBytes += move.replacedSize
	}
	if replacedBytes > 0 {
		shadows.pendingBytes.Add(-replacedBytes)
	}
	return true, nil
}
