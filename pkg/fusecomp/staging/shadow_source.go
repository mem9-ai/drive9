package staging

// SetShadowStore attaches the shadow store used for payload recovery;
// exported bridge for the writeback extension's commit queue.
func (idx *PendingIndex) SetShadowStore(shadows *ShadowStore) {
	idx.setShadowStore(shadows)
}

// RecoverShadowSource selects the recovery payload for a pending entry;
// exported bridge for the writeback extension's commit queue.
func (idx *PendingIndex) RecoverShadowSource(path string, generation uint64, shadows *ShadowStore) uint64 {
	return idx.recoverShadowSource(path, generation, shadows)
}

// setShadowStore wires publication before the mount starts serving requests.
func (idx *PendingIndex) setShadowStore(shadows *ShadowStore) {
	if idx == nil {
		return
	}
	idx.mu.Lock()
	idx.shadows = shadows
	idx.mu.Unlock()
}

// recoverShadowSource selects a payload for startup recovery or the final
// unmount drain. Ordinary reads never bind arbitrary disk bytes to previously
// published metadata. The metadata CAS prevents binding a successor.
func (idx *PendingIndex) recoverShadowSource(path string, metaGeneration uint64, shadows *ShadowStore) uint64 {
	if idx == nil || shadows == nil {
		return 0
	}
	meta, ok := idx.GetMeta(path)
	if !ok || meta.Generation != metaGeneration || meta.Kind == PendingChmod {
		return 0
	}
	// Both journal replay and .meta-only recovery must reject a short payload.
	// The store checks size and captures the token together; a concurrent
	// mutation invalidates that token for both readers and uploaders.
	generation := shadows.ensureActiveGeneration(path, meta.Size)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if current := idx.items[path]; current != nil && current.Generation == metaGeneration {
		return generation
	}
	return 0
}
