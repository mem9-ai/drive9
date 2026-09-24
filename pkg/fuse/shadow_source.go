package fuse

// shadowReadSource is process-local evidence about one staged content image.
// A metadata generation alone identifies neither a shadow store nor its bytes.
type shadowReadSource struct {
	store      *ShadowStore
	generation uint64
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

func (idx *PendingIndex) shadowReadGeneration(path string, shadows *ShadowStore) uint64 {
	if idx == nil {
		return 0
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	meta := idx.items[path]
	if meta == nil || meta.Kind == PendingChmod || meta.shadowSource.store != shadows {
		return 0
	}
	return meta.shadowSource.generation
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
		// A failed recovery check must also revoke any earlier process-local
		// claim on this exact metadata entry.
		current.shadowSource = shadowReadSource{shadows, generation}
		return generation
	}
	return 0
}
