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

// recoverShadowSource is only for startup recovery, after replay/migration has
// selected the recoverable payload. Reads never bind arbitrary disk bytes to
// previously published metadata. The metadata CAS prevents binding a successor.
func (idx *PendingIndex) recoverShadowSource(path string, metaGeneration uint64, shadows *ShadowStore) {
	if idx == nil || shadows == nil {
		return
	}
	generation := shadows.EnsureActiveGeneration(path)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if meta := idx.items[path]; meta != nil && meta.Generation == metaGeneration && meta.Kind != PendingChmod {
		meta.shadowSource = shadowReadSource{shadows, generation}
	}
}
