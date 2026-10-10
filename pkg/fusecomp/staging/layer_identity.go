package staging

// LayerCacheIdentity identifies the layer incarnation a pending entry was
// staged against.
type LayerCacheIdentity struct {
	LayerID  string
	EntrySeq int64
}
