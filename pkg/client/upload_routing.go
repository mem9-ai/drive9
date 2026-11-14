package client

// UseDirectPUT selects the whole-file PUT protocol from a file size and the
// negotiated inline threshold. Empty files always use PUT; nonempty files use
// multipart when the threshold is unknown (zero) or the size reaches it.
// Callers must validate the file size before publishing.
func UseDirectPUT(size, threshold int64) bool {
	return size == 0 || (threshold > 0 && size < threshold)
}
