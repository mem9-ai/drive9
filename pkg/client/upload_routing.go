package client

// UseDirectPUT selects the whole-file PUT protocol from a file size and the
// negotiated inline threshold. Empty files always use PUT; nonempty files use
// multipart when the threshold is unknown (zero) or the size reaches it.
// size must be the exact nonnegative byte count of the body to be sent.
func UseDirectPUT(size, threshold int64) bool {
	return size == 0 || (threshold > 0 && size < threshold)
}
