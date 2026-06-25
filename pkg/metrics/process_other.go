//go:build !unix

package metrics

// maxFileDescriptors is unsupported off-Unix; process metrics are Linux-only in
// practice (they read /proc), so the fd-limit gauge is simply omitted there.
func maxFileDescriptors() (uint64, bool) {
	return 0, false
}
