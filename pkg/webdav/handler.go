// Package webdav provides an http.Handler that serves a drive9 filesystem
// over the WebDAV protocol. It bridges golang.org/x/net/webdav to the
// drive9 client fs API, allowing macOS mount_webdav (and other WebDAV
// clients) to access drive9 as a mounted filesystem.
package webdav

import (
	"net/http"

	"github.com/mem9-ai/dat9/pkg/client"
	"golang.org/x/net/webdav"
)

// Options configures the WebDAV handler.
type Options struct {
	// Prefix is the URL path prefix stripped before mapping to drive9 paths.
	// Empty means no prefix (root mount).
	Prefix string
}

// NewHandler returns an http.Handler that serves drive9 content over WebDAV.
func NewHandler(c *client.Client, opts Options) http.Handler {
	return &webdav.Handler{
		Prefix:     opts.Prefix,
		FileSystem: &fileSystem{client: c},
		LockSystem: webdav.NewMemLS(),
	}
}
