// Package webdav provides an http.Handler that serves a drive9 filesystem
// over the WebDAV protocol. It bridges golang.org/x/net/webdav to the
// drive9 client fs API, allowing macOS mount_webdav (and other WebDAV
// clients) to access drive9 as a mounted filesystem.
package webdav

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"golang.org/x/net/webdav"
)

// Options configures the WebDAV handler.
type Options struct {
	// Prefix is the URL path prefix stripped before mapping to drive9 paths.
	// Empty means no prefix (root mount).
	Prefix string

	// RemoteRoot is the remote drive9 directory that becomes the mount root.
	// Empty or "/" means the entire remote filesystem.
	RemoteRoot string
}

// NewHandler returns an http.Handler that serves drive9 content over WebDAV.
func NewHandler(c *client.Client, opts Options) http.Handler {
	remoteRoot := opts.RemoteRoot
	if remoteRoot == "" {
		remoteRoot = "/"
	}
	metrics.SetModuleAvailability("webdav", true)
	next := &webdav.Handler{
		Prefix:     opts.Prefix,
		FileSystem: &fileSystem{client: c, remoteRoot: remoteRoot, props: newDeadPropStore()},
		LockSystem: webdav.NewMemLS(),
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusCapturingResponseWriter{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		metrics.RecordOperation("webdav", webdavMetricOperation(r.Method), webdavMetricResult(recorder.statusCode()), time.Since(start))
	})
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusCapturingResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusCapturingResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusCapturingResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *statusCapturingResponseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusCapturingResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func webdavMetricOperation(method string) string {
	method = strings.TrimSpace(strings.ToUpper(method))
	if method == "" {
		return "unknown"
	}
	return strings.ToLower(method)
}

func webdavMetricResult(status int) string {
	switch {
	case status == http.StatusMultiStatus:
		return "multi_status"
	case status >= 200 && status < 300:
		return "ok"
	case status == http.StatusConflict:
		return "conflict"
	case status == http.StatusNotFound:
		return "not_found"
	case status == http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		return "permission_denied"
	case status >= 500:
		return "error"
	default:
		return strconv.Itoa(status)
	}
}
