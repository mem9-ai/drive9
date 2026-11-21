// Package webdav provides an http.Handler that serves a drive9 filesystem
// over the WebDAV protocol. It bridges golang.org/x/net/webdav to the
// drive9 client fs API, allowing macOS mount_webdav (and other WebDAV
// clients) to access drive9 as a mounted filesystem.
package webdav

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/logger"
	"github.com/mem9-ai/drive9/pkg/metrics"
	"go.uber.org/zap"
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

	// PutBufferBytes bounds the part of a PUT body held in memory. A body
	// that outgrows it is staged to a temporary file and committed through the
	// client's streaming upload path, so a large upload costs disk and the
	// negotiated transfer window instead of holding the whole body in RSS.
	// Zero uses DefaultPutBufferBytes.
	PutBufferBytes int64

	// PutStagingDir is where a PUT body beyond PutBufferBytes is staged.
	// Empty uses os.TempDir().
	PutStagingDir string

	// PutTimeout bounds the commit a PUT performs at Close. Zero uses
	// DefaultPutTimeout. It is a client-side give-up, not a rollback: a commit
	// that fails before the server publishes leaves the target untouched, but
	// one that TIMES OUT is indeterminate — the server may have applied the
	// write before the deadline passed, and only the acknowledgement was lost.
	PutTimeout time.Duration
}

const (
	// DefaultPutBufferBytes is the in-memory staging bound of one PUT.
	DefaultPutBufferBytes = 8 << 20
	// DefaultPutTimeout bounds one PUT's commit.
	DefaultPutTimeout = 5 * time.Minute
)

// NewHandler returns an http.Handler that serves drive9 content over WebDAV.
//
// # Commit-point and error semantics
//
// A PUT's body is STAGED first — up to PutBufferBytes in memory, the overflow
// in a temporary file — and committed in one request when the WebDAV file is
// closed. Close is therefore the commit point:
//
//   - The body is committed whole or not at all. A body shorter than the
//     request's declared Content-Length (an aborted upload) is refused, and a
//     commit that fails BEFORE the server publishes the revision leaves the
//     target exactly as it was.
//   - A commit that times out is INDETERMINATE, not failed: the server may
//     have published the revision before the PutTimeout deadline expired, with
//     only the acknowledgement lost. There is no client-side rollback and the
//     WebDAV protocol carries no compare-and-swap to resolve it, so a retry
//     rewrites the file.
//   - A successful response (201 Created) means the Drive9 server has
//     acknowledged the committed revision: the bytes are durable, not
//     buffered somewhere in this process.
//   - A failed commit is answered with 500 (or 507 when the storage behind
//     the write is full), never with 405 Method Not Allowed — which is how
//     x/net/webdav collapses every write/close error, and which a client
//     would read as "PUT is unsupported" instead of "nothing was written".
func NewHandler(c *client.Client, opts Options) http.Handler {
	remoteRoot := opts.RemoteRoot
	if remoteRoot == "" {
		remoteRoot = "/"
	}
	metrics.SetModuleAvailability("webdav", true)
	next := &webdav.Handler{
		Prefix: opts.Prefix,
		FileSystem: &fileSystem{
			client:     c,
			remoteRoot: remoteRoot,
			props:      newDeadPropStore(),
			put: putConfig{
				bufferBytes: opts.PutBufferBytes,
				stagingDir:  opts.PutStagingDir,
				timeout:     opts.PutTimeout,
			},
		},
		LockSystem: webdav.NewMemLS(),
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusCapturingResponseWriter{ResponseWriter: w}
		var put *putRequest
		if r.Method == http.MethodPut {
			// A PUT's outcome is decided at Close, below x/net/webdav's
			// status mapping: hold the response, hand the file layer the
			// facts only this layer knows (the declared body length), and
			// collect the commit result to answer with.
			body := &putBody{rc: r.Body}
			r.Body = body
			put = &putRequest{expected: r.ContentLength, body: body}
			recorder.buffered = true
			r = r.WithContext(context.WithValue(r.Context(), putRequestKey{}, put))
		}
		next.ServeHTTP(recorder, r)
		recorder.commit(r.Context(), put)
		metrics.RecordOperation("webdav", webdavMetricOperation(r.Method), webdavMetricResult(recorder.statusCode()), time.Since(start))
	})
}

type statusCapturingResponseWriter struct {
	http.ResponseWriter
	status int
	// buffered holds the response until the handler's outcome is known (a
	// PUT: see commit). A PUT reply is a header plus a short status text, so
	// buffering costs nothing and lets a failed commit answer truthfully.
	buffered bool
	body     bytes.Buffer
}

func (w *statusCapturingResponseWriter) WriteHeader(code int) {
	if w.status != 0 {
		return // net/http: the first status wins
	}
	w.status = code
	if !w.buffered {
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *statusCapturingResponseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.buffered {
		return w.body.Write(p)
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusCapturingResponseWriter) Flush() {
	if w.buffered {
		return // the response is not being streamed
	}
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

// commit flushes a buffered PUT response, replacing x/net/webdav's blanket 405
// for a failed write with the status that tells the client nothing was
// committed. PUT is always a supported method, so 405 on this path can only
// mean the write, stat or close failed.
func (w *statusCapturingResponseWriter) commit(ctx context.Context, req *putRequest) {
	if !w.buffered {
		return
	}
	code := w.statusCode()
	if code == http.StatusMethodNotAllowed {
		commitErr := req.failure()
		code = putFailureStatus(commitErr)
		// The buffered body is x/net/webdav's text for the ORIGINAL status:
		// left as-is, a client that shows it would still read "Method Not
		// Allowed" for a committed-nothing failure.
		w.body.Reset()
		w.body.WriteString(http.StatusText(code))
		// The client only ever sees the status text, so the captured error —
		// including the indeterminate-timeout annotation — is logged here or
		// it is observable to nobody.
		path, size, elapsed, logErr := req.snapshot()
		logger.Error(ctx, "webdav put commit failed",
			zap.String("path", path),
			zap.Int64("body_bytes", size),
			zap.Duration("elapsed", elapsed),
			zap.Int("status", code),
			zap.Error(logErr),
		)
	}
	w.status = code
	w.ResponseWriter.WriteHeader(code)
	if w.body.Len() > 0 {
		_, _ = w.ResponseWriter.Write(w.body.Bytes())
	}
}

// putFailureStatus maps a commit failure onto the status a WebDAV client can
// act on: 507 when the storage behind the write is full (here or on the
// server), 500 otherwise. The server-side check must cover the presigned part
// PUTs a staged body uses, whose storage-full answer is a different error
// shape from the API plane's — hence the client predicate.
func putFailureStatus(err error) int {
	if client.IsInsufficientStorage(err) || errors.Is(err, syscall.ENOSPC) {
		return http.StatusInsufficientStorage
	}
	return http.StatusInternalServerError
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
