package webdav

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"sync"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
	"golang.org/x/net/webdav"
)

// putConfig bounds one PUT's staging and commit.
type putConfig struct {
	// bufferBytes is how much of the body may be held in memory; the rest
	// is staged to a temporary file.
	bufferBytes int64
	// stagingDir is where the overflow is staged ("" = os.TempDir()).
	stagingDir string
	// timeout bounds the commit Close performs.
	timeout time.Duration
	// noSpill keeps the historical unbounded in-memory staging. The HTTP
	// layer sets it for a non-PUT write: x/net/webdav's COPY removes an
	// existing destination BEFORE it opens the replacement, so a staging
	// failure there would destroy the old content with nothing to put in its
	// place. COPY keeps the pre-existing in-memory behaviour (with the known
	// ~2N RSS cost tracked as a follow-up: a server-side copy is the fix).
	noSpill bool
}

func (cfg putConfig) bufferLimit() int64 {
	if cfg.bufferBytes > 0 {
		return cfg.bufferBytes
	}
	return DefaultPutBufferBytes
}

func (cfg putConfig) commitTimeout() time.Duration {
	if cfg.timeout > 0 {
		return cfg.timeout
	}
	return DefaultPutTimeout
}

// putRequest carries what the HTTP layer knows about one PUT and the file
// layer must not guess: the declared body length and the body's own read
// outcome, so a truncated upload is never committed, and the commit result,
// which the HTTP layer turns into a truthful status (x/net/webdav answers 405
// for every write/close error).
type putRequest struct {
	expected int64 // declared body length; -1 when the request did not say
	body     *putBody
	mu       sync.Mutex
	err      error
	// Facts captured for the failure log: the remote path, the staged size
	// and when the write started.
	path    string
	size    int64
	started time.Time
}

// putBody observes the request body's reads. x/net/webdav copies the body and
// still calls File.Close on the write file after the copy failed, so without
// this record a connection that died mid-body would commit the bytes that did
// arrive — a truncated image published as if complete.
type putBody struct {
	rc  io.ReadCloser
	mu  sync.Mutex
	err error
}

func (b *putBody) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if err != nil && err != io.EOF {
		b.mu.Lock()
		if b.err == nil {
			b.err = err
		}
		b.mu.Unlock()
	}
	return n, err
}

func (b *putBody) Close() error { return b.rc.Close() }

// failure reports the first read error the body produced, if any.
func (b *putBody) failure() error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.err
}

func (p *putRequest) bodyFailure() error {
	if p == nil {
		return nil
	}
	return p.body.failure()
}

// putRequestKey is the request-context key carrying the *putRequest.
type putRequestKey struct{}

func putRequestFrom(ctx context.Context) *putRequest {
	req, _ := ctx.Value(putRequestKey{}).(*putRequest)
	return req
}

func (p *putRequest) expectedLen() int64 {
	if p == nil {
		return -1
	}
	return p.expected
}

// recordStart captures the put's remote path and start time.
func (p *putRequest) recordStart(path string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.path, p.started = path, time.Now()
}

// recordSize captures the staged body length for the failure log.
func (p *putRequest) recordSize(size int64) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.size = size
}

// snapshot returns the facts the failure log reports.
func (p *putRequest) snapshot() (path string, size int64, elapsed time.Duration, err error) {
	if p == nil {
		return "", 0, 0, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started.IsZero() {
		return p.path, p.size, 0, p.err
	}
	return p.path, p.size, time.Since(p.started), p.err
}

// fail records the first failure of this PUT.
func (p *putRequest) fail(err error) {
	if p == nil || err == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err == nil {
		p.err = err
	}
}

// failure reports the recorded commit error, if any.
func (p *putRequest) failure() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// writeFile is a webdav.File that stages a write and commits it on Close.
//
// Staging is bounded: the first PutBufferBytes of the body stay in memory and
// the overflow moves to an unlinked temporary file, so a large PUT costs disk
// rather than RSS and its failure point is a write error, not an OOM kill far
// from the request. Close is the single commit point — the body is committed
// whole or not at all, in one Drive9 request, and a nil return means the
// server acknowledged the new revision.
type writeFile struct {
	client *client.Client
	path   string
	props  *deadPropStore
	put    putConfig
	req    *putRequest

	// buf holds the staged prefix; past the memory bound file takes over
	// and buf is empty.
	buf  bytes.Buffer
	file *os.File
	name string // staged file's name (already unlinked where the OS allows)
	size int64

	// werr latches the first staging failure: a body that was not fully
	// recorded must not be committed later.
	werr   error
	closed bool
}

var _ webdav.File = (*writeFile)(nil)
var _ webdav.DeadPropsHolder = (*writeFile)(nil)

func (w *writeFile) Read([]byte) (int, error)           { return 0, fmt.Errorf("write-only") }
func (w *writeFile) Readdir(int) ([]fs.FileInfo, error) { return nil, fmt.Errorf("not a directory") }
func (w *writeFile) Seek(int64, int) (int64, error)     { return 0, fmt.Errorf("not seekable") }

// Write stages p, spilling to disk once the body outgrows the memory bound.
func (w *writeFile) Write(p []byte) (int, error) {
	if w.werr != nil {
		return 0, w.werr
	}
	if w.file == nil && !w.put.noSpill && w.size+int64(len(p)) > w.put.bufferLimit() {
		if err := w.spill(); err != nil {
			return 0, w.stageFail(err)
		}
	}
	var (
		n   int
		err error
	)
	if w.file != nil {
		n, err = w.file.Write(p)
	} else {
		n, err = w.buf.Write(p)
	}
	w.size += int64(n)
	if err != nil {
		return n, w.stageFail(fmt.Errorf("webdav: stage PUT body: %w", err))
	}
	return n, nil
}

// spill moves the staged prefix into a temporary file and switches the rest of
// the body to it. The file is unlinked immediately where the OS allows it, so
// a crashed process leaves no PUT bodies behind; the name is kept for systems
// that cannot unlink an open file.
func (w *writeFile) spill() error {
	dir := w.put.stagingDir
	if dir == "" {
		dir = os.TempDir()
	}
	file, err := os.CreateTemp(dir, "drive9-webdav-put-*")
	if err != nil {
		return fmt.Errorf("webdav: stage PUT body: %w", err)
	}
	w.name = file.Name()
	_ = os.Remove(w.name)
	if _, err := file.Write(w.buf.Bytes()); err != nil {
		_ = file.Close()
		_ = os.Remove(w.name)
		return fmt.Errorf("webdav: stage PUT body: %w", err)
	}
	w.buf.Reset()
	w.file = file
	return nil
}

// Close commits the staged body. It is the PUT's commit point: the body is
// written in one request, and only a server acknowledgement (a committed
// revision) returns nil. A commit that fails before the server publishes
// leaves the target untouched; one that times out is indeterminate (the server
// may have published before the acknowledgement deadline).
func (w *writeFile) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	defer w.cleanup()
	w.req.recordSize(w.size)

	if w.werr != nil {
		return w.werr
	}
	// A body shorter than the request declared is an aborted upload: the
	// bytes that did arrive are a truncated image, never a commit.
	if expected := w.req.expectedLen(); expected >= 0 && w.size != expected {
		return w.fail(fmt.Errorf("webdav: PUT body ended after %d of %d declared bytes", w.size, expected))
	}
	if err := w.req.bodyFailure(); err != nil {
		return w.fail(fmt.Errorf("webdav: PUT body aborted after %d bytes: %w", w.size, err))
	}

	// webdav.File.Close has no context parameter. Do not retain the OpenFile
	// request context here: clients can finish sending a PUT body and close
	// the connection before the server flushes the staged bytes to Drive9.
	// The commit therefore runs on its own context, but a BOUNDED one — an
	// untimed background context let an unresponsive server park the close
	// (and the PUT) forever.
	ctx, cancel := context.WithTimeout(context.Background(), w.put.commitTimeout())
	defer cancel()

	if w.file != nil {
		// A spilled body is a spill-file upload: the multipart plane is
		// forced, because the direct-PUT branch reads the WHOLE reader into
		// memory and would undo the staging bound whenever the server's
		// inline threshold is larger than PutBufferBytes. Layout routing
		// (extent vs classic) is the client's, unchanged.
		staged := io.NewSectionReader(w.file, 0, w.size)
		_, err := w.client.WriteStreamWithSummary(ctx, w.path, staged, w.size, nil, client.WithMultipartOnly())
		return w.commitError(err)
	}
	// The in-memory body takes the client's own transport decision too:
	// WriteStreamWithSummary keeps a direct PUT below the server's inline
	// threshold and switches to the multipart plane at or above it. A single
	// direct PUT (WriteCtx) would be REJECTED by an S3-backed server for any
	// body at or above that threshold — its large-file branch requires part
	// checksums — so bodies between the threshold and the staging bound would
	// not save at all.
	data := w.buf.Bytes()
	_, err := w.client.WriteStreamWithSummary(ctx, w.path, bytes.NewReader(data), int64(len(data)), nil)
	return w.commitError(err)
}

// cleanup releases the staged file. A committed or failed PUT leaves nothing
// behind in the staging directory.
func (w *writeFile) cleanup() {
	if w.file != nil {
		_ = w.file.Close()
		w.file = nil
	}
	if w.name != "" {
		_ = os.Remove(w.name)
		w.name = ""
	}
}

// fail records the PUT's failure for the HTTP layer and returns it.
func (w *writeFile) fail(err error) error {
	w.req.fail(err)
	return err
}

// commitError records a failed commit. A deadline is annotated as
// INDETERMINATE rather than failed: the server may have published the revision
// before the acknowledgement deadline expired, and nothing in the protocol can
// tell the two apart or roll the write back.
func (w *writeFile) commitError(err error) error {
	if err == nil {
		return nil
	}
	err = mapError(err)
	if errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("webdav: commit outcome unknown after PutTimeout: %w", err)
	}
	return w.fail(err)
}

// stageFail latches a STAGING failure on the handle and reports it. The latch
// is what keeps Close from committing a body that was never fully recorded: a
// request-body read error is detected separately (putBody), but a failed spill
// or a short disk write is only visible here, and x/net/webdav still calls
// Close after the copy fails.
func (w *writeFile) stageFail(err error) error {
	if w.werr == nil {
		w.werr = err
	}
	return w.fail(err)
}

func (w *writeFile) Stat() (os.FileInfo, error) {
	return &fileInfo{
		name: path.Base(w.path),
		stat: &client.StatResult{Size: w.size},
	}, nil
}

func (w *writeFile) DeadProps() (map[xml.Name]webdav.Property, error) {
	return w.props.deadProps(w.path)
}

func (w *writeFile) Patch(patches []webdav.Proppatch) ([]webdav.Propstat, error) {
	return w.props.patch(w.path, patches)
}
