package webdav

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"github.com/mem9-ai/drive9/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// putRecorder is a minimal drive9 fs server that records the bytes of each
// PUT and can be told to fail them.
type putRecorder struct {
	mu     sync.Mutex
	bodies map[string][]byte
	puts   int
	status int
	delay  time.Duration
}

func newPutRecorder() *putRecorder {
	return &putRecorder{bodies: map[string][]byte{}}
}

func (p *putRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","max_upload_bytes":1073741824,"inline_threshold":1048576}`))
	case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/dir":
		w.Header().Set("X-Dat9-IsDir", "true")
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		p.mu.Lock()
		status, delay := p.status, p.delay
		p.puts++
		p.bodies[r.URL.Path] = body
		p.mu.Unlock()
		if delay > 0 {
			time.Sleep(delay)
		}
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"revision":1}`))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (p *putRecorder) body(path string) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	b, ok := p.bodies[path]
	return append([]byte(nil), b...), ok
}

func (p *putRecorder) putCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.puts
}

func newPUTSemanticsHandler(t *testing.T, rec *putRecorder, opts Options) http.Handler {
	t.Helper()
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	// Warm the tenant status so the streaming commit path picks the direct
	// PUT branch instead of assuming the multipart protocol is required.
	c := client.New(srv.URL, "test-key")
	c.Warm(context.Background())
	return NewHandler(c, opts)
}

// A PUT whose declared body never arrives must not be committed: a truncated
// upload replaces the destination with a short image otherwise, which is the
// silent data-loss mode the commit point exists to prevent.
func TestPutTruncatedBodyIsNotCommitted(t *testing.T) {
	rec := newPutRecorder()
	handler := newPUTSemanticsHandler(t, rec, Options{})

	req := httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader(""))
	req.ContentLength = 10 // the client promised ten bytes and sent four
	req.Body = io.NopCloser(strings.NewReader("four"))
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("truncated PUT status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	if puts := rec.putCount(); puts != 0 {
		t.Fatalf("truncated PUT reached the server (%d PUTs)", puts)
	}
	if body, ok := rec.body("/v1/fs/dir/file.txt"); ok {
		t.Fatalf("truncated PUT committed %q", body)
	}
}

// A commit that fails is a SERVER-side failure: x/net/webdav answers 405
// Method Not Allowed for every write/close error, which tells a WebDAV client
// the method is unsupported and hides that nothing was written.
func TestPutCommitFailureAnswersServerError(t *testing.T) {
	rec := newPutRecorder()
	rec.status = http.StatusInternalServerError
	handler := newPUTSemanticsHandler(t, rec, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader("payload")))

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("failed commit status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	if body := rr.Body.String(); strings.Contains(body, "Method Not Allowed") {
		t.Fatalf("failed PUT body = %q; still the 405 text for the rewritten status", body)
	}
	if puts := rec.putCount(); puts != 1 {
		t.Fatalf("server PUTs = %d, want 1", puts)
	}
}

// The commit is bounded: an unresponsive server must not park Close (and the
// PUT) forever on an untimed background context.
func TestPutCommitIsBoundedByPutTimeout(t *testing.T) {
	rec := newPutRecorder()
	rec.delay = 300 * time.Millisecond
	handler := newPUTSemanticsHandler(t, rec, Options{PutTimeout: 20 * time.Millisecond})

	start := time.Now()
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader("payload")))
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("PUT took %s, want it bounded by PutTimeout", elapsed)
	}
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("timed-out commit status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
}

// The body is staged, not accumulated: past the in-memory bound the bytes go
// to a temporary file, and the staged image is what commits.
func TestPutStagesBodyBeyondMemoryBound(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(context.Background())

	staging := t.TempDir()
	fs := &fileSystem{client: c, props: newDeadPropStore(), put: putConfig{
		bufferBytes: 8, stagingDir: staging, timeout: 5 * time.Second,
	}}
	file, err := fs.OpenFile(context.Background(), "/dir/file.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	wf, ok := file.(*writeFile)
	if !ok {
		t.Fatalf("OpenFile returned %T, want *writeFile", file)
	}
	payload := strings.Repeat("x", 1024)
	if _, err := wf.Write([]byte(payload[:512])); err != nil {
		t.Fatalf("first Write: %v", err)
	}
	if _, err := wf.Write([]byte(payload[512:])); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	if wf.file == nil {
		t.Fatal("oversized PUT body stayed in memory")
	}
	if buf := wf.buf.Len(); int64(buf) > 8 {
		t.Fatalf("in-memory prefix = %d bytes, want it bounded by 8", buf)
	}
	fi, err := wf.Stat()
	if err != nil || fi.Size() != int64(len(payload)) {
		t.Fatalf("Stat = %v/%v, want size %d", fi, err, len(payload))
	}
	if err := wf.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if wf.file != nil {
		t.Fatal("staged file not released")
	}
	if entries, _ := os.ReadDir(staging); len(entries) != 0 {
		t.Fatalf("staging directory kept %d files after commit", len(entries))
	}
	node, ok := b.Node("/dir/file.txt")
	if !ok || string(node.Data) != payload {
		t.Fatalf("committed body = %d bytes, want %d", len(node.Data), len(payload))
	}
}

// A staged body is committed through the multipart plane: the direct-PUT
// branch reads the whole reader into memory, which would undo the staging
// bound whenever the server's inline threshold is larger than the spill point.
func TestPutSpilledBodyCommitsThroughMultipart(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(context.Background()) // inline_threshold = 1 MiB, above the body below

	fs := &fileSystem{client: c, props: newDeadPropStore(), put: putConfig{
		bufferBytes: 4 << 10, stagingDir: t.TempDir(), timeout: 5 * time.Second,
	}}
	file, err := fs.OpenFile(context.Background(), "/dir/file.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	payload := strings.Repeat("m", 256<<10) // below the inline threshold, above the spill bound
	if _, err := file.Write([]byte(payload)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := b.Requests()["POST /v2/uploads/initiate"]; got != 1 {
		t.Fatalf("multipart initiate requests = %d, want 1 (spilled body took the direct-PUT branch)", got)
	}
	node, ok := b.Node("/dir/file.txt")
	if !ok || string(node.Data) != payload {
		t.Fatalf("committed body = %d bytes, want %d", len(node.Data), len(payload))
	}
}

// A staging failure must latch: x/net/webdav still calls Close after the copy
// failed, and an unknown-length (chunked) body has no declared length to
// compare against, so without the latch Close would publish the staged prefix
// and report the failure afterwards.
func TestPutStagingFailureIsNotCommitted(t *testing.T) {
	rec := newPutRecorder()
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, "test-key")
	c.Warm(context.Background()) // so a missing latch really would commit

	fs := &fileSystem{client: c, props: newDeadPropStore(), put: putConfig{
		bufferBytes: 4,
		// A path that cannot be created: the spill fails.
		stagingDir: filepath.Join(t.TempDir(), "missing", "staging"),
		timeout:    5 * time.Second,
	}}
	file, err := fs.OpenFile(context.Background(), "/dir/file.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	wf := file.(*writeFile)
	// A prefix is staged in memory; the SECOND write crosses the bound and
	// its spill fails. Without the latch Close would publish the prefix.
	if _, err := wf.Write([]byte("abc")); err != nil {
		t.Fatalf("buffered Write: %v", err)
	}
	if _, err := wf.Write([]byte("defgh")); err == nil {
		t.Fatal("Write succeeded although the spill could not be staged")
	}
	if wf.werr == nil {
		t.Fatal("staging failure was not latched on the handle")
	}
	if err := wf.Close(); err == nil {
		t.Fatal("Close committed after a staging failure")
	}
	if puts := rec.putCount(); puts != 0 {
		t.Fatalf("staging failure reached the server (%d PUTs)", puts)
	}
	if body, ok := rec.body("/v1/fs/dir/file.txt"); ok {
		t.Fatalf("staging failure committed the truncated prefix %q", body)
	}
}

func TestPutFailureStatusMapsInsufficientStorage(t *testing.T) {
	if got := putFailureStatus(errors.Join(io.ErrShortWrite, syscall.ENOSPC)); got != http.StatusInsufficientStorage {
		t.Fatalf("ENOSPC commit status = %d, want %d", got, http.StatusInsufficientStorage)
	}
	if got := putFailureStatus(errors.New("boom")); got != http.StatusInternalServerError {
		t.Fatalf("generic commit status = %d, want %d", got, http.StatusInternalServerError)
	}
}

// A server that reports the write could not be stored propagates the same
// answer: the client sees "out of storage", not a generic failure.
func TestPutServerInsufficientStorageAnswers507(t *testing.T) {
	rec := newPutRecorder()
	rec.status = http.StatusInsufficientStorage
	handler := newPUTSemanticsHandler(t, rec, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader("payload")))
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("insufficient-storage PUT status = %d, want %d", rr.Code, http.StatusInsufficientStorage)
	}
}

// abortReader yields its bytes and then fails, the way a connection lost
// mid-body reads.
type abortReader struct {
	data []byte
	sent bool
}

func (r *abortReader) Read(p []byte) (int, error) {
	if r.sent {
		return 0, io.ErrUnexpectedEOF
	}
	r.sent = true
	return copy(p, r.data), nil
}

// A body whose read FAILS (not merely ends early) is an aborted upload even
// when the request declared no length: the staged bytes must not be published.
func TestPutBodyReadErrorIsNotCommitted(t *testing.T) {
	rec := newPutRecorder()
	handler := newPUTSemanticsHandler(t, rec, Options{})

	req := httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader(""))
	req.ContentLength = -1 // chunked: no declared length to compare against
	req.Body = io.NopCloser(&abortReader{data: []byte("partial")})
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("aborted PUT status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	if puts := rec.putCount(); puts != 0 {
		t.Fatalf("aborted PUT reached the server (%d PUTs)", puts)
	}
}

// An in-memory body must take the same transport decision as a staged one: at
// or above the server's inline threshold the server routes the write to its
// large-file branch, which refuses a plain direct PUT for lack of part
// checksums. Bodies between the threshold and the staging bound would not save
// at all if the bridge always sent one direct PUT.
func TestPutInMemoryBodyAboveInlineThresholdUsesMultipart(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(context.Background()) // inline_threshold = 1 MiB

	handler := NewHandler(c, Options{PutBufferBytes: 4 << 20}) // body stays in memory
	payload := strings.Repeat("k", 2<<20)                      // 2 MiB, above the threshold
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader(payload)))
	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT status = %d, want %d: %s", rr.Code, http.StatusCreated, rr.Body.String())
	}
	if got := b.Requests()["POST /v2/uploads/initiate"]; got != 1 {
		t.Fatalf("multipart initiate requests = %d, want 1 (above-threshold body direct-PUT'd)", got)
	}
	if got := b.Requests()["PUT /v1/fs/dir/file.txt"]; got != 0 {
		t.Fatalf("direct PUT requests = %d, want 0 for an above-threshold body", got)
	}
	node, ok := b.Node("/dir/file.txt")
	if !ok || string(node.Data) != payload {
		t.Fatalf("committed body = %d bytes, want %d", len(node.Data), len(payload))
	}
}

// A staged body uploads its parts to PRESIGNED urls; a storage-full answer
// there is not a *client.StatusError, so the status must still reach the
// client as 507 rather than a generic 500 (the contract NewHandler documents).
func TestPutPresignedPartInsufficientStorageAnswers507(t *testing.T) {
	var parts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/status":
			_, _ = w.Write([]byte(`{"status":"ok","inline_threshold":1048576}`))
		case r.Method == http.MethodHead && r.URL.Path == "/v1/fs/dir":
			w.Header().Set("X-Dat9-IsDir", "true")
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/initiate":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"upload_id": "u1", "key": "/dir/file.txt", "part_size": 16 << 20, "total_parts": 1,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v2/uploads/u1/presign-batch":
			_ = json.NewEncoder(w).Encode(map[string]any{"parts": []map[string]any{{
				"number": 1, "url": "http://" + r.Host + "/presigned/part/1", "size": 65536,
			}}})
		case r.Method == http.MethodPut && r.URL.Path == "/presigned/part/1":
			parts.Add(1)
			http.Error(w, "insufficient storage", http.StatusInsufficientStorage)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, "test-key")
	c.Warm(context.Background())

	handler := NewHandler(c, Options{PutBufferBytes: 4 << 10})
	rr := httptest.NewRecorder()
	payload := strings.Repeat("q", 256<<10) // spilled: multipart, presigned part PUT
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader(payload)))

	if parts.Load() == 0 {
		t.Fatal("presigned part PUT never attempted")
	}
	if rr.Code != http.StatusInsufficientStorage {
		t.Fatalf("storage-full part PUT status = %d, want %d: %s", rr.Code, http.StatusInsufficientStorage, rr.Body.String())
	}
}

// The captured commit error reaches neither the client (which sees only the
// status text) nor an operator unless it is logged. A failed PUT must leave a
// server-side record with the path, the staged size and the error.
func TestPutCommitFailureIsLogged(t *testing.T) {
	observed, logs := observer.New(zap.ErrorLevel)
	previous := logger.L()
	logger.Set(zap.New(observed))
	t.Cleanup(func() { logger.Set(previous) })

	rec := newPutRecorder()
	rec.status = http.StatusInternalServerError
	handler := newPUTSemanticsHandler(t, rec, Options{})
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/dir/file.txt", strings.NewReader("payload")))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("PUT status = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	entries := logs.FilterMessage("webdav put commit failed").All()
	if len(entries) != 1 {
		t.Fatalf("commit-failure log entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["path"] != "/dir/file.txt" {
		t.Fatalf("logged path = %v, want /dir/file.txt", fields["path"])
	}
	if size, _ := fields["body_bytes"].(int64); size != int64(len("payload")) {
		t.Fatalf("logged body_bytes = %v, want %d", fields["body_bytes"], len("payload"))
	}
	if fields["error"] == nil {
		t.Fatalf("logged entry carries no error: %v", fields)
	}
}

// A COPY writes its destination through the same writer, but only AFTER
// x/net/webdav has deleted the old destination: a staging failure there would
// destroy the old content without a replacement. Non-PUT writes therefore keep
// the in-memory staging, and an overwrite survives an unusable staging dir.
func TestCopyDestinationSurvivesSpillFailure(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	src := strings.Repeat("s", 4096)
	b.Seed("/dir/src", []byte(src))
	b.Seed("/dir/dst", []byte("ORIGINAL"))
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(context.Background())

	handler := NewHandler(c, Options{
		PutBufferBytes: 8, // any source byte would spill a PUT
		PutStagingDir:  filepath.Join(t.TempDir(), "missing", "staging"),
	})
	req := httptest.NewRequest(http.MethodGet, "/dir/src", nil)
	req.Method = "COPY"
	req.Header.Set("Destination", "/dir/dst")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code >= 300 {
		t.Fatalf("COPY status = %d: %s", rr.Code, rr.Body.String())
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != src {
		t.Fatalf("copy destination = %q (ok=%v), want the source content", node.Data, ok)
	}
}

// The failure log must not record a presigned URL's signature: it is a
// temporary write credential, and Go's *url.Error prints the full URL.
func TestCommitFailureLogRedactsPresignedURL(t *testing.T) {
	signed := &url.Error{
		Op:  "Put",
		URL: "https://bucket.s3.amazonaws.com/obj?X-Amz-Signature=SECRETSIG&X-Amz-Credential=AKIASECRET",
		Err: errors.New("dial tcp 10.0.0.1:443: i/o timeout"),
	}
	logged := redactURLCredentials(signed.Error())
	if strings.Contains(logged, "SECRETSIG") || strings.Contains(logged, "AKIASECRET") {
		t.Fatalf("logged error still carries credentials: %q", logged)
	}
	if !strings.Contains(logged, "dial tcp") {
		t.Fatalf("logged error lost its diagnosis: %q", logged)
	}
	if got := commitErrorKind(signed); got != "transport" {
		t.Fatalf("error kind = %q, want transport", got)
	}
	if got := commitErrorKind(&client.StatusError{StatusCode: http.StatusInsufficientStorage}); got != "insufficient_storage" {
		t.Fatalf("storage-full kind = %q", got)
	}
}
