package webdav

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/mem9-ai/drive9/pkg/client"
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
	rec := newPutRecorder()
	staging := t.TempDir()
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	c := client.New(srv.URL, "test-key")
	c.Warm(context.Background())

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
	body, ok := rec.body("/v1/fs/dir/file.txt")
	if !ok || string(body) != payload {
		t.Fatalf("committed body = %d bytes, want %d", len(body), len(payload))
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
