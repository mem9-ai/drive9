package webdav

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/mem9-ai/drive9/internal/testbackend"
	"github.com/mem9-ai/drive9/pkg/client"
	"golang.org/x/net/webdav"
)

// newCopySemanticsHandler builds a handler over the shared fake backend with
// the PUT staging bound small enough that a copy of any real payload spills.
func newCopySemanticsHandler(t *testing.T, b *testbackend.Backend, opts Options) (http.Handler, *client.Client) {
	t.Helper()
	ts := httptest.NewServer(b)
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	return NewHandler(c, opts), c
}

func copyRequest(t *testing.T, src, dst string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, src, nil)
	req.Method = "COPY"
	req.Header.Set("Destination", dst)
	return req
}

// A file COPY goes through the staged writer: the destination is independent
// content (not a server-side alias) and the copy reports the WebDAV status for
// created vs replaced.
func TestCopyFileWritesIndependentContent(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	src := strings.Repeat("payload-", 512)
	b.Seed("/dir/src", []byte(src))
	handler, _ := newCopySemanticsHandler(t, b, Options{PutBufferBytes: 64})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != http.StatusCreated {
		t.Fatalf("COPY create status = %d, want %d: %s", rr.Code, http.StatusCreated, rr.Body.String())
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != src {
		t.Fatalf("destination = %q (ok=%v), want the source bytes", node.Data, ok)
	}
	// Bytes were copied, not aliased: the server-side ?copy endpoint (which
	// attaches the source's file id) must not have been used.
	if got := b.Requests()["POST /v1/fs/dir/dst"]; got != 0 {
		t.Fatalf("server-side copy requests = %d, want 0 (the destination must hold independent bytes)", got)
	}
	if got := b.Requests()["GET /v1/fs/dir/src"]; got != 1 {
		t.Fatalf("source GETs = %d, want 1", got)
	}

	// Overwriting an existing file reports 204 and replaces the bytes.
	b.Seed("/dir/src", []byte("second"))
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("COPY replace status = %d, want %d: %s", rr.Code, http.StatusNoContent, rr.Body.String())
	}
	if node, _ := b.Node("/dir/dst"); string(node.Data) != "second" {
		t.Fatalf("destination = %q, want second", node.Data)
	}
	// The replacement is atomic: the fast path never deletes the destination
	// first, which is exactly the window x/net's copyFiles opens.
	if got := b.Requests()["DELETE /v1/fs/dir/dst"]; got != 0 {
		t.Fatalf("overwriting COPY issued %d DELETE(s) on the destination, want 0", got)
	}
}

// The whole point of the fast path: x/net's copy deletes the destination
// before writing the replacement, so a copy that fails mid-stream would leave
// the old content gone. Here the destination is committed atomically, so a
// failure keeps it.
func TestCopyFailureKeepsOldDestination(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	full := strings.Repeat("z", 4096)
	b.Seed("/dir/src", []byte(full))
	b.Seed("/dir/dst", []byte("ORIGINAL"))

	// Truncate the source body: the client's copy hits a read error after
	// some bytes, exactly like a lost connection.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir/src" {
			w.Header().Set("Content-Length", strconv.Itoa(len(full)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(full[:16]))
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
				}
			}
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	handler := NewHandler(c, Options{PutBufferBytes: 64})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("COPY of a truncated source = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != "ORIGINAL" {
		t.Fatalf("failed copy changed the destination: %q (ok=%v)", node.Data, ok)
	}
	if got := b.Requests()["PUT /v1/fs/dir/dst"]; got != 0 {
		t.Fatalf("truncated copy still committed (%d PUTs)", got)
	}
}

// Overwrite: F with an existing destination is a precondition failure, and the
// destination is untouched.
func TestCopyOverwriteFalseAnswersPreconditionFailed(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.Seed("/dir/dst", []byte("ORIGINAL"))
	handler, _ := newCopySemanticsHandler(t, b, Options{})

	req := copyRequest(t, "/dir/src", "/dir/dst")
	req.Header.Set("Overwrite", "F")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusPreconditionFailed {
		t.Fatalf("COPY Overwrite:F = %d, want %d", rr.Code, http.StatusPreconditionFailed)
	}
	if node, _ := b.Node("/dir/dst"); string(node.Data) != "ORIGINAL" {
		t.Fatalf("COPY Overwrite:F changed the destination: %q", node.Data)
	}
}

// A collection COPY keeps x/net/webdav's recursive semantics (the fast path
// only answers for regular files).
func TestCopyCollectionDelegatesToXNet(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/tree", 0o755)
	b.Seed("/tree/f.txt", []byte("leaf"))
	handler, _ := newCopySemanticsHandler(t, b, Options{PutBufferBytes: 64})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/tree", "/copy"))
	if rr.Code != http.StatusCreated && rr.Code != http.StatusNoContent {
		t.Fatalf("collection COPY status = %d: %s", rr.Code, rr.Body.String())
	}
	node, ok := b.Node("/copy/f.txt")
	if !ok || string(node.Data) != "leaf" {
		t.Fatalf("copied collection child = %q (ok=%v), want leaf", node.Data, ok)
	}
}

// Dead properties follow the resource, and a replaced destination does not
// keep its old ones.
func TestCopyCarriesDeadProperties(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("payload"))
	handler, _ := newCopySemanticsHandler(t, b, Options{})

	patchBody := `<?xml version="1.0"?>` +
		`<D:propertyupdate xmlns:D="DAV:" xmlns:Z="urn:schemas-microsoft-com:">` +
		`<D:set><D:prop><Z:Win32FileAttributes>00000020</Z:Win32FileAttributes></D:prop></D:set>` +
		`</D:propertyupdate>`
	patchReq := httptest.NewRequest("PROPPATCH", "/dir/src", strings.NewReader(patchBody))
	patchReq.Header.Set("Content-Type", "application/xml")
	patchRR := httptest.NewRecorder()
	handler.ServeHTTP(patchRR, patchReq)
	if patchRR.Code != http.StatusMultiStatus {
		t.Fatalf("PROPPATCH status = %d: %s", patchRR.Code, patchRR.Body.String())
	}

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != http.StatusCreated {
		t.Fatalf("COPY status = %d: %s", rr.Code, rr.Body.String())
	}

	findBody := `<?xml version="1.0"?>` +
		`<D:propfind xmlns:D="DAV:" xmlns:Z="urn:schemas-microsoft-com:">` +
		`<D:prop><Z:Win32FileAttributes/></D:prop>` +
		`</D:propfind>`
	findReq := httptest.NewRequest("PROPFIND", "/dir/dst", strings.NewReader(findBody))
	findReq.Header.Set("Depth", "0")
	findReq.Header.Set("Content-Type", "application/xml")
	findRR := httptest.NewRecorder()
	handler.ServeHTTP(findRR, findReq)
	if findRR.Code != http.StatusMultiStatus {
		t.Fatalf("PROPFIND status = %d: %s", findRR.Code, findRR.Body.String())
	}
	if !strings.Contains(findRR.Body.String(), "00000020") {
		t.Fatalf("copied resource lost its dead property: %q", findRR.Body.String())
	}
}

// A commit that fails must not have removed the destination first. Under
// x/net's copyFiles the destination is deleted before the write, so the same
// failure loses it; the fast path keeps it until the atomic replace lands.
func TestCopyCommitFailureKeepsOldDestination(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh bytes"))
	b.Seed("/dir/dst", []byte("ORIGINAL"))

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/dir/dst" {
			http.Error(w, "storage exploded", http.StatusInternalServerError)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	handler := NewHandler(c, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("COPY with a failing commit = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != "ORIGINAL" {
		t.Fatalf("failed commit lost the old destination: %q (ok=%v)", node.Data, ok)
	}
}

// Lock parity: x/net's COPY takes a temporary lock on the destination, which
// fails when another client holds a conflicting one. The fast path must not
// let a locked destination be replaced.
func TestCopyToLockedDestinationAnswersLocked(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.Seed("/dir/dst", []byte("ORIGINAL"))
	handler, _ := newCopySemanticsHandler(t, b, Options{})

	lockBody := `<?xml version="1.0"?>` +
		`<D:lockinfo xmlns:D="DAV:"><D:lockscope><D:exclusive/></D:lockscope>` +
		`<D:locktype><D:write/></D:locktype><D:owner><D:href>tester</D:href></D:owner></D:lockinfo>`
	lockReq := httptest.NewRequest("LOCK", "/dir/dst", strings.NewReader(lockBody))
	lockReq.Header.Set("Content-Type", "application/xml")
	lockReq.Header.Set("Timeout", "Second-600")
	lockRR := httptest.NewRecorder()
	handler.ServeHTTP(lockRR, lockReq)
	if lockRR.Code != http.StatusOK {
		t.Fatalf("LOCK status = %d: %s", lockRR.Code, lockRR.Body.String())
	}

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != webdav.StatusLocked {
		t.Fatalf("COPY onto a locked destination = %d, want %d", rr.Code, webdav.StatusLocked)
	}
	if node, _ := b.Node("/dir/dst"); string(node.Data) != "ORIGINAL" {
		t.Fatalf("COPY onto a locked destination changed it: %q", node.Data)
	}
}
