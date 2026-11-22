package webdav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	// Bytes were copied, not aliased: the destination must be its own object,
	// not the source's file id behind a second name (the server-side ?copy
	// endpoint attaches the source's file id, i.e. a hardlink).
	if b.SharesObject("/dir/src", "/dir/dst") {
		t.Fatal("the copy produced an alias of the source, not an independent object")
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

// A hard-linked destination must not be written in place: the other name would
// change too. x/net replaces the name instead (POSIX copy breaks the link).
func TestCopyHardLinkedDestinationDelegates(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/alias", []byte("ALIAS CONTENT"))
	b.SeedHardlink("/dir/alias", "/dir/dst")
	b.Seed("/dir/src", []byte("copied bytes"))
	handler, _ := newCopySemanticsHandler(t, b, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code >= 300 {
		t.Fatalf("COPY onto a hard link = %d: %s", rr.Code, rr.Body.String())
	}
	if node, ok := b.Node("/dir/dst"); !ok || string(node.Data) != "copied bytes" {
		t.Fatalf("destination = %q (ok=%v), want the copied bytes", node.Data, ok)
	}
	// The shared name keeps the old content: the copy replaced the name, it
	// did not write through the alias.
	if node, ok := b.Node("/dir/alias"); !ok || string(node.Data) != "ALIAS CONTENT" {
		t.Fatalf("hard-linked alias changed to %q (ok=%v); the copy wrote through the link", node.Data, ok)
	}
}

// x/net rejects a Depth other than 0 or infinity BEFORE touching the
// destination; the fast path must not turn that into an overwrite.
func TestCopyInvalidDepthDelegatesAndRejects(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.Seed("/dir/dst", []byte("ORIGINAL"))
	handler, _ := newCopySemanticsHandler(t, b, Options{})

	for _, depth := range []string{"1", "INFINITY", "bogus"} {
		req := copyRequest(t, "/dir/src", "/dir/dst")
		req.Header.Set("Depth", depth)
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		// x/net's parseDepth is case-SENSITIVE: only "0", "1" and "infinity"
		// parse, and handleCopyMove rejects anything but 0/infinity.
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("COPY with Depth:%s = %d, want %d", depth, rr.Code, http.StatusBadRequest)
		}
		if node, _ := b.Node("/dir/dst"); string(node.Data) != "ORIGINAL" {
			t.Fatalf("invalid-Depth COPY (%s) changed the destination: %q", depth, node.Data)
		}
	}
}

// Overwrite: F must still hold at COMMIT time: a writer that takes the
// destination name while the copy streams must not be clobbered.
func TestCopyOverwriteFalseSurvivesConcurrentCreate(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("copied bytes"))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir/src" {
			// An API writer takes the destination name while the source
			// streams, after the copy's existence check.
			b.Seed("/dir/dst", []byte("INTRUDER"))
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	handler := NewHandler(c, Options{})

	req := copyRequest(t, "/dir/src", "/dir/dst")
	req.Header.Set("Overwrite", "F")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusPreconditionFailed {
		t.Fatalf("COPY that lost the Overwrite:F race = %d, want %d", rr.Code, http.StatusPreconditionFailed)
	}
	if node, ok := b.Node("/dir/dst"); !ok || string(node.Data) != "INTRUDER" {
		t.Fatalf("concurrent writer's content = %q (ok=%v), want INTRUDER", node.Data, ok)
	}
}

// A replaced destination is a fresh resource (x/net removes the name first),
// so its old user tags must go: otherwise `drive9 fs find -tag` matches
// content that no longer exists.
func TestCopyReplaceClearsStaleDestinationTags(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.SeedWithTags("/dir/dst", []byte("ORIGINAL"), map[string]string{"team": "old"})
	handler, _ := newCopySemanticsHandler(t, b, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("COPY replace status = %d, want %d: %s", rr.Code, http.StatusNoContent, rr.Body.String())
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != "fresh" {
		t.Fatalf("destination = %q (ok=%v), want fresh", node.Data, ok)
	}
	if len(node.Tags) != 0 {
		t.Fatalf("replaced destination kept stale tags: %v", node.Tags)
	}
}

// The tag clear honours the PUT commit's bound: a shortened PutTimeout must cap
// the clear instead of letting a stalled setmeta hold the request open.
func TestCopyTagClearIsBoundedByPutTimeout(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.SeedWithTags("/dir/dst", []byte("ORIGINAL"), map[string]string{"team": "old"})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("setmeta") {
			// Stall until the client gives up, with a hard cap so a broken
			// bound fails the assertion instead of hanging the suite.
			select {
			case <-r.Context().Done():
			case <-time.After(3 * time.Second):
			}
			http.Error(w, "setmeta stalled", http.StatusInternalServerError)
			return
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	handler := NewHandler(c, Options{PutTimeout: 100 * time.Millisecond})

	start := time.Now()
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	elapsed := time.Since(start)
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("COPY whose tag clear timed out = %d, want %d: %s", rr.Code, http.StatusInternalServerError, rr.Body.String())
	}
	if elapsed > time.Second {
		t.Fatalf("tag clear took %v with a 100ms PutTimeout: the bound is not applied", elapsed)
	}
}

// A create-only COPY whose body outgrows the staging bound must carry its
// transport option through the conditional commit: a spill-file upload has to
// take the multipart plane, not a direct PUT that reads the whole staged file
// back into memory.
func TestCopyCreateOnlySpilledBodyCommitsThroughMultipart(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	payload := strings.Repeat("s", 256<<10) // below the 1 MiB inline threshold, above the 4 KiB spill bound
	b.Seed("/dir/src", []byte(payload))
	handler, _ := newCopySemanticsHandler(t, b, Options{PutBufferBytes: 4 << 10})

	req := copyRequest(t, "/dir/src", "/dir/dst")
	req.Header.Set("Overwrite", "F")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusCreated {
		t.Fatalf("create-only COPY of a spilled body = %d, want %d: %s", rr.Code, http.StatusCreated, rr.Body.String())
	}
	if got := b.Requests()["POST /v2/uploads/initiate"]; got != 1 {
		t.Fatalf("multipart initiate requests = %d, want 1 (the conditional commit dropped its transport option)", got)
	}
	if got := b.Requests()["PUT /v1/fs/dir/dst"]; got != 0 {
		t.Fatalf("direct PUT requests = %d, want 0 (the spilled body took the direct-PUT branch)", got)
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != payload {
		t.Fatalf("committed body = %d bytes (ok=%v), want %d", len(node.Data), ok, len(payload))
	}
}

// The commit's failure status keeps the two shapes apart: an unconditional
// commit that loses a revision race is a server error (500), and a create-only
// commit that fails for another reason is not a precondition failure (412).
func TestCopyCommitFailureStatuses(t *testing.T) {
	t.Run("unconditional conflict is 500", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.SeedDir("/dir", 0o755)
		b.Seed("/dir/src", []byte("fresh"))
		b.Seed("/dir/dst", []byte("ORIGINAL"))
		b.ForceConflict("/dir/dst", 1)
		handler, _ := newCopySemanticsHandler(t, b, Options{})

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("unconditional COPY whose commit conflicted = %d, want %d: %s", rr.Code, http.StatusInternalServerError, rr.Body.String())
		}
		if node, ok := b.Node("/dir/dst"); !ok || string(node.Data) != "ORIGINAL" {
			t.Fatalf("destination = %q (ok=%v), want the untouched ORIGINAL", node.Data, ok)
		}
	})

	t.Run("create-only server error is 500", func(t *testing.T) {
		b := testbackend.NewBackend()
		b.SeedDir("/dir", 0o755)
		b.Seed("/dir/src", []byte("fresh"))
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/dir/dst" {
				http.Error(w, "commit unavailable", http.StatusInternalServerError)
				return
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(ts.Close)
		c := client.New(ts.URL, "test-key")
		c.Warm(t.Context())
		handler := NewHandler(c, Options{})

		req := copyRequest(t, "/dir/src", "/dir/dst")
		req.Header.Set("Overwrite", "F")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusInternalServerError {
			t.Fatalf("create-only COPY whose commit failed = %d, want %d: %s", rr.Code, http.StatusInternalServerError, rr.Body.String())
		}
		if node, ok := b.Node("/dir/dst"); ok {
			t.Fatalf("destination = %q after a failed create-only commit, want it absent", node.Data)
		}
	})
}

// The tag clear is retried once: a transient failure on the first attempt must
// not turn a landed copy into a 500 when the second attempt succeeds.
func TestCopyRetriesTagClearOnce(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.SeedWithTags("/dir/dst", []byte("ORIGINAL"), map[string]string{"team": "old"})
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("setmeta") {
			if attempts.Add(1) == 1 {
				http.Error(w, "setmeta temporarily unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	handler := NewHandler(c, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("COPY whose first tag clear failed = %d, want %d: %s", rr.Code, http.StatusNoContent, rr.Body.String())
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("tag clear attempts = %d, want 2 (first fails, second succeeds)", got)
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != "fresh" {
		t.Fatalf("destination = %q (ok=%v), want fresh", node.Data, ok)
	}
	if len(node.Tags) != 0 {
		t.Fatalf("replaced destination kept stale tags: %v", node.Tags)
	}
}

// The destination's parent is validated the way x/net validates it: a missing
// parent is 409 (Conflict) and a parent that is not a directory is 403
// (Forbidden), not the other way round.
func TestCopyRejectsUnusableDestinationParent(t *testing.T) {
	cases := []struct {
		name   string
		dst    string
		seed   func(*testbackend.Backend)
		status int
	}{
		{
			name:   "missing parent is 409",
			dst:    "/missing/dst",
			status: http.StatusConflict,
		},
		{
			name: "non-directory parent is 403",
			dst:  "/dir/file/dst",
			seed: func(b *testbackend.Backend) {
				b.Seed("/dir/file", []byte("not a directory"))
			},
			status: http.StatusForbidden,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := testbackend.NewBackend()
			b.SeedDir("/dir", 0o755)
			b.Seed("/dir/src", []byte("copied bytes"))
			if tc.seed != nil {
				tc.seed(b)
			}
			handler, _ := newCopySemanticsHandler(t, b, Options{})

			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, copyRequest(t, "/dir/src", tc.dst))
			if rr.Code != tc.status {
				t.Fatalf("COPY to %s = %d, want %d: %s", tc.dst, rr.Code, tc.status, rr.Body.String())
			}
			if node, ok := b.Node(tc.dst); ok {
				t.Fatalf("COPY to %s created %q despite the %d", tc.dst, node.Data, tc.status)
			}
		})
	}
}

// A symlink destination must be replaced by a regular file, as x/net does by
// removing the name first; writing through it would corrupt the link's target.
func TestCopySymlinkDestinationDelegates(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("copied bytes"))
	b.SeedSymlink("/dir/dst", "/dir/elsewhere")
	handler, _ := newCopySemanticsHandler(t, b, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code >= 300 {
		t.Fatalf("COPY onto a symlink = %d: %s", rr.Code, rr.Body.String())
	}
	node, ok := b.Node("/dir/dst")
	if !ok {
		t.Fatal("destination missing after COPY")
	}
	if node.Mode&0o170000 == 0o120000 {
		t.Fatalf("destination is still a symlink (mode %o)", node.Mode)
	}
	if string(node.Data) != "copied bytes" {
		t.Fatalf("destination = %q, want the copied bytes", node.Data)
	}
}

// A replaced destination's tag clear is a post-condition: if it cannot
// settle, the COPY must not report success (the retry rewrites and clears it).
func TestCopyReplaceFailsWhenTagsCannotBeCleared(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.SeedWithTags("/dir/dst", []byte("ORIGINAL"), map[string]string{"team": "old"})
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("setmeta") {
			attempts.Add(1)
			http.Error(w, "setmeta unavailable", http.StatusInternalServerError)
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
		t.Fatalf("COPY with an un-clearable destination = %d, want %d", rr.Code, http.StatusInternalServerError)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("tag clear attempts against a persistent failure = %d, want exactly 2", got)
	}
}

// A conditional commit keeps the size-aware transport decision: a classic body
// at or above the inline threshold takes the multipart plane, not a single
// direct PUT the server rejects. Both outcomes are pinned — the create-only CAS
// wins on an absent destination and loses to one an API writer takes mid-stream
// — so a commit path that failed unconditionally, or one that stopped sending
// the precondition, cannot pass.
func TestCopyConditionalCommitAboveInlineThresholdUsesMultipart(t *testing.T) {
	const payloadSize = 2 << 20 // above the 1 MiB inline threshold

	// conditionalCopy runs one Overwrite:F COPY of a payloadSize body and
	// returns the response, the backend it ran against and the number of tag
	// clears that reached the server.
	conditionalCopy := func(t *testing.T, onSourceRead func(*testbackend.Backend)) (*httptest.ResponseRecorder, *testbackend.Backend, int32) {
		t.Helper()
		b := testbackend.NewBackend()
		b.SeedDir("/dir", 0o755)
		payload := strings.Repeat("k", payloadSize)
		b.Seed("/dir/src", []byte(payload))
		var setmeta atomic.Int32
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && r.URL.Query().Has("setmeta") {
				setmeta.Add(1)
			}
			if onSourceRead != nil && r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir/src" {
				onSourceRead(b)
			}
			b.ServeHTTP(w, r)
		}))
		t.Cleanup(ts.Close)
		c := client.New(ts.URL, "test-key")
		c.Warm(t.Context())
		handler := NewHandler(c, Options{PutBufferBytes: 4 << 20})

		req := copyRequest(t, "/dir/src", "/dir/dst")
		req.Header.Set("Overwrite", "F") // create-only CAS commit
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		return rr, b, setmeta.Load()
	}

	t.Run("create-only commit succeeds", func(t *testing.T) {
		rr, b, clears := conditionalCopy(t, nil)
		if rr.Code != http.StatusCreated {
			t.Fatalf("conditional COPY onto an absent destination = %d, want %d: %s", rr.Code, http.StatusCreated, rr.Body.String())
		}
		if got := b.Requests()["POST /v2/uploads/initiate"]; got != 1 {
			t.Fatalf("multipart initiate requests = %d, want 1 (conditional commit took the direct-PUT branch)", got)
		}
		node, ok := b.Node("/dir/dst")
		if !ok {
			t.Fatal("destination missing after a successful conditional COPY")
		}
		if len(node.Data) != payloadSize || strings.Trim(string(node.Data), "k") != "" {
			t.Fatalf("destination = %d bytes (%q...), want the %d-byte source", len(node.Data), string(node.Data[:8]), payloadSize)
		}
		if clears != 0 {
			t.Fatalf("tag clears on a create-only COPY = %d, want 0: a created destination never had tags", clears)
		}
	})

	t.Run("lost race answers 412", func(t *testing.T) {
		rr, b, _ := conditionalCopy(t, func(b *testbackend.Backend) {
			// The destination appears while the source streams: the create-only
			// CAS must reject the multipart complete, not just the small-body
			// direct PUT.
			b.Seed("/dir/dst", []byte("INTRUDER"))
		})
		if rr.Code != http.StatusPreconditionFailed {
			t.Fatalf("lost-race multipart COPY = %d, want %d: %s", rr.Code, http.StatusPreconditionFailed, rr.Body.String())
		}
		if got := b.Requests()["POST /v2/uploads/initiate"]; got != 1 {
			t.Fatalf("multipart initiate requests = %d, want 1 (CAS commit took the direct-PUT branch)", got)
		}
		if node, ok := b.Node("/dir/dst"); !ok || string(node.Data) != "INTRUDER" {
			t.Fatalf("concurrent writer's content = %q (ok=%v), want INTRUDER", node.Data, ok)
		}
	})
}

// A destination absent at the check can still be created by an API writer
// before the unconditional commit lands; that writer's tags must not survive
// the replacement.
func TestCopyClearsTagsOfConcurrentlyCreatedDestination(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("copied bytes"))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/fs/dir/src" {
			// An API writer takes the absent destination while the source
			// streams, carrying its own tags.
			b.SeedWithTags("/dir/dst", []byte("INTRUDER"), map[string]string{"team": "new"})
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	handler := NewHandler(c, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst"))
	if rr.Code >= 300 {
		t.Fatalf("COPY over a concurrently created destination = %d: %s", rr.Code, rr.Body.String())
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != "copied bytes" {
		t.Fatalf("destination = %q (ok=%v), want the copied bytes", node.Data, ok)
	}
	if len(node.Tags) != 0 {
		t.Fatalf("concurrent writer's tags survived the replacement: %v", node.Tags)
	}
}

// A client that disconnects once the body is handed to the commit must not
// take the tag clear with it: the clear runs on the commit's own detached,
// bounded context, exactly like the content publish. The WebDAV status here is
// what the handler would have written; the connection is already gone.
func TestCopyTagClearSurvivesClientDisconnect(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.SeedWithTags("/dir/dst", []byte("ORIGINAL"), map[string]string{"team": "old"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var setmeta atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("setmeta") {
			setmeta.Add(1)
		}
		if r.Method == http.MethodPut && r.URL.Path == "/v1/fs/dir/dst" {
			// The client goes away at the commit point.
			cancel()
		}
		b.ServeHTTP(w, r)
	}))
	t.Cleanup(ts.Close)
	c := client.New(ts.URL, "test-key")
	c.Warm(t.Context())
	handler := NewHandler(c, Options{})

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, copyRequest(t, "/dir/src", "/dir/dst").WithContext(ctx))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("COPY whose client disconnected at the commit = %d, want %d: %s", rr.Code, http.StatusNoContent, rr.Body.String())
	}
	if got := setmeta.Load(); got == 0 {
		t.Fatal("tag clear never reached the server after the client disconnected: it is running on the request context")
	}
	node, ok := b.Node("/dir/dst")
	if !ok || string(node.Data) != "fresh" {
		t.Fatalf("destination = %q (ok=%v), want the copied bytes", node.Data, ok)
	}
	if len(node.Tags) != 0 {
		t.Fatalf("replaced destination kept its tags after the client disconnected: %v", node.Tags)
	}
}

// A destination another writer removed after the commit has no tags left to
// clear: that is a settled outcome, not a failed copy.
func TestCopyReplaceToleratesRemovedDestination(t *testing.T) {
	b := testbackend.NewBackend()
	b.SeedDir("/dir", 0o755)
	b.Seed("/dir/src", []byte("fresh"))
	b.SeedWithTags("/dir/dst", []byte("ORIGINAL"), map[string]string{"team": "old"})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Query().Has("setmeta") {
			// Another writer deletes the path between the content commit and
			// the tag clear, so there is nothing left to clear.
			b.Remove("/dir/dst")
			http.Error(w, "not found", http.StatusNotFound)
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
	if rr.Code != http.StatusNoContent {
		t.Fatalf("COPY whose destination vanished before the clear = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if node, ok := b.Node("/dir/dst"); ok {
		t.Fatalf("destination = %q (ok=%v), want it gone: the copy must not recreate a path another writer removed", node.Data, ok)
	}
}
