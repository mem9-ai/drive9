package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/mem9-ai/dat9/pkg/meta"
)

// newScopedRequest constructs a synthetic *http.Request suitable for unit-
// testing isScopedBusinessRequestAllowed. Body is empty; we only inspect
// method, URL.Path, and URL.Query() in the gate.
func newScopedRequest(t *testing.T, method, path, rawQuery string) *http.Request {
	t.Helper()
	url := path
	if rawQuery != "" {
		url = path + "?" + rawQuery
	}
	r := httptest.NewRequest(method, url, nil)
	return r
}

func TestAuthorizeFSOwnerAllowsAll(t *testing.T) {
	scope := &TenantScope{}
	if err := scope.AuthorizeFS(FSOpDelete, "/main/secrets.txt"); err != nil {
		t.Fatalf("owner AuthorizeFS error = %v, want nil", err)
	}
}

func TestAuthorizeFSScopedAllowsMatchingOpAndPrefix(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpList: true, FSOpWrite: true},
		}},
	}

	if err := scope.AuthorizeFS(FSOpRead, "/scratch/run-1/input.txt"); err != nil {
		t.Fatalf("AuthorizeFS allowed path error = %v, want nil", err)
	}
	if err := scope.AuthorizeFS(FSOpWrite, ":/scratch/run-1/out.txt"); err != nil {
		t.Fatalf("AuthorizeFS drive-style path error = %v, want nil", err)
	}
}

func TestAuthorizeFSDeniesOutsidePrefix(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}

	err := scope.AuthorizeFS(FSOpRead, "/scratch/run-2/input.txt")
	if !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("AuthorizeFS error = %v, want ErrFSAccessDenied", err)
	}
}

func TestAuthorizeFSUsesSegmentBoundary(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/foo",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}

	if err := scope.AuthorizeFS(FSOpRead, "/foo/bar.txt"); err != nil {
		t.Fatalf("AuthorizeFS /foo/bar.txt error = %v, want nil", err)
	}
	err := scope.AuthorizeFS(FSOpRead, "/foobar/secrets.txt")
	if !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("AuthorizeFS /foobar error = %v, want ErrFSAccessDenied", err)
	}
}

func TestAuthorizeFSRejectsEscapingPathBeforeMatching(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}

	err := scope.AuthorizeFS(FSOpRead, "/scratch/run-1/../main/secrets.txt")
	if !errors.Is(err, ErrFSInvalidPath) {
		t.Fatalf("AuthorizeFS escaped path error = %v, want ErrFSInvalidPath", err)
	}
}

func TestAuthorizeFSPairRequiresBothPaths(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{
			{Prefix: "/source", Ops: map[FSOp]bool{FSOpRead: true}},
			{Prefix: "/dest", Ops: map[FSOp]bool{FSOpWrite: true}},
		},
	}

	if err := scope.AuthorizeFSPair(FSOpRead, "/source/a.txt", FSOpWrite, "/dest/a.txt"); err != nil {
		t.Fatalf("AuthorizeFSPair allowed paths error = %v, want nil", err)
	}
	err := scope.AuthorizeFSPair(FSOpRead, "/source/a.txt", FSOpWrite, "/other/a.txt")
	if !errors.Is(err, ErrFSAccessDenied) {
		t.Fatalf("AuthorizeFSPair dst error = %v, want ErrFSAccessDenied", err)
	}
}

func TestParseFSScopeOpsRejectsSearchWithoutRead(t *testing.T) {
	if _, err := parseFSScopeOps("search"); err == nil {
		t.Fatal("parseFSScopeOps(search) error = nil, want error")
	}
	if _, err := parseFSScopeOps("read,search"); err != nil {
		t.Fatalf("parseFSScopeOps(read,search) error = %v, want nil", err)
	}
}

func TestFSScopeFromMetaRejectsEmptyPrefix(t *testing.T) {
	if _, err := fsScopesFromMeta(nil); err != nil {
		t.Fatalf("fsScopesFromMeta(nil) error = %v, want nil", err)
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: "", Ops: "read"}}); err == nil {
		t.Fatal("fsScopesFromMeta(empty prefix) error = nil, want error")
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: "   ", Ops: "read"}}); err == nil {
		t.Fatal("fsScopesFromMeta(blank prefix) error = nil, want error")
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: ":", Ops: "read"}}); err == nil {
		t.Fatal("fsScopesFromMeta(bare colon prefix) error = nil, want error")
	}
	if _, err := fsScopesFromMeta([]meta.APIKeyFSScope{{Prefix: ":/", Ops: "read"}}); err != nil {
		t.Fatalf("fsScopesFromMeta(explicit root prefix) error = %v, want nil", err)
	}
}

// TestIsScopedBusinessRequestAllowed verifies the dispatcher-level guard
// admits the read-side endpoints PR C1 wires (so the handlers can run their
// own AuthorizeFS) and continues to deny every write-side / non-FS endpoint.
// The intent matches the @adversary-1 / @dev-1 review on the C1 thread
// (msg b6f53023 / 4619c945 / 005e8b0b): release-order safety — never open
// a route before a handler in this PR is known to authorize its target path.
func TestIsScopedBusinessRequestAllowed(t *testing.T) {
	t.Run("read-side endpoints allowed", func(t *testing.T) {
		cases := []struct {
			method string
			path   string
			query  string
		}{
			{http.MethodGet, "/v1/fs/main.txt", ""},
			{http.MethodGet, "/v1/fs/dir/", "list=1"},
			{http.MethodGet, "/v1/fs/dir/file.txt", "stat=1"},
			{http.MethodGet, "/v1/fs/dir/", "grep=hello"},
			// Regression for @adversary-1 msg 00efe734: grep + limit must
			// pass dispatcher (handleGrep reads ?limit).
			{http.MethodGet, "/v1/fs/dir/", "grep=hello&limit=20"},
			{http.MethodGet, "/v1/fs/dir/", "find=name:foo"},
			// Regression for @adversary-1 msg 00efe734: find + filter params
			// must pass dispatcher (handleFind reads name/tag/newer/older/
			// minsize/maxsize/limit).
			{http.MethodGet, "/v1/fs/dir/", "find=&name=*.yaml&newer=2026-03-01"},
			{http.MethodGet, "/v1/fs/dir/", "find=&tag=k=v&minsize=10&maxsize=100&limit=50"},
			{http.MethodGet, "/v1/fs/dir/", "find=&older=2026-01-01"},
			{http.MethodHead, "/v1/fs/main.txt", ""},
			{http.MethodPost, "/v1/fs:batch-stat", ""},
			{http.MethodPost, "/v1/fs:batch-read-small", ""},
		}
		for _, tc := range cases {
			r := newScopedRequest(t, tc.method, tc.path, tc.query)
			if !isScopedBusinessRequestAllowed(r) {
				t.Errorf("isScopedBusinessRequestAllowed(%s %s?%s) = false, want true",
					tc.method, tc.path, tc.query)
			}
		}
	})

	t.Run("write-side endpoints denied (left for C2)", func(t *testing.T) {
		cases := []struct {
			method string
			path   string
			query  string
		}{
			{http.MethodPut, "/v1/fs/main.txt", ""},
			{http.MethodPatch, "/v1/fs/main.txt", ""},
			{http.MethodDelete, "/v1/fs/main.txt", ""},
			{http.MethodPost, "/v1/fs/main.txt", "append=1"},
			{http.MethodPost, "/v1/fs/main.txt", "copy=1"},
			{http.MethodPost, "/v1/fs/main.txt", "rename=1"},
			{http.MethodPost, "/v1/fs/main.txt", "mkdir=1"},
			{http.MethodPost, "/v1/fs/main.txt", "chmod=1"},
			{http.MethodPost, "/v1/fs/main.txt", "create=1"},
			{http.MethodGet, "/v1/fs:batch-stat", ""},          // wrong method for this endpoint
			{http.MethodGet, "/v1/fs:batch-read-small", ""},    // wrong method
			{http.MethodPost, "/v1/uploads", ""},
			{http.MethodPost, "/v1/uploads/initiate", ""},
			{http.MethodPost, "/v1/uploads/upload-1/complete", ""},
			{http.MethodPost, "/v2/uploads/upload-1/parts", ""},
			{http.MethodPost, "/v1/sql", ""},
			{http.MethodPost, "/v1/fork", ""},
			{http.MethodGet, "/v1/events", ""},
			{http.MethodGet, "/v1/journals", ""},
			{http.MethodGet, "/v1/vault/secrets", ""},
		}
		for _, tc := range cases {
			r := newScopedRequest(t, tc.method, tc.path, tc.query)
			if isScopedBusinessRequestAllowed(r) {
				t.Errorf("isScopedBusinessRequestAllowed(%s %s?%s) = true, want false",
					tc.method, tc.path, tc.query)
			}
		}
	})

	t.Run("unknown GET query keys are denied (no silent inheritance)", func(t *testing.T) {
		// release-order safety per @dev-1 msg 005e8b0b: future GET-side
		// query actions must not silently inherit the C1 whitelist.
		cases := []struct {
			query string
			why   string
		}{
			{"newaction=1", "unknown action selector → no arm matches → deny"},
			{"name=foo", "find filter param without ?find selector → handleRead arm rejects"},
			{"limit=20", "limit param without ?grep or ?find selector → handleRead arm rejects"},
			{"grep=hello&newer=2026-01-01", "newer is a find-arm key, not a grep-arm key → grep arm rejects"},
			{"find=&secret=value", "unknown filter param on find arm → deny"},
			{"stat=1&extra=x", "stat arm only accepts ?stat → deny"},
			{"list=1&filter=foo", "list arm only accepts ?list (no filters today) → deny"},
		}
		for _, tc := range cases {
			r := newScopedRequest(t, http.MethodGet, "/v1/fs/dir/", tc.query)
			if isScopedBusinessRequestAllowed(r) {
				t.Errorf("isScopedBusinessRequestAllowed(GET /v1/fs/dir/?%s) = true, want false (%s)",
					tc.query, tc.why)
			}
		}
	})
}

// TestAuthorizeFSHTTPHelperOwnerAllows verifies the authorizeFS HTTP helper
// short-circuits to true for owner tokens (no FSScopes), so wired handlers
// have zero behavioral change for legacy/owner callers. This is the per-
// request flip side of the AuthorizeFS owner fast-path.
func TestAuthorizeFSHTTPHelperOwnerAllows(t *testing.T) {
	r := newScopedRequest(t, http.MethodGet, "/v1/fs/main.txt", "")
	r = r.WithContext(withScope(r.Context(), &TenantScope{ /* IsScoped: false */ }))
	w := httptest.NewRecorder()
	if ok := authorizeFS(w, r, FSOpRead, "/main.txt"); !ok {
		t.Fatalf("authorizeFS owner = false, want true")
	}
	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Errorf("response status = %d, want %d (no body written for allow)", got, http.StatusOK)
	}
}

// TestAuthorizeFSHTTPHelperScopedAllowsInZone confirms a scoped token whose
// FSScopes match the requested op+path passes through with no body written.
func TestAuthorizeFSHTTPHelperScopedAllowsInZone(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}
	r := newScopedRequest(t, http.MethodGet, "/v1/fs/scratch/run-1/input.txt", "")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	if ok := authorizeFS(w, r, FSOpRead, "/scratch/run-1/input.txt"); !ok {
		t.Fatalf("authorizeFS scoped in-zone = false, want true")
	}
	if got := w.Result().StatusCode; got != http.StatusOK {
		t.Errorf("response status = %d, want %d", got, http.StatusOK)
	}
}

// TestAuthorizeFSHTTPHelperScopedDeniesOutOfZone confirms a scoped token
// outside its FSScopes returns 403 and writes a JSON error body.
func TestAuthorizeFSHTTPHelperScopedDeniesOutOfZone(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}
	r := newScopedRequest(t, http.MethodGet, "/v1/fs/main/secrets.env", "")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	if ok := authorizeFS(w, r, FSOpRead, "/main/secrets.env"); ok {
		t.Fatalf("authorizeFS scoped out-of-zone = true, want false")
	}
	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Errorf("response status = %d, want %d (403)", got, http.StatusForbidden)
	}
}

// TestAuthorizeFSHTTPHelperRejectsEscape confirms a path that escapes its
// canonical form via "../" gets mapped to 400 (invalid fs path), not 403.
// The distinction matters: 400 says "your input is broken", 403 says "your
// token can't do this on a valid path".
func TestAuthorizeFSHTTPHelperRejectsEscape(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}
	r := newScopedRequest(t, http.MethodGet, "/v1/fs/scratch/run-1/../main/secrets.env", "")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	if ok := authorizeFS(w, r, FSOpRead, "/scratch/run-1/../main/secrets.env"); ok {
		t.Fatalf("authorizeFS path-escape = true, want false")
	}
	if got := w.Result().StatusCode; got != http.StatusBadRequest {
		t.Errorf("response status = %d, want %d (400)", got, http.StatusBadRequest)
	}
}

// TestAuthorizeFSPathForBatchReturnsPerPathStatus is the batch-endpoint
// invariant: a single denied path becomes one 403 in the result array; the
// rest of the batch is NOT short-circuited. This preserves the cp -r
// preflight contract (PR #434) where ONE out-of-zone file in a 256-path
// batch must not bulk-reject the entire preflight.
func TestAuthorizeFSPathForBatchReturnsPerPathStatus(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}

	// In zone → allowed.
	status, msg, allowed := authorizeFSPathForBatch(scope, FSOpRead, "/scratch/file.txt")
	if !allowed {
		t.Errorf("in-zone batch path: allowed=false, want true (status=%d msg=%q)", status, msg)
	}

	// Out of zone → 403, no batch-wide reject.
	status, msg, allowed = authorizeFSPathForBatch(scope, FSOpRead, "/main/secrets.env")
	if allowed {
		t.Errorf("out-of-zone batch path: allowed=true, want false")
	}
	if status != http.StatusForbidden {
		t.Errorf("out-of-zone batch path: status=%d, want %d", status, http.StatusForbidden)
	}
	if msg == "" {
		t.Errorf("out-of-zone batch path: msg empty, want non-empty")
	}

	// Escape path → 400.
	status, _, allowed = authorizeFSPathForBatch(scope, FSOpRead, "/scratch/../main/x")
	if allowed {
		t.Errorf("escape batch path: allowed=true, want false")
	}
	if status != http.StatusBadRequest {
		t.Errorf("escape batch path: status=%d, want %d", status, http.StatusBadRequest)
	}
}

// TestAuthorizeFSPathForBatchOwnerAllows confirms the owner fast-path
// applies to the batch helper too — a nil-IsScoped owner gets allowed=true
// for every path with zero per-path overhead.
func TestAuthorizeFSPathForBatchOwnerAllows(t *testing.T) {
	scope := &TenantScope{ /* IsScoped: false */ }
	for _, p := range []string{"/main.txt", "/scratch/x.txt", "/secrets/key.env"} {
		status, msg, allowed := authorizeFSPathForBatch(scope, FSOpRead, p)
		if !allowed {
			t.Errorf("owner batch path %q: allowed=false, want true (status=%d msg=%q)", p, status, msg)
		}
	}
}

// TestHandlersRejectScopedRequestBeforeBackend is a smoke test for the C1
// wiring chain: when a scoped token whose FSScopes don't cover the request
// path enters one of the 7 read-side handlers, the handler must call
// authorizeFS first and return 403 BEFORE touching the backend. We exercise
// this by injecting a TenantScope into the request context with NO backend
// — if any handler tries backendFromRequest before authorize it'll return
// 401 ("missing tenant scope") instead of the expected 403. So this test
// also asserts ordering, not just outcome.
func TestHandlersRejectScopedRequestBeforeBackend(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpList: true, FSOpSearch: true},
		}},
	}

	cases := []struct {
		name string
		op   FSOp
		invoke func(s *Server, w http.ResponseWriter, r *http.Request)
	}{
		{"handleRead", FSOpRead, func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleRead(w, r, "/main/secrets.env")
		}},
		{"handleList", FSOpList, func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleList(w, r, "/main/")
		}},
		{"handleStat", FSOpRead, func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleStat(w, r, "/main/secrets.env")
		}},
		{"handleStatMetadata", FSOpRead, func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleStatMetadata(w, r, "/main/secrets.env")
		}},
		{"handleGrep", FSOpSearch, func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleGrep(w, r, "/main/")
		}},
		{"handleFind", FSOpSearch, func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleFind(w, r, "/main/")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newScopedRequest(t, http.MethodGet, "/v1/fs/main/secrets.env", "")
			r = r.WithContext(withScope(r.Context(), scope))
			w := httptest.NewRecorder()
			tc.invoke(&Server{}, w, r)

			// Out-of-zone scoped request: handler MUST short-circuit at
			// authorizeFS with 403 — not reach backendFromRequest (would
			// give 401) or run actual backend work.
			if got := w.Result().StatusCode; got != http.StatusForbidden {
				t.Errorf("%s out-of-zone status = %d, want %d (403). Got body: %s",
					tc.name, got, http.StatusForbidden, w.Body.String())
			}
		})
	}
}

func BenchmarkAuthorizeFSOwnerToken(b *testing.B) {
	scope := &TenantScope{}
	for i := 0; i < b.N; i++ {
		if err := scope.AuthorizeFS(FSOpRead, "/main/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuthorizeFSSingleZone(b *testing.B) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true},
		}},
	}
	for i := 0; i < b.N; i++ {
		if err := scope.AuthorizeFS(FSOpRead, "/scratch/run-1/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAuthorizeFSTenZones(b *testing.B) {
	scope := &TenantScope{IsScoped: true}
	for _, prefix := range []string{"/zone0", "/zone1", "/zone2", "/zone3", "/zone4", "/zone5", "/zone6", "/zone7", "/zone8", "/zone9"} {
		scope.FSScopes = append(scope.FSScopes, FSScope{
			Prefix: prefix,
			Ops:    map[FSOp]bool{FSOpRead: true},
		})
	}
	for i := 0; i < b.N; i++ {
		if err := scope.AuthorizeFS(FSOpRead, "/zone9/file.txt"); err != nil {
			b.Fatal(err)
		}
	}
}
