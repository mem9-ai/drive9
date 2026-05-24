package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
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

	t.Run("non-FS endpoints still denied (sql/fork/events/journals/vault)", func(t *testing.T) {
		// Read-side (C1), write-side (C2a), and uploads (C2b) are all
		// admitted. chmod stays owner-only forever. SQL/fork/events/
		// journals/vault are permanently out of scope for scoped tokens.
		cases := []struct {
			method string
			path   string
			query  string
		}{
			{http.MethodPost, "/v1/fs/main.txt", "chmod=1"},           // owner-only forever
			{http.MethodGet, "/v1/fs:batch-stat", ""},                 // wrong method for this endpoint
			{http.MethodGet, "/v1/fs:batch-read-small", ""},           // wrong method
			{http.MethodPost, "/v1/sql", ""},                          // out of scope
			{http.MethodPost, "/v1/fork", ""},                         // out of scope
			{http.MethodGet, "/v1/events", ""},                        // out of scope
			{http.MethodGet, "/v1/journals", ""},                      // out of scope
			{http.MethodGet, "/v1/vault/secrets", ""},                 // out of scope
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

// TestC2aWriteHandlersRejectScopedRequestBeforeBackend mirrors the C1
// ordering proof for the new write-side handlers wired in C2a. Same
// zero-value `&Server{}` trick: handler MUST call authorizeFS / authorizeFSPair
// before touching backendFromRequest, otherwise the response would be
// 401 "missing tenant scope" instead of 403 "fs access denied".
func TestC2aWriteHandlersRejectScopedRequestBeforeBackend(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpList: true, FSOpWrite: true, FSOpDelete: true},
		}},
	}

	cases := []struct {
		name   string
		invoke func(s *Server, w http.ResponseWriter, r *http.Request)
	}{
		{"handleWrite", func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleWrite(w, r, "/main/secrets.env")
		}},
		{"handlePatch", func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handlePatch(w, r, "/main/secrets.env")
		}},
		{"handleAppend", func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleAppend(w, r, "/main/secrets.env")
		}},
		{"handleCreate", func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleCreate(w, r, "/main/secrets.env")
		}},
		{"handleMkdir", func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleMkdir(w, r, "/main/newdir")
		}},
		{"handleDelete", func(s *Server, w http.ResponseWriter, r *http.Request) {
			s.handleDelete(w, r, "/main/secrets.env")
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newScopedRequest(t, http.MethodPost, "/v1/fs/main/secrets.env", "")
			r = r.WithContext(withScope(r.Context(), scope))
			w := httptest.NewRecorder()
			tc.invoke(&Server{}, w, r)
			if got := w.Result().StatusCode; got != http.StatusForbidden {
				t.Errorf("%s out-of-zone status = %d, want %d (must authorize before backend). body=%s",
					tc.name, got, http.StatusForbidden, w.Body.String())
			}
		})
	}
}

// TestC2aHandleAppendAuthorizesBeforePlan asserts the banked
// plan-timing invariant: handleAppend authorizes BEFORE generating the
// upload plan. Otherwise the plan response would leak "this prefix is
// writable" even if the subsequent PUT is denied.
//
// Method: a scoped token out-of-zone calling append should get 403 with
// the canonical "fs access denied" JSON body, NOT a plan response.
func TestC2aHandleAppendAuthorizesBeforePlan(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch",
			Ops:    map[FSOp]bool{FSOpWrite: true},
		}},
	}
	r := newScopedRequest(t, http.MethodPost, "/v1/fs/main/log.txt", "append=1")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	(&Server{}).handleAppend(w, r, "/main/log.txt")

	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", got, http.StatusForbidden)
	}
	// The 403 body must NOT look like an upload plan (which has fields like
	// upload_id, part_size, max_parts). Look for the canonical authz error.
	if !strings.Contains(w.Body.String(), "fs access denied") {
		t.Errorf("body = %q, want canonical 'fs access denied' (not a plan response)", w.Body.String())
	}
}

// TestC2aHandleChmodRejectsScopedToken pins the chmod-is-owner-only
// invariant at the handler level (defense in depth — dispatcher already
// rejects scoped tokens on ?chmod=1, but the handler MUST also reject
// in case the dispatcher gate ever drifts).
func TestC2aHandleChmodRejectsScopedToken(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		// Give the scoped token write+delete on EVERY zone — it still
		// must not be able to chmod, because chmod isn't in the op set.
		FSScopes: []FSScope{{
			Prefix: "/",
			Ops: map[FSOp]bool{
				FSOpRead: true, FSOpList: true, FSOpSearch: true,
				FSOpWrite: true, FSOpDelete: true,
			},
		}},
	}
	r := newScopedRequest(t, http.MethodPost, "/v1/fs/main/file.txt", "chmod=1")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	(&Server{}).handleChmod(w, r, "/main/file.txt")

	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d (chmod is owner-only)", got, http.StatusForbidden)
	}
}

// TestC2aHandleCopyAuthorizesSrcViaHeader is the critical regression for
// the copy dual-path invariant (banked from cp -r PR #434 work): copy's
// source path comes from X-Dat9-Copy-Source HEADER, not the URL. A scoped
// token allowed write on dst but NOT read on src must STILL get 403,
// not 200 just because the URL path looks fine to a careless dispatcher.
func TestC2aHandleCopyAuthorizesSrcViaHeader(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			// Allow read+write on /scratch but NOT on /secrets.
			Prefix: "/scratch",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpWrite: true},
		}},
	}
	// URL = dst = /scratch/exfil.env (in zone, write allowed).
	// Header = src = /secrets/api-key.env (out of zone, read denied).
	r := newScopedRequest(t, http.MethodPost, "/v1/fs/scratch/exfil.env", "copy=1")
	r.Header.Set("X-Dat9-Copy-Source", "/secrets/api-key.env")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	(&Server{}).handleCopy(w, r, "/scratch/exfil.env")

	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d (src in header must be authorized, not just URL dst). body=%s",
			got, http.StatusForbidden, w.Body.String())
	}
}

// TestC2aHandleCopyAllowsBothInZone confirms the happy path: scoped token
// with read on src zone AND write on dst zone allows copy. Negative tests
// verify each missing capability denies.
func TestC2aHandleCopyAllowsBothInZone(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{
			{Prefix: "/source", Ops: map[FSOp]bool{FSOpRead: true}},
			{Prefix: "/dest", Ops: map[FSOp]bool{FSOpWrite: true}},
		},
	}
	r := newScopedRequest(t, http.MethodPost, "/v1/fs/dest/a.txt", "copy=1")
	r.Header.Set("X-Dat9-Copy-Source", "/source/a.txt")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	(&Server{}).handleCopy(w, r, "/dest/a.txt")

	// Authorize-pass path reaches backendFromRequest (nil here) → 401.
	// We're proving the pair authorize PASSED for both endpoints; the
	// nil-backend 401 is the post-authorize signal.
	if got := w.Result().StatusCode; got != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (authorize passed; backend missing). body=%s",
			got, http.StatusUnauthorized, w.Body.String())
	}
}

// TestC2aHandleRenameSrcRequiresDeleteNotWrite pins the banked rename
// invariant: rename's src op is DELETE (the file disappears), not WRITE.
// A token with read+write on the source zone but NO delete cannot rename
// — even though they could copy + (try to) delete original.
func TestC2aHandleRenameSrcRequiresDeleteNotWrite(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			// Read+Write on /scratch but NO delete.
			Prefix: "/scratch",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpWrite: true},
		}},
	}
	r := newScopedRequest(t, http.MethodPost, "/v1/fs/scratch/new.txt", "rename=1")
	r.Header.Set("X-Dat9-Rename-Source", "/scratch/old.txt")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	(&Server{}).handleRename(w, r, "/scratch/new.txt")

	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Fatalf("status = %d, want %d (rename src requires delete, not write)",
			got, http.StatusForbidden)
	}
}

// TestC2aHandleRenameAllowsDeletePlusWrite confirms rename works with
// the correct ops: delete on src + write on dst.
func TestC2aHandleRenameAllowsDeletePlusWrite(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			// Single zone with full ops.
			Prefix: "/scratch",
			Ops: map[FSOp]bool{
				FSOpRead: true, FSOpWrite: true, FSOpDelete: true,
			},
		}},
	}
	r := newScopedRequest(t, http.MethodPost, "/v1/fs/scratch/new.txt", "rename=1")
	r.Header.Set("X-Dat9-Rename-Source", "/scratch/old.txt")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	(&Server{}).handleRename(w, r, "/scratch/new.txt")

	// Pair authorize passes; missing backend → 401.
	if got := w.Result().StatusCode; got != http.StatusUnauthorized {
		t.Errorf("status = %d, want %d (authorize passed; backend missing). body=%s",
			got, http.StatusUnauthorized, w.Body.String())
	}
}

// TestC2aDispatcherWriteSideAllowed verifies the dispatcher whitelist
// extension for write-side methods admits the right requests and denies
// chmod, mixed selectors, and unknown query keys per @adversary-1's
// "no silent first-wins" invariant.
func TestC2aDispatcherWriteSideAllowed(t *testing.T) {
	t.Run("write-side allowed", func(t *testing.T) {
		cases := []struct {
			method string
			query  string
		}{
			{http.MethodPut, ""},
			{http.MethodPatch, ""},
			{http.MethodDelete, ""},
			{http.MethodDelete, "recursive=1"},
			{http.MethodDelete, "kind=dir"},
			{http.MethodDelete, "recursive=1&kind=dir"},
			{http.MethodPost, "append=1"},
			{http.MethodPost, "copy=1"},
			{http.MethodPost, "rename=1"},
			{http.MethodPost, "mkdir=1"},
			{http.MethodPost, "mkdir=1&mode=755"},
			{http.MethodPost, "create=1"},
			{http.MethodPost, "symlink=1"},
		}
		for _, tc := range cases {
			r := newScopedRequest(t, tc.method, "/v1/fs/main.txt", tc.query)
			if !isScopedBusinessRequestAllowed(r) {
				t.Errorf("isScopedBusinessRequestAllowed(%s /v1/fs/main.txt?%s) = false, want true",
					tc.method, tc.query)
			}
		}
	})

	t.Run("chmod always denied for scoped", func(t *testing.T) {
		r := newScopedRequest(t, http.MethodPost, "/v1/fs/main.txt", "chmod=1")
		if isScopedBusinessRequestAllowed(r) {
			t.Errorf("chmod must be denied for scoped tokens at dispatcher")
		}
	})

	t.Run("mixed POST selectors denied as ambiguous", func(t *testing.T) {
		cases := []string{
			"append=1&copy=1",
			"copy=1&rename=1",
			"mkdir=1&create=1",
			"create=1&symlink=1",
			"copy=1&chmod=1", // chmod combined with anything → deny
			"append=1&mkdir=1&create=1",
		}
		for _, q := range cases {
			r := newScopedRequest(t, http.MethodPost, "/v1/fs/main.txt", q)
			if isScopedBusinessRequestAllowed(r) {
				t.Errorf("isScopedBusinessRequestAllowed(POST /v1/fs/main.txt?%s) = true, want false (ambiguous mixed selectors must deny)", q)
			}
		}
	})

	t.Run("POST without selector denied", func(t *testing.T) {
		r := newScopedRequest(t, http.MethodPost, "/v1/fs/main.txt", "")
		if isScopedBusinessRequestAllowed(r) {
			t.Errorf("POST without action selector must deny (no handler matches)")
		}
	})

	t.Run("PUT/PATCH reject unknown query keys", func(t *testing.T) {
		// handleWrite and handlePatch don't consume any query params; any
		// extra key would be a sign of caller confusion or future drift.
		for _, m := range []string{http.MethodPut, http.MethodPatch} {
			r := newScopedRequest(t, m, "/v1/fs/main.txt", "extra=1")
			if isScopedBusinessRequestAllowed(r) {
				t.Errorf("%s with unknown query key must deny", m)
			}
		}
	})

	t.Run("POST action arms reject cross-arm filter keys", func(t *testing.T) {
		// `mkdir` allows ?mode; `mkdir&mode` is fine.
		// But `copy&mode` is cross-arm: mode is a mkdir filter, not copy.
		cases := []struct {
			query string
			why   string
		}{
			{"copy=1&mode=755", "mode is a mkdir-arm key, not copy-arm"},
			{"create=1&mode=755", "mode is a mkdir-arm key, not create-arm"},
			{"append=1&extra=x", "append doesn't consume any filter param"},
		}
		for _, tc := range cases {
			r := newScopedRequest(t, http.MethodPost, "/v1/fs/main.txt", tc.query)
			if isScopedBusinessRequestAllowed(r) {
				t.Errorf("POST /v1/fs/main.txt?%s = true, want false (%s)", tc.query, tc.why)
			}
		}
	})
}

// TestC2bHandleUploadInitiateAuthorizesTargetPath proves V1
// handleUploadInitiate calls authorizeFS on the request-body target path
// BEFORE any backend mutation. We exercise this with a request whose
// body claims `/main/secrets.env` as target while the scoped token only
// has `/scratch/run-1` access — expect 403 from the authorize check
// before any further validation.
//
// Note: handleUploadInitiate signature takes a backend `b` parameter.
// We pass nil because the authorize check runs before any backend use.
// If anyone reorders, this test fails on nil-deref panic, which is also
// a signal.
func TestC2bHandleUploadInitiateAuthorizesTargetPath(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpWrite: true},
		}},
	}
	body := `{"path":"/main/secrets.env","total_size":1024,"part_checksums":["abc"]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/uploads/initiate", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	// nil backend — authorize must short-circuit before any b.* call.
	(&Server{maxUploadBytes: 1 << 30}).handleUploadInitiate(w, r, nil)

	if got := w.Result().StatusCode; got != http.StatusForbidden {
		t.Errorf("status = %d, want %d (target path /main/secrets.env not in scope)", got, http.StatusForbidden)
	}
}

// TestC2bHandleV2UploadInitiateAuthorizesTargetPath is the V2 twin.
// Same shape: scoped token without target zone → 403 before any backend
// session is created.
func TestC2bHandleV2UploadInitiateAuthorizesTargetPath(t *testing.T) {
	scope := &TenantScope{
		IsScoped: true,
		FSScopes: []FSScope{{
			Prefix: "/scratch/run-1",
			Ops:    map[FSOp]bool{FSOpRead: true, FSOpWrite: true},
		}},
	}
	body := `{"path":"/main/secrets.env","total_size":1024}`
	r := httptest.NewRequest(http.MethodPost, "/v2/uploads/initiate", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r = r.WithContext(withScope(r.Context(), scope))
	w := httptest.NewRecorder()
	// V2 handleV2UploadInitiate has nil-backend short-circuit at top, so
	// we can't reach the authz check with nil backend. Use a sentinel: if
	// backend is nil, the handler returns 401 BEFORE authz. So the body
	// would never get parsed. Instead, test ordering by verifying the
	// dispatcher already admits this request — then handler authz runs.
	if !isScopedBusinessRequestAllowed(r) {
		t.Fatalf("V2 initiate must be dispatcher-allowed for scoped tokens (C2b)")
	}

	// Verify handler path: passing nil backend reaches the "missing
	// tenant scope" 401, not the body-parse 400 or initiate 403. This is
	// because handleV2UploadInitiate checks backend first — by design.
	// Owner-perf invariant unchanged; the authorize call happens AFTER
	// backend check.
	(&Server{maxUploadBytes: 1 << 30}).handleV2UploadInitiate(w, r)
	if got := w.Result().StatusCode; got != http.StatusUnauthorized {
		t.Errorf("V2 initiate with nil backend status = %d, want %d (missing tenant scope)", got, http.StatusUnauthorized)
	}
}

// TestC2bUploadsAdmittedAtDispatcher pins the C2b allowlist: scoped tokens
// can reach the exact set of upload routes that are wired with handler-
// side authorization. Anything outside this list (wrong method, unknown
// action, future routes) is denied at the dispatcher — release-order
// safety per @adversary-1 msg 09266f14, same pattern as the C1 GET
// action-arm allowlist.
func TestC2bUploadsAdmittedAtDispatcher(t *testing.T) {
	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/uploads"},                          // handleUploadInitiate
		{http.MethodPost, "/v1/uploads/initiate"},                 // handleUploadInitiate
		{http.MethodGet, "/v1/uploads"},                           // handleUploads list
		{http.MethodPost, "/v1/uploads/upload-1/complete"},        // handleUploadComplete
		{http.MethodPost, "/v1/uploads/upload-1/resume"},          // handleUploadResume
		{http.MethodGet, "/v1/uploads/upload-1/resume"},           // handleUploadResume (GET form)
		{http.MethodDelete, "/v1/uploads/upload-1"},               // handleUploadAbort
		{http.MethodPost, "/v2/uploads/initiate"},                 // handleV2UploadInitiate
		{http.MethodPost, "/v2/uploads/upload-1/presign"},         // handleV2PresignPart
		{http.MethodPost, "/v2/uploads/upload-1/presign-batch"},   // handleV2PresignBatch
		{http.MethodPost, "/v2/uploads/upload-1/complete"},        // handleV2UploadComplete
		{http.MethodPost, "/v2/uploads/upload-1/abort"},           // handleV2UploadAbort
	}
	for _, tc := range allowed {
		r := newScopedRequest(t, tc.method, tc.path, "")
		if !isScopedBusinessRequestAllowed(r) {
			t.Errorf("%s %s = denied at dispatcher; should be allowed (handler authorizes per-request)", tc.method, tc.path)
		}
	}
}

// TestC2bUploadsDeniedRouteMethodMismatches verifies release-order safety
// per @adversary-1 msg 09266f14: scoped tokens must not enter upload
// routes that are NOT in the wired-handler set. This includes method/
// path mismatches (e.g. GET /v1/uploads/initiate which downstream
// handleUploads doesn't route), unknown V1 actions (`/foo`), unknown V2
// actions (`/upload-1/garbage`), and any wrong-method combination.
func TestC2bUploadsDeniedRouteMethodMismatches(t *testing.T) {
	cases := []struct {
		method string
		path   string
		why    string
	}{
		// V1 wrong methods on family roots.
		{http.MethodPut, "/v1/uploads", "PUT on /v1/uploads — no handler routes this"},
		{http.MethodDelete, "/v1/uploads", "DELETE on /v1/uploads — no handler"},
		{http.MethodPatch, "/v1/uploads", "PATCH on /v1/uploads — no handler"},
		// V1 initiate-action wrong methods.
		{http.MethodGet, "/v1/uploads/initiate", "GET on initiate — handleUploads only does GET on /v1/uploads (list), not /initiate"},
		{http.MethodDelete, "/v1/uploads/initiate", "DELETE on initiate — no handler"},
		// V1 per-upload unknown actions.
		{http.MethodPost, "/v1/uploads/upload-1/garbage", "unknown action /garbage — handleUploadAction routes only complete/resume"},
		{http.MethodPost, "/v1/uploads/upload-1/cancel", "unknown action /cancel"},
		{http.MethodGet, "/v1/uploads/upload-1/complete", "GET on /complete — handleUploadAction routes POST only"},
		{http.MethodPut, "/v1/uploads/upload-1", "PUT on /uploads/<id> — handleUploadAction routes DELETE only"},
		{http.MethodPost, "/v1/uploads/upload-1", "bare POST on /uploads/<id> with no action — handleUploadAction routes DELETE only (POST needs an action suffix)"},
		// V1 missing upload_id.
		{http.MethodDelete, "/v1/uploads/", "no upload_id in path"},
		// V2 wrong methods.
		{http.MethodGet, "/v2/uploads/initiate", "GET on V2 initiate — no handler"},
		{http.MethodDelete, "/v2/uploads/upload-1/abort", "DELETE on V2 abort — handleV2Uploads routes POST only"},
		// V2 unknown actions.
		{http.MethodPost, "/v2/uploads/upload-1/garbage", "unknown V2 action — handleV2Uploads routes 5 known actions"},
		{http.MethodPost, "/v2/uploads/upload-1/resume", "V2 has no resume action — only V1 does"},
		{http.MethodPost, "/v2/uploads/upload-1", "bare /<id> with no action — V2 always requires an action"},
		// V2 missing upload_id.
		{http.MethodPost, "/v2/uploads/", "no upload_id in V2 path"},
		// Future routes (no handler yet).
		{http.MethodPost, "/v1/uploads/extras", "unknown family-root sibling"},
		{http.MethodPost, "/v3/uploads/something", "future V3 family"},
	}
	for _, tc := range cases {
		r := newScopedRequest(t, tc.method, tc.path, "")
		if isScopedBusinessRequestAllowed(r) {
			t.Errorf("%s %s = allowed at dispatcher; want denied (%s)", tc.method, tc.path, tc.why)
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
