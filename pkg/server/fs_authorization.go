package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mem9-ai/drive9/pkg/backend"
	"github.com/mem9-ai/drive9/pkg/datastore"
	"github.com/mem9-ai/drive9/pkg/meta"
	"github.com/mem9-ai/drive9/pkg/pathutil"
)

type FSOp string

const (
	FSOpRead   FSOp = "read"
	FSOpList   FSOp = "list"
	FSOpSearch FSOp = "search"
	FSOpWrite  FSOp = "write"
	FSOpDelete FSOp = "delete"
)

var (
	ErrFSAccessDenied = errors.New("fs access denied")
	ErrFSInvalidPath  = errors.New("invalid fs path")
)

type FSScope struct {
	Prefix string
	Ops    map[FSOp]bool
}

func (s *TenantScope) AuthorizeFS(op FSOp, rawPath string) error {
	if s == nil {
		return ErrFSAccessDenied
	}
	if !s.IsScoped {
		return nil
	}
	if !isKnownFSOp(op) {
		return ErrFSAccessDenied
	}
	p, err := normalizeFSAuthorizationPath(rawPath)
	if err != nil {
		return err
	}
	for _, scope := range s.FSScopes {
		if !scopeAllows(scope, op, p) {
			continue
		}
		return nil
	}
	return ErrFSAccessDenied
}

func (s *TenantScope) AuthorizeFSPair(srcOp FSOp, srcPath string, dstOp FSOp, dstPath string) error {
	if err := s.AuthorizeFS(srcOp, srcPath); err != nil {
		return err
	}
	return s.AuthorizeFS(dstOp, dstPath)
}

func fsScopesFromMeta(rows []meta.APIKeyFSScope) ([]FSScope, error) {
	scopes := make([]FSScope, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.Prefix) == "" {
			return nil, errors.New("empty fs scope prefix")
		}
		if strings.TrimSpace(row.Prefix) == ":" {
			return nil, errors.New("empty fs scope prefix")
		}
		prefix, err := normalizeFSAuthorizationPath(row.Prefix)
		if err != nil {
			return nil, fmt.Errorf("normalize prefix %q: %w", row.Prefix, err)
		}
		ops, err := parseFSScopeOps(row.Ops)
		if err != nil {
			return nil, err
		}
		scopes = append(scopes, FSScope{Prefix: prefix, Ops: ops})
	}
	return scopes, nil
}

func parseFSScopeOps(raw string) (map[FSOp]bool, error) {
	ops := make(map[FSOp]bool)
	for _, part := range strings.Split(raw, ",") {
		op := FSOp(strings.TrimSpace(part))
		if op == "" {
			continue
		}
		if !isKnownFSOp(op) {
			return nil, fmt.Errorf("unknown fs scope op %q", op)
		}
		ops[op] = true
	}
	if len(ops) == 0 {
		return nil, errors.New("empty fs scope ops")
	}
	if ops[FSOpSearch] && !ops[FSOpRead] {
		return nil, errors.New("search fs scope requires read")
	}
	return ops, nil
}

func scopeAllows(scope FSScope, op FSOp, p string) bool {
	if op == FSOpSearch && !scope.Ops[FSOpRead] {
		return false
	}
	if !scope.Ops[op] {
		return false
	}
	return pathMatchesScope(scope.Prefix, p)
}

func pathMatchesScope(prefix, p string) bool {
	if prefix == "/" {
		return true
	}
	if p == prefix {
		return true
	}
	return strings.HasPrefix(p, prefix+"/")
}

func normalizeFSAuthorizationPath(raw string) (string, error) {
	raw = strings.TrimPrefix(raw, ":")
	p, err := pathutil.Canonicalize(raw)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrFSInvalidPath, err)
	}
	return p, nil
}

func isKnownFSOp(op FSOp) bool {
	switch op {
	case FSOpRead, FSOpList, FSOpSearch, FSOpWrite, FSOpDelete:
		return true
	default:
		return false
	}
}

// writeFSAuthzError maps a workspace-zone authorization error to the canonical
// HTTP error response shape used elsewhere in pkg/server. ErrFSAccessDenied
// becomes 403; ErrFSInvalidPath becomes 400 because the path itself is the
// problem (escapes the workspace, contains forbidden characters, etc.) and the
// caller cannot recover by changing credentials. Any other error is wrapped
// generically as 500 — that path is only hit when scope state itself is
// malformed, which is server-side state, not a client mistake.
func writeFSAuthzError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrFSAccessDenied):
		errJSON(w, http.StatusForbidden, "fs access denied")
	case errors.Is(err, ErrFSInvalidPath):
		errJSON(w, http.StatusBadRequest, "invalid fs path")
	default:
		errJSON(w, http.StatusInternalServerError, "fs authorization failed")
	}
}

// authorizeFS is a thin wrapper that lets handlers do
//
//	if !authorizeFS(w, r, FSOpRead, path) { return }
//
// instead of repeating the scope-extract + writeFSAuthzError boilerplate at
// every call site. Returns true iff the request may proceed; on false the
// response has already been written.
func authorizeFS(w http.ResponseWriter, r *http.Request, op FSOp, rawPath string) bool {
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return false
	}
	if err := scope.AuthorizeFS(op, rawPath); err != nil {
		writeFSAuthzError(w, err)
		return false
	}
	return true
}

// authorizeFSPair is the dual-path variant for copy / rename handlers,
// where one operation needs different op semantics on src vs dst.
//
//	copy:   src=read,   dst=write   (read the source, write the dest)
//	rename: src=delete, dst=write   (the source disappears = delete; dest = write)
//
// Owner short-circuit (IsScoped=false) inside AuthorizeFS still applies for
// both sides — same per-arm fast-path as single-path authorize.
func authorizeFSPair(w http.ResponseWriter, r *http.Request, srcOp FSOp, srcPath string, dstOp FSOp, dstPath string) bool {
	scope := ScopeFromContext(r.Context())
	if scope == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return false
	}
	if err := scope.AuthorizeFSPair(srcOp, srcPath, dstOp, dstPath); err != nil {
		writeFSAuthzError(w, err)
		return false
	}
	return true
}

// authorizeUploadSession is the upload-continuation gate for resume /
// complete / abort handlers (V1 and V2). It looks up the session by
// upload_id, then re-authorizes the session's TargetPath against the
// CURRENT request scope (NOT the scope of whoever initiated the upload).
//
// This is the banked invariant from C2 review (msgs `efb1e56c` /
// `08848b1a` / `6e17765f`): a scoped token's policy can change between
// initiate and complete (revoke + reissue with narrower zones is the
// supported policy-change mechanism). Trusting the original initiator
// would let a since-narrowed token finish a write outside its current
// scope.
//
// Error handling preserves the existing upload error shapes so client
// code (FUSE, SDK, CLI) keeps working unchanged for owner tokens:
//   - session missing (datastore.ErrNotFound)        → 404 "upload not found"
//   - session expired (datastore.ErrUploadExpired)   → 410 "upload expired"
//   - any other lookup error                          → 500 (storage)
//   - authorize denied                                → 403 (fs access denied)
//   - authorize invalid path (server-stored garbage) → 400
//
// Returns the session on allow so callers don't have to look it up twice.
// On any non-nil error, the response has been written and the caller should
// return.
func authorizeUploadSession(ctx context.Context, w http.ResponseWriter, scope *TenantScope, b *backend.Dat9Backend, uploadID string, op FSOp) (*datastore.Upload, error) {
	if scope == nil {
		errJSON(w, http.StatusUnauthorized, "missing tenant scope")
		return nil, errors.New("missing tenant scope")
	}
	// Owner short-circuit: legacy / owner tokens (IsScoped=false) skip
	// the session lookup AND the authorize check entirely so this helper
	// preserves exact pre-C2b behavior for owner callers — the upload
	// handler will do its own GetUpload (or call b.ConfirmUploadWithTags
	// etc.) and surface the same error shape it always did. This matches
	// the @qiffang owner-perf invariant: no new SQL on the owner path.
	//
	// For scoped tokens we MUST look up the session here to authorize
	// against its TargetPath, even at the cost of an extra GetUpload —
	// this is exactly the re-authz-against-current-scope invariant.
	if !scope.IsScoped {
		return nil, nil
	}
	upload, err := b.Store().GetUpload(ctx, uploadID)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			errJSON(w, http.StatusNotFound, "upload not found")
			return nil, err
		}
		if errors.Is(err, datastore.ErrUploadExpired) {
			errJSON(w, http.StatusGone, "upload expired")
			return nil, err
		}
		errJSON(w, http.StatusInternalServerError, "upload session lookup failed")
		return nil, err
	}
	if authErr := scope.AuthorizeFS(op, upload.TargetPath); authErr != nil {
		writeFSAuthzError(w, authErr)
		return nil, authErr
	}
	return upload, nil
}

// authorizeFSPathForBatch is the per-path variant for batch endpoints
// (batch-stat / batch-read-small). It returns the HTTP status + error string
// to embed in the per-path result so callers can short-circuit a single
// element without rejecting the entire batch. Returns (0, "", true) on
// allow; otherwise the returned status/message describes the denial.
//
// Per-path is critical here: a 256-path batch with one out-of-zone entry
// must NOT 403 the whole call — that would break preflight workflows
// (recursive cp, FUSE readdir prefetch) where one denied path is normal.
func authorizeFSPathForBatch(scope *TenantScope, op FSOp, rawPath string) (status int, message string, allowed bool) {
	if scope == nil {
		return http.StatusUnauthorized, "missing tenant scope", false
	}
	err := scope.AuthorizeFS(op, rawPath)
	if err == nil {
		return 0, "", true
	}
	switch {
	case errors.Is(err, ErrFSAccessDenied):
		return http.StatusForbidden, "fs access denied", false
	case errors.Is(err, ErrFSInvalidPath):
		return http.StatusBadRequest, "invalid fs path", false
	default:
		return http.StatusInternalServerError, "fs authorization failed", false
	}
}
