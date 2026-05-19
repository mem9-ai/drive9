package server

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/mem9-ai/dat9/pkg/meta"
	"github.com/mem9-ai/dat9/pkg/pathutil"
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
