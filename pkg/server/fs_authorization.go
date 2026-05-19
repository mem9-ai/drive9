package server

import (
	"errors"
	"fmt"
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
