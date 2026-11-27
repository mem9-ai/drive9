package viewprotocol

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// RouteAction classifies how a Layer-bound client or server view gateway must
// treat an HTTP request.
type RouteAction uint8

const (
	// RoutePassThrough sends the request directly to the root control plane
	// without "/v1/views/{layer_id}" and without X-FS-Layer-Fence (Set A).
	RoutePassThrough RouteAction = iota
	// RouteRewriteView routes the request through "/v1/views/{layer_id}" and
	// attaches X-FS-Layer-Fence when Fence > 0 (Set B).
	RouteRewriteView
	// RouteRejectUnsupported rejects the operation as 501 UNSUPPORTED_IN_LAYER_VIEW (Set C).
	RouteRejectUnsupported
	// RouteRejectInvalid rejects a malformed or nested view request as 400 INVALID_ARGUMENT (Set C).
	RouteRejectInvalid
)

// FormatViewPath prepends "/v1/views/{layer_id}" onto an escaped inner path.
func FormatViewPath(layerID, innerEscapedPath string) (string, error) {
	if err := ValidateLayerID(layerID); err != nil {
		return "", err
	}
	if strings.ContainsAny(innerEscapedPath, "?#") ||
		(!strings.HasPrefix(innerEscapedPath, "/v1/") && !strings.HasPrefix(innerEscapedPath, "/v2/")) ||
		innerEscapedPath == "/v1/views" || strings.HasPrefix(innerEscapedPath, RoutePrefix) {
		return "", fmt.Errorf("%w: %q", ErrInvalidViewRoute, innerEscapedPath)
	}
	return RoutePrefix + layerID + innerEscapedPath, nil
}

// RouteMutatesView reports whether an admitted view data route requires its write guard.
func RouteMutatesView(method, innerEscaped string) bool {
	if method == http.MethodPost && (innerEscaped == "/v1/fs:batch-stat" || innerEscaped == "/v1/fs:batch-read-small") {
		return false
	}
	if method != http.MethodGet && method != http.MethodHead {
		return true
	}
	return method == http.MethodGet &&
		strings.HasPrefix(innerEscaped, "/v1/uploads/") &&
		strings.HasSuffix(innerEscaped, "/resume")
}

// ClassifyClientRoute applies the exhaustive 3-set routing table (v35 §6.5 #3)
// for a Client bound to a non-empty ViewBinding.LayerID.
func ClassifyClientRoute(method, escapedPath string, query url.Values) RouteAction {
	if escapedPath == "/v1/views" || strings.HasPrefix(escapedPath, RoutePrefix) {
		return RouteRejectInvalid
	}
	if isPassThroughControlRoute(method, escapedPath, query) {
		return RoutePassThrough
	}
	return ClassifyViewDataRoute(method, escapedPath, query)
}

// ClassifyViewDataRoute validates inner requests targeting "/v1/views/{layer_id}".
// Only Set B returns RouteRewriteView; Set A control routes are NOT allowed inside "/v1/views/*".
func ClassifyViewDataRoute(method, escapedPath string, query url.Values) RouteAction {
	if escapedPath == "/v1/views" || strings.HasPrefix(escapedPath, RoutePrefix) {
		return RouteRejectInvalid
	}
	switch escapedPath {
	case "/v1/fs:batch-stat", "/v1/fs:batch-read-small", "/v1/fs:batch-write":
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	if escapedPath == "/v1/fs" || strings.HasPrefix(escapedPath, "/v1/fs/") {
		return classifyFSViewRoute(method, query)
	}
	if escapedPath == "/v1/uploads" || strings.HasPrefix(escapedPath, "/v1/uploads/") {
		return classifyV1UploadViewRoute(method, escapedPath, query)
	}
	if strings.HasPrefix(escapedPath, "/v2/uploads/") {
		return classifyV2UploadViewRoute(method, escapedPath, query)
	}
	return RouteRejectUnsupported
}

func isPassThroughControlRoute(method, escapedPath string, query url.Values) bool {
	if escapedPath == "/v1/status" {
		return method == http.MethodGet && len(query) == 0
	}
	if escapedPath == "/v1/layers" {
		if method == http.MethodPost {
			return len(query) == 0
		}
		if method == http.MethodGet {
			return queryKeysSubsetOf(query, "limit", "after")
		}
		return false
	}
	if strings.HasPrefix(escapedPath, "/v1/layers/") {
		rest := strings.TrimPrefix(escapedPath, "/v1/layers/")
		if rest == "" || strings.HasSuffix(rest, "/") {
			return false
		}
		ref, action, hasSlash := strings.Cut(rest, "/")
		if ref == "" || strings.Contains(action, "/") {
			return false
		}
		if !hasSlash {
			if method == http.MethodGet {
				return len(query) == 0
			}
			if method == http.MethodDelete {
				return queryKeysSubsetOf(query, "force")
			}
			return false
		}
		if method != http.MethodPost || len(query) != 0 {
			return false
		}
		switch action {
		case "fork", "merge", "bind-mount", "renew-mount", "unbind-mount":
			return true
		default:
			return false
		}
	}
	if escapedPath == "/v1/mounts" {
		return method == http.MethodPost && len(query) == 0
	}
	if strings.HasPrefix(escapedPath, "/v1/mounts/") {
		if len(query) != 0 {
			return false
		}
		rest := strings.TrimPrefix(escapedPath, "/v1/mounts/")
		if rest == "" || strings.HasSuffix(rest, "/") {
			return false
		}
		if strings.HasSuffix(rest, "/heartbeat") {
			id := strings.TrimSuffix(rest, "/heartbeat")
			return id != "" && !strings.Contains(id, "/") && method == http.MethodPost
		}
		return !strings.Contains(rest, "/") && method == http.MethodDelete
	}
	return false
}

func classifyFSViewRoute(method string, query url.Values) RouteAction {
	switch method {
	case http.MethodHead, http.MethodPut, http.MethodPatch:
		if len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	case http.MethodDelete:
		if queryKeysSubsetOf(query, "kind", "recursive") {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	case http.MethodGet:
		if query.Has("grep") || query.Has("find") || query.Has("tasks") {
			return RouteRejectUnsupported
		}
		switch {
		case query.Has("stat"):
			if queryKeysSubsetOf(query, "stat") {
				return RouteRewriteView
			}
			return RouteRejectUnsupported
		case query.Has("list"):
			if queryKeysSubsetOf(query, "list", "limit", "cursor") {
				return RouteRewriteView
			}
			return RouteRejectUnsupported
		case len(query) == 0:
			return RouteRewriteView
		default:
			return RouteRejectUnsupported
		}
	case http.MethodPost:
		return classifyFSPostViewQuery(query)
	default:
		return RouteRejectUnsupported
	}
}

func classifyFSPostViewQuery(query url.Values) RouteAction {
	selectors := [...]string{
		"append-log", "append", "copy", "rename", "mkdir",
		"chmod", "setmeta", "create", "symlink", "hardlink",
	}
	count := 0
	selected := ""
	for _, k := range selectors {
		if query.Has(k) {
			count++
			selected = k
		}
	}
	if count != 1 {
		if count == 0 && len(query) > 0 {
			return RouteRejectUnsupported
		}
		return RouteRejectInvalid
	}
	switch selected {
	case "append", "append-log":
		return RouteRejectUnsupported
	case "create":
		if queryKeysSubsetOf(query, "create", "layout") {
			return RouteRewriteView
		}
	case "mkdir":
		if queryKeysSubsetOf(query, "mkdir", "mode") {
			return RouteRewriteView
		}
	case "chmod", "rename", "copy", "hardlink", "symlink", "setmeta":
		if queryKeysSubsetOf(query, selected) {
			return RouteRewriteView
		}
	}
	return RouteRejectInvalid
}

func classifyV1UploadViewRoute(method, escapedPath string, query url.Values) RouteAction {
	if escapedPath == "/v1/uploads" {
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
		if method == http.MethodGet && queryKeysSubsetOf(query, "path", "status") {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	if escapedPath == "/v1/uploads/initiate" {
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	rest := strings.TrimPrefix(escapedPath, "/v1/uploads/")
	if rest == "" || strings.HasSuffix(rest, "/") {
		return RouteRejectUnsupported
	}
	id, action, hasSlash := strings.Cut(rest, "/")
	if id == "" || strings.Contains(action, "/") {
		return RouteRejectUnsupported
	}
	if !hasSlash {
		if method == http.MethodDelete && len(query) == 0 {
			return RouteRewriteView
		}
		return RouteRejectUnsupported
	}
	switch action {
	case "complete":
		if method == http.MethodPost && len(query) == 0 {
			return RouteRewriteView
		}
	case "resume":
		if (method == http.MethodGet || method == http.MethodPost) && len(query) == 0 {
			return RouteRewriteView
		}
	}
	return RouteRejectUnsupported
}

func classifyV2UploadViewRoute(method, escapedPath string, query url.Values) RouteAction {
	if method != http.MethodPost || len(query) != 0 {
		return RouteRejectUnsupported
	}
	rest := strings.TrimPrefix(escapedPath, "/v2/uploads/")
	if rest == "" || strings.HasSuffix(rest, "/") {
		return RouteRejectUnsupported
	}
	if rest == "initiate" {
		return RouteRewriteView
	}
	id, action, ok := strings.Cut(rest, "/")
	if !ok || id == "" || id == "initiate" || strings.Contains(action, "/") {
		return RouteRejectUnsupported
	}
	switch action {
	case "presign", "presign-batch", "complete", "abort":
		return RouteRewriteView
	default:
		return RouteRejectUnsupported
	}
}

func queryKeysSubsetOf(q url.Values, allowed ...string) bool {
	for k := range q {
		found := false
		for _, a := range allowed {
			if k == a {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
